"""AI COLUMN retry/strict-read SQL regressions on an isolated instance.

Covers ai_column_retry_strict.test plus compound primary keys and quoted names.
Requires PyMySQL. The deterministic public HTTP mock listens on --mock-port.
"""
import argparse
import importlib.util
import os
import pathlib
import subprocess
import tempfile
import time

import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--mock-port', type=int, default=19124)
    args = parser.parse_args()
    repo = pathlib.Path(__file__).resolve().parents[4]
    spec = importlib.util.spec_from_file_location(
        'retry_mock', repo / 'tools/deploy/mysql_test/test_suite/ai_column/include/ai_column_mock.py')
    mock = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mock)
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-retry-', dir='/tmp'))
    console = open(runtime / 'console.log', 'w')
    server = subprocess.Popen([
        args.binary, '--nodaemon', '--base-dir=' + str(runtime),
        '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
        '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
        '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
    ], stdout=console, stderr=subprocess.STDOUT)
    connection = None
    failed = 0
    passed = 0
    try:
        for _ in range(180):
            if server.poll() is not None:
                raise RuntimeError('server exited: ' + str(runtime))
            try:
                connection = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'),
                    user='root', password='', autocommit=True, charset='utf8mb4',
                    connect_timeout=1, read_timeout=30, write_timeout=30)
                break
            except pymysql.MySQLError:
                time.sleep(.2)
        if connection is None:
            raise RuntimeError('startup timed out: ' + str(runtime))
        cursor = connection.cursor()

        def query(sql, params=None):
            cursor.execute(sql, params)
            return cursor.fetchall()

        def equal(actual, expected):
            if actual != expected:
                raise AssertionError((actual, expected))

        def error(sql, expected, message=None):
            try:
                query(sql)
            except pymysql.MySQLError as exception:
                equal(exception.args[0], expected)
                if message is not None:
                    equal(exception.args[1], message)
                return
            raise AssertionError('expected SQL error ' + str(expected) + ': ' + sql)

        def wait(sql, expected, timeout=20):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                value = query(sql)
                if value == expected:
                    return
                time.sleep(.1)
            raise AssertionError(('wait timeout', sql, value, expected))

        def check(name, function):
            nonlocal passed, failed
            try:
                function()
                passed += 1
                print('PASS', name, flush=True)
            except Exception as exception:
                failed += 1
                print('FAIL', name, type(exception).__name__, str(exception), flush=True)

        mock.start_server(args.mock_port)
        db = 'ai_retry_regression_' + str(os.getpid())
        model = db + '_model'
        endpoint = db + '_endpoint'
        query('create database `' + db + '`')
        query('use `' + db + '`')
        query('call DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',
              (model, '{"type":"completion","model_name":"mock-complete"}'))
        query('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)', (endpoint,
              '{"ai_model_name":"' + model + '","url":"http://127.0.0.1:' + str(args.mock_port)
              + '/v1/chat/completions","access_key":"retry-key","provider":"openai",'
                '"request_model_name":"mock-complete"}'))
        model_literal = connection.escape(model)
        query('create table docs(id bigint primary key,category varchar(20),status varchar(20),'
              'content varchar(200),result varchar(400) AI COLUMN (AI_COMPLETE(' + model_literal
              + ',content)) WITH(sync_mode=\'async\',max_retries=0),'
              'label varchar(400) AI COLUMN (AI_COMPLETE(' + model_literal
              + ',concat(\'Label: \',content))) WITH(sync_mode=\'async\',max_retries=0))')
        query("insert into docs(id,category,status,content) values"
              "(1,'news','active','row-one'),(2,'sports','active','row-two'),"
              "(3,'news','inactive','row-three')")
        wait('select count(*) from docs where result is not null and label is not null', ((3,),))
        check('strict read defaults OFF', lambda: equal(query('select @@ai_column_strict_read'), ((0,),)))
        mock.stop_server(args.mock_port)

        def ready_selected():
            query('alter table docs retry ai column result where id in (1,3)')
            equal(query('select id,result is null,label is not null from docs order by id'),
                  ((1, 1, 1), (2, 0, 1), (3, 1, 1)))
        check('READY rows retry only selected AI column', ready_selected)
        query('set session ai_column_strict_read=on')
        check('ordinary columns remain readable', lambda: equal(query('select count(id) from docs'), ((3,),)))
        check('other ready AI column remains readable', lambda: equal(
            query('select count(*) from docs where label is not null'), ((3,),)))
        for name, sql in [
            ('selected READY row still checks whole column', 'select result from docs where id=2'),
            ('AI projection checks whole column', 'select id,result from docs order by id'),
            ('AI predicate checks whole column', 'select id from docs where result is null'),
            ('nested AI predicate checks whole column', 'select id from docs where id in(select id from docs where result is null)'),
            ('UPDATE AI predicate checks whole column', 'update docs set category=category where result is null'),
            ('DELETE AI predicate checks whole column', 'delete from docs where result is null'),
        ]:
            check(name, lambda sql=sql: error(sql, 11120))
        query('set session ai_column_strict_read=off')
        check('strict rejections preserve rows', lambda: equal(query('select count(*) from docs'), ((3,),)))
        tasks = "select count(*) from oceanbase.__all_ai_column_task where table_id=" \
                "(select table_id from oceanbase.__all_table where database_id=" \
                "(select database_id from oceanbase.__all_database where database_name=" \
                + connection.escape(db) + ") and table_name='docs')"
        wait(tasks, ((0,),))
        query('set session ai_column_strict_read=on')
        check('FAILED rows remain incomplete after task cleanup', lambda: error('select result from docs where id=1', 11120))
        query('set session ai_column_strict_read=off')
        check('STATUS pseudo column combines business condition', lambda: query(
            "alter table docs retry ai column result where status='FAILED' and category='news'"))

        mock.start_server(args.mock_port)
        query('alter table docs retry ai column result')
        wait('select count(*) from docs where result is not null', ((3,),))
        query('set session ai_column_strict_read=on')
        check('all READY permits strict read', lambda: equal(
            query('select count(*) from docs where result is not null'), ((3,),)))
        query('set session ai_column_strict_read=off')
        wait(tasks, ((0,),))
        mock.stop_server(args.mock_port)

        def no_match():
            query("alter table docs retry ai column result where status='READY' and id=999")
            equal(query('select count(*) from docs where result is not null'), ((3,),))
            equal(query(tasks), ((0,),))
        check('zero-row RETRY keeps results and creates no tasks', no_match)

        def ready_status():
            query("alter table docs retry ai column result where status='READY' and category='sports'")
            equal(query('select id,result is null from docs order by id'), ((1, 0), (2, 1), (3, 0)))
        check('READY STATUS predicate retries matching row', ready_status)
        mock.start_server(args.mock_port)
        query('alter table docs retry ai column result')
        wait('select count(*) from docs where result is not null', ((3,),))
        mock.stop_server(args.mock_port)

        def quoted_status():
            query("alter table docs retry ai column result where `status`='active' and id=1")
            equal(query('select id,result is null from docs order by id'), ((1, 1), (2, 0), (3, 0)))
        check('quoted status targets physical business column', quoted_status)
        check('non-AI retry rejected', lambda: error('alter table docs retry ai column content where id=1', 1235,
              'retrying a column that is not an AI column not supported'))
        mixed_message = 'combining AI COLUMN RETRY with another ALTER action not supported'
        check('RETRY then ALTER rejected', lambda: error('alter table docs retry ai column result where id=1, add column extra_col int', 1235, mixed_message))
        check('ALTER then RETRY rejected', lambda: error('alter table docs add column extra_col int, retry ai column result where id=1', 1235, mixed_message))
        check('mixed actions preserve schema', lambda: equal(query("select count(*) from information_schema.columns where table_schema=%s and table_name='docs' and column_name='extra_col'", (db,)), ((0,),)))

        mock.start_server(args.mock_port)
        query('create table `quoted``docs`(tenant varchar(30),id int,content varchar(200),'
              '`ai``result` varchar(400) AI COLUMN (AI_COMPLETE(' + model_literal + ',content)) '
              "WITH(sync_mode='async',max_retries=0),primary key(tenant,id))")
        query('insert into `quoted``docs`(tenant,id,content) values(%s,1,%s),(%s,2,%s)',
              ("a'b\\c", 'compound-one', "a'b\\c", 'compound-two'))
        wait('select count(*) from `quoted``docs` where `ai``result` is not null', ((2,),))
        mock.stop_server(args.mock_port)

        def compound_retry():
            query('alter table `quoted``docs` retry ai column `ai``result` where tenant=%s and id=1', ("a'b\\c",))
            equal(query('select id,`ai``result` is null from `quoted``docs` order by id'), ((1, 1), (2, 0)))
        check('compound primary key and escaped identifiers', compound_retry)
        check('RETRY restores assignment protection', lambda: error("update docs set result='forged' where id=2", 1235))

        def normal_dependency_change():
            query("update docs set content=concat(content,' changed') where id=2")
            equal(query('select result is null,label is null from docs where id=2'), ((1, 1),))
        check('RETRY restores normal dependency processing for all AI columns', normal_dependency_change)

        query('create table private_rows(id int primary key)')
        query('insert into private_rows values(2)')
        limited_user = 'retry_limited_' + str(os.getpid())
        limited_account = connection.escape(limited_user) + "@'%'"
        query('create user ' + limited_account + " identified by 'retry-test-only'")
        query('grant alter,update,select on `' + db + '`.docs to ' + limited_account)
        limited = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'),
            user=limited_user, password='retry-test-only', autocommit=True,
            database=db, charset='utf8mb4', read_timeout=30, write_timeout=30)
        try:
            def own_table_privileges():
                with limited.cursor() as limited_cursor:
                    limited_cursor.execute('alter table docs retry ai column result where id=3')
                equal(query('select result is null,label is not null from docs where id=3'), ((1, 1),))
            check('RETRY accepts ordinary grants on target table', own_table_privileges)

            def protected_subquery(prepared=False):
                sql = 'alter table docs retry ai column result where id in(select id from private_rows)'
                with limited.cursor() as limited_cursor:
                    try:
                        if prepared:
                            limited_cursor.execute('prepare denied_retry from %s', (sql,))
                            limited_cursor.execute('execute denied_retry')
                        else:
                            limited_cursor.execute(sql)
                    except pymysql.MySQLError as exception:
                        equal(exception.args[0], 1142)
                        return
                    raise AssertionError('RETRY predicate read a protected table')
            check('RETRY subquery preserves ordinary SELECT privileges', protected_subquery)
            check('prepared RETRY preserves ordinary SELECT privileges', lambda: protected_subquery(True))

            def prepared_no_match():
                with limited.cursor() as limited_cursor:
                    limited_cursor.execute("prepare allowed_retry from 'alter table docs retry ai column result where id=999'")
                    limited_cursor.execute('execute allowed_retry')
                    limited_cursor.execute('deallocate prepare allowed_retry')
            check('literal PREPARE EXECUTE RETRY', prepared_no_match)
        finally:
            limited.close()
            query('drop user ' + limited_account)
        check('server remains usable', lambda: equal(query('select 1'), ((1,),)))
        print('RUNTIME', runtime, flush=True)
        print('SUMMARY', {'passed': passed, 'failed': failed}, flush=True)
    finally:
        mock.stop_server(args.mock_port)
        if connection is not None:
            connection.close()
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
        console.close()
    raise SystemExit(bool(failed))


if __name__ == '__main__':
    main()
