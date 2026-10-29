"""AI definition SQL context and dependency DDL regression on an isolated instance."""
import argparse
import json
import pathlib
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    args = parser.parse_args()
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-definition-', dir='/tmp'))
    requests = []
    request_paths = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            user = next((str(message.get('content', '')) for message in body.get('messages', [])
                         if message.get('role') == 'user'), '')
            requests.append(user)
            request_paths.append(self.path)
            payload = json.dumps({'choices': [{'message': {'role': 'assistant', 'content': 'mock:' + user}}]}).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    mock = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=mock.serve_forever, daemon=True).start()
    console = open(runtime / 'console.log', 'w')
    server = subprocess.Popen([
        args.binary, '--nodaemon', '--base-dir=' + str(runtime),
        '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
        '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
        '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
    ], stdout=console, stderr=subprocess.STDOUT)
    connection = None
    passed = failed = 0
    try:
        for _ in range(200):
            if server.poll() is not None:
                raise RuntimeError('server exited: ' + str(runtime))
            try:
                connection = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                             autocommit=True, charset='utf8mb4', read_timeout=30)
                break
            except pymysql.MySQLError:
                time.sleep(.2)
        if connection is None:
            raise RuntimeError('startup timeout: ' + str(runtime))
        cursor = connection.cursor()
        def query(sql, params=None):
            cursor.execute(sql, params)
            return cursor.fetchall()
        def equal(actual, expected):
            if actual != expected:
                raise AssertionError((actual, expected))
        def check(name, action):
            nonlocal passed, failed
            try:
                action()
                passed += 1
                print('PASS', name, flush=True)
            except Exception as error:
                failed += 1
                print('FAIL', name, type(error).__name__, str(error), flush=True)
            finally:
                query("set session sql_mode='STRICT_ALL_TABLES'")
        def result(table, expected):
            rows = ()
            for _ in range(150):
                rows = query('select result from ' + table)
                if rows == ((expected,),):
                    return
                time.sleep(.1)
            raise AssertionError((rows, expected, requests))
        def rejected(sql):
            try:
                query(sql)
            except pymysql.MySQLError as error:
                equal(error.args[0], 1235)
                return
            raise AssertionError('expected dependency DDL rejection: ' + sql)
        def define(table, expression):
            query('create table ' + table + '(id int primary key, content varchar(100), extra int, '
                  'result varchar(400) AI COLUMN(' + expression + ") WITH(sync_mode='async',max_retries=0))")
        query('create database ai_definition_review')
        query('use ai_definition_review')
        query("call DBMS_AI_SERVICE.CREATE_AI_MODEL('review_model','{\"type\":\"completion\",\"model_name\":\"mock\"}')")
        endpoint = json.dumps({'ai_model_name': 'review_model',
                               'url': 'http://127.0.0.1:%d/v1/chat/completions' % mock.server_address[1],
                               'access_key': 'test', 'provider': 'openai', 'request_model_name': 'mock'})
        query('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)', ('review_endpoint', endpoint))
        def ansi_quotes():
            query("set session sql_mode='STRICT_ALL_TABLES,ANSI_QUOTES'")
            define('ansi_docs', "AI_COMPLETE('review_model',\"content\")")
            equal(query("select count(*) from oceanbase.__all_column c join oceanbase.__all_table t on c.table_id=t.table_id "
                        "join oceanbase.__all_database d on t.database_id=d.database_id "
                        "where d.database_name='ai_definition_review' and t.table_name='ansi_docs' "
                        "and c.column_name='result' and length(c.local_session_vars)>0"), ((1,),))
            query("set session sql_mode='STRICT_ALL_TABLES'")
            query("insert into ansi_docs(id,content) values(1,'ansi-payload')")
            result('ansi_docs', 'mock:ansi-payload')
        check('CREATE preserves ANSI_QUOTES identifier semantics in worker', ansi_quotes)
        def no_backslash():
            query("set session sql_mode='STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'")
            define('slash_docs', r"AI_COMPLETE('review_model',CONCAT('slash\n:',content))")
            query("set session sql_mode='STRICT_ALL_TABLES'")
            query("insert into slash_docs(id,content) values(1,'slash-payload')")
            result('slash_docs', 'mock:slash\\n:slash-payload')
        check('CREATE preserves NO_BACKSLASH_ESCAPES literal bytes', no_backslash)
        def trailing_backslash():
            query("set session sql_mode='STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES'")
            define('trailing_docs', r"AI_COMPLETE('review_model',CONCAT(content,'\'))")
            query("set session sql_mode='STRICT_ALL_TABLES'")
            query("insert into trailing_docs(id,content) values(1,'trailing')")
            result('trailing_docs', 'mock:trailing\\')
        check('DDL argument splitting respects trailing backslash in literal', trailing_backslash)
        def comments():
            define('comment_docs', "AI_COMPLETE('review_model' /* comma, paren ( ) */,content)")
            query("insert into comment_docs(id,content) values(1,'comment-payload')")
            result('comment_docs', 'mock:comment-payload')
        check('DDL argument splitting ignores punctuation inside comments', comments)
        def add_column():
            query('create table add_docs(id int primary key,content varchar(100))')
            query("insert into add_docs values(1,'add-payload')")
            query("set session sql_mode='STRICT_ALL_TABLES,ANSI_QUOTES'")
            query("alter table add_docs add column result varchar(400) AI COLUMN(AI_COMPLETE('review_model',\"content\")) WITH(sync_mode='async',max_retries=0)")
            query("set session sql_mode='STRICT_ALL_TABLES'")
            result('add_docs', 'mock:add-payload')
        check('ADD captures its own expression SQL mode and backfills existing row', add_column)
        def modify_column():
            define('modify_docs', "AI_COMPLETE('review_model',content)")
            query("insert into modify_docs(id,content) values(1,'modify-payload')")
            result('modify_docs', 'mock:modify-payload')
            query("set session sql_mode='STRICT_ALL_TABLES,ANSI_QUOTES'")
            query("alter table modify_docs modify column result varchar(400) AI COLUMN(AI_COMPLETE('review_model',concat('new:',\"content\"))) WITH(sync_mode='async',max_retries=0)")
            query("set session sql_mode='STRICT_ALL_TABLES'")
            result('modify_docs', 'mock:new:modify-payload')
        check('MODIFY captures replacement expression SQL mode', modify_column)
        define('guard_docs', "AI_COMPLETE('review_model',content)")
        query("insert into guard_docs(id,content,extra) values(1,'before',4)")
        result('guard_docs', 'mock:before')
        for name, sql in [
            ('DROP of an AI input is rejected', 'alter table guard_docs drop column content'),
            ('RENAME of an AI input is rejected', 'alter table guard_docs rename column content to body'),
            ('CHANGE rename of an AI input is rejected', 'alter table guard_docs change column content body varchar(100)'),
        ]:
            check(name, lambda sql=sql: rejected(sql))
        def still_works():
            query("update guard_docs set content='after' where id=1")
            result('guard_docs', 'mock:after')
        check('rejected dependency DDL leaves valid generation intact', still_works)
        def unrelated_drop():
            query('alter table guard_docs drop column extra')
            query("update guard_docs set content='unrelated-drop' where id=1")
            result('guard_docs', 'mock:unrelated-drop')
        check('ordinary unrelated column DROP remains supported', unrelated_drop)
        check('CHANGE on an AI result is rejected before metadata mutation', lambda: rejected(
            'alter table guard_docs change column result renamed varchar(400)'))
        def obsolete_group():
            query("create table group_docs(id int primary key,content varchar(100),"
                  "removed varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0),"
                  "result varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0))")
            before = len(requests)
            query('start transaction')
            query("insert into group_docs(id,content) values(1,'group-payload')")
            query("update oceanbase.__all_ai_column_task set state=4,result_value='persisted-survivor',result_is_null=0,"
                  "owner_id='dead-group-owner',lease_until=DATE_ADD(now(6),INTERVAL 1 HOUR)")
            equal(cursor.rowcount, 2)
            query('commit')
            query('alter table group_docs drop column removed')
            query('update oceanbase.__all_ai_column_task set lease_until=NULL')
            result('group_docs', 'persisted-survivor')
            equal(query('select count(*) from oceanbase.__all_ai_column_task'), ((0,),))
            equal(len(requests), before)
        check('obsolete group member does not discard surviving terminal response', obsolete_group)
        def redefined_group():
            query("create table redefine_docs(id int primary key,content varchar(100),"
                  "changed varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0),"
                  "result varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0))")
            before = len(requests)
            query('start transaction')
            query("insert into redefine_docs(id,content) values(1,'redefine-payload')")
            query("update oceanbase.__all_ai_column_task set state=4,result_value='persisted-redefined-sibling',result_is_null=0,"
                  "owner_id='dead-definition-owner',lease_until=DATE_ADD(now(6),INTERVAL 1 HOUR)")
            equal(cursor.rowcount, 2)
            query('commit')
            query("alter table redefine_docs modify column changed varchar(400) AI COLUMN("
                  "AI_COMPLETE('review_model',concat('new:',content))) WITH(sync_mode='async',max_retries=0)")
            # Release only the simulated dead owner's saved sibling. ALTER
            # has queued a fresh runnable task, which a live worker may already
            # own; clearing that live lease would manufacture duplicate claims.
            query("update oceanbase.__all_ai_column_task set lease_until=NULL "
                  "where owner_id='dead-definition-owner'")
            result('redefine_docs', 'persisted-redefined-sibling')
            for _ in range(150):
                if query('select changed from redefine_docs') == (('mock:new:redefine-payload',),):
                    break
                time.sleep(.1)
            equal(query('select changed,result from redefine_docs'),
                  (('mock:new:redefine-payload', 'persisted-redefined-sibling'),))
            equal(query('select count(*) from oceanbase.__all_ai_column_task'), ((0,),))
            equal(requests[before:], ['new:redefine-payload'])
        check('redefining one grouped AI column preserves the other saved result', redefined_group)
        def timestamp_primary_key():
            query("set session time_zone='+00:00'")
            query("create table timestamp_docs(id timestamp(6) primary key,content varchar(100),"
                  "result varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0))")
            query("set session time_zone='+08:00'")
            query("insert into timestamp_docs(id,content) values('2026-01-01 08:00:00.123456','timestamp-payload')")
            result('timestamp_docs', 'mock:timestamp-payload')
            query("set session time_zone='+00:00'")
            equal(query("select date_format(id,'%Y-%m-%d %H:%i:%s.%f') from timestamp_docs"),
                  (('2026-01-01 00:00:00.123456',),))
        check('TIMESTAMP(6) PK replays exact microseconds across session time zones', timestamp_primary_key)
        def no_primary_key():
            query("create table no_pk_docs(content varchar(100),result varchar(400) AI COLUMN("
                  "AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0))")
            query("insert into no_pk_docs(content) values('heap-payload')")
            result('no_pk_docs', 'mock:heap-payload')
        check('hidden primary key supports a table without user PK', no_primary_key)
        def failed_integer_conversion():
            query("set session sql_mode=''")
            query("create table invalid_int_docs(id int primary key,content varchar(100),"
                  "result int AI COLUMN(AI_COMPLETE('review_model',content)) WITH(sync_mode='async',max_retries=0))")
            query("set session sql_mode='STRICT_ALL_TABLES'")
            query("insert into invalid_int_docs(id,content) values(1,'not-an-integer')")
            for _ in range(150):
                if query('select count(*) from oceanbase.__all_ai_column_task') == ((0,),):
                    break
                time.sleep(.1)
            equal(query('select result from invalid_int_docs'), ((None,),))
            equal(query('select count(*) from oceanbase.__all_ai_column_task'), ((0,),))
            query('set session ai_column_strict_read=on')
            try:
                try:
                    query('select result from invalid_int_docs')
                except pymysql.MySQLError as error:
                    equal(error.args[0], 11120)
                else:
                    raise AssertionError('invalid model integer became READY')
            finally:
                query('set session ai_column_strict_read=off')
        check('nonstrict definition context cannot weaken result conversion failure', failed_integer_conversion)
        def longtext_dependency_unchanged():
            query("create table lob_dependency_docs(id int primary key, content longtext, extra int, "
                  "result longtext AI COLUMN(AI_COMPLETE('review_model',content)) "
                  "WITH(sync_mode='async',max_retries=0))")
            payload = '数据库\n' * 10000
            query('insert into lob_dependency_docs(id,content,extra) values(1,%s,1)', (payload,))
            result('lob_dependency_docs', 'mock:' + payload)
            before = len(requests)
            query('update lob_dependency_docs set content=%s,extra=2 where id=1', (payload,))
            equal(query('select result from lob_dependency_docs'), (('mock:' + payload,),))
            time.sleep(.3)
            equal(len(requests), before)
            query("update lob_dependency_docs set content=concat('',content),extra=3 where id=1")
            equal(query('select result from lob_dependency_docs'), (('mock:' + payload,),))
            time.sleep(.3)
            equal(len(requests), before)
            changed_payload = payload + 'changed'
            query('update lob_dependency_docs set content=%s,extra=4 where id=1', (changed_payload,))
            result('lob_dependency_docs', 'mock:' + changed_payload)
            equal(len(requests), before + 1)
        check('LONGTEXT compares logical dependencies and regenerates only on change', longtext_dependency_unchanged)
        def null_embedding_ready():
            query("call DBMS_AI_SERVICE.CREATE_AI_MODEL('review_embed','{\"type\":\"dense_embedding\",\"model_name\":\"mock-embed\"}')")
            embed_endpoint = json.dumps({'ai_model_name':'review_embed',
                'url':'http://127.0.0.1:%d/v1/embeddings' % mock.server_address[1],
                'access_key':'test','provider':'openai','request_model_name':'mock-embed'})
            query('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)',('review_embed_endpoint',embed_endpoint))
            query("create table null_embedding_docs(id int primary key, content varchar(100), "
                  "result vector(4) AI COLUMN(AI_EMBED('review_embed',content)) "
                  "WITH(sync_mode='async',max_retries=0))")
            before = request_paths.count('/v1/embeddings')
            query('insert into null_embedding_docs(id,content) values(1,NULL)')
            for _ in range(150):
                if query("select count(*) from oceanbase.__all_ai_column_task task join oceanbase.__all_table t "
                         "on task.table_id=t.table_id where t.table_name='null_embedding_docs'") == ((0,),):
                    break
                time.sleep(.1)
            query('set session ai_column_strict_read=on')
            try:
                equal(query('select id,result from null_embedding_docs'), ((1,None),))
                equal(request_paths.count('/v1/embeddings'), before)
            finally:
                query('set session ai_column_strict_read=off')
        check('NULL embedding result reaches READY without an HTTP request', null_embedding_ready)
        def wide_primary_key():
            query("create table wide_primary_docs(k1 varchar(350), k2 varchar(350), content varchar(100), "
                  "result varchar(400) AI COLUMN(AI_COMPLETE('review_model',content)) "
                  "WITH(sync_mode='async',max_retries=0), primary key(k1,k2))")
            keys = ('界' * 350, '字' * 350)
            query('insert into wide_primary_docs(k1,k2,content) values(%s,%s,%s)', (*keys,'wide-key'))
            result('wide_primary_docs', 'mock:wide-key')
            query('update wide_primary_docs set content=%s where k1=%s and k2=%s', ('wide-update',*keys))
            result('wide_primary_docs', 'mock:wide-update')
        check('valid wide Unicode primary key survives persistent predicate serialization', wide_primary_key)
        print('RUNTIME', runtime, flush=True)
        print('SUMMARY', {'passed': passed, 'failed': failed}, flush=True)
    finally:
        if connection:
            connection.close()
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        mock.shutdown()
        mock.server_close()
        console.close()
    raise SystemExit(bool(failed))


if __name__ == '__main__':
    main()
