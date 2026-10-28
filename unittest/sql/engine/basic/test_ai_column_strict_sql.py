"""AI strict-read regression on an isolated server and deterministic HTTP mock."""
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
    parser.add_argument('--mock-port', type=int, default=19125)
    args = parser.parse_args()
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-strict-', dir='/tmp'))
    console = open(runtime / 'console.log', 'w')
    accepting = threading.Event()
    accepting.set()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            user = next((str(m.get('content', '')) for m in body.get('messages', [])
                         if m.get('role') == 'user'), '')
            ok = accepting.is_set()
            payload = json.dumps({'choices': [{'message': {'role': 'assistant', 'content': 'mock:' + user}}]}
                                 if ok else {'error': {'message': 'strict fixture offline'}}).encode()
            self.send_response(200 if ok else 400)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    mock = ThreadingHTTPServer(('127.0.0.1', args.mock_port), Handler)
    threading.Thread(target=mock.serve_forever, daemon=True).start()
    server = subprocess.Popen([
        args.binary, '--nodaemon', '--base-dir=' + str(runtime),
        '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
        '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
        '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
    ], stdout=console, stderr=subprocess.STDOUT)
    connection = None
    passed = failed = 0
    try:
        def connect():
            return pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                   autocommit=True, charset='utf8mb4', connect_timeout=1,
                                   read_timeout=30, write_timeout=30)
        for _ in range(150):
            if server.poll() is not None:
                raise RuntimeError('server exited: ' + str(runtime))
            try:
                connection = connect()
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
        def strict_error(sql):
            try:
                query(sql)
            except pymysql.MySQLError as error:
                equal(error.args[0], 11120)
                if not error.args[1].startswith('AI column has unfinished tasks for table_id='):
                    raise AssertionError(error.args)
                return
            raise AssertionError('expected 11120: ' + sql)
        def check(name, function):
            nonlocal passed, failed
            try:
                function()
                passed += 1
                print('PASS', name, flush=True)
            except Exception as error:
                failed += 1
                print('FAIL', name, type(error).__name__, str(error), flush=True)
        def wait_ready():
            for _ in range(300):
                if query('select count(*) from docs where result is null or label is null') == ((0,),):
                    return
                time.sleep(.1)
            raise AssertionError('AI results did not become READY')
        query('create database strict_regression')
        query('use strict_regression')
        query("call DBMS_AI_SERVICE.CREATE_AI_MODEL('strict_model','{\"type\":\"completion\",\"model_name\":\"mock\"}')")
        endpoint = json.dumps({'ai_model_name': 'strict_model',
                               'url': 'http://127.0.0.1:%d/v1/chat/completions' % args.mock_port,
                               'access_key': 'strict-key', 'provider': 'openai', 'request_model_name': 'mock'})
        query('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)', ('strict_ep', endpoint))
        query("""create table docs(id int primary key, category varchar(30), content varchar(100),
                 result varchar(400) AI COLUMN(AI_COMPLETE('strict_model',content))
                   WITH(sync_mode='async',max_retries=0),
                 label varchar(400) AI COLUMN(AI_COMPLETE('strict_model',concat('Label:',content)))
                   WITH(sync_mode='async',max_retries=0))""")
        query("insert into docs(id,category,content) values(1,'one','alpha'),(2,'two','beta'),(3,'three','gamma')")
        wait_ready()
        query('create table rhs(id int primary key,result varchar(400))')
        query('insert into rhs select id,result from docs')
        query('prepare cached_ai from %s', ('select result from docs where id=2',))
        accepting.clear()
        query('alter table docs retry ai column result where id in (1,3)')
        query('set session ai_column_strict_read=on')
        check('READY column remains readable', lambda: equal(query('select count(label) from docs'), ((3,),)))
        for name, sql in [
            ('qualified column and table alias', 'select d.result from docs d where id=2'),
            ('SELECT star', 'select * from docs where id=2'),
            ('JOIN USING', 'select docs.id from docs join rhs using(result)'),
            ('NATURAL JOIN', 'select docs.id from docs natural join rhs'),
            ('optimized false predicate', 'select result from docs where 1=0'),
            ('unused CASE branch', 'select case when 1=1 then 1 else result end from docs'),
            ('nested SELECT', 'select id from docs where exists(select 1 from docs d where d.result is null)'),
            ('cached prepared execution strict ON', 'execute cached_ai'),
            ('UPDATE expression reads AI result', 'update docs set category=concat(category,result) where id=2'),
            ('DELETE subquery reads AI result', 'delete from rhs where id in(select id from docs where result is null)'),
        ]:
            check(name, lambda sql=sql: strict_error(sql))
        def ordinary_writes():
            query("update docs set category='ordinary' where id=2")
            equal(query('select category from docs where id=2'), (('ordinary',),))
            query('delete from rhs where id=1')
            equal(query('select count(*) from rhs'), ((2,),))
            query('delete from docs where id=3')
            equal(query('select count(*) from docs'), ((2,),))
        check('ordinary UPDATE and DELETE do not inspect unfinished AI columns', ordinary_writes)
        def strict_insert():
            query("insert into docs(id,category,content) values(4,'new','delta')")
            equal(query('select count(*) from docs'), ((3,),))
        check('INSERT inputs do not read AI output columns', strict_insert)
        query('set session ai_column_strict_read=off')
        check('cached prepared execution strict OFF', lambda: equal(len(query('execute cached_ai')), 1))
        equal(query('select count(*) from docs'), ((3,),))
        accepting.set()
        query('alter table docs retry ai column result')
        query('alter table docs retry ai column label')
        wait_ready()
        query('set session ai_column_strict_read=on')
        check('same prepared statement succeeds after READY transition', lambda: equal(len(query('execute cached_ai')), 1))
        def snapshot_consistency():
            query('set session transaction_isolation="REPEATABLE-READ"')
            query('start transaction with consistent snapshot')
            original = query('select result from docs where id=2')
            other = connect()
            try:
                other.cursor().execute('use strict_regression')
                accepting.clear()
                other.cursor().execute('alter table docs retry ai column result where id=2')
                equal(query('select result from docs where id=2'), original)
            finally:
                other.close()
                query('rollback')
                accepting.set()
        check('guard and query respect the transaction snapshot', snapshot_consistency)
        query('deallocate prepare cached_ai')
        check('strict error leaves normal session usable', lambda: equal(query('select 1'), ((1,),)))
        print('RUNTIME', runtime, flush=True)
        print('SUMMARY', {'passed': passed, 'failed': failed}, flush=True)
    finally:
        if connection:
            connection.close()
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
