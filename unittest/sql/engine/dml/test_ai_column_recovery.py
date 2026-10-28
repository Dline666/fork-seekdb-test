"""Crash-window and rollback regressions for the native AI COLUMN outbox.

The terminal task fixtures reproduce an interrupted worker after saving its
model response but before publishing it. The script also kills only its own
isolated server process, restarts the same data directory, and checks terminal
publication and expired in-flight lease recovery across a real abrupt restart.
"""
import argparse
import json
import pathlib
import signal
import sys
import subprocess
import tempfile
import time
import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    args = parser.parse_args()
    repo = pathlib.Path(__file__).resolve().parents[4]
    mock = repo / 'tools/deploy/mysql_test/test_suite/ai_column/include/ai_column_mock.py'
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-recovery-', dir='/tmp'))
    port = 19126
    server = None
    connection = None
    passed = 0
    subprocess.run([sys.executable, str(mock), 'start', str(port)], check=True)
    console = None
    cursor = None
    def start_instance(phase):
        nonlocal server, connection, console, cursor
        console = open(runtime / ('console-' + phase + '.log'), 'w')
        server = subprocess.Popen([
            args.binary, '--nodaemon', '--base-dir=' + str(runtime),
            '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
            '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
            '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
        ], stdout=console, stderr=subprocess.STDOUT)
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
    try:
        start_instance('initial')
        def query(sql):
            cursor.execute(sql)
            return cursor.fetchall()
        def check(name, condition):
            nonlocal passed
            if not condition:
                raise AssertionError(name)
            passed += 1
            print('PASS', name, flush=True)
        def wait_until(sql, expected):
            for _ in range(150):
                if query(sql) == expected:
                    return True
                time.sleep(.1)
            return False
        def events():
            path = pathlib.Path('/tmp/seekdb_ai_column_mock_%d/events.jsonl' % port)
            return [json.loads(line) for line in path.read_text().splitlines() if line]
        query('create database ai_recovery')
        query('use ai_recovery')
        query("call DBMS_AI_SERVICE.CREATE_AI_MODEL('recovery_model','{\"type\":\"completion\",\"model_name\":\"mock-complete\"}')")
        endpoint = json.dumps({'ai_model_name': 'recovery_model', 'url': 'http://127.0.0.1:%d/v1/chat/completions' % port,
                               'access_key': 'test', 'provider': 'openai', 'request_model_name': 'mock-complete'})
        cursor.execute('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)', ('recovery_endpoint', endpoint))
        query("create table docs(id bigint primary key, content varchar(200), result varchar(400) AI COLUMN (AI_COMPLETE('recovery_model',content)) WITH (sync_mode='async',max_retries=0))")
        query('start transaction')
        query("insert into docs(id,content) values(1,'must-rollback')")
        time.sleep(.4)
        check('uncommitted outbox causes no HTTP request', not events())
        check('T1 sees its own pending task', query('select count(*) from oceanbase.__all_ai_column_task') == ((1,),))
        query('rollback')
        time.sleep(.4)
        check('rollback leaves no task or request', query('select count(*) from oceanbase.__all_ai_column_task') == ((0,),) and not events())
        for row_id, lease in [(2, 'NULL'), (3, 'DATE_SUB(now(6), INTERVAL 1 SECOND)')]:
            query('start transaction')
            query("insert into docs(id,content) values(%d,'saved-response')" % row_id)
            cursor.execute("update oceanbase.__all_ai_column_task set state=4,result_value=%s,result_is_null=0,owner_id='dead-owner',lease_until=" + lease,
                           ('persisted-before-crash-%d' % row_id,))
            query('commit')
            check('terminal result recovers with %s lease' % ('NULL' if row_id == 2 else 'expired'),
                  wait_until('select result from docs where id=%d' % row_id, (('persisted-before-crash-%d' % row_id,),)))
            check('recovered task is removed', query('select count(*) from oceanbase.__all_ai_column_task') == ((0,),))
        check('terminal recovery does not repeat inference', not events())

        # Commit terminal and in-flight tasks with a future lease so the live
        # worker cannot consume either fixture before the abrupt process exit.
        query('start transaction')
        query("insert into docs(id,content) values(4,'terminal-across-restart')")
        cursor.execute("update oceanbase.__all_ai_column_task set state=4,result_value=%s,"
                       "result_is_null=0,owner_id='dead-terminal-owner',"
                       "lease_until=DATE_ADD(now(6), INTERVAL 1 HOUR) where state=1",
                       ('persisted-across-sigkill',))
        check('terminal restart fixture seeded inside T1', cursor.rowcount == 1)
        query("insert into docs(id,content) values(5,'claimed-before-crash')")
        query("update oceanbase.__all_ai_column_task set state=2,result_value=NULL,"
              "result_is_null=1,owner_id='dead-inflight-owner',"
              "lease_until=DATE_ADD(now(6), INTERVAL 1 HOUR) where state=1")
        check('in-flight restart fixture seeded inside T1', cursor.rowcount == 1)
        query('commit')
        observer = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                   autocommit=True, charset='utf8mb4', read_timeout=30)
        try:
            with observer.cursor() as visible:
                visible.execute("select state,owner_id,result_value,lease_until>now(6) "
                                "from oceanbase.__all_ai_column_task order by state")
                check('committed crash fixtures visible from another session', visible.fetchall() == (
                    (2, 'dead-inflight-owner', None, 1),
                    (4, 'dead-terminal-owner', 'persisted-across-sigkill', 1)))
        finally:
            observer.close()
        check('future leases prevent inference before crash', not events())

        # Never search for or signal a process by name: this handle belongs to
        # the one instance started above under this test's fresh temp directory.
        crashed_pid = server.pid
        server.kill()
        server.wait(timeout=10)
        check('owned server exited through SIGKILL', server.returncode == -signal.SIGKILL)
        connection.close()
        connection = None
        cursor = None
        console.close()
        console = None
        print('RESTART runtime=%s killed_pid=%d' % (runtime, crashed_pid), flush=True)
        start_instance('restart')
        query('use ai_recovery')
        check('same data directory restarted in a new process', server.pid != crashed_pid)
        check('terminal response and future lease survive restart', query(
            "select state,owner_id,result_value,lease_until>now(6) "
            "from oceanbase.__all_ai_column_task where state=4") == (
                (4, 'dead-terminal-owner', 'persisted-across-sigkill', 1),))
        check('in-flight task and owner survive restart', query(
            "select state,owner_id,result_is_null,lease_until>now(6) "
            "from oceanbase.__all_ai_column_task where state=2") == (
                (2, 'dead-inflight-owner', 1, 1),))
        time.sleep(.4)
        check('future leases block premature result publication after restart', query(
            'select id,result from docs where id in(4,5) order by id') == ((4, None), (5, None))
            and query('select count(*) from oceanbase.__all_ai_column_task') == ((2,),)
            and not events())
        query("update oceanbase.__all_ai_column_task set lease_until=NULL where state=4")
        check('native worker publishes response persisted before SIGKILL', wait_until(
            'select result from docs where id=4', (('persisted-across-sigkill',),)))
        check('restart terminal recovery makes no additional HTTP request', not events())
        check('restart terminal task is cleaned and in-flight task remains', query(
            'select state,count(*) from oceanbase.__all_ai_column_task group by state') == ((2, 1),))
        query("update oceanbase.__all_ai_column_task set lease_until=DATE_SUB(now(6), INTERVAL 1 SECOND) where state=2")
        check('expired in-flight lease is reclaimed after restart', wait_until(
            'select result from docs where id=5', (('mock:claimed-before-crash',),)))
        check('in-flight recovery invokes inference and removes its task',
              any(event.get('user') == 'claimed-before-crash' for event in events())
              and query('select count(*) from oceanbase.__all_ai_column_task') == ((0,),))
        for sql in ["update docs set result='forbidden' where id=2", "insert into docs(id,result) values(6,'forbidden')"]:
            try:
                query(sql)
            except pymysql.MySQLError:
                check('business assignment rejected', True)
            else:
                check('business assignment rejected', False)
        print('RESULT %d/%d passed; runtime=%s' % (passed, passed, runtime), flush=True)
    finally:
        if connection:
            connection.close()
        if server and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        if console:
            console.close()
        subprocess.run([sys.executable, str(mock), 'stop', str(port)], check=False)


if __name__ == '__main__':
    main()
