"""COPY SQL end-to-end regression on a new disposable instance. Requires PyMySQL."""
import argparse
import pathlib
import subprocess
import tempfile
import threading
import time
import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    args = parser.parse_args()
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-copy-', dir='/tmp'))
    fixtures = runtime / 'allowed'
    fixtures.mkdir()
    console = open(runtime / 'console.log', 'w')
    server = subprocess.Popen([
        args.binary, '--nodaemon', '--base-dir=' + str(runtime),
        '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
        '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
        '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
        '--variable', 'secure_file_priv=' + str(fixtures),
    ], stdout=console, stderr=subprocess.STDOUT)
    connection = None
    passed = 0
    failed = 0
    try:
        def connect():
            return pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                   password='', autocommit=True, charset='utf8mb4',
                                   connect_timeout=1, read_timeout=30, write_timeout=30)
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
        def sql_error(sql, params=None):
            try:
                query(sql, params)
            except pymysql.MySQLError:
                return
            raise AssertionError('expected SQL error: ' + sql)
        def target(name):
            return connection.escape(str(fixtures / name))
        def no_temporaries():
            equal(list(fixtures.glob('.seekdb-copy-*')), [])
        def check(name, function):
            nonlocal passed, failed
            try:
                function()
                passed += 1
                print('PASS', name, flush=True)
            except Exception as error:
                failed += 1
                print('FAIL', name, type(error).__name__, str(error), flush=True)
        query('create database copy_regression')
        query('use copy_regression')
        query('create table docs(id int primary key, content longtext)')
        cursor.executemany('insert into docs values(%s,%s)', [(1, 'one'), (2, 'comma,quote"\r\nline'), (3, None), (4, ''), (5, 'NUL\0value')])
        check('COPY defaults disabled', lambda: sql_error('copy docs to ' + target('disabled.csv')))
        query('set session enable_file_sql=on')
        def table_roundtrip():
            query('copy copy_regression.docs to ' + target('table.csv') + ' (format csv,header)')
            equal(query('select id,content from read_csv(' + target('table.csv') + ') order by id'),
                  ((1, 'one'), (2, 'comma,quote"\r\nline'), (3, None), (4, ''), (5, 'NUL\0value')))
            no_temporaries()
        check('COPY qualified table NULL/empty/CRLF/NUL roundtrip', table_roundtrip)
        def nonqualified():
            query('copy docs to ' + target('local.csv') + ' (header false)')
            equal(query('select count(*) from read_csv(' + target('local.csv') + ',header=false)'), ((5,),))
        check('COPY unqualified table', nonqualified)
        def null_column():
            query('copy (select NULL as v union all select NULL) to ' + target('null.csv') + ' (header)')
            equal(query('select count(*),count(v) from read_csv(' + target('null.csv') + ')'), ((2, 0),))
        check('COPY single NULL column roundtrip', null_column)
        def empty_header():
            query('copy (select id,content from docs where 1=0) to ' + target('empty-header.csv') + ' (header)')
            equal((fixtures / 'empty-header.csv').read_bytes(), b'id,content\n')
            no_temporaries()
        check('COPY empty result header', empty_header)
        def empty_no_header():
            query('copy (select id from docs where 1=0) to ' + target('empty.csv') + ' (header false)')
            equal((fixtures / 'empty.csv').read_bytes(), b'')
        check('COPY empty result no header', empty_no_header)
        def empty_runtime():
            query('copy (select id from docs where id<0) to ' + target('runtime-empty.csv') + ' (header)')
            equal((fixtures / 'runtime-empty.csv').read_bytes(), b'id\n')
            no_temporaries()
        check('COPY runtime empty scan', empty_runtime)
        def limit_zero():
            query('copy (select id from docs limit 0) to ' + target('limit-zero.csv') + ' (header)')
            equal((fixtures / 'limit-zero.csv').read_bytes(), b'id\n')
            no_temporaries()
        check('COPY LIMIT zero', limit_zero)
        def scalar_empty():
            query("copy (select /*+ opt_param('_rowsets_max_rows' 0) */ id from docs where 1=0) to " + target('scalar-empty.csv') + ' (header)')
            equal((fixtures / 'scalar-empty.csv').read_bytes(), b'id\n')
            no_temporaries()
        check('COPY scalar false startup filter', scalar_empty)
        def options():
            query('copy (select %s as `header;slash\\name`,NULL as n) to ' + target('options.csv') +
                  " (delimiter ';',quote '|',escape '\\\\',nullstr 'NA',header)", ('slash\\ quote|semi;\r\n',))
            rows = query('select * from read_csv(' + target('options.csv') + ",delimiter=';',quote='|',escape='\\\\',nullstr='NA')")
            equal(rows, (('slash\\ quote|semi;\r\n', None),))
        check('COPY custom delimiter/quote/escape/header/nullstr', options)
        def existing():
            path = fixtures / 'exists.csv'
            path.write_bytes(b'sentinel')
            sql_error('copy docs to ' + target('exists.csv'))
            equal(path.read_bytes(), b'sentinel')
            no_temporaries()
        check('COPY existing file untouched', existing)
        def dangling():
            path = fixtures / 'dangling.csv'
            path.symlink_to(runtime / 'not-there')
            sql_error('copy docs to ' + target('dangling.csv'))
            equal(path.is_symlink(), True)
        check('COPY dangling target symlink rejected', dangling)
        check('COPY outside directory denied', lambda: sql_error('copy docs to %s', (str(runtime / 'outside.csv'),)))
        (fixtures / 'outside-link').symlink_to(runtime, target_is_directory=True)
        check('COPY parent symlink escape denied', lambda: sql_error('copy docs to %s', (str(fixtures / 'outside-link' / 'bad.csv'),)))
        for option in ["delimiter '" + '"' + "'", "quote ''", "nullstr 'a,b'", "nullstr 'a\nb'", "header=17"]:
            check('COPY invalid options ' + repr(option), lambda option=option: sql_error('copy docs to ' + target('invalid.csv') + ' (' + option + ')'))
        query('create table many(id int primary key)')
        cursor.executemany('insert into many values(%s)', [(i,) for i in range(1200)])
        def runtime_failure():
            sql_error("copy (select id,json_extract(if(id<700,'{}','{'),'$') from many) to " + target('failed.csv'))
            equal((fixtures / 'failed.csv').exists(), False)
            no_temporaries()
        check('COPY runtime expression error discards partial output', runtime_failure)
        def competing_target():
            worker = connect()
            worker.cursor().execute('use copy_regression')
            errors = []
            def export():
                try:
                    worker.cursor().execute('copy (select id,sleep(0.002) as delay from many) to ' + target('race.csv'))
                except pymysql.MySQLError as error:
                    errors.append(error)
            thread = threading.Thread(target=export)
            thread.start()
            for _ in range(100):
                if list(fixtures.glob('.seekdb-copy-*')):
                    break
                if not thread.is_alive():
                    break
                time.sleep(.01)
            equal((fixtures / 'race.csv').exists(), False)
            (fixtures / 'race.csv').write_bytes(b'other writer')
            thread.join(timeout=15)
            equal(thread.is_alive(), False)
            equal(len(errors), 1)
            equal((fixtures / 'race.csv').read_bytes(), b'other writer')
            worker.close()
            no_temporaries()
        check('COPY racing destination not replaced', competing_target)
        def cancelled():
            worker = connect()
            worker.cursor().execute('use copy_regression')
            errors = []
            def export():
                try:
                    worker.cursor().execute('copy (select id,sleep(0.01) as delay from many) to ' + target('cancel.csv'))
                except pymysql.MySQLError as error:
                    errors.append(error)
            thread = threading.Thread(target=export)
            thread.start()
            for _ in range(100):
                if list(fixtures.glob('.seekdb-copy-*')):
                    break
                time.sleep(.01)
            equal((fixtures / 'cancel.csv').exists(), False)
            query('kill query ' + str(worker.thread_id()))
            thread.join(timeout=15)
            equal(thread.is_alive(), False)
            equal(len(errors), 1)
            equal((fixtures / 'cancel.csv').exists(), False)
            worker.close()
            no_temporaries()
        check('COPY cancellation discards temporary output', cancelled)
        check('ordinary SELECT INTO variable unchanged', lambda: (query('select 7 into @copy_regression_probe'), equal(query('select @copy_regression_probe'), ((7,),))))
        def ordinary_outfile():
            query('select 17 into outfile ' + target('ordinary.txt'))
            equal((fixtures / 'ordinary.txt').exists(), True)
        check('ordinary SELECT INTO OUTFILE remains available', ordinary_outfile)
        check('server remains usable', lambda: equal(query('select 1'), ((1,),)))
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
        console.close()
    raise SystemExit(bool(failed))


if __name__ == '__main__':
    main()
