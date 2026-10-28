"""Run public AI COLUMN suites and native transaction/recovery regressions."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import time

CASES = ('ai_column_ddl', 'ai_column_t1_dml', 'ai_column_worker',
         'ai_column_retry_strict', 'ai_column_vector_index')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=pathlib.Path)
    parser.add_argument('--mysqltest', type=pathlib.Path, default=shutil.which('mysqltest'))
    parser.add_argument('--repo', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[4])
    parser.add_argument('--output-dir', type=pathlib.Path, default=pathlib.Path.cwd() / 'work/ai-column-validation')
    args = parser.parse_args()
    if args.mysqltest is None:
        parser.error('mysqltest is required; pass --mysqltest /path/to/mysqltest')
    try:
        import pymysql
    except ImportError:
        parser.error('PyMySQL must be available in this Python environment or on PYTHONPATH')
    args.repo = args.repo.resolve(strict=True)
    args.binary = args.binary.resolve(strict=True)
    args.mysqltest = pathlib.Path(args.mysqltest).resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.repo, text=True).strip()
    digest = hashlib.sha256()
    with args.binary.open('rb') as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b''):
            digest.update(chunk)
    summary = {'revision':revision, 'binary':str(args.binary), 'binary_sha256':digest.hexdigest(),
               'started_at':datetime.datetime.now().astimezone().isoformat(),
               'public':[], 'regressions':[], 'passed':False, 'error':None}
    env = os.environ.copy()
    env['PATH'] = str(pathlib.Path(sys.executable).parent) + os.pathsep + env.get('PATH', '')
    suite = args.repo / 'tools/deploy/mysql_test/test_suite/ai_column'
    mock = suite / 'include/ai_column_mock.py'
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-column-public-', dir='/tmp'))
    server = connection = None
    def stop_owned(process):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    try:
        with (runtime / 'server-console.log').open('w') as console:
            server = subprocess.Popen([str(args.binary), '--nodaemon', '--base-dir=' + str(runtime),
                '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
                '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
                '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M'],
                stdout=console, stderr=subprocess.STDOUT, env=env)
            for _ in range(200):
                if server.poll() is not None:
                    raise RuntimeError('server exited: ' + str(runtime))
                try:
                    connection = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                                 autocommit=True, connect_timeout=1, read_timeout=30)
                    break
                except pymysql.MySQLError:
                    time.sleep(.2)
            if connection is None:
                raise RuntimeError('server startup timed out: ' + str(runtime))
            with connection.cursor() as cursor:
                cursor.execute('CREATE DATABASE ai_column_public_validation')
            connection.close()
            connection = None
            for case in CASES:
                command = [str(args.mysqltest), '--socket=' + str(runtime / 'run/sql.sock'), '-uroot',
                    '--database=ai_column_public_validation', '--test-file=' + str(suite / 't' / (case + '.test')),
                    '--result-file=' + str(suite / 'r/mysql' / (case + '.result')),
                    '--logdir=' + str(runtime), '--tmpdir=' + str(runtime)]
                result = subprocess.run(command, cwd=args.repo / 'tools/deploy', env=env,
                                        capture_output=True, text=True, timeout=240)
                log = output / ('ai-column-ready-' + case + '.log')
                log.write_text(result.stdout + result.stderr)
                summary['public'].append({'name':case, 'returncode':result.returncode, 'log':str(log)})
                print(case, 'PASS' if result.returncode == 0 else 'FAIL', flush=True)
            stop_owned(server)
            server = None
        # Each regression owns one fresh instance. Run them sequentially to
        # avoid HTTP mock port interference and keep resource use bounded.
        regressions = [
            ('definition-context', 'unittest/sql/engine/dml/test_ai_column_definition_context.py'),
            ('recovery', 'unittest/sql/engine/dml/test_ai_column_recovery.py'),
            ('retry', 'unittest/sql/engine/basic/test_ai_column_retry_sql.py'),
            ('strict', 'unittest/sql/engine/basic/test_ai_column_strict_sql.py')]
        for name, relative in regressions:
            log = output / ('ai-column-ready-' + name + '.log')
            with log.open('w') as stream:
                result = subprocess.run([sys.executable, str(args.repo / relative), '--binary', str(args.binary)],
                                        cwd=args.repo, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=240)
            summary['regressions'].append({'name':name, 'returncode':result.returncode, 'log':str(log)})
            print(name, 'PASS' if result.returncode == 0 else 'FAIL', flush=True)
        summary['runtime'] = str(runtime)
    except Exception as error:
        summary['error'] = type(error).__name__ + ': ' + str(error)
        print(summary['error'], flush=True)
    finally:
        if connection:
            connection.close()
        stop_owned(server)
        for port in (19120,19121,19122,19123,19124):
            subprocess.run([sys.executable, str(mock), 'stop', str(port)], env=env,
                           capture_output=True, timeout=10, check=False)
        summary['passed'] = (summary['error'] is None and len(summary['public']) == 5
            and len(summary['regressions']) == 4 and all(
                item['returncode'] == 0 for item in summary['public'] + summary['regressions']))
        path = output / 'ai-column-ready-summary.json'
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        print('SUMMARY', 'PASS' if summary['passed'] else 'FAIL', path, flush=True)
    raise SystemExit(0 if summary['passed'] else 1)


if __name__ == '__main__':
    main()
