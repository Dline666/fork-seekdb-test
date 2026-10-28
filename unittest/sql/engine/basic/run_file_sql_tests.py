"""Validate native File SQL and COPY on disposable seekdb instances."""
import argparse
import base64
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=pathlib.Path)
    parser.add_argument('--mysqltest', type=pathlib.Path, default=shutil.which('mysqltest'))
    parser.add_argument('--repo', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[4])
    parser.add_argument('--output-dir', type=pathlib.Path, default=pathlib.Path.cwd() / 'work/file-sql-validation')
    parser.add_argument('--sanitize', action='store_true', help='Enable ASan/UBSan for the standalone file tests.')
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
    try:
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.repo, text=True).strip()
    except subprocess.CalledProcessError:
        revision = None
    checksum = hashlib.sha256()
    with args.binary.open('rb') as binary:
        for chunk in iter(lambda: binary.read(1024 * 1024), b''):
            checksum.update(chunk)
    digest = checksum.hexdigest()
    summary = {'revision': revision, 'binary': str(args.binary), 'binary_sha256': digest,
               'repo': str(args.repo), 'started_at': datetime.datetime.now().astimezone().isoformat(),
               'stages': [], 'passed': False, 'error': None}
    env = os.environ.copy()
    env['PATH'] = str(pathlib.Path(sys.executable).parent) + os.pathsep + env.get('PATH', '')
    env['SANITIZE'] = '1' if args.sanitize else '0'
    def stage(name, command, cwd=None, timeout=240):
        log = output / ('file-sql-ready-' + name + '.log')
        with log.open('w') as stream:
            result = subprocess.run(command, cwd=cwd or args.repo, env=env,
                                    stdout=stream, stderr=subprocess.STDOUT, timeout=timeout)
        summary['stages'].append({'name': name, 'returncode': result.returncode, 'log': str(log)})
        print(name, 'PASS' if result.returncode == 0 else 'FAIL', flush=True)
        return result.returncode == 0
    def stop_owned(process):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    try:
        stage('core', ['bash', str(args.repo / 'unittest/sql/engine/basic/run_file_format_tests.sh')])
        with tempfile.TemporaryDirectory(prefix='seekdb-copy-core-', dir='/tmp') as temp:
            temp = pathlib.Path(temp)
            executable = temp / 'test-copy-publication'
            flags = ['-std=c++17', '-Wall', '-Wextra', '-Werror']
            if args.sanitize:
                flags += ['-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-g']
            compiler = env.get('CXX', 'c++')
            if stage('publication-build', [compiler, *flags, '-I' + str(args.repo / 'src'),
                      str(args.repo / 'unittest/sql/engine/basic/test_copy_publication.cpp'), '-o', str(executable)]):
                stage('publication', [str(executable), str(temp / 'fixtures')])
        runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-file-public-', dir='/tmp'))
        fixtures = runtime / 'data_for_mysqltest_file_sql'
        fixtures.mkdir()
        suite = args.repo / 'tools/deploy/mysql_test/test_suite/file_sql'
        for source in (suite / 'data').iterdir():
            if source.suffix == '.b64':
                (fixtures / source.stem).write_bytes(base64.b64decode(source.read_bytes()))
            elif source.is_file():
                shutil.copyfile(source, fixtures / source.name)
        # Preserve the official SQL and expected output. Only replace the SSH
        # fixture transport with files already staged beside this local server.
        include = runtime / 'deploy/mysql_test/test_suite/file_sql/include'
        include.mkdir(parents=True)
        for name in ('copy_file_sql_data.inc', 'remove_file_sql_data.inc'):
            (include / name).write_text('# Fixtures are managed by this disposable runner.\n')
        wrapper = runtime / 'file_sql_basic.test'
        wrapper.write_text('let $OBSERVER_DIR = ' + str(runtime) + ';\n--source ' +
                           str(suite / 't/file_sql_basic.test') + '\n')
        server = connection = None
        with (runtime / 'server-console.log').open('w') as console:
            try:
                server = subprocess.Popen([str(args.binary), '--nodaemon', '--base-dir=' + str(runtime),
                    '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
                    '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
                    '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
                    '--variable', 'secure_file_priv=' + str(fixtures)], stdout=console, stderr=subprocess.STDOUT, env=env)
                for _ in range(200):
                    if server.poll() is not None:
                        raise RuntimeError('public-suite server exited: ' + str(runtime))
                    try:
                        connection = pymysql.connect(unix_socket=str(runtime / 'run/sql.sock'), user='root',
                                                     autocommit=True, connect_timeout=1, read_timeout=30)
                        break
                    except pymysql.MySQLError:
                        time.sleep(.2)
                if connection is None:
                    raise RuntimeError('public-suite server startup timed out: ' + str(runtime))
                with connection.cursor() as cursor:
                    cursor.execute('CREATE DATABASE file_sql_public_validation')
                connection.close()
                connection = None
                stage('public-basic', [str(args.mysqltest), '--socket=' + str(runtime / 'run/sql.sock'), '-uroot',
                      '--database=file_sql_public_validation', '--test-file=' + str(wrapper),
                      '--result-file=' + str(suite / 'r/mysql/file_sql_basic.result'),
                      '--logdir=' + str(runtime), '--tmpdir=' + str(runtime)], cwd=runtime / 'deploy')
                stage('parquet-sql', [sys.executable, str(args.repo / 'unittest/sql/engine/basic/test_parquet_file_sql.py'),
                      '--socket', str(runtime / 'run/sql.sock'), '--fixtures', str(fixtures)])
                summary['public_runtime'] = str(runtime)
            finally:
                if connection:
                    connection.close()
                stop_owned(server)
        for name, script in [('sql', 'test_file_sql.py'), ('copy-sql', 'test_copy_sql.py')]:
            stage(name, [sys.executable, str(args.repo / 'unittest/sql/engine/basic' / script),
                         '--binary', str(args.binary)])
    except Exception as error:
        summary['error'] = type(error).__name__ + ': ' + str(error)
        print(summary['error'], flush=True)
    finally:
        summary['passed'] = summary['error'] is None and len(summary['stages']) == 7 and all(
            item['returncode'] == 0 for item in summary['stages'])
        path = output / 'file-sql-ready-summary.json'
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        print('SUMMARY', 'PASS' if summary['passed'] else 'FAIL', path, flush=True)
    raise SystemExit(0 if summary['passed'] else 1)


if __name__ == '__main__':
    main()
