"""Run two public AI suites, two regression suites, and 31 provider checks.

All SQL/mock assets come from --repo. The executable is always --binary.
"""
import argparse
import hashlib
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import time


CASES = ('ai_prompt', 'ai_complete_json_config', 'ai_embed_null_dimension', 'ai_function_enhancement')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', required=True, type=pathlib.Path)
    parser.add_argument('--repo', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[4])
    parser.add_argument('--mysqltest', type=pathlib.Path, default=shutil.which('mysqltest'),
                        help='mysqltest executable (defaults to mysqltest on PATH).')
    parser.add_argument('--output-dir', type=pathlib.Path,
                        default=pathlib.Path.cwd() / 'work/ai-functions-validation')
    parser.add_argument('--enhancement-first', action='store_true', help='Run AI enhancement before any warm-up suites to exercise cold startup.')
    parser.add_argument('--skip-provider', action='store_true', help='Run only the four mysqltest suites.')
    args = parser.parse_args()
    cases = (CASES[-1], *CASES[:-1]) if args.enhancement_first else CASES
    if args.mysqltest is None:
        parser.error('mysqltest was not found on PATH; pass --mysqltest /path/to/mysqltest')
    for name in ('binary', 'mysqltest'):
        path = pathlib.Path(getattr(args, name)).resolve()
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error('--' + name + ' must name an executable file: ' + str(path))
        setattr(args, name, path)
    args.repo = args.repo.resolve()
    if not args.repo.is_dir():
        parser.error('--repo must name an existing seekdb checkout: ' + str(args.repo))
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    try:
        import pymysql
    except ImportError:
        parser.error('PyMySQL is required; install it in this Python environment or provide it on PYTHONPATH')
    env = os.environ.copy()
    env['PATH'] = str(pathlib.Path(sys.executable).parent) + os.pathsep + env.get('PATH', '')
    suite = args.repo / 'tools/deploy/mysql_test/test_suite/ai_function'
    generic_mock = suite / 'include/ai_function_mock.py'
    enhancement_mock = suite / 'include/ai_function_enhancement_mock.py'
    provider = args.repo / 'unittest/sql/engine/expr/test_ai_provider_sql.py'
    summary_path = output / 'ai-functions-ready-summary.json'
    log_paths = [output / ('ai-functions-ready-' + case + '.log') for case in cases]
    log_paths.append(output / 'ai-functions-ready-provider.log')
    # Clear exactly this runner's previous results before producing new evidence.
    for path in log_paths + [summary_path]:
        path.unlink(missing_ok=True)
    for path in (generic_mock, enhancement_mock, provider):
        if not path.is_file():
            raise FileNotFoundError(path)
    runtime = pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-ready-', dir='/tmp'))
    digest = hashlib.sha256()
    with args.binary.open('rb') as executable:
        for chunk in iter(lambda: executable.read(1024 * 1024), b''):
            digest.update(chunk)
    try:
        revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.repo,
                                           text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = None
    summary = {'binary': str(args.binary), 'binary_sha256': digest.hexdigest(),
               'repo': str(args.repo), 'revision': revision, 'runtime': str(runtime),
               'case_order': list(cases), 'sql_suites': [], 'provider': None, 'error': None}
    server = mock = None
    connection = None
    console = mock_console = None
    def stop_owned(process):
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    try:
        console = open(runtime / 'server-console.log', 'w')
        server = subprocess.Popen([
            str(args.binary), '--nodaemon', '--base-dir=' + str(runtime),
            '--parameter', 'mysql_port_mode=disabled', '--parameter', 'enable_rpc_service=false',
            '--parameter', 'memory_budget=2G', '--parameter', 'log_disk_size=2G',
            '--parameter', 'cpu_count=2', '--parameter', 'vector_memory_limit=128M',
        ], stdout=console, stderr=subprocess.STDOUT, env=env)
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
            cursor.execute('create database ai_functions_validation')
        connection.close()
        connection = None
        for case in cases:
            if case == 'ai_function_enhancement':
                try:
                    with socket.create_connection(('127.0.0.1', 8000), timeout=.1):
                        raise RuntimeError('port 8000 is already in use; refusing to reuse another service')
                except OSError:
                    pass
                mock_console = open(runtime / 'enhancement-mock.log', 'w')
                mock = subprocess.Popen([sys.executable, str(enhancement_mock), '--port', '8000'],
                                        stdout=mock_console, stderr=subprocess.STDOUT, env=env)
                for _ in range(100):
                    if mock.poll() is not None:
                        raise RuntimeError('enhancement mock exited; see ' + str(runtime))
                    try:
                        with socket.create_connection(('127.0.0.1', 8000), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise RuntimeError('enhancement mock startup timed out')
            command = [str(args.mysqltest), '--socket=' + str(runtime / 'run/sql.sock'), '-uroot',
                       '--database=ai_functions_validation', '--test-file=' + str(suite / 't' / (case + '.test')),
                       '--result-file=' + str(suite / 'r/mysql' / (case + '.result')),
                       '--logdir=' + str(runtime), '--tmpdir=' + str(runtime)]
            result = subprocess.run(command, cwd=args.repo / 'tools/deploy', env=env,
                                    capture_output=True, text=True, timeout=240)
            log = output / ('ai-functions-ready-' + case + '.log')
            log.write_text(result.stdout + result.stderr)
            summary['sql_suites'].append({'case': case, 'source': 'public' if case in ('ai_prompt', 'ai_function_enhancement') else 'regression', 'returncode': result.returncode, 'log': str(log)})
            print(case, result.returncode, result.stdout.strip(), result.stderr.strip(), flush=True)
        stop_owned(mock)
        mock = None
        stop_owned(server)
        server = None
        if not args.skip_provider:
            log = output / 'ai-functions-ready-provider.log'
            with open(log, 'w') as stream:
                result = subprocess.run([sys.executable, str(provider), '--binary', str(args.binary)],
                                        cwd=args.repo, env=env, stdout=stream, stderr=subprocess.STDOUT, timeout=240)
            summary['provider'] = {'returncode': result.returncode, 'expected_checks': 31, 'log': str(log)}
            print('AI provider 31 checks', result.returncode, flush=True)
    except Exception as error:
        summary['error'] = type(error).__name__ + ': ' + str(error)
        print(summary['error'], flush=True)
    finally:
        if connection:
            connection.close()
        stop_owned(mock)
        stop_owned(server)
        if console:
            console.close()
        if mock_console:
            mock_console.close()
        subprocess.run([sys.executable, str(generic_mock), 'stop', '19125'],
                       env=env, capture_output=True, timeout=10, check=False)
        summary['passed'] = (summary['error'] is None and len(summary['sql_suites']) == 4
                             and all(case['returncode'] == 0 for case in summary['sql_suites'])
                             and (args.skip_provider or summary['provider'] is not None
                                  and summary['provider']['returncode'] == 0))
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
        print('RUNTIME', runtime, flush=True)
        print('SUMMARY', summary_path, 'PASS' if summary['passed'] else 'FAIL', flush=True)
    raise SystemExit(0 if summary['passed'] else 1)


if __name__ == '__main__':
    main()
