# AI COLUMN validation

The asynchronous AI column path uses a transactional outbox and background
workers. It supports CREATE/ADD/MODIFY definitions, dependency invalidation,
request/definition fencing, grouped publication, bounded retries, selected-row
RETRY, strict reads and native vector-index maintenance.

From the repository root after building seekdb:

```sh
python3 unittest/sql/engine/dml/run_ai_column_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest
```

Requirements: Linux or macOS, Python 3.9+ with PyMySQL, a compatible mysqltest,
local ports 19120 through 19126 available, and enough disk space for disposable
instances. Tests run sequentially and stop their own server/mock processes.
Diagnostic runtime directories remain under `/tmp`.

The runner executes all five public mysqltest suites against their original
expected results. It then checks definition SQL modes and dependency DDL,
transaction rollback and real process-crash recovery, RETRY and privileges, and
strict-read references including prepared statements and transaction snapshots.
The definition suite also exercises LONGTEXT dependencies and READY NULL values.
Results, source revision and executable SHA256 are written to
`work/ai-column-validation` (override with `--output-dir`).

Some macOS mysqltest distributions implement fractional `--real_sleep` waits
using whole wall-clock seconds. In a local phase-controlled probe, a requested
0.3-second wait completed in 0.084 to 0.107 seconds. That can update a row before
the old HTTP request starts, so the public stale-response assertion cannot
exercise the intended race. For such clients only, pass
`--precise-fractional-sleeps`. The runner stages test copies under its disposable
runtime and replaces only fractional sleep commands with Python `time.sleep`.
SQL, mock services, assertions, expected results and the original test files
remain unchanged. The summary explicitly records this compatibility mode,
original source hashes and replacement counts. This is local timing-adapted
validation, not a claim of an unmodified official run.

To check AI function enhancement before other suites warm the database:

```sh
python3 unittest/sql/engine/expr/run_ai_function_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest --enhancement-first
```

That ordering keeps the original SQL and timeout settings, so it can expose
cold-start problems hidden by a warmed test sequence. Local success is evidence
for covered scenarios; official competition scoring remains separate.

To stress the original worker suite without adapting any timing commands:

```sh
python3 unittest/sql/engine/dml/run_ai_column_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest --cases ai_column_worker \
  --worker-repeats 10 --skip-regressions
```

Each iteration retains its own client log. The original worker's old-response
assertion can fail when the original request has not started before an update.
The dispatch fix wakes workers from transaction completion, drains runnable
groups without an imposed wait and reuses the fresh group lease before its
first request. These scheduling hints do not replace durable claim/owner checks.
