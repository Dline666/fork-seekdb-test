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

To check AI function enhancement before other suites warm the database:

```sh
python3 unittest/sql/engine/expr/run_ai_function_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest --enhancement-first
```

That ordering keeps the original SQL and timeout settings, so it can expose
cold-start problems hidden by a warmed test sequence. Local success is evidence
for covered scenarios; official competition scoring remains separate.
