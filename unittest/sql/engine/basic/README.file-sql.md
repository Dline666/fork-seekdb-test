# Native File SQL validation

This validates `read_csv`, `read_json` / `read_ndjson`, `read_parquet`, `glob`,
`DESCRIBE`, native SQL aggregation and joins, and CSV `COPY` exports. It requires
Linux or macOS, Python 3.9+ with PyMySQL, a compiled seekdb executable, a compatible
`mysqltest`, and a C++17 compiler. ASan/UBSan are optional.

From the repository root:

```sh
python3 unittest/sql/engine/basic/run_file_sql_tests.py \
  --binary "$PWD/build_release/src/observer/seekdb" \
  --mysqltest /path/to/mysqltest --sanitize
```

The runner executes 37 standalone file-format cases, the COPY publication
protocol suite, the public `file_sql_basic` SQL and expected output, 20 additional
file SQL cases, 23 Parquet type/encoding/row-group/pruning cases, and 25 COPY SQL cases. Results and a machine-readable summary are
written to `work/file-sql-validation` (override with `--output-dir`).

The public SQL and expected result are unchanged. The runner stages the original
fixtures locally and replaces only its SSH fixture-copy/cleanup includes, so it
can run with a disposable instance using a Unix socket and no RPC listener.
Every SQL script starts and stops its own temporary instance. Diagnostic runtime
files remain under `/tmp`; existing user instances are not used.

The COPY protocol tests cover destination races, failure cleanup, parent-directory
replacement, temporary-file symlink substitution and descriptor cleanup. The SQL
cases include empty exports and headers, special CSV characters, cancellation,
query errors, path confinement and ordinary SELECT INTO regression.

All query execution uses seekdb. No external SQL engine is required. Official
competition scores still require the organizer's evaluation.
