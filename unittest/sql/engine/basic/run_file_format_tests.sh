#!/usr/bin/env bash
# Run the standard-library-only file SQL regressions without a full server build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
BUILD="$(mktemp -d "${TMPDIR:-/tmp}/seekdb-file-sql-test.XXXXXX")"
trap 'rm -rf "$BUILD"' EXIT
cd "$ROOT"
FLAGS=(-std=c++17 -fno-exceptions -Wall -Wextra)
if [[ "${SANITIZE:-0}" == 1 ]]; then
  FLAGS+=(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
fi
"${CXX:-c++}" "${FLAGS[@]}" -Isrc \
  unittest/sql/engine/basic/test_file_format.cpp \
  src/sql/engine/basic/ob_file_format.cpp \
  src/sql/engine/basic/ob_parquet_format.cpp -o "$BUILD/test_file_format"
python3 - "$BUILD/fixture.parquet" <<'PY'
import base64, pathlib, sys
pathlib.Path(sys.argv[1]).write_bytes(base64.b64decode(
    pathlib.Path('tools/deploy/mysql_test/test_suite/file_sql/data/simple.parquet.b64').read_bytes()))
PY
"$BUILD/test_file_format" "$BUILD/fixtures" "$BUILD/fixture.parquet"
