#!/usr/bin/env bash
set -Eeuo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TESTS="$ROOT/unittest/oblib/vector"
BUILD=$(mktemp -d "${TMPDIR:-/tmp}/seekdb-cuvs-host.XXXXXX")
trap 'rm -rf "$BUILD"' EXIT
CXX=${CXX:-c++}
CC=${CC:-cc}
SANITIZERS=${SANITIZERS:-address,undefined}
LIBS=""
if [[ $(uname -s) == Linux ]]; then LIBS="-ldl"; fi
FLAGS=(-g -O1 -Wall -Wextra -Werror -fno-omit-frame-pointer -pthread)
if [[ -n "$SANITIZERS" ]]; then FLAGS+=("-fsanitize=$SANITIZERS"); fi
"$CXX" -std=c++17 "${FLAGS[@]}" -I"$ROOT/src/oblib" \
  "$TESTS/test_cuvs_worker.cpp" -o "$BUILD/test_worker"
"$BUILD/test_worker"
"$CXX" -std=c++17 "${FLAGS[@]}" -I"$ROOT/src/oblib" \
  "$TESTS/test_cuvs_snapshot_file.cpp" -o "$BUILD/test_snapshot"
"$BUILD/test_snapshot"
for language in c cxx; do
  if [[ "$language" == c ]]; then COMPILER="$CC"; STANDARD=c11; else COMPILER="$CXX"; STANDARD=c++17; fi
  "$COMPILER" -x "${language/cxx/c++}" -std="$STANDARD" "${FLAGS[@]}" \
    -I"$TESTS/cuvs_mock" -I"$ROOT/tools/hipvs_bridge" \
    -c "$ROOT/tools/hipvs_bridge/seekdb_cuvs_bridge.c" -o "$BUILD/bridge.o"
  "$CXX" -std=c++17 "${FLAGS[@]}" -I"$TESTS/cuvs_mock" -I"$ROOT/tools/hipvs_bridge" \
    "$TESTS/test_cuvs_bridge.cpp" "$BUILD/bridge.o" $LIBS -o "$BUILD/test_bridge"
  "$BUILD/test_bridge"
done

bash "$TESTS/run_cuvs_omp_tests.sh"
