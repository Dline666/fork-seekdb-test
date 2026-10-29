#!/usr/bin/env bash
set -Eeuo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
TESTS="$ROOT/unittest/oblib/vector"
BUILD=$(mktemp -d "${TMPDIR:-/tmp}/seekdb-cuvs-omp.XXXXXX")
trap 'rm -rf "$BUILD"' EXIT
CC=${CC:-cc}
CXX=${CXX:-c++}
FLAGS=(-g -O1 -Wall -Wextra -Werror -fno-omit-frame-pointer -pthread -fsanitize=address,undefined)
if [[ $(uname -s) == Linux ]]; then FLAGS+=(-D_GNU_SOURCE); fi
for language in c c++; do
  if [[ $language == c ]]; then COMPILER=$CC; STANDARD=c11; else COMPILER=$CXX; STANDARD=c++17; fi
  "$COMPILER" -x "$language" -std="$STANDARD" "${FLAGS[@]}" \
    -I"$ROOT/tools/hipvs_bridge" "$TESTS/test_cuvs_omp_scope.c" -o "$BUILD/test_omp_scope"
  "$BUILD/test_omp_scope"
done
