#!/usr/bin/env bash
set -Eeuo pipefail
TASK_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$TASK_DIR/../../../.." && pwd)
BUILD=$(mktemp -d "${TMPDIR:-/tmp}/cuvs-startup-omp.XXXXXX")
trap 'rm -rf "$BUILD"' EXIT
CC=${CC:-cc}
CXX=${CXX:-c++}
FLAGS=(-O1 -g -Wall -Wextra -Werror -fsanitize=address,undefined -fno-omit-frame-pointer)
for SOURCE_LANGUAGE in c c++; do
  if [[ $SOURCE_LANGUAGE == c ]]; then COMPILER=$CC; STANDARD=c11; else COMPILER=$CXX; STANDARD=c++17; fi
  "$COMPILER" -x "$SOURCE_LANGUAGE" -std="$STANDARD" "${FLAGS[@]}" -I"$ROOT/src" "$TASK_DIR/test_startup_policy.c" -o "$BUILD/policy"
  "$BUILD/policy"
done
if [[ $(uname -s) == Linux ]]; then
  "$CXX" -std=c++17 -O1 -g -Wall -Wextra -Werror -fopenmp -pthread -I"$ROOT/src" \
    "$TASK_DIR/test_startup_self_exec.cpp" -o "$BUILD/self_exec"
  for MODE in normal fail cpu-only; do
    WRAPPER_FLAGS=(-std=c++17 -O1 -g -Wall -Wextra -Werror -fopenmp -pthread -D_GNU_SOURCE)
    if [[ $MODE != cpu-only ]]; then WRAPPER_FLAGS+=(-DOB_BUILD_CUVS); fi
    if [[ $MODE == fail ]]; then WRAPPER_FLAGS+=(-DSTARTUP_TEST_FORCE_EXEC_FAILURE); fi
    "$CXX" "${WRAPPER_FLAGS[@]}" -I"$ROOT/src" \
      "$TASK_DIR/test_linux_wrapper.cpp" -o "$BUILD/wrapper-$MODE"
  done
  python3 "$TASK_DIR/run_linux_wrapper_tests.py" --probe "$BUILD/wrapper-normal" \
    --failure-probe "$BUILD/wrapper-fail" --cpu-only-probe "$BUILD/wrapper-cpu-only"
elif [[ -n ${OPENMP_LIBRARY:-} ]]; then
  "$CXX" -std=c++17 -O1 -g -Wall -Wextra -Werror -Xclang -fopenmp -pthread -Wl,-headerpad_max_install_names \
    -I"$ROOT/src" "$TASK_DIR/test_startup_self_exec.cpp" "$OPENMP_LIBRARY" -o "$BUILD/self_exec"
  INSTALL_NAME=$(otool -D "$OPENMP_LIBRARY" | sed -n '2p')
  install_name_tool -change "$INSTALL_NAME" "$OPENMP_LIBRARY" "$BUILD/self_exec"
else
  echo 'SKIP actual OpenMP self-exec: set OPENMP_LIBRARY to an existing libomp.dylib on macOS.'
  exit 0
fi
env -u OMP_NUM_THREADS -u OMP_WAIT_POLICY -u SEEKDB_CUVS_OMP_REEXEC \
  -u OMP_PROC_BIND -u OMP_PLACES -u GOMP_CPU_AFFINITY -u KMP_AFFINITY \
  -u KMP_HW_SUBSET -u KMP_PLACE_THREADS -u KMP_HIDDEN_HELPER_AFFINITY \
  -u OMP_NUM_THREADS_ALL -u OMP_WAIT_POLICY_ALL -u OMP_PROC_BIND_ALL \
  "$BUILD/self_exec" 'argument with spaces' 'literal-$HOME;quotes-'\''"'
