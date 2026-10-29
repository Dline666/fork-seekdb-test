#!/usr/bin/env bash
# Build seekdb + bridge for one source tree with the organizer's flags.
# usage: cloud_build.sh <tree-dir> <artifact-dir>
set -Eeuo pipefail
SRC=$(cd "$1" && pwd); ART=$2; mkdir -p "$ART"; ART=$(cd "$ART" && pwd)
cd "$SRC"
if [[ ! -e deps/3rd ]]; then
  CACHE=$(ls -d /opt/seekdb-deps-cache/*/3rd 2>/dev/null | head -1)
  [[ -n "$CACHE" ]] || { echo "no deps cache" > "$ART/sha256.txt"; exit 4; }
  mkdir -p deps && ln -s "$CACHE" deps/3rd
fi
HIPVS_ROOT=/opt/hipvs ROCM_ROOT=/opt/rocm tools/hipvs_bridge/build.sh "$ART/libseekdb_cuvs_bridge.so" > "$ART/bridge-build.log" 2>&1
cmake -S . -B build_release -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=RelWithDebInfo -DOB_ENABLE_UNITY=ON \
  -DOB_USE_LLD=ON -DOB_BUILD_CUVS=ON -DOB_BUILD_CUVS_TRACE=OFF \
  -DCUVS_BRIDGE_LIB="$ART/libseekdb_cuvs_bridge.so" > "$ART/cmake.log" 2>&1
cmake --build build_release --target seekdb -j "${JOBS:-48}" > "$ART/seekdb-build.log" 2>&1
cp build_release/src/observer/seekdb "$ART/seekdb"
sha256sum "$ART/seekdb" "$ART/libseekdb_cuvs_bridge.so" > "$ART/sha256.txt"
echo "BUILD_OK $SRC" >> "$ART/sha256.txt"
