#!/usr/bin/env bash
# Build V5 and niter candidate in parallel (each 48 jobs of 128 CPUs).
cd /workspace/amd20 || exit 1
fetch() { local sha=$1; [[ -d tree-$sha/src ]] && return 0; mkdir -p tree-$sha && curl -sSL --max-time 900 "https://codeload.github.com/Dline666/seekdb-2026/tar.gz/$sha" | tar xz -C tree-$sha --strip-components=1; }
fetch 4d7826a0e9caa666a8280c54110dc52ee2fce4ad
( JOBS=56 bash cloud_build.sh tree-634888d9b037153fa143d2c913ba4b25503e1d62 art-v5 > build-v5.log 2>&1 ) &
( JOBS=56 bash cloud_build.sh tree-4d7826a0e9caa666a8280c54110dc52ee2fce4ad art-niter > build-niter.log 2>&1 ) &
wait
cat art-v5/sha256.txt art-niter/sha256.txt > builds-done.txt 2>&1
