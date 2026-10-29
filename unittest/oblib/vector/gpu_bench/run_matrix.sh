#!/usr/bin/env bash
# Paired GPU CAGRA build matrix under a 24-CPU formal-like limit. Appends JSON lines.
set -u
B=${1:?bench}; D=${2:?datadir}; OUT=${3:?out}; CPUS=${CPUS:-0-23}
run() { # data algo degree inter niter omp
  local env=(); [[ "$6" != unset ]] && env=(OMP_NUM_THREADS=$6)
  env "${env[@]}" timeout 1800 taskset -c "$CPUS" "$B" "$D/$1/base.fvecs" "$D/$1/query.fvecs" \
    "$D/$1/truth.ivecs" "$2" "$3" "$4" "$5" 1024 \
    | sed "s/^{/{\"data\":\"$1\",\"cpus\":\"$CPUS\",/" | tee -a "$OUT"
}
for data in ${DATASETS:-hard256 sift250k mid256}; do
  [[ -f "$D/$data/base.fvecs" ]] || continue
  while read -r cfg; do
    [[ -n "$cfg" ]] && run "$data" $cfg
  done <<< "${CONFIGS:-nnd 128 256 100 unset
nnd 128 256 100 24
nnd 128 128 100 24
nnd 64 128 100 24
iter 128 256 100 24}"
done
