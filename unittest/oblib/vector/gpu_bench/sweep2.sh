#!/usr/bin/env bash
# Sweep inter-degree x niter on all three synthetic sets; append JSON + short summary.
cd /workspace/amd20 || exit 1
for ds in int128 hard256 mid256; do
  for cfg in "256 100" "256 10" "256 8" "256 6" "256 5" "192 10" "192 6"; do
    set -- $cfg
    OMP_NUM_THREADS=24 taskset -c 0-23 ./cagra_bench2 data/$ds/base.fvecs data/$ds/query.fvecs \
      data/$ds/truth.ivecs nnd 128 "$1" "$2" 2048 2>/dev/null \
      | sed "s/^{/{\"data\":\"$ds\",/" | tee -a sweep2.jsonl \
      | python3 -c "import json,sys;d=json.loads(sys.stdin.read());print(d['data'],d['inter'],d['niter'],d['build_s'],d['recall10'],d['recall100'],d['qps10'],d['peak_vram_mb'],flush=True)"
  done
done > sweep2.txt 2>&1
echo SWEEP2_DONE >> sweep2.txt
