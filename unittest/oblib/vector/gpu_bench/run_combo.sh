#!/usr/bin/env bash
# Paired SQL: V5 (seekdb+bridge from 634888d) vs combo seekdb (3929580) with bridge variants.
cd /workspace/amd20 || exit 1
until grep -q BUILD_OK art-combo/sha256.txt 2>/dev/null; do sleep 15; done
SRC=tree-392958099df9e75a993151752d8ec4e22c523e21/tools/hipvs_bridge
variant() { # name inter niter
  local d=art-$1; mkdir -p "$d/src"; cp "$SRC"/* "$d/src/"
  sed -i -e "s/intermediate_graph_degree = 256;/intermediate_graph_degree = $2;/" \
         -e "s/nn_descent_niter = 100;/nn_descent_niter = $3;/" "$d/src/seekdb_cuvs_bridge.c"
  grep -n "intermediate_graph_degree =\|nn_descent_niter =" "$d/src/seekdb_cuvs_bridge.c" > "$d/params.txt"
  HIPVS_ROOT=/opt/hipvs ROCM_ROOT=/opt/rocm bash "$d/src/build.sh" "$PWD/$d/libseekdb_cuvs_bridge.so" > "$d/build.log" 2>&1
}
variant c256n100 256 100
variant c256n10 256 10
variant c192n10 192 10
rev() { LD_LIBRARY_PATH=$PWD/$2:/opt/hipvs/lib:/opt/rocm/lib "$1" --version 2>&1 | sed -n 's/^REVISION: 1-//p' | head -1; }
BREV=$(rev art-v5/seekdb art-v5); CREV=$(rev art-combo/seekdb art-combo)
echo "baseline $BREV candidate $CREV" > combo-status.txt
pair() { # dataset variant
  local ds=$1 v=$2 dim=256; [[ $ds == int128 ]] && dim=128
  rm -rf "sqlc-$ds-$v"
  PYTHONPATH=$PWD/pydeps python3 paired_sql_benchmark.py --base data/$ds/base.fvecs --queries data/$ds/query.fvecs \
    --ground-truth data/$ds/truth.ivecs --output-dir "sqlc-$ds-$v" \
    --baseline-binary $PWD/art-v5/seekdb --baseline-bridge $PWD/art-v5/libseekdb_cuvs_bridge.so \
    --candidate-binary $PWD/art-combo/seekdb --candidate-bridge $PWD/art-$v/libseekdb_cuvs_bridge.so \
    --baseline-commit "$BREV" --candidate-commit "$CREV" --expected-dim $dim --nq 1024 --repeats 2 \
    --cpu-count 24 --memory-budget 24G > "sqlc-$ds-$v.log" 2>&1
  echo "$ds $v exit $?" >> combo-status.txt
}
for job in "hard256 c192n10" "mid256 c192n10" "hard256 c256n10" "mid256 c256n10" "int128 c192n10" \
           "int128 c256n10" "hard256 c256n100"; do
  pair $job
done
echo COMBO_DONE >> combo-status.txt
