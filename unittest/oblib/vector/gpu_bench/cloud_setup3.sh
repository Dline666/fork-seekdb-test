#!/usr/bin/env bash
# Rebuild the benchmark environment on a fresh instance (V5 + final candidate trees, data).
set -u
cd /workspace && mkdir -p amd20 && cd amd20
pip install --target /workspace/amd20/pydeps pymysql h5py > pip.log 2>&1
fetch() { local sha=$1; [[ -d tree-$sha/src ]] && return 0; mkdir -p tree-$sha && curl -sSL --max-time 900 "https://codeload.github.com/Dline666/seekdb-2026/tar.gz/$sha" | tar xz -C tree-$sha --strip-components=1; }
fetch ac1ee2667cbebdd3db847370c2bff4954b074803   # gpu-bench tools + SIFT inputs
G=tree-ac1ee2667cbebdd3db847370c2bff4954b074803/unittest/oblib/vector/gpu_bench
cp $G/*.py $G/*.sh $G/*.c . 2>/dev/null
mkdir -p data && tar xzf $G/sift250k-validation-inputs.tar.gz -C data
python3 gen_data.py /workspace/amd20/data > gen.log 2>&1 &
for c in cagra_bench3; do /opt/rocm/bin/hipcc -O2 -D__HIP_PLATFORM_AMD__ -I/opt/rocm/include -I/opt/hipvs/include $c.c -L/opt/hipvs/lib -Wl,-rpath,/opt/hipvs/lib:/opt/rocm/lib -lcuvs_c -o $c; done
fetch 634888d9b037153fa143d2c913ba4b25503e1d62
fetch 68e5b2633f0c967963050097e563bb92f865b029
wait
ls data > setup3-done.txt
