# AMD CAGRA candidate validation

`run_cuvs_host_tests.sh` compiles the actual bridge as C11 and C++17 against
host-only ABI mocks, and tests the actual worker and snapshot helper. It needs
only a C/C++ compiler and pthreads. ASan/UBSan are on by default. Run `SANITIZERS=thread bash unittest/oblib/vector/run_cuvs_host_tests.sh` for ThreadSanitizer. These tests
validate ownership, overflow rejection, failed growth/retry, same-handle
concurrency, parameter reset, resource-stream synchronization (also after search failure), stack size, and descriptor cleanup. They do not
validate the real ROCm/hipVS ABI, CAGRA Recall, SQL semantics or GPU performance.
The mock include directory is used only by this test script, never production.

```sh
bash unittest/oblib/vector/run_cuvs_host_tests.sh
```

`run_cuvs_gpu_benchmark.py` needs Python 3.9+ and the real baseline and candidate
bridge libraries plus ROCm/hipVS runtime. It uses only Python's standard library.
Run it on an otherwise idle AMD GPU after compiling both libraries. Each run is
a fresh child process, builds a fresh index, serializes it to a fresh temporary
file, releases it, reloads it, and includes the first query in its bridge timing.
Version order alternates between repetitions. Buffers and search parameters are
reused across requests within that index, including changes in k.

```sh
python3 unittest/oblib/vector/run_cuvs_gpu_benchmark.py \
  --baseline-lib /path/to/baseline/libseekdb_cuvs_bridge.so \
  --candidate-lib /path/to/candidate/libseekdb_cuvs_bridge.so \
  --base /data/base250k.fvecs --queries /data/query.fvecs \
  --ground-truth /data/truth-for-this-exact-base.ivecs \
  --nq 4096 --topk 10 100 --repeats 2 --output /results/bridge.json
```

Without input files it generates a small synthetic dataset for a smoke test;
without ground truth it reports Recall as null. A precomputed ground truth for
a larger dataset is not valid after truncating the base file. Bridge timings
exclude seekdb scanning, DDL, SQL and adaptor thread dispatch. They do not measure
the new snapshot staging helper either. They are diagnostics, never official
scores. Use the organizer's full SQL harness and external GPU profiler for formal
validation, including CPU VSAG behavior and fallback checks.

The candidate keeps V5's FP32 input and final degree 128, builds CAGRA NN_DESCENT with intermediate degree 128 and 4 rounds,
and searches with MULTI_CTA, itopk=max(library_default,2048,4*k), search_width=4, and exact original CAGRA graph/data values. New snapshots use OBCUVS03:
one outer FP32 dataset plus the graph-only SKCGRPH1 payload. Existing OBCUVS02
snapshots remain readable through the original hipVS deserializer.
The persistent worker is synchronous and must not recursively submit work to
itself. The adaptor's existing index-registry serialization remains in place.
The bridge serializes search/serialize calls per handle; freeing a handle requires
exclusive ownership. Device workspace is retained until index destruction at its
largest requested shape. Linux snapshot scratch uses memfd (host RAM), with
`tmpfile()` fallback if memfd creation is unavailable; it is destroyed after every
snapshot operation and is never reused across builds or benchmark rounds.


Graph-only real-GPU validation (not yet run on the new candidate):

```sh
c++ -std=c++17 unittest/oblib/vector/test_cuvs_graph_gpu.cpp \
  -Itools/hipvs_bridge -L/path/to/candidate/bridge \
  -Wl,-rpath,/path/to/candidate/bridge -lseekdb_cuvs_bridge \
  -o /results/test_cuvs_graph_gpu
LD_LIBRARY_PATH=/path/to/candidate/bridge:/opt/hipvs/lib:/opt/rocm/lib \
  /results/test_cuvs_graph_gpu /results/new-graph-roundtrip
```

It requires a new output directory and checks that original, legacy-reloaded and
graph-only-reloaded indexes return the same IDs/distances and export exactly the
same graph bytes. A passing host protocol test does not replace this GPU run.
Also validate an actual OBCUVS02 SQL index created with V5 across a server binary
upgrade, and a fresh OBCUVS03 SQL index across restart.
