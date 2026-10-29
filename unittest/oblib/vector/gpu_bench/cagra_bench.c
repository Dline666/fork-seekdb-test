// Paired GPU CAGRA build/search benchmark on the fixed hipVS C API.
// usage: cagra_bench base.fvecs query.fvecs truth.ivecs algo degree inter niter [nq]
//   algo: nnd | iter | ivfpq
// Prints one JSON line: build seconds, peak VRAM delta, Recall@10/@100, QPS.
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#include <hip/hip_runtime.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }

static float *read_fvecs(const char *p, long *n, int *d) {
  FILE *f = fopen(p, "rb"); if (!f) { perror(p); exit(1); }
  fseek(f, 0, SEEK_END); long bytes = ftell(f); fseek(f, 0, SEEK_SET);
  int dim; if (fread(&dim, 4, 1, f) != 1) exit(1); fseek(f, 0, SEEK_SET);
  long rows = bytes / (4L * (dim + 1)); float *v = malloc((size_t)rows * dim * 4); float *row = malloc(4 * (dim + 1));
  for (long i = 0; i < rows; ++i) { if (fread(row, 4, dim + 1, f) != (size_t)dim + 1) exit(1); memcpy(v + (size_t)i * dim, row + 1, 4 * dim); }
  free(row); fclose(f); *n = rows; *d = dim; return v;
}
static int *read_ivecs(const char *p, long *n, int *k) { int d; float *v = read_fvecs(p, n, &d); *k = d; return (int *)v; }

static DLManagedTensor tensor(void *data, int64_t *shape, int dev, int code, int bits) {
  DLManagedTensor t; memset(&t, 0, sizeof(t));
  t.dl_tensor.data = data; t.dl_tensor.device.device_type = (DLDeviceType)dev; t.dl_tensor.ndim = 2;
  t.dl_tensor.dtype.code = (uint8_t)code; t.dl_tensor.dtype.bits = (uint8_t)bits; t.dl_tensor.dtype.lanes = 1;
  t.dl_tensor.shape = shape; return t;
}

static volatile int sampling = 1; static size_t min_free = (size_t)-1;
static void *vram_sampler(void *arg) {
  (void)arg; hipSetDevice(0);
  while (sampling) { size_t fr, tot; if (hipMemGetInfo(&fr, &tot) == hipSuccess && fr < min_free) min_free = fr; usleep(2000); }
  return NULL;
}
#define CK(x) do { if ((x) != CUVS_SUCCESS) { fprintf(stderr, "%s failed: %s\n", #x, cuvsGetLastErrorText()); exit(2); } } while (0)

int main(int argc, char **argv) {
  if (argc < 8) { fprintf(stderr, "usage\n"); return 1; }
  long n, nq_all, tn; int dim, qd, tk;
  float *base = read_fvecs(argv[1], &n, &dim); float *qs = read_fvecs(argv[2], &nq_all, &qd);
  int *truth = read_ivecs(argv[3], &tn, &tk);
  const char *algo = argv[4]; int degree = atoi(argv[5]), inter = atoi(argv[6]), niter = atoi(argv[7]);
  long nq = argc > 8 ? atol(argv[8]) : nq_all; if (nq > nq_all) nq = nq_all;

  hipSetDevice(0); size_t free0, total; hipMemGetInfo(&free0, &total);
  cuvsResources_t res; CK(cuvsResourcesCreate(&res));
  cuvsCagraIndexParams_t ip; CK(cuvsCagraIndexParamsCreate(&ip));
  ip->build_algo = !strcmp(algo, "iter") ? ITERATIVE_CAGRA_SEARCH : !strcmp(algo, "ivfpq") ? IVF_PQ : NN_DESCENT;
  ip->graph_degree = degree; ip->intermediate_graph_degree = inter; ip->nn_descent_niter = niter;
  cuvsCagraIndex_t idx; CK(cuvsCagraIndexCreate(&idx));
  int64_t ds[2] = {n, dim}; DLManagedTensor dset = tensor(base, ds, kDLCPU, kDLFloat, 32);

  pthread_t th; pthread_create(&th, NULL, vram_sampler, NULL);
  double t0 = now();
  CK(cuvsCagraBuild(res, ip, &dset, idx)); CK(cuvsStreamSync(res));
  double build_s = now() - t0;
  sampling = 0; pthread_join(th, NULL);

  cuvsCagraSearchParams_t sp; CK(cuvsCagraSearchParamsCreate(&sp)); size_t default_itopk = sp->itopk_size;
  float *dq; unsigned *dn; float *dd; size_t qb = (size_t)nq * dim * 4;
  double recall[2] = {0, 0}, qps[2] = {0, 0}; int ks[2] = {10, 100};
  for (int r = 0; r < 2; ++r) {
    int k = ks[r]; size_t rb = (size_t)nq * k * 4;
    CK(cuvsRMMAlloc(res, (void **)&dq, qb)); CK(cuvsRMMAlloc(res, (void **)&dn, rb)); CK(cuvsRMMAlloc(res, (void **)&dd, rb));
    size_t need = (size_t)k * 4; if (need < 512) need = 512;
    sp->itopk_size = default_itopk > need ? default_itopk : need; sp->search_width = 4;
    unsigned *ids = malloc(rb);
    // One query per call, like SQL; measures per-call QPS including H2D/D2H.
    int64_t q1[2] = {1, dim}, r1[2] = {1, k};
    double s0 = now();
    for (long i = 0; i < nq; ++i) {
      hipMemcpy(dq, qs + (size_t)i * dim, dim * 4, hipMemcpyHostToDevice);
      DLManagedTensor qt = tensor(dq, q1, kDLCUDA, kDLFloat, 32), nt = tensor(dn, r1, kDLCUDA, kDLUInt, 32), dt = tensor(dd, r1, kDLCUDA, kDLFloat, 32);
      cuvsFilter f; f.type = NO_FILTER; f.addr = 0;
      CK(cuvsCagraSearch(res, sp, idx, &qt, &nt, &dt, f)); CK(cuvsStreamSync(res));
      hipMemcpy(ids + (size_t)i * k, dn, k * 4, hipMemcpyDeviceToHost);
    }
    qps[r] = nq / (now() - s0);
    long hit = 0, bad = 0;
    for (long i = 0; i < nq; ++i) for (int a = 0; a < k; ++a) {
      unsigned id = ids[(size_t)i * k + a]; if (id >= (unsigned)n) ++bad;
      for (int b = 0; b < k; ++b) if ((int)id == truth[(size_t)i * tk + b]) { ++hit; break; }
    }
    recall[r] = (double)hit / ((double)nq * k); if (bad) recall[r] = -bad;
    free(ids); cuvsRMMFree(res, dq, qb); cuvsRMMFree(res, dn, rb); cuvsRMMFree(res, dd, rb);
  }
  printf("{\"algo\":\"%s\",\"degree\":%d,\"inter\":%d,\"niter\":%d,\"n\":%ld,\"dim\":%d,\"nq\":%ld,"
         "\"build_s\":%.3f,\"peak_vram_mb\":%.1f,\"recall10\":%.5f,\"recall100\":%.5f,\"qps10\":%.1f,\"qps100\":%.1f,"
         "\"omp\":\"%s\"}\n", algo, degree, inter, niter, n, dim, nq, build_s, (free0 - min_free) / 1048576.0,
         recall[0], recall[1], qps[0], qps[1], getenv("OMP_NUM_THREADS") ? getenv("OMP_NUM_THREADS") : "unset");
  cuvsCagraSearchParamsDestroy(sp); cuvsCagraIndexDestroy(idx); cuvsCagraIndexParamsDestroy(ip); cuvsResourcesDestroy(res);
  return 0;
}
