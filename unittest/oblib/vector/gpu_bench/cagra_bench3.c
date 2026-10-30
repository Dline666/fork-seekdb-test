// Paired GPU CAGRA build/search benchmark on the fixed hipVS C API.
// usage: cagra_bench3 base query truth algo degree inter niter nq
// env ITOPKS="512,768,1024" (search itopk floors), WIDTH (search_width, default 4)
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#include <hip/hip_runtime.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static double now(void) { struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t); return t.tv_sec + t.tv_nsec * 1e-9; }
static float *read_vecs(const char *p, long *n, int *d) {
  FILE *f = fopen(p, "rb"); if (!f) { perror(p); exit(1); }
  fseek(f, 0, SEEK_END); long bytes = ftell(f); fseek(f, 0, SEEK_SET);
  int dim; if (fread(&dim, 4, 1, f) != 1) exit(1); fseek(f, 0, SEEK_SET);
  long rows = bytes / (4L * (dim + 1)); float *v = malloc((size_t)rows * dim * 4); float *row = malloc(4 * (dim + 1));
  for (long i = 0; i < rows; ++i) { if (fread(row, 4, dim + 1, f) != (size_t)dim + 1) exit(1); memcpy(v + (size_t)i * dim, row + 1, 4 * dim); }
  free(row); fclose(f); *n = rows; *d = dim; return v;
}
static DLManagedTensor tensor(void *data, int64_t *shape, int dev, int code) {
  DLManagedTensor t; memset(&t, 0, sizeof(t));
  t.dl_tensor.data = data; t.dl_tensor.device.device_type = (DLDeviceType)dev; t.dl_tensor.ndim = 2;
  t.dl_tensor.dtype.code = (uint8_t)code; t.dl_tensor.dtype.bits = 32; t.dl_tensor.dtype.lanes = 1;
  t.dl_tensor.shape = shape; return t;
}
#define CK(x) do { if ((x) != CUVS_SUCCESS) { fprintf(stderr, "%s failed: %s\n", #x, cuvsGetLastErrorText()); exit(2); } } while (0)

int main(int argc, char **argv) {
  if (argc < 9) { fprintf(stderr, "usage\n"); return 1; }
  long n, nq_all, tn; int dim, qd, tk;
  float *base = read_vecs(argv[1], &n, &dim); float *qs = read_vecs(argv[2], &nq_all, &qd);
  int *truth = (int *)read_vecs(argv[3], &tn, &tk);
  const char *algo = argv[4]; int degree = atoi(argv[5]), inter = atoi(argv[6]), niter = atoi(argv[7]);
  long nq = atol(argv[8]); if (nq > nq_all) nq = nq_all;
  const char *itopks = getenv("ITOPKS") ? getenv("ITOPKS") : "512";
  int width = getenv("WIDTH") ? atoi(getenv("WIDTH")) : 4;
  hipSetDevice(0);
  cuvsResources_t res; CK(cuvsResourcesCreate(&res));
  cuvsCagraIndexParams_t ip; CK(cuvsCagraIndexParamsCreate(&ip));
  ip->build_algo = !strcmp(algo, "iter") ? ITERATIVE_CAGRA_SEARCH : NN_DESCENT;
  ip->graph_degree = degree; ip->intermediate_graph_degree = inter; ip->nn_descent_niter = niter;
  cuvsCagraIndex_t idx; CK(cuvsCagraIndexCreate(&idx));
  int64_t ds[2] = {n, dim}; DLManagedTensor dset = tensor(base, ds, kDLCPU, kDLFloat);
  double t0 = now();
  CK(cuvsCagraBuild(res, ip, &dset, idx)); CK(cuvsStreamSync(res));
  double build_s = now() - t0;
  cuvsCagraSearchParams_t sp; CK(cuvsCagraSearchParamsCreate(&sp)); size_t default_itopk = sp->itopk_size;
  printf("{\"algo\":\"%s\",\"degree\":%d,\"inter\":%d,\"niter\":%d,\"dim\":%d,\"nq\":%ld,\"build_s\":%.3f,\"width\":%d,\"search\":[",
         algo, degree, inter, niter, dim, nq, build_s, width);
  char buf[256]; strncpy(buf, itopks, sizeof(buf) - 1); buf[sizeof(buf) - 1] = 0;
  int first = 1;
  for (char *tok = strtok(buf, ","); tok; tok = strtok(NULL, ",")) {
    size_t floor_itopk = (size_t)atol(tok);
    printf("%s{\"itopk_floor\":%zu", first ? "" : ",", floor_itopk); first = 0;
    int ks[2] = {10, 100};
    for (int r = 0; r < 2; ++r) {
      int k = ks[r]; size_t rb = (size_t)nq * k * 4;
      float *dq; unsigned *dn; float *dd;
      CK(cuvsRMMAlloc(res, (void **)&dq, (size_t)dim * 4)); CK(cuvsRMMAlloc(res, (void **)&dn, (size_t)k * 4)); CK(cuvsRMMAlloc(res, (void **)&dd, (size_t)k * 4));
      size_t need = (size_t)k * 4; if (need < floor_itopk) need = floor_itopk;
      sp->itopk_size = default_itopk > need ? default_itopk : need; sp->search_width = width;
      unsigned *ids = malloc(rb); int64_t q1[2] = {1, dim}, r1[2] = {1, k};
      double s0 = now();
      for (long i = 0; i < nq; ++i) {
        hipMemcpy(dq, qs + (size_t)i * dim, dim * 4, hipMemcpyHostToDevice);
        DLManagedTensor qt = tensor(dq, q1, kDLCUDA, kDLFloat), nt = tensor(dn, r1, kDLCUDA, kDLUInt), dt = tensor(dd, r1, kDLCUDA, kDLFloat);
        cuvsFilter f; f.type = NO_FILTER; f.addr = 0;
        CK(cuvsCagraSearch(res, sp, idx, &qt, &nt, &dt, f)); CK(cuvsStreamSync(res));
        hipMemcpy(ids + (size_t)i * k, dn, k * 4, hipMemcpyDeviceToHost);
      }
      double qps = nq / (now() - s0); long hit = 0;
      for (long i = 0; i < nq; ++i) for (int a = 0; a < k; ++a) for (int b = 0; b < k; ++b)
        if ((int)ids[(size_t)i * k + a] == truth[(size_t)i * tk + b]) { ++hit; break; }
      printf(",\"r%d\":%.5f,\"qps%d\":%.1f", k, (double)hit / ((double)nq * k), k, qps);
      free(ids); cuvsRMMFree(res, dq, (size_t)dim * 4); cuvsRMMFree(res, dn, (size_t)k * 4); cuvsRMMFree(res, dd, (size_t)k * 4);
    }
    printf("}");
  }
  printf("]}\n");
  return 0;
}
