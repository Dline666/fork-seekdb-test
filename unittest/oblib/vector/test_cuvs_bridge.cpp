// Host-only ABI mocks verify ownership and errors, NOT ANN quality or ROCm ABI.
#include "seekdb_cuvs_bridge.h"
#include <cuvs/neighbors/cagra.h>
#include <cuda_runtime.h>
#include <cassert>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <thread>
#include <vector>

static std::map<void *, size_t> allocations;
static int resources = 0, params = 0, indices = 0, searches = 0;
static int allocation_calls = 0, search_param_calls = 0;
static std::string failure;
static int skip = 0;
static bool pending_work = false;
static bool bad(const char *operation)
{
  if (failure != operation) return false;
  if (skip > 0) { --skip; return false; }
  failure.clear(); return true;
}
static void fail(const char *operation, int after = 0) { failure = operation; skip = after; }
static void clean()
{
  assert(allocations.empty());
  assert(resources == 0 && params == 0 && indices == 0 && searches == 0);
  assert(failure.empty() && !pending_work);
}
extern "C" {
cuvsError_t cuvsResourcesCreate(cuvsResources_t *p)
{ if (bad("resource")) return CUVS_ERROR; *p = 1; ++resources; return CUVS_SUCCESS; }
cuvsError_t cuvsResourcesDestroy(cuvsResources_t) { --resources; return CUVS_SUCCESS; }
cuvsError_t cuvsStreamSync(cuvsResources_t)
{ pending_work = false; return bad("sync") ? CUVS_ERROR : CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexParamsCreate(cuvsCagraIndexParams_t *p)
{ if (bad("params")) return CUVS_ERROR; *p = new cuvsCagraIndexParams{}; ++params; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexParamsDestroy(cuvsCagraIndexParams_t p)
{ delete p; --params; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexCreate(cuvsCagraIndex_t *p)
{ if (bad("index")) return CUVS_ERROR; *p = new cuvsCagraIndex{}; ++indices; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexDestroy(cuvsCagraIndex_t p) { delete p; --indices; return CUVS_SUCCESS; }
const char *cuvsGetLastErrorText(void) { return "injected test failure"; }
cuvsError_t cuvsRMMAlloc(cuvsResources_t, void **p, size_t n)
{
  ++allocation_calls;
  if (bad("alloc")) return CUVS_ERROR;
  *p = std::malloc(n); assert(*p); allocations[*p] = n; return CUVS_SUCCESS;
}
cuvsError_t cuvsRMMFree(cuvsResources_t, void *p, size_t n)
{ assert(!pending_work); assert(allocations.at(p) == n); allocations.erase(p); std::free(p); return CUVS_SUCCESS; }
cuvsError_t cuvsCagraSearchParamsCreate(cuvsCagraSearchParams_t *p)
{
  ++search_param_calls;
  if (bad("search_params")) return CUVS_ERROR;
  *p = new cuvsCagraSearchParams{64, 1}; ++searches; return CUVS_SUCCESS;
}
cuvsError_t cuvsCagraSearchParamsDestroy(cuvsCagraSearchParams_t p)
{ delete p; --searches; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraBuild(cuvsResources_t, cuvsCagraIndexParams_t p, DLManagedTensor *d, cuvsCagraIndex_t idx)
{
  assert(p->build_algo == NN_DESCENT && p->graph_degree == 128);
  assert(p->intermediate_graph_degree == 256 && p->nn_descent_niter == 100);
  assert(d->manager_ctx == nullptr && d->deleter == nullptr);
  assert(d->dl_tensor.dtype.code == kDLFloat && d->dl_tensor.dtype.bits == 32);
  if (bad("build")) return CUVS_ERROR;
  idx->n = d->dl_tensor.shape[0]; idx->dim = d->dl_tensor.shape[1]; pending_work = true; return CUVS_SUCCESS;
}
int cudaMemcpy(void *to, const void *from, size_t n, int)
{ assert(!pending_work); if (bad("copy")) return 1; std::memcpy(to, from, n); return cudaSuccess; }
cuvsError_t cuvsCagraSearch(cuvsResources_t, cuvsCagraSearchParams_t p, cuvsCagraIndex_t idx,
                           DLManagedTensor *q, DLManagedTensor *neighbors, DLManagedTensor *distances, cuvsFilter f)
{
  // A failed GPU call may have submitted kernels before reporting its error.
  assert(!pending_work); pending_work = true;
  if (bad("search")) return CUVS_ERROR;
  assert(f.type == NO_FILTER);
  int64_t nq = q->dl_tensor.shape[0], k = neighbors->dl_tensor.shape[1];
  assert(p->itopk_size == (size_t)std::max<int64_t>(512, k * 4) && p->search_width == 4);
  assert(q->dl_tensor.shape[1] == idx->dim && k <= idx->n);
  for (int64_t row = 0; row < nq; ++row) {
    for (int64_t column = 0; column < k; ++column) {
      static_cast<unsigned *>(neighbors->dl_tensor.data)[row * k + column] = (unsigned)column;
      static_cast<float *>(distances->dl_tensor.data)[row * k + column] =
          static_cast<float *>(q->dl_tensor.data)[row * idx->dim] + (float)column;
    }
  }
  return CUVS_SUCCESS;
}
cuvsError_t cuvsCagraSerialize(cuvsResources_t, const char *, cuvsCagraIndex_t, bool dataset)
{ assert(dataset); return bad("serialize") ? CUVS_ERROR : CUVS_SUCCESS; }
cuvsError_t cuvsCagraDeserialize(cuvsResources_t, const char *, cuvsCagraIndex_t p)
{ if (bad("deserialize")) return CUVS_ERROR; p->n = 1024; p->dim = 8; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexGetSize(cuvsCagraIndex_t p, int64_t *n)
{ if (bad("size")) return CUVS_ERROR; *n = p->n; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexGetDims(cuvsCagraIndex_t p, int64_t *dim)
{ if (bad("dims")) return CUVS_ERROR; *dim = p->dim; return CUVS_SUCCESS; }
}
int main()
{
  float base[1024 * 8] = {};
  float query[16 * 8] = {};
  unsigned ids[16 * 256] = {};
  float distances[16 * 256] = {};
  assert(!seekdb_cuvs_build(nullptr, 1024, 8));
  assert(!seekdb_cuvs_build(base, 0, 8));
  assert(!seekdb_cuvs_build(base, 1024, -1));
  assert(!seekdb_cuvs_build(base, LONG_MAX, LONG_MAX));
  assert(!seekdb_cuvs_deserialize(nullptr, 1024, 8));
  assert(!seekdb_cuvs_deserialize("snapshot", -1, 8));
  assert(seekdb_cuvs_search(nullptr, query, 1, 10, ids, distances) != 0);
  seekdb_cuvs_free(nullptr);
  for (const char *operation : {"resource", "params", "index", "build", "sync"}) {
    fail(operation); assert(!seekdb_cuvs_build(base, 1024, 8)); clean();
  }
  void *h = seekdb_cuvs_build(base, 1024, 8); assert(h);
  query[0] = 23;
  assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  assert(ids[9] == 9 && distances[9] == 32);
  int allocations_before = allocation_calls, params_before = search_param_calls;
  for (int i = 0; i < 1000; ++i) assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  assert(allocations_before == allocation_calls && params_before == search_param_calls);
  // Grow both nq and k, then shrink k: parameters must return to the V5 value.
  assert(seekdb_cuvs_search(h, query, 16, 256, ids, distances) == 0);
  assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  assert(allocations.size() == 3);
  allocations_before = allocation_calls;
  assert(seekdb_cuvs_search(h, query, 0, 10, ids, distances) != 0);
  assert(seekdb_cuvs_search(h, query, 1, 1025, ids, distances) != 0);
  assert(seekdb_cuvs_search(h, nullptr, 1, 10, ids, distances) != 0);
  assert(seekdb_cuvs_search(h, query, LONG_MAX, LONG_MAX, ids, distances) != 0);
  assert(seekdb_cuvs_search(h, query, LONG_MAX, 10, ids, distances) != 0);
  assert(seekdb_cuvs_search(h, query, 1, 10, nullptr, distances) != 0);
  assert(allocations_before == allocation_calls);
  for (int copy = 0; copy < 3; ++copy) {
    fail("copy", copy); assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 13 + (copy ? copy + 2 : 0));
    assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  }
  fail("search"); assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 15);
  fail("sync"); assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 18);
  assert(seekdb_cuvs_search(h, query, 1, 10, ids, nullptr) == 0);
  fail("serialize"); assert(seekdb_cuvs_serialize(h, "snapshot") != 0);
  fail("sync"); assert(seekdb_cuvs_serialize(h, "snapshot") != 0);
  assert(seekdb_cuvs_serialize(h, "snapshot") == 0);
  // Same-handle concurrent calls must not overwrite each other's workspace.
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) threads.emplace_back([h, t] {
    float q[8] = {(float)t}; unsigned i[10]; float d[10];
    for (int repetition = 0; repetition < 100; ++repetition) {
      assert(seekdb_cuvs_search(h, q, 1, 10, i, d) == 0);
      assert(i[9] == 9 && d[9] == (float)t + 9);
    }
  });
  for (auto &thread : threads) thread.join();
  seekdb_cuvs_free(h); clean();
  for (int buffer = 0; buffer < 3; ++buffer) {
    h = seekdb_cuvs_build(base, 1024, 8); assert(h);
    fail("alloc", buffer); assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 10 + buffer);
    assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
    fail("alloc", buffer); assert(seekdb_cuvs_search(h, query, 16, 256, ids, distances) == 10 + buffer);
    assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
    assert(seekdb_cuvs_search(h, query, 16, 256, ids, distances) == 0);
    seekdb_cuvs_free(h); clean();
  }
  h = seekdb_cuvs_build(base, 1024, 8);
  fail("search_params"); assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 14);
  assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  seekdb_cuvs_free(h); clean();
  for (const char *operation : {"resource", "params", "index", "deserialize", "size", "dims", "sync"}) {
    fail(operation); assert(!seekdb_cuvs_deserialize("snapshot", 1024, 8)); clean();
  }
  assert(!seekdb_cuvs_deserialize("snapshot", 1023, 8)); clean();
  assert(!seekdb_cuvs_deserialize("snapshot", 1024, 9)); clean();
  h = seekdb_cuvs_deserialize("snapshot", 1024, 8); assert(h);
  assert(seekdb_cuvs_search(h, query, 1, 10, ids, distances) == 0);
  seekdb_cuvs_free(h); clean();
  assert(seekdb_cuvs_cagra_knn(base, 1024, 8, query, 1, 10, ids) == 0); clean();
  std::puts("PASS bridge: workspace reuse/growth/shrink, overflow rejection, constructor/allocation/copy/search/stream-sync/serialize/load faults, retry, 800 concurrent calls, complete cleanup");
}
