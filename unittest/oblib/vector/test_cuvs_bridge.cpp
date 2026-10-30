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
#include <fstream>
#include <iterator>
#include <sys/mman.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

static std::map<void *, size_t> allocations;
static int resources = 0, params = 0, indices = 0, searches = 0;
static int graph_deleters = 0, graph_views = 0, from_args_calls = 0, build_calls = 0;
static int graph_view_mode = 0, graph_copies = 0;
static size_t largest_graph_copy = 0;
struct GraphState { std::vector<uint32_t> graph; std::vector<float> base; };
static std::map<cuvsCagraIndex_t, GraphState> graph_states;
static cuvsCagraIndex_t pending_index = nullptr;
static const uint32_t *pending_graph = nullptr;
static const float *pending_base = nullptr;
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
  assert(graph_views == 0 && graph_states.empty() && pending_index == nullptr);
  assert(failure.empty() && !pending_work);
}
extern "C" {
cuvsError_t cuvsResourcesCreate(cuvsResources_t *p)
{ if (bad("resource")) return CUVS_ERROR; *p = 1; ++resources; return CUVS_SUCCESS; }
cuvsError_t cuvsResourcesDestroy(cuvsResources_t) { --resources; return CUVS_SUCCESS; }
cuvsError_t cuvsStreamSync(cuvsResources_t)
{
  if (pending_index) {
    GraphState &state = graph_states[pending_index];
    state.graph.assign(pending_graph, pending_graph + pending_index->n * pending_index->degree);
    state.base.assign(pending_base, pending_base + pending_index->n * pending_index->dim);
    pending_index = nullptr; pending_graph = nullptr; pending_base = nullptr;
  }
  pending_work = false; return bad("sync") ? CUVS_ERROR : CUVS_SUCCESS;
}
cuvsError_t cuvsCagraIndexParamsCreate(cuvsCagraIndexParams_t *p)
{ if (bad("params")) return CUVS_ERROR; *p = new cuvsCagraIndexParams{}; ++params; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexParamsDestroy(cuvsCagraIndexParams_t p)
{ delete p; --params; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexCreate(cuvsCagraIndex_t *p)
{ if (bad("index")) return CUVS_ERROR; *p = new cuvsCagraIndex{}; ++indices; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraIndexDestroy(cuvsCagraIndex_t p)
{ assert(p != pending_index); graph_states.erase(p); delete p; --indices; return CUVS_SUCCESS; }
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
  *p = new cuvsCagraSearchParams{64, 1, SINGLE_CTA}; ++searches; return CUVS_SUCCESS;
}
cuvsError_t cuvsCagraSearchParamsDestroy(cuvsCagraSearchParams_t p)
{ delete p; --searches; return CUVS_SUCCESS; }
cuvsError_t cuvsCagraBuild(cuvsResources_t, cuvsCagraIndexParams_t p, DLManagedTensor *d, cuvsCagraIndex_t idx)
{
  ++build_calls;
  assert(p->build_algo == NN_DESCENT && p->graph_degree == 128);
  assert(p->intermediate_graph_degree == 160 && p->nn_descent_niter == 6);
  assert(d->manager_ctx == nullptr && d->deleter == nullptr);
  assert(d->dl_tensor.dtype.code == kDLFloat && d->dl_tensor.dtype.bits == 32);
  if (bad("build")) return CUVS_ERROR;
  idx->n = d->dl_tensor.shape[0]; idx->dim = d->dl_tensor.shape[1]; idx->degree = 128;
  GraphState &state = graph_states[idx]; state.graph.resize(idx->n * idx->degree);
  for (int64_t row = 0; row < idx->n; ++row) for (int64_t col = 0; col < idx->degree; ++col)
    state.graph[row * idx->degree + col] = (uint32_t)((row + col + 1) % idx->n);
  state.base.assign((const float *)d->dl_tensor.data,
                    (const float *)d->dl_tensor.data + idx->n * idx->dim);
  pending_work = true; return CUVS_SUCCESS;
}
int cudaMemcpy(void *to, const void *from, size_t n, int)
{
  assert(!pending_work);
  if (graph_views) { ++graph_copies; largest_graph_copy = std::max(largest_graph_copy, n); }
  if (bad("copy")) { return 1; }
  std::memcpy(to, from, n);
  return cudaSuccess;
}
cuvsError_t cuvsCagraSearch(cuvsResources_t, cuvsCagraSearchParams_t p, cuvsCagraIndex_t idx,
                           DLManagedTensor *q, DLManagedTensor *neighbors, DLManagedTensor *distances, cuvsFilter f)
{
  // A failed GPU call may have submitted kernels before reporting its error.
  assert(!pending_work); pending_work = true;
  if (bad("search")) return CUVS_ERROR;
  assert(f.type == NO_FILTER);
  int64_t nq = q->dl_tensor.shape[0], k = neighbors->dl_tensor.shape[1];
  assert(p->itopk_size == (size_t)std::max<int64_t>(2048, k * 4) && p->search_width == 4);
  assert(p->algo == MULTI_CTA);
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
cuvsError_t cuvsCagraIndexGetGraphDegree(cuvsCagraIndex_t p, int64_t *degree)
{ if (bad("degree")) return CUVS_ERROR; *degree = p->degree; return CUVS_SUCCESS; }
static void graph_deleter(DLManagedTensor *tensor)
{
  ++graph_deleters; --graph_views;
  std::free(tensor->dl_tensor.shape);
  std::free(tensor->dl_tensor.strides);
  std::free(tensor->dl_tensor.data);
  tensor->dl_tensor.shape = nullptr; tensor->dl_tensor.strides = nullptr;
  tensor->dl_tensor.data = nullptr; tensor->deleter = nullptr;
}
cuvsError_t cuvsCagraIndexGetGraph(cuvsCagraIndex_t p, DLManagedTensor *out)
{
  if (bad("get_graph")) return CUVS_ERROR;
  std::memset(out, 0, sizeof(*out));
  const size_t prefix = graph_view_mode == 2 ? 16 : 0;
  const size_t bytes = (size_t)p->n * (size_t)p->degree * sizeof(uint32_t);
  out->dl_tensor.data = std::malloc(prefix + bytes); assert(out->dl_tensor.data);
  out->dl_tensor.device.device_type = kDLCUDA; out->dl_tensor.device.device_id = 0;
  out->dl_tensor.ndim = 2; out->dl_tensor.dtype.code = kDLUInt; out->dl_tensor.dtype.bits = 32;
  out->dl_tensor.dtype.lanes = 1; out->dl_tensor.byte_offset = prefix;
  out->dl_tensor.shape = (int64_t *)std::malloc(2 * sizeof(int64_t)); assert(out->dl_tensor.shape);
  out->dl_tensor.shape[0] = p->n; out->dl_tensor.shape[1] = p->degree;
  out->dl_tensor.strides = nullptr;
  std::memcpy((unsigned char *)out->dl_tensor.data + prefix, graph_states.at(p).graph.data(), bytes);
  out->deleter = graph_deleter; ++graph_views;
  if (graph_view_mode == 1 || graph_view_mode == 3 || graph_view_mode == 4) {
    out->dl_tensor.strides = (int64_t *)std::malloc(2 * sizeof(int64_t));
    out->dl_tensor.strides[0] = p->degree + (graph_view_mode == 3 ? 1 : 0);
    out->dl_tensor.strides[1] = graph_view_mode == 4 ? 2 : 1;
  }
  if (graph_view_mode == 5) out->dl_tensor.ndim = 1;
  if (graph_view_mode == 6) out->dl_tensor.dtype.code = kDLFloat;
  if (graph_view_mode == 7) out->dl_tensor.dtype.bits = 16;
  if (graph_view_mode == 8) out->dl_tensor.dtype.lanes = 2;
  if (graph_view_mode == 9) out->dl_tensor.shape[0] = p->n - 1;
  if (graph_view_mode == 10) out->dl_tensor.shape[1] = p->degree - 1;
  if (graph_view_mode == 11) out->dl_tensor.device.device_type = kDLCPU;
  if (graph_view_mode == 12) out->dl_tensor.byte_offset = UINT64_MAX;
  if (graph_view_mode == 13) ((uint32_t *)out->dl_tensor.data)[0] = (uint32_t)p->n;
  if (graph_view_mode == 14) ((uint32_t *)out->dl_tensor.data)[0] = UINT32_MAX;
  if (graph_view_mode == 15) { std::free(out->dl_tensor.shape); out->dl_tensor.shape = nullptr; }
  if (graph_view_mode == 16) { std::free(out->dl_tensor.data); out->dl_tensor.data = nullptr; }
  if (bad("get_graph_partial")) return CUVS_ERROR;
  return CUVS_SUCCESS;
}
cuvsError_t cuvsCagraIndexFromArgs(cuvsResources_t, cuvsDistanceType metric,
                                   DLManagedTensor *graph, DLManagedTensor *dataset,
                                   cuvsCagraIndex_t p)
{
  ++from_args_calls;
  assert(metric == L2Expanded && graph && dataset);
  assert(graph->dl_tensor.device.device_type == kDLCPU && dataset->dl_tensor.device.device_type == kDLCPU);
  assert(graph->dl_tensor.dtype.code == kDLUInt && graph->dl_tensor.dtype.bits == 32);
  assert(dataset->dl_tensor.dtype.code == kDLFloat && dataset->dl_tensor.dtype.bits == 32);
  assert(!graph->dl_tensor.strides && !dataset->dl_tensor.strides);
  assert(!graph->manager_ctx && !graph->deleter && !dataset->manager_ctx && !dataset->deleter);
  p->n = graph->dl_tensor.shape[0]; p->degree = graph->dl_tensor.shape[1]; p->dim = dataset->dl_tensor.shape[1];
  assert(dataset->dl_tensor.shape[0] == p->n);
  pending_index = p; pending_graph = (const uint32_t *)graph->dl_tensor.data;
  pending_base = (const float *)dataset->dl_tensor.data; pending_work = true;
  return bad("from_args") ? CUVS_ERROR : CUVS_SUCCESS;
}
}
static void test_graph_snapshot();
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
  test_graph_snapshot();
  assert(seekdb_cuvs_cagra_knn(base, 1024, 8, query, 1, 10, ids) == 0); clean();
  std::puts("PASS bridge: workspace reuse/growth/shrink, overflow rejection, constructor/allocation/copy/search/stream-sync/serialize/load faults, retry, 800 concurrent calls, complete cleanup");
}

static std::vector<unsigned char> read_file(const std::string &path)
{
  std::ifstream input(path, std::ios::binary); assert(input);
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
static void write_file(const std::string &path, const std::vector<unsigned char> &data)
{
  std::ofstream output(path, std::ios::binary | std::ios::trunc); assert(output);
  output.write((const char *)data.data(), (std::streamsize)data.size()); assert(output.good());
}
static void put_u64(std::vector<unsigned char> &data, size_t offset, uint64_t value)
{
  assert(offset + 8 <= data.size());
  for (unsigned i = 0; i < 8; ++i) data[offset + i] = (unsigned char)(value >> (8 * i));
}
static void update_checksum(std::vector<unsigned char> &data)
{
  assert(data.size() >= 56);
  uint64_t sum = UINT64_C(14695981039346656037);
  for (size_t i = 0; i + 8 < data.size(); ++i) { sum ^= data[i]; sum *= UINT64_C(1099511628211); }
  put_u64(data, data.size() - 8, sum);
}
static void test_graph_snapshot()
{
  clean();
  char directory[] = "/tmp/seekdb-graph-protocol.XXXXXX";
  assert(mkdtemp(directory));
  std::string filename = std::string(directory) + "/graph";
  std::string copyname = std::string(directory) + "/copy";
  const long n = 4096, dim = 8;
  const size_t base_bytes = (size_t)n * dim * sizeof(float);
  float *base = (float *)mmap(nullptr, base_bytes, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANON, -1, 0);
  assert(base != MAP_FAILED);
  for (long i = 0; i < n * dim; ++i) base[i] = (float)(i % 997) * .125f;
  base[0] = -0.0f;
  std::vector<float> reference(base, base + n * dim);
  assert(mprotect(base, base_bytes, PROT_READ) == 0);
  void *source = seekdb_cuvs_build(base, n, dim); assert(source);
  // A varied legal graph, unrelated to query values, must survive bit-for-bit.
  // n is a power of two; an odd step gives distinct neighbors within a row.
  for (auto &entry : graph_states) {
    for (long row = 0; row < n; ++row) {
      const uint32_t step = ((uint32_t)row * 17U + 3U) | 1U;
      const uint32_t start = (uint32_t)row * 104729U + 37U;
      for (long col = 0; col < 128; ++col)
        entry.second.graph[row * 128 + col] = (start + (uint32_t)col * step) % (uint32_t)n;
    }
  }
  int builds = build_calls, previous_from_args = from_args_calls;
  int previous_deleters = graph_deleters;
  graph_copies = 0; largest_graph_copy = 0;
  assert(seekdb_cuvs_serialize_graph(source, filename.c_str()) == 0);
  assert(graph_deleters == previous_deleters + 1 && graph_views == 0);
  assert(graph_copies == 2 && largest_graph_copy <= 1024U * 1024U);
  std::vector<unsigned char> original = read_file(filename);
  assert(original.size() == 48 + (size_t)n * 128 * 4 + 8);
  assert(std::memcmp(original.data(), "SKCGRPH1", 8) == 0);
  assert(original[8] == 1 && original[12] == 48);
  // The first edge of row zero is row 37: explicit little endian on disk.
  assert(original[48] == 37 && original[49] == 0 && original[50] == 0 && original[51] == 0);
  void *restored = seekdb_cuvs_deserialize_graph(filename.c_str(), base, n, dim); assert(restored);
  assert(build_calls == builds && from_args_calls == previous_from_args + 1);
  assert(!pending_work && !pending_index);
  assert(seekdb_cuvs_serialize_graph(restored, copyname.c_str()) == 0);
  assert(read_file(copyname) == original);
  // Host graph is already freed by the API; CAGRA must have copied it. This
  // mock delays its copies until StreamSync, exposing premature buffer free.
  for (const auto &entry : graph_states) {
    assert(entry.second.base.size() == reference.size());
    assert(std::memcmp(entry.second.base.data(), reference.data(), base_bytes) == 0);
  }
  assert(std::memcmp(base, reference.data(), base_bytes) == 0);
  seekdb_cuvs_free(restored);

  // Nullable strides and explicit contiguous strides have identical bytes;
  // a nonzero byte offset also refers to the same original device graph.
  for (int mode : {1, 2}) {
    graph_view_mode = mode;
    assert(seekdb_cuvs_serialize_graph(source, copyname.c_str()) == 0);
    assert(read_file(copyname) == original && graph_views == 0);
  }
  for (int mode = 3; mode <= 16; ++mode) {
    graph_view_mode = mode; previous_deleters = graph_deleters;
    assert(seekdb_cuvs_serialize_graph(source, copyname.c_str()) != 0);
    assert(graph_views == 0 && graph_deleters == previous_deleters + 1);
  }
  graph_view_mode = 0;
  for (const char *operation : {"sync", "size", "dims", "degree", "get_graph", "get_graph_partial", "copy"}) {
    fail(operation);
    assert(seekdb_cuvs_serialize_graph(source, copyname.c_str()) != 0);
    assert(failure.empty() && graph_views == 0);
  }
  fail("copy", 1); assert(seekdb_cuvs_serialize_graph(source, copyname.c_str()) != 0);
  assert(failure.empty() && graph_views == 0);
  assert(seekdb_cuvs_serialize_graph(source, directory) != 0 && graph_views == 0);
#ifdef __linux__
  assert(seekdb_cuvs_serialize_graph(source, "/dev/full") != 0 && graph_views == 0);
#endif
  assert(seekdb_cuvs_serialize_graph(source, copyname.c_str()) == 0);
  seekdb_cuvs_free(source); clean();
  assert(seekdb_cuvs_serialize_graph(nullptr, filename.c_str()) != 0);
  assert(!seekdb_cuvs_deserialize_graph(nullptr, base, n, dim));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), nullptr, n, dim));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, -1, dim));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, n, LONG_MAX));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, LONG_MAX, dim));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, n - 1, dim));
  assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, n, dim + 1));
  assert(!seekdb_cuvs_deserialize_graph(directory, base, n, dim));
  assert(!seekdb_cuvs_deserialize_graph((std::string(directory)+"/absent").c_str(), base, n, dim));
  clean();
  previous_from_args = from_args_calls;
  // Every fixed header field, payload checksum, trailer, truncation and tail is
  // validated before any GPU constructor. A forged-valid checksum still does
  // not permit an out-of-range row offset.
  for (size_t field : {size_t(0), size_t(8), size_t(12), size_t(16), size_t(24), size_t(32), size_t(40)}) {
    auto malformed = original; malformed[field] ^= 1; update_checksum(malformed);
    write_file(copyname, malformed);
    assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
  }
  for (size_t length : {size_t(0), size_t(47), size_t(48), original.size() - 8, original.size() - 1}) {
    write_file(copyname, std::vector<unsigned char>(original.begin(), original.begin() + length));
    assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
  }
  {
    auto malformed = original; malformed.push_back(0);
    write_file(copyname, malformed); assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
    malformed = original; malformed[48] = 2; // valid ID but checksum must catch it
    write_file(copyname, malformed); assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
    malformed = original; malformed.back() ^= 1;
    write_file(copyname, malformed); assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
    malformed = original;
    for (size_t i = 0; i < 4; ++i) malformed[48 + i] = (unsigned char)((uint32_t)n >> (8 * i));
    update_checksum(malformed); write_file(copyname, malformed);
    assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
    malformed = original; put_u64(malformed, 32, UINT64_MAX); update_checksum(malformed); write_file(copyname, malformed);
    assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
    malformed = original; put_u64(malformed, 40, UINT64_MAX); update_checksum(malformed); write_file(copyname, malformed);
    assert(!seekdb_cuvs_deserialize_graph(copyname.c_str(), base, n, dim)); clean();
  }
  assert(from_args_calls == previous_from_args);
  for (const char *operation : {"resource", "params", "index", "from_args", "sync", "size", "dims", "degree"}) {
    fail(operation); assert(!seekdb_cuvs_deserialize_graph(filename.c_str(), base, n, dim)); clean();
  }
  restored = seekdb_cuvs_deserialize_graph(filename.c_str(), base, n, dim); assert(restored);
  assert(build_calls == builds); // No fresh CAGRA build anywhere in restore.
  assert(std::memcmp(base, reference.data(), base_bytes) == 0);
  assert(munmap(base, base_bytes) == 0); // Caller can release read-only base immediately.
  unsigned ids[10]; float distances[10], query[8] = {};
  assert(seekdb_cuvs_search(restored, query, 1, 10, ids, distances) == 0);
  assert(seekdb_cuvs_serialize_graph(restored, copyname.c_str()) == 0);
  assert(read_file(copyname) == original);
  seekdb_cuvs_free(restored); clean();
  assert(unlink(filename.c_str()) == 0); assert(unlink(copyname.c_str()) == 0); assert(rmdir(directory) == 0);
  std::puts("PASS graph-only protocol: exact byte roundtrip, bounded D2H chunks, read-only FP32 base, no rebuild, offset/stride validation, deleter, corruption/truncation/tail/ID rejection, async-copy lifetime and all C API failure cleanup");
}
