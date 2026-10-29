#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* seekdb <-> hipVS bridge: CAGRA with per-index reusable search workspace. */
#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>

typedef struct {
  cuvsResources_t res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t idx;
  cuvsCagraSearchParams_t sp;
  size_t default_itopk;
  long n, dim;
  void *queries, *neighbors, *distances;
  size_t query_bytes, neighbor_bytes, distance_bytes;
  pthread_mutex_t mutex;
} sk_cuvs_index;

static int tensor_bytes(long rows, long columns, size_t element, size_t *bytes)
{
  if (rows <= 0 || columns <= 0 || (uint64_t)columns > SIZE_MAX / element ||
      (uint64_t)rows > SIZE_MAX / element / (uint64_t)columns) return 1;
  *bytes = (size_t)rows * (size_t)columns * element;
  return 0;
}

static sk_cuvs_index *create_index(void)
{
  sk_cuvs_index *h = (sk_cuvs_index *)calloc(1, sizeof(*h));
  if (!h) return NULL;
  if (pthread_mutex_init(&h->mutex, NULL) != 0) { free(h); return NULL; }
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS ||
      cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS ||
      cuvsCagraIndexCreate(&h->idx) != CUVS_SUCCESS) {
    seekdb_cuvs_free(h);
    return NULL;
  }
  return h;
}

static DLManagedTensor tensor(void *data, int64_t *shape, int device, int code)
{
  DLManagedTensor result;
  memset(&result, 0, sizeof(result));
  result.dl_tensor.data = data;
  result.dl_tensor.device.device_type = (DLDeviceType)device;
  result.dl_tensor.device.device_id = 0;
  result.dl_tensor.ndim = 2;
  result.dl_tensor.dtype.code = (uint8_t)code;
  result.dl_tensor.dtype.bits = 32;
  result.dl_tensor.dtype.lanes = 1;
  result.dl_tensor.shape = shape;
  return result;
}

void *seekdb_cuvs_build(const float *base, long n, long dim)
{
  size_t bytes;
  if (!base || (uint64_t)n > UINT32_MAX || tensor_bytes(n, dim, sizeof(float), &bytes)) return NULL;
  sk_cuvs_index *h = create_index();
  if (!h) return NULL;
  int64_t shape[2] = {n, dim};
  DLManagedTensor dataset = tensor((void *)base, shape, kDLCPU, kDLFloat);
  // FP32 NN_DESCENT CAGRA, final graph degree 128 (unchanged from V5).
  // The fixed hipVS NN-descent never meets its termination threshold on
  // 250K inputs, so V5's niter=100 always ran 100 rounds; Recall saturates
  // after a few rounds. Measured on gfx1100 (250K, 128-D and 256-D sets):
  // intermediate 160 / 6 rounds builds the graph in ~3.3 s versus ~18 s,
  // with Recall@10/@100 equal to or above the 256/100 graph.
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = 128;
  h->ip->intermediate_graph_degree = 160;
  h->ip->nn_descent_niter = 6;
  if (cuvsCagraBuild(h->res, h->ip, &dataset, h->idx) != CUVS_SUCCESS ||
      cuvsStreamSync(h->res) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
    seekdb_cuvs_free(h);
    return NULL;
  }
  h->n = n; h->dim = dim;
  return h;
}

// Allocate before releasing the previous buffer. A failed growth leaves the
// handle usable; capacities always describe the actual allocation, not a query.
static int reserve_buffer(sk_cuvs_index *h, void **buffer, size_t *capacity, size_t required)
{
  if (*capacity >= required) return 0;
  void *replacement = NULL;
  if (cuvsRMMAlloc(h->res, &replacement, required) != CUVS_SUCCESS) return 1;
  if (*buffer) cuvsRMMFree(h->res, *buffer, *capacity);
  *buffer = replacement;
  *capacity = required;
  return 0;
}

int seekdb_cuvs_search(void *handle, const float *query, long nq, long topk,
                       unsigned int *out_ids, float *out_dist)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  size_t query_bytes, result_bytes;
  if (!h || !query || !out_ids || topk > h->n ||
      tensor_bytes(nq, h->dim, sizeof(float), &query_bytes) ||
      tensor_bytes(nq, topk, sizeof(unsigned int), &result_bytes) ||
      (uint64_t)topk > SIZE_MAX / 4) return 1;
  if (pthread_mutex_lock(&h->mutex) != 0) return 1;
  int rc = 0;
  if (reserve_buffer(h, &h->queries, &h->query_bytes, query_bytes)) { rc = 10; goto done; }
  if (reserve_buffer(h, &h->neighbors, &h->neighbor_bytes, result_bytes)) { rc = 11; goto done; }
  if (reserve_buffer(h, &h->distances, &h->distance_bytes, result_bytes)) { rc = 12; goto done; }
  if (!h->sp) {
    if (cuvsCagraSearchParamsCreate(&h->sp) != CUVS_SUCCESS) { rc = 14; goto done; }
    h->default_itopk = h->sp->itopk_size;
  }
  if (cudaMemcpy(h->queries, query, query_bytes, cudaMemcpyDefault) != cudaSuccess) { rc = 13; goto done; }
  {
    int64_t qs[2] = {nq, h->dim}, rs[2] = {nq, topk};
    DLManagedTensor qt = tensor(h->queries, qs, kDLCUDA, kDLFloat);
    DLManagedTensor nt = tensor(h->neighbors, rs, kDLCUDA, kDLUInt);
    DLManagedTensor dt = tensor(h->distances, rs, kDLCUDA, kDLFloat);
    size_t required_itopk = (size_t)topk * 4;
    if (required_itopk < 512) required_itopk = 512;
    // Reset from the library default on EVERY request, including k decreasing.
    h->sp->itopk_size = h->default_itopk > required_itopk ? h->default_itopk : required_itopk;
    h->sp->search_width = 4;
    cuvsFilter filter; filter.type = NO_FILTER; filter.addr = (uintptr_t)0;
    cuvsError_t search_status = cuvsCagraSearch(h->res, h->sp, h->idx, &qt, &nt, &dt, filter);
    // Cached buffers survive this call. Drain the resource stream even when a
    // failed search may have queued work, before reading or reusing its buffers.
    cuvsError_t sync_status = cuvsStreamSync(h->res);
    if (search_status != CUVS_SUCCESS) { rc = 15; goto done; }
    if (sync_status != CUVS_SUCCESS) { rc = 18; goto done; }
    if (cudaMemcpy(out_ids, h->neighbors, result_bytes, cudaMemcpyDefault) != cudaSuccess) { rc = 16; goto done; }
    if (out_dist && cudaMemcpy(out_dist, h->distances, result_bytes, cudaMemcpyDefault) != cudaSuccess) { rc = 17; goto done; }
  }
done:
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  pthread_mutex_unlock(&h->mutex);
  return rc;
}

void seekdb_cuvs_free(void *handle)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  if (!h) return;
  // Ownership must exclude concurrent free, as with the original bridge API.
  if (h->res) cuvsStreamSync(h->res);
  if (h->queries) cuvsRMMFree(h->res, h->queries, h->query_bytes);
  if (h->neighbors) cuvsRMMFree(h->res, h->neighbors, h->neighbor_bytes);
  if (h->distances) cuvsRMMFree(h->res, h->distances, h->distance_bytes);
  if (h->sp) cuvsCagraSearchParamsDestroy(h->sp);
  if (h->idx) cuvsCagraIndexDestroy(h->idx);
  if (h->ip) cuvsCagraIndexParamsDestroy(h->ip);
  if (h->res) cuvsResourcesDestroy(h->res);
  pthread_mutex_destroy(&h->mutex);
  free(h);
}

int seekdb_cuvs_serialize(void *handle, const char *filename)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  if (!h || !filename || pthread_mutex_lock(&h->mutex) != 0) return 1;
  int rc = cuvsCagraSerialize(h->res, filename, h->idx, true) == CUVS_SUCCESS ? 0 : 1;
  if (!rc && cuvsStreamSync(h->res) != CUVS_SUCCESS) rc = 1;
  if (rc) fprintf(stderr, "[seekdb_cuvs] serialize failed: %s\n", cuvsGetLastErrorText());
  pthread_mutex_unlock(&h->mutex);
  return rc;
}

void *seekdb_cuvs_deserialize(const char *filename, long n, long dim)
{
  size_t bytes;
  if (!filename || (uint64_t)n > UINT32_MAX || tensor_bytes(n, dim, sizeof(float), &bytes)) return NULL;
  sk_cuvs_index *h = create_index();
  if (!h) return NULL;
  int64_t actual_n = 0, actual_dim = 0;
  if (cuvsCagraDeserialize(h->res, filename, h->idx) != CUVS_SUCCESS ||
      cuvsCagraIndexGetSize(h->idx, &actual_n) != CUVS_SUCCESS ||
      cuvsCagraIndexGetDims(h->idx, &actual_dim) != CUVS_SUCCESS ||
      actual_n != n || actual_dim != dim || cuvsStreamSync(h->res) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] deserialize failed or snapshot shape mismatch: %s\n", cuvsGetLastErrorText());
    seekdb_cuvs_free(h);
    return NULL;
  }
  h->n = n; h->dim = dim;
  return h;
}

/* Graph-only format: 48-byte little-endian header followed by unchanged
 * uint32 graph edges and an 8-byte FNV-1a checksum over header + payload.
 * The adapter persists the FP32 base once, outside this graph file.
 * Version 1 is intentionally independent of the legacy hipVS snapshot format. */
#define SK_CUVS_GRAPH_HEADER_BYTES 48
#define SK_CUVS_GRAPH_TRAILER_BYTES 8
#define SK_CUVS_GRAPH_CHUNK_BYTES (1024U * 1024U)
static const unsigned char sk_graph_magic[8] = {'S', 'K', 'C', 'G', 'R', 'P', 'H', '1'};

static void sk_graph_put_u32(unsigned char *to, uint32_t value)
{
  for (unsigned i = 0; i < 4; ++i) to[i] = (unsigned char)(value >> (8 * i));
}

static void sk_graph_put_u64(unsigned char *to, uint64_t value)
{
  for (unsigned i = 0; i < 8; ++i) to[i] = (unsigned char)(value >> (8 * i));
}

static uint32_t sk_graph_get_u32(const unsigned char *from)
{
  uint32_t value = 0;
  for (unsigned i = 0; i < 4; ++i) value |= (uint32_t)from[i] << (8 * i);
  return value;
}

static uint64_t sk_graph_get_u64(const unsigned char *from)
{
  uint64_t value = 0;
  for (unsigned i = 0; i < 8; ++i) value |= (uint64_t)from[i] << (8 * i);
  return value;
}

static uint64_t sk_graph_checksum(uint64_t sum, const unsigned char *data, size_t bytes)
{
  for (size_t i = 0; i < bytes; ++i) {
    sum ^= data[i];
    sum *= UINT64_C(1099511628211);
  }
  return sum;
}

static int sk_graph_payload_bytes(uint64_t n, uint64_t degree, size_t *bytes)
{
  if (n == 0 || n > UINT32_MAX || degree == 0 || degree > n ||
      degree > SIZE_MAX / sizeof(uint32_t) || n > SIZE_MAX / sizeof(uint32_t) / degree ||
      n > (INT64_MAX - SK_CUVS_GRAPH_HEADER_BYTES - SK_CUVS_GRAPH_TRAILER_BYTES) /
          sizeof(uint32_t) / degree) return 1;
  *bytes = (size_t)n * (size_t)degree * sizeof(uint32_t);
  return 0;
}

int seekdb_cuvs_serialize_graph(void *handle, const char *filename)
{
  sk_cuvs_index *h = (sk_cuvs_index *)handle;
  if (!h || !filename || pthread_mutex_lock(&h->mutex) != 0) return 1;
  int rc = 1;
  FILE *file = NULL;
  unsigned char *chunk = NULL;
  DLManagedTensor graph;
  memset(&graph, 0, sizeof(graph));
  int64_t actual_n = 0, actual_dim = 0, degree = 0;
  size_t payload_bytes = 0, chunk_bytes = 0;
  uintptr_t source_address = 0;
  unsigned char header[SK_CUVS_GRAPH_HEADER_BYTES] = {0};
  uint64_t checksum = UINT64_C(14695981039346656037);
  if (cuvsStreamSync(h->res) != CUVS_SUCCESS ||
      cuvsCagraIndexGetSize(h->idx, &actual_n) != CUVS_SUCCESS ||
      cuvsCagraIndexGetDims(h->idx, &actual_dim) != CUVS_SUCCESS ||
      cuvsCagraIndexGetGraphDegree(h->idx, &degree) != CUVS_SUCCESS ||
      actual_n != h->n || actual_dim != h->dim || degree <= 0 ||
      sk_graph_payload_bytes((uint64_t)actual_n, (uint64_t)degree, &payload_bytes)) goto done;
  /* GetGraph allocates only shape/stride metadata, not the device graph.
   * Its supplied deleter must run even when a subsequent step fails. */
  if (cuvsCagraIndexGetGraph(h->idx, &graph) != CUVS_SUCCESS) goto done;
  if (!graph.dl_tensor.data || !graph.dl_tensor.shape || graph.dl_tensor.ndim != 2 ||
      graph.dl_tensor.device.device_type != kDLCUDA ||
      graph.dl_tensor.dtype.code != kDLUInt || graph.dl_tensor.dtype.bits != 32 ||
      graph.dl_tensor.dtype.lanes != 1 || graph.dl_tensor.shape[0] != actual_n ||
      graph.dl_tensor.shape[1] != degree ||
      (graph.dl_tensor.strides &&
       (graph.dl_tensor.strides[0] != degree || graph.dl_tensor.strides[1] != 1))) goto done;
  /* Non-contiguous views are explicitly rejected. Contiguous views with a
   * byte offset are supported without ever dereferencing device memory here. */
  source_address = (uintptr_t)graph.dl_tensor.data;
  if (graph.dl_tensor.byte_offset > UINTPTR_MAX - source_address) goto done;
  source_address += (uintptr_t)graph.dl_tensor.byte_offset;
  if (payload_bytes > UINTPTR_MAX - source_address) goto done;
  chunk_bytes = payload_bytes < SK_CUVS_GRAPH_CHUNK_BYTES ? payload_bytes : SK_CUVS_GRAPH_CHUNK_BYTES;
  chunk = (unsigned char *)malloc(chunk_bytes);
  if (!chunk) goto done;
  file = fopen(filename, "wb");
  if (!file) goto done;
  memcpy(header, sk_graph_magic, sizeof(sk_graph_magic));
  sk_graph_put_u32(header + 8, 1);
  sk_graph_put_u32(header + 12, SK_CUVS_GRAPH_HEADER_BYTES);
  sk_graph_put_u64(header + 16, (uint64_t)h->n);
  sk_graph_put_u64(header + 24, (uint64_t)h->dim);
  sk_graph_put_u64(header + 32, (uint64_t)degree);
  sk_graph_put_u64(header + 40, (uint64_t)payload_bytes);
  checksum = sk_graph_checksum(checksum, header, sizeof(header));
  if (fwrite(header, 1, sizeof(header), file) != sizeof(header)) goto done;
  for (size_t offset = 0; offset < payload_bytes;) {
    const size_t remaining = payload_bytes - offset;
    const size_t bytes = remaining < chunk_bytes ? remaining : chunk_bytes;
    if (cudaMemcpy(chunk, (const void *)(source_address + offset), bytes,
                   cudaMemcpyDefault) != cudaSuccess) goto done;
    for (size_t i = 0; i < bytes; i += sizeof(uint32_t)) {
      uint32_t neighbor;
      memcpy(&neighbor, chunk + i, sizeof(neighbor));
      if ((uint64_t)neighbor >= (uint64_t)h->n) goto done;
      sk_graph_put_u32(chunk + i, neighbor);
    }
    checksum = sk_graph_checksum(checksum, chunk, bytes);
    if (fwrite(chunk, 1, bytes, file) != bytes) goto done;
    offset += bytes;
  }
  {
    unsigned char trailer[SK_CUVS_GRAPH_TRAILER_BYTES];
    sk_graph_put_u64(trailer, checksum);
    if (fwrite(trailer, 1, sizeof(trailer), file) != sizeof(trailer) || fflush(file) != 0) goto done;
  }
  rc = 0;
done:
  if (file && fclose(file) != 0) rc = 1;
  free(chunk);
  if (graph.deleter) graph.deleter(&graph);
  pthread_mutex_unlock(&h->mutex);
  if (rc) fprintf(stderr, "[seekdb_cuvs] graph-only serialize failed\n");
  return rc;
}

void *seekdb_cuvs_deserialize_graph(const char *filename, const float *base, long n, long dim)
{
  size_t dataset_bytes = 0, payload_bytes = 0;
  if (!filename || !base || (uint64_t)n > UINT32_MAX ||
      tensor_bytes(n, dim, sizeof(float), &dataset_bytes)) return NULL;
  FILE *file = fopen(filename, "rb");
  if (!file) return NULL;
  sk_cuvs_index *h = NULL;
  uint32_t *host_graph = NULL;
  unsigned char header[SK_CUVS_GRAPH_HEADER_BYTES];
  unsigned char trailer[SK_CUVS_GRAPH_TRAILER_BYTES];
  uint64_t degree = 0, stored_bytes = 0, checksum = UINT64_C(14695981039346656037);
  off_t file_bytes = 0;
  int success = 0;
  if (sizeof(off_t) < 8 || fseeko(file, 0, SEEK_END) != 0 || (file_bytes = ftello(file)) < 0 ||
      fseeko(file, 0, SEEK_SET) != 0 || fread(header, 1, sizeof(header), file) != sizeof(header)) goto done;
  if (memcmp(header, sk_graph_magic, sizeof(sk_graph_magic)) != 0 ||
      sk_graph_get_u32(header + 8) != 1 || sk_graph_get_u32(header + 12) != sizeof(header) ||
      sk_graph_get_u64(header + 16) != (uint64_t)n ||
      sk_graph_get_u64(header + 24) != (uint64_t)dim) goto done;
  degree = sk_graph_get_u64(header + 32);
  stored_bytes = sk_graph_get_u64(header + 40);
  if (sk_graph_payload_bytes((uint64_t)n, degree, &payload_bytes) ||
      stored_bytes != (uint64_t)payload_bytes ||
      (uint64_t)file_bytes != sizeof(header) + stored_bytes + sizeof(trailer)) goto done;
  host_graph = (uint32_t *)malloc(payload_bytes);
  if (!host_graph) goto done;
  checksum = sk_graph_checksum(checksum, header, sizeof(header));
  for (size_t offset = 0; offset < payload_bytes;) {
    size_t bytes = payload_bytes - offset;
    if (bytes > SK_CUVS_GRAPH_CHUNK_BYTES) bytes = SK_CUVS_GRAPH_CHUNK_BYTES;
    unsigned char *chunk = (unsigned char *)host_graph + offset;
    if (fread(chunk, 1, bytes, file) != bytes) goto done;
    checksum = sk_graph_checksum(checksum, chunk, bytes);
    for (size_t i = 0; i < bytes; i += sizeof(uint32_t)) {
      uint32_t neighbor = sk_graph_get_u32(chunk + i);
      if ((uint64_t)neighbor >= (uint64_t)n) goto done;
      memcpy(chunk + i, &neighbor, sizeof(neighbor));
    }
    offset += bytes;
  }
  if (fread(trailer, 1, sizeof(trailer), file) != sizeof(trailer) ||
      sk_graph_get_u64(trailer) != checksum || fgetc(file) != EOF || ferror(file)) goto done;
  if (fclose(file) != 0) { file = NULL; goto done; }
  file = NULL;
  h = create_index();
  if (!h) goto done;
  {
    int64_t graph_shape[2] = {n, (int64_t)degree};
    int64_t base_shape[2] = {n, dim};
    DLManagedTensor graph = tensor(host_graph, graph_shape, kDLCPU, kDLUInt);
    DLManagedTensor dataset = tensor((void *)base, base_shape, kDLCPU, kDLFloat);
    /* The frozen C API selects the host/host CAGRA constructor, which copies
     * both arrays to index-owned GPU storage. It never runs build/optimize.
     * Drain even on failure before releasing the temporary host graph. */
    cuvsError_t status = cuvsCagraIndexFromArgs(h->res, L2Expanded, &graph, &dataset, h->idx);
    cuvsError_t sync_status = cuvsStreamSync(h->res);
    if (status != CUVS_SUCCESS || sync_status != CUVS_SUCCESS) goto done;
  }
  {
    int64_t actual_n = 0, actual_dim = 0, actual_degree = 0;
    if (cuvsCagraIndexGetSize(h->idx, &actual_n) != CUVS_SUCCESS ||
        cuvsCagraIndexGetDims(h->idx, &actual_dim) != CUVS_SUCCESS ||
        cuvsCagraIndexGetGraphDegree(h->idx, &actual_degree) != CUVS_SUCCESS ||
        actual_n != n || actual_dim != dim || (uint64_t)actual_degree != degree) goto done;
  }
  h->n = n;
  h->dim = dim;
  success = 1;
done:
  if (file) fclose(file);
  if (!success) {
    seekdb_cuvs_free(h);
    h = NULL;
    fprintf(stderr, "[seekdb_cuvs] graph-only deserialize failed\n");
  }
  free(host_graph);
  return h;
}

int seekdb_cuvs_cagra_knn(const float *base, long n, long dim,
                          const float *query, long nq, long topk,
                          unsigned int *out_ids)
{
  void *h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
