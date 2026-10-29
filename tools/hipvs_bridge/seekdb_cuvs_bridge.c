#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

/* seekdb <-> hipVS bridge: CAGRA with per-index reusable search workspace. */
#include "seekdb_cuvs_bridge.h"
#include "seekdb_cuvs_omp.h"
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
#include <time.h>

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
  // Keep the V5 graph construction and precision parameters unchanged.
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = 128;
  h->ip->intermediate_graph_degree = 256;
  h->ip->nn_descent_niter = 100;
  sk_cuvs_omp_scope omp_scope;
  sk_cuvs_omp_begin(&omp_scope);
  const int omp_previous = omp_scope.previous_threads;
  const int omp_applied = omp_scope.applied_threads;
  struct timespec build_start, build_end;
  const int timer_valid = clock_gettime(CLOCK_MONOTONIC, &build_start) == 0;
  cuvsError_t build_status = cuvsCagraBuild(h->res, h->ip, &dataset, h->idx);
  if (build_status == CUVS_SUCCESS) build_status = cuvsStreamSync(h->res);
  sk_cuvs_omp_end(&omp_scope);
  if (timer_valid && clock_gettime(CLOCK_MONOTONIC, &build_end) == 0) {
    const double seconds = (double)(build_end.tv_sec - build_start.tv_sec) +
                           (double)(build_end.tv_nsec - build_start.tv_nsec) / 1e9;
    fprintf(stderr, "[seekdb_cuvs] cagra_build n=%ld dim=%ld seconds=%.6f omp_previous=%d omp_applied=%d success=%d\n",
            n, dim, seconds, omp_previous, omp_applied, build_status == CUVS_SUCCESS);
  }
  if (build_status != CUVS_SUCCESS) {
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
