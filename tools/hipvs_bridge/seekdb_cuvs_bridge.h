#ifndef SEEKDB_CUVS_BRIDGE_H
#define SEEKDB_CUVS_BRIDGE_H
#ifdef __cplusplus
extern "C" {
#endif
/* One-shot: build a CAGRA index over base (n x dim, row-major f32) on the GPU and
 * search queries (nq x dim), writing top-k uint32 neighbor ids to out_ids
 * (length nq*topk). Returns 0 on success, non-zero on error. */
int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids);

/* Build-once / search-many (used by seekdb's ob_vsag_adaptor GPU path).
 *  - seekdb_cuvs_build: builds a CAGRA index on the GPU from base (n x dim,
 *    row-major f32) and returns an opaque handle (NULL on error). cuVS copies
 *    the dataset to the device, so `base` may be freed after this returns.
 *  - seekdb_cuvs_search: runs top-k search for nq queries (row-major f32, dim
 *    cols), writing uint32 row offsets to out_ids[nq*topk] and (if non-NULL) L2
 *    distances to out_dist[nq*topk]. Returns 0 on success.
 *  - seekdb_cuvs_free: releases the handle.
 * Search calls on a handle serialize access to its reusable device workspace.
 * Free requires exclusive ownership (no in-flight calls).
 * All ROCm/cuVS state lives inside the .so; seekdb only ever sees these C symbols. */
void* seekdb_cuvs_build(const float* base, long n, long dim);
int   seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                         unsigned int* out_ids, float* out_dist);
void  seekdb_cuvs_free(void* handle);
int seekdb_cuvs_serialize(void* handle, const char* filename);
void* seekdb_cuvs_deserialize(const char* filename, long n, long dim);
/* Graph-only snapshot helpers. Serialize stores an exact uint32 CAGRA graph
 * (no vectors) with a checked versioned header/checksum. Deserialization takes
 * the separately persisted row-major FP32 base and copies both graph and base
 * to index-owned GPU storage, synchronously, without rebuilding the graph.
 * The caller may release base after return. Legacy full snapshots remain valid
 * through seekdb_cuvs_serialize / seekdb_cuvs_deserialize above.
 * Serialize requires the existing graph's contiguous uint32 device view;
 * unsupported strides/layouts fail explicitly. A failed write may leave a
 * partial file; the caller must publish only on success. */
int seekdb_cuvs_serialize_graph(void* handle, const char* filename);
void* seekdb_cuvs_deserialize_graph(const char* filename, const float* base,
                                    long n, long dim);

#ifdef __cplusplus
}
#endif
#endif
