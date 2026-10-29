#pragma once
#include <stddef.h>
#include <stdint.h>
typedef uintptr_t cuvsResources_t;
typedef enum { CUVS_ERROR = 0, CUVS_SUCCESS = 1 } cuvsError_t;
#ifdef __cplusplus
extern "C" {
#endif
cuvsError_t cuvsResourcesCreate(cuvsResources_t *);
cuvsError_t cuvsResourcesDestroy(cuvsResources_t);
cuvsError_t cuvsStreamSync(cuvsResources_t);
cuvsError_t cuvsRMMAlloc(cuvsResources_t, void **, size_t);
cuvsError_t cuvsRMMFree(cuvsResources_t, void *, size_t);
const char *cuvsGetLastErrorText(void);
#ifdef __cplusplus
}
#endif
