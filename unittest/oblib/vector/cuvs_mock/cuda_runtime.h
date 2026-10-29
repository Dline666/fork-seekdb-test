#pragma once
#include <stddef.h>
enum { cudaSuccess = 0, cudaMemcpyDefault = 0 };
#ifdef __cplusplus
extern "C" {
#endif
int cudaMemcpy(void *, const void *, size_t, int);
#ifdef __cplusplus
}
#endif
