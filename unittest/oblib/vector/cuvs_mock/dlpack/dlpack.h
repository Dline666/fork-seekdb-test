#pragma once
#include <stdint.h>
typedef enum { kDLCPU = 1, kDLCUDA = 2 } DLDeviceType;
enum { kDLUInt = 1, kDLFloat = 2 };
typedef struct { DLDeviceType device_type; int32_t device_id; } DLDevice;
typedef struct { uint8_t code; uint8_t bits; uint16_t lanes; } DLDataType;
typedef struct {
  void *data; DLDevice device; int32_t ndim; DLDataType dtype;
  int64_t *shape; int64_t *strides; uint64_t byte_offset;
} DLTensor;
typedef struct DLManagedTensor {
  DLTensor dl_tensor; void *manager_ctx;
  void (*deleter)(struct DLManagedTensor *self);
} DLManagedTensor;
