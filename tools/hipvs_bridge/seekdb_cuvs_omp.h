/* Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 */
#pragma once

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#if defined(__linux__)
#include <sched.h>
#endif
#if defined(__linux__) || defined(__APPLE__)
#include <dlfcn.h>
#endif

/* These public OpenMP APIs affect the calling thread's ICVs. They do NOT
 * control the new std::threads that hipVS creates inside NN-descent. */
typedef int (*sk_cuvs_omp_get_fn)(void);
typedef void (*sk_cuvs_omp_set_fn)(int);
typedef struct {
  void *library;
  sk_cuvs_omp_get_fn get_max_threads;
  sk_cuvs_omp_get_fn get_dynamic;
  sk_cuvs_omp_set_fn set_num_threads;
  sk_cuvs_omp_set_fn set_dynamic;
  int previous_threads;
  int previous_dynamic;
  int applied_threads;
  int active;
} sk_cuvs_omp_scope;

static inline const char *sk_cuvs_omp_skip_space(const char *text)
{
  while (*text && isspace((unsigned char)*text)) ++text;
  return text;
}

static inline int sk_cuvs_omp_read_uint(const char **text, uint64_t *value)
{
  const char *start = sk_cuvs_omp_skip_space(*text);
  if (*start < '0' || *start > '9') return 0;
  errno = 0;
  char *end = NULL;
  unsigned long long parsed = strtoull(start, &end, 10);
  if (errno == ERANGE || start == end || (*end && !isspace((unsigned char)*end))) return 0;
  *text = end;
  *value = (uint64_t)parsed;
  return 1;
}

/* cpu.max: returns 1 for a finite quota, 0 for max, -1 for malformed input.
 * Rounding upward preserves fractional CPU entitlement without yielding zero.
 * Do not reinterpret a malformed or unlimited quota as a one-thread limit. */
static inline int sk_cuvs_omp_parse_cpu_max(const char *text, int *cpus)
{
  if (!text || !cpus) return -1;
  text = sk_cuvs_omp_skip_space(text);
  uint64_t quota = 0, period = 0;
  const int unlimited = strncmp(text, "max", 3) == 0 &&
                        (text[3] == '\0' || isspace((unsigned char)text[3]));
  if (unlimited) text += 3;
  else if (!sk_cuvs_omp_read_uint(&text, &quota) || quota == 0) return -1;
  if (!sk_cuvs_omp_read_uint(&text, &period) || period == 0 ||
      *sk_cuvs_omp_skip_space(text) != '\0') return -1;
  if (unlimited) return 0;
  uint64_t ceiling = quota / period + (quota % period != 0);
  *cpus = ceiling > INT_MAX ? INT_MAX : (int)ceiling;
  return 1;
}

static inline int sk_cuvs_omp_min_positive(int first, int second)
{
  if (first <= 0) return second > 0 ? second : 0;
  if (second <= 0) return first;
  return first < second ? first : second;
}

static inline int sk_cuvs_omp_cpu_cap(void)
{
  int available = 0;
#if defined(__linux__) && defined(CPU_COUNT) && defined(CPU_SETSIZE)
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) {
    available = CPU_COUNT(&allowed);
  }
#endif
  if (available <= 0) {
    long online = sysconf(_SC_NPROCESSORS_ONLN);
    if (online > 0) available = online > INT_MAX ? INT_MAX : (int)online;
  }
#if defined(__linux__)
  /* The container's visible cgroup-v2 root. Unavailable/custom mounts and v1
   * retain the affinity/online cap; this does not change cgroups or affinity. */
  FILE *file = fopen("/sys/fs/cgroup/cpu.max", "r");
  if (file) {
    char text[128];
    if (fgets(text, sizeof(text), file)) {
      int quota_cpus = 0;
      if (sk_cuvs_omp_parse_cpu_max(text, &quota_cpus) == 1) {
        available = sk_cuvs_omp_min_positive(available, quota_cpus);
      }
    }
    fclose(file);
  }
#endif
  return available;
}

/* Resolve only from the resident hipVS C library's dependency scope. Using
 * RTLD_DEFAULT could select a different OpenMP runtime loaded by VSAG.
 * If hipVS or any required public API is absent, retain original behavior. */
static inline void sk_cuvs_omp_begin_with_cap(sk_cuvs_omp_scope *scope, int cap)
{
  if (!scope) return;
  memset(scope, 0, sizeof(*scope));
  if (cap <= 0) return;
#if (defined(__linux__) || defined(__APPLE__)) && defined(RTLD_NOLOAD)
  scope->library = dlopen("libcuvs_c.so", RTLD_LAZY | RTLD_NOLOAD);
  if (!scope->library) return;
  void *symbol = dlsym(scope->library, "omp_get_max_threads");
  if (sizeof(symbol) != sizeof(scope->get_max_threads)) goto unavailable;
  memcpy(&scope->get_max_threads, &symbol, sizeof(symbol));
  symbol = dlsym(scope->library, "omp_get_dynamic");
  memcpy(&scope->get_dynamic, &symbol, sizeof(symbol));
  symbol = dlsym(scope->library, "omp_set_num_threads");
  memcpy(&scope->set_num_threads, &symbol, sizeof(symbol));
  symbol = dlsym(scope->library, "omp_set_dynamic");
  memcpy(&scope->set_dynamic, &symbol, sizeof(symbol));
  if (!scope->get_max_threads || !scope->get_dynamic ||
      !scope->set_num_threads || !scope->set_dynamic) goto unavailable;
  scope->previous_threads = scope->get_max_threads();
  scope->previous_dynamic = scope->get_dynamic();
  if (scope->previous_threads <= 0) goto unavailable;
  /* Respect an existing smaller OMP_NUM_THREADS/user setting. */
  scope->applied_threads = sk_cuvs_omp_min_positive(scope->previous_threads, cap);
  scope->set_dynamic(0);
  scope->set_num_threads(scope->applied_threads);
  scope->active = 1;
  return;
unavailable:
  dlclose(scope->library);
  memset(scope, 0, sizeof(*scope));
#else
  (void)cap;
#endif
}

static inline void sk_cuvs_omp_begin(sk_cuvs_omp_scope *scope)
{
  sk_cuvs_omp_begin_with_cap(scope, sk_cuvs_omp_cpu_cap());
}

/* Call on every exit after begin, including a failed GPU build. Idempotent. */
static inline void sk_cuvs_omp_end(sk_cuvs_omp_scope *scope)
{
  if (!scope) return;
  if (scope->active) {
    scope->set_num_threads(scope->previous_threads);
    scope->set_dynamic(scope->previous_dynamic);
  }
#if defined(__linux__) || defined(__APPLE__)
  if (scope->library) dlclose(scope->library);
#endif
  memset(scope, 0, sizeof(*scope));
}
