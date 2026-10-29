/* Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0.
 * Experimental Linux/CUVS startup policy; no GPU or OpenMP API dependency.
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#define SEEKDB_CUVS_OMP_MARKER "SEEKDB_CUVS_OMP_REEXEC"
/* Defensive bound for inherited environ traversal. At most roughly 8 MiB of
 * pointers on a 64-bit host; normal Linux ARG_MAX rejects much smaller input. */
#define SEEKDB_CUVS_OMP_MAX_ENV 1048576U

typedef struct {
  int online;
  int affinity;
  int quota;
} SeekdbCuvsStartupCpus;

static inline int seekdb_cuvs_env_present(char *const envp[], const char *name)
{
  if (!envp || !name) return 0;
  const size_t length = strlen(name);
  for (size_t i = 0; i < SEEKDB_CUVS_OMP_MAX_ENV; ++i) {
    if (!envp[i]) return 0;
    if (strncmp(envp[i], name, length) == 0 && envp[i][length] == '=') return 1;
  }
  /* Invalid/unterminated environment: refuse the restart rather than guess. */
  return 1;
}

/* An OpenMP runtime can bind the initial thread in a library constructor,
 * before main. Its current affinity then describes one place, not the process's
 * original allocation. Re-exec would preserve that narrowed CPU mask. Respect
 * explicit binding settings, including empty values, instead of deriving a cap
 * from a possibly already bound thread. */
static inline int seekdb_cuvs_startup_explicit_binding(char *const envp[])
{
  static const char *const names[] = {
    "OMP_PROC_BIND", "OMP_PROC_BIND_ALL", "OMP_PLACES", "GOMP_CPU_AFFINITY", "KMP_AFFINITY",
    "KMP_HW_SUBSET", "KMP_PLACE_THREADS", "KMP_HIDDEN_HELPER_AFFINITY"
  };
  for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
    if (seekdb_cuvs_env_present(envp, names[i])) return 1;
  }
  return 0;
}

static inline int seekdb_cuvs_env_count(char *const envp[], size_t limit, size_t *count)
{
  if (!envp || !count || limit == 0 || limit > SEEKDB_CUVS_OMP_MAX_ENV) return 0;
  for (size_t i = 0; i < limit; ++i) {
    if (!envp[i]) { *count = i; return 1; }
  }
  return 0;
}

static inline int seekdb_cuvs_env_allocation_bytes(size_t count, size_t *bytes)
{
  /* Existing strings + three additions + terminating NULL. */
  if (!bytes || count > SIZE_MAX - 4 || count + 4 > SIZE_MAX / sizeof(char *)) return 0;
  *bytes = (count + 4) * sizeof(char *);
  return 1;
}

static inline int seekdb_cuvs_startup_information_only(int argc, char *const argv[])
{
  if (argc < 1 || !argv) return 1;
  /* Exact argument matching only, no getopt state/argv mutation. Seeing such a
   * token even as an option value conservatively skips optimization. */
  for (int i = 1; i < argc; ++i) {
    if (!argv[i]) return 1;
    if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0 ||
        strcmp(argv[i], "--version") == 0 || strcmp(argv[i], "-V") == 0) return 1;
  }
  return 0;
}

static inline int seekdb_cuvs_startup_effective_cap(SeekdbCuvsStartupCpus cpus)
{
  if (cpus.online <= 0) return 0;
  int cap = cpus.online;
  if (cpus.affinity > 0 && cpus.affinity < cap) cap = cpus.affinity;
  if (cpus.quota > 0 && cpus.quota < cap) cap = cpus.quota;
  /* Ignore rounding-sized differences. A real reduction of at least 25% is
   * needed to justify reloading the process. Quota/affinity must both remain
   * inherited; no cgroup, scheduler or security state is modified. */
  return (int64_t)cap * 4 <= (int64_t)cpus.online * 3 ? cap : 0;
}

static inline const char *seekdb_cuvs_startup_space(const char *text)
{
  while (*text == ' ' || *text == '\t' || *text == '\n' || *text == '\r' ||
         *text == '\f' || *text == '\v') ++text;
  return text;
}

static inline int seekdb_cuvs_startup_uint(const char **text, uint64_t *value)
{
  const char *start = seekdb_cuvs_startup_space(*text);
  if (*start < '0' || *start > '9') return 0;
  errno = 0;
  char *end = NULL;
  unsigned long long parsed = strtoull(start, &end, 10);
  if (errno == ERANGE || start == end) return 0;
  if (*end && seekdb_cuvs_startup_space(end) == end) return 0;
  *text = end; *value = (uint64_t)parsed; return 1;
}

/* 1 finite, 0 unlimited, -1 invalid. Output remains untouched unless finite. */
static inline int seekdb_cuvs_startup_cpu_max(const char *text, int *cpus)
{
  if (!text || !cpus) return -1;
  text = seekdb_cuvs_startup_space(text);
  uint64_t quota = 0, period = 0;
  int unlimited = strncmp(text, "max", 3) == 0 &&
                  (text[3] == '\0' || seekdb_cuvs_startup_space(text + 3) != text + 3);
  if (unlimited) text += 3;
  else if (!seekdb_cuvs_startup_uint(&text, &quota) || quota == 0) return -1;
  if (!seekdb_cuvs_startup_uint(&text, &period) || period == 0 ||
      *seekdb_cuvs_startup_space(text) != '\0') return -1;
  if (unlimited) return 0;
  uint64_t ceiling = quota / period + (quota % period != 0);
  *cpus = ceiling > INT_MAX ? INT_MAX : (int)ceiling;
  return 1;
}

#if defined(__linux__) && defined(OB_BUILD_CUVS)
#include <stdio.h>
#include <unistd.h>
#include <sched.h>

extern char **environ;

static inline SeekdbCuvsStartupCpus seekdb_cuvs_startup_detect_cpus(void)
{
  SeekdbCuvsStartupCpus cpus = {0, 0, 0};
  long online = sysconf(_SC_NPROCESSORS_ONLN);
  if (online > 0) cpus.online = online > INT_MAX ? INT_MAX : (int)online;
#if defined(CPU_COUNT) && defined(CPU_SETSIZE)
  cpu_set_t allowed;
  CPU_ZERO(&allowed);
  if (sched_getaffinity(0, sizeof(allowed), &allowed) == 0) cpus.affinity = CPU_COUNT(&allowed);
#endif
  FILE *file = fopen("/sys/fs/cgroup/cpu.max", "r");
  if (file) {
    char text[128];
    if (fgets(text, sizeof(text), file) &&
        (strchr(text, '\n') != NULL || feof(file))) {
      (void)seekdb_cuvs_startup_cpu_max(text, &cpus.quota);
    }
    fclose(file);
  }
  return cpus;
}
#endif

/* Small dependency surface permits failure injection without invoking seekdb. */
typedef void *(*SeekdbCuvsStartupAlloc)(size_t);
typedef void (*SeekdbCuvsStartupFree)(void *);
typedef int (*SeekdbCuvsStartupExec)(const char *, char *const[], char *const[]);

static inline int seekdb_cuvs_startup_attempt(
    int argc, char *const argv[], char *const inherited_env[], SeekdbCuvsStartupCpus cpus,
    const char *executable, SeekdbCuvsStartupAlloc allocate,
    SeekdbCuvsStartupFree deallocate, SeekdbCuvsStartupExec execute)
{
  if (!allocate || !deallocate || !execute || !executable || !*executable ||
      seekdb_cuvs_startup_information_only(argc, argv)) return 0;
  size_t count = 0, bytes = 0;
  if (!seekdb_cuvs_env_count(inherited_env, SEEKDB_CUVS_OMP_MAX_ENV, &count) ||
      seekdb_cuvs_env_present(inherited_env, SEEKDB_CUVS_OMP_MARKER) ||
      seekdb_cuvs_env_present(inherited_env, "OMP_NUM_THREADS") ||
      seekdb_cuvs_env_present(inherited_env, "OMP_WAIT_POLICY") ||
      seekdb_cuvs_env_present(inherited_env, "OMP_NUM_THREADS_ALL") ||
      seekdb_cuvs_env_present(inherited_env, "OMP_WAIT_POLICY_ALL") ||
      seekdb_cuvs_startup_explicit_binding(inherited_env)) return 0;
  int cap = seekdb_cuvs_startup_effective_cap(cpus);
  if (cap <= 0 || !seekdb_cuvs_env_allocation_bytes(count, &bytes)) return 0;
  char threads[64] = "OMP_NUM_THREADS=";
  /* No snprintf dependency in the host-testable policy helper. */
  char digits[16]; size_t digit_count = 0; unsigned value = (unsigned)cap;
  do { digits[digit_count++] = (char)('0' + value % 10); value /= 10; } while (value);
  size_t prefix = sizeof("OMP_NUM_THREADS=") - 1;
  if (prefix + digit_count >= sizeof(threads)) return 0;
  for (size_t i = 0; i < digit_count; ++i) threads[prefix + i] = digits[digit_count - i - 1];
  threads[prefix + digit_count] = '\0';
  char wait_policy[] = "OMP_WAIT_POLICY=PASSIVE";
  char marker[] = SEEKDB_CUVS_OMP_MARKER "=1";
  char **envp = (char **)allocate(bytes);
  if (!envp) return 0;
  for (size_t i = 0; i < count; ++i) envp[i] = inherited_env[i];
  envp[count] = threads; envp[count + 1] = wait_policy; envp[count + 2] = marker;
  envp[count + 3] = NULL;
  const int rc = execute(executable, argv, envp);
  const int saved_errno = errno;
  deallocate(envp);
  errno = saved_errno;
  /* Real successful exec never returns. This value is for instrumentation and
   * host tests only; callers always continue original startup if exec fails. */
  return rc == -1 ? -1 : 0;
}

static inline void seekdb_cuvs_maybe_restart_for_cpu_quota(int argc, char *argv[])
{
#if defined(__linux__) && defined(OB_BUILD_CUVS)
  const int saved_errno = errno;
  if (!seekdb_cuvs_startup_information_only(argc, argv) &&
      !seekdb_cuvs_env_present(environ, SEEKDB_CUVS_OMP_MARKER) &&
      !seekdb_cuvs_env_present(environ, "OMP_NUM_THREADS") &&
      !seekdb_cuvs_env_present(environ, "OMP_WAIT_POLICY") &&
      !seekdb_cuvs_env_present(environ, "OMP_NUM_THREADS_ALL") &&
      !seekdb_cuvs_env_present(environ, "OMP_WAIT_POLICY_ALL") &&
      !seekdb_cuvs_startup_explicit_binding(environ)) {
    (void)seekdb_cuvs_startup_attempt(argc, argv, environ,
        seekdb_cuvs_startup_detect_cpus(), "/proc/self/exe", malloc, free, execve);
  }
  errno = saved_errno;
#else
  (void)argc; (void)argv;
#endif
}
