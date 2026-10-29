/* Host-only scoped OpenMP control tests; no HIP or GPU dependency. */
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <dlfcn.h>

static int max_threads = 512, dynamic_threads = 1, set_calls = 0;
static int library_available = 1, closes = 0, opens = 0;
static const char *missing_symbol = NULL;
static int library_token;
static int mock_get_max(void) { return max_threads; }
static int mock_get_dynamic(void) { return dynamic_threads; }
static void mock_set_max(int n) { assert(n > 0); max_threads = n; ++set_calls; }
static void mock_set_dynamic(int n) { dynamic_threads = n; ++set_calls; }
static void *mock_open(const char *name, int flags)
{
  assert(strcmp(name, "libcuvs_c.so") == 0);
  assert(flags == (RTLD_LAZY | RTLD_NOLOAD));
  ++opens;
  return library_available ? &library_token : NULL;
}
static void *mock_symbol(void *library, const char *name)
{
  assert(library == &library_token);
  if (missing_symbol && strcmp(missing_symbol, name) == 0) return NULL;
  void *out = NULL;
  if (strcmp(name, "omp_get_max_threads") == 0) {
    int (*fn)(void) = mock_get_max; memcpy(&out, &fn, sizeof(out));
  } else if (strcmp(name, "omp_get_dynamic") == 0) {
    int (*fn)(void) = mock_get_dynamic; memcpy(&out, &fn, sizeof(out));
  } else if (strcmp(name, "omp_set_num_threads") == 0) {
    void (*fn)(int) = mock_set_max; memcpy(&out, &fn, sizeof(out));
  } else if (strcmp(name, "omp_set_dynamic") == 0) {
    void (*fn)(int) = mock_set_dynamic; memcpy(&out, &fn, sizeof(out));
  } else assert(0);
  return out;
}
static int mock_close(void *library)
{
  assert(library == &library_token); ++closes; return 0;
}
#define dlopen mock_open
#define dlsym mock_symbol
#define dlclose mock_close
#include "seekdb_cuvs_omp.h"
#undef dlopen
#undef dlsym
#undef dlclose

static void parse(const char *text, int expected_rc, int expected_cpus)
{
  int cpus = 73;
  assert(sk_cuvs_omp_parse_cpu_max(text, &cpus) == expected_rc);
  assert(cpus == (expected_rc == 1 ? expected_cpus : 73));
}
static int simulated_build(int fail)
{
  sk_cuvs_omp_scope scope;
  sk_cuvs_omp_begin_with_cap(&scope, 24);
  assert(max_threads == 24 && dynamic_threads == 0);
  int rc = fail ? -1 : 0;
  sk_cuvs_omp_end(&scope);
  return rc;
}
int main(void)
{
  parse("2400000 100000", 1, 24);
  parse("1600000 100000\n", 1, 16);
  parse("150000 100000", 1, 2);
  parse("1 100000", 1, 1);
  parse(" max\t100000 \n", 0, 0);
  parse("18446744073709551615 1", 1, INT_MAX);
  parse("18446744073709551615 18446744073709551615", 1, 1);
  const char *bad[] = {"", "max", "maximum 100", "max 0", "1 0", "0 100000",
                       "-1 100000", "1 -100", "1 100 foo", "1x 100", "1.5 100",
                       "18446744073709551616 100", "1 18446744073709551616"};
  for (size_t i = 0; i < sizeof(bad)/sizeof(*bad); ++i) parse(bad[i], -1, 0);
  assert(sk_cuvs_omp_parse_cpu_max(NULL, NULL) == -1);
  assert(sk_cuvs_omp_min_positive(32, 24) == 24);
  assert(sk_cuvs_omp_min_positive(0, 24) == 24);
  assert(sk_cuvs_omp_min_positive(32, 0) == 32);
  assert(sk_cuvs_omp_min_positive(0, 0) == 0);
  assert(sk_cuvs_omp_cpu_cap() > 0);

  sk_cuvs_omp_scope scope;
  sk_cuvs_omp_begin_with_cap(NULL, 4); sk_cuvs_omp_end(NULL);
  sk_cuvs_omp_begin_with_cap(&scope, 0); sk_cuvs_omp_end(&scope);
  assert(opens == 0 && closes == 0 && set_calls == 0);
  library_available = 0;
  sk_cuvs_omp_begin_with_cap(&scope, 24); sk_cuvs_omp_end(&scope);
  assert(opens == 1 && closes == 0 && set_calls == 0);
  library_available = 1;
  const char *names[] = {"omp_get_max_threads", "omp_get_dynamic", "omp_set_num_threads", "omp_set_dynamic"};
  for (size_t i = 0; i < sizeof(names)/sizeof(*names); ++i) {
    missing_symbol = names[i]; sk_cuvs_omp_begin_with_cap(&scope, 24);
    assert(!scope.active && !scope.library); sk_cuvs_omp_end(&scope);
  }
  assert(set_calls == 0 && closes == 4);
  missing_symbol = NULL;
  max_threads = 0;
  sk_cuvs_omp_begin_with_cap(&scope, 24); sk_cuvs_omp_end(&scope);
  assert(set_calls == 0);
  max_threads = 512;
  assert(simulated_build(0) == 0); assert(max_threads == 512 && dynamic_threads == 1);
  assert(simulated_build(1) == -1); assert(max_threads == 512 && dynamic_threads == 1);
  sk_cuvs_omp_begin_with_cap(&scope, 24);
  sk_cuvs_omp_scope nested;
  sk_cuvs_omp_begin_with_cap(&nested, 8);
  assert(max_threads == 8);
  sk_cuvs_omp_end(&nested); assert(max_threads == 24);
  sk_cuvs_omp_end(&scope); assert(max_threads == 512 && dynamic_threads == 1);
  int saved_calls = set_calls, saved_closes = closes;
  sk_cuvs_omp_end(&scope);
  assert(set_calls == saved_calls && closes == saved_closes);
  max_threads = 2;
  sk_cuvs_omp_begin_with_cap(&scope, 24); assert(max_threads == 2);
  sk_cuvs_omp_end(&scope); assert(max_threads == 2 && dynamic_threads == 1);
  assert(opens == closes + 1); /* One dlopen deliberately failed. */
  puts("PASS scoped OpenMP: quota parser, CPU cap, dependency-scope resolution, absent runtime/API, unchanged lower setting, success/failure/nested restore, idempotent cleanup");
  return 0;
}
