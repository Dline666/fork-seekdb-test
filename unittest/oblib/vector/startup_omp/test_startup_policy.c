#include "observer/seekdb_cuvs_startup_omp.h"
#include <assert.h>
#include <stdio.h>

static int allocations, releases, executions, fail_alloc;
static int wanted_cap;
static char *const *expected_argv;
static char *const *expected_env;
static size_t expected_count;
static void *allocate(size_t bytes) { ++allocations; return fail_alloc ? NULL : malloc(bytes); }
static void release(void *data) { ++releases; free(data); }
static int execute(const char *path, char *const argv[], char *const envp[])
{
  ++executions;
  assert(strcmp(path, "/proc/self/exe") == 0);
  assert(argv == expected_argv);
  for (size_t i = 0; i < expected_count; ++i) assert(envp[i] == expected_env[i]);
  char wanted[64]; assert(snprintf(wanted, sizeof(wanted), "OMP_NUM_THREADS=%d", wanted_cap) > 0);
  assert(strcmp(envp[expected_count], wanted) == 0);
  assert(strcmp(envp[expected_count + 1], "OMP_WAIT_POLICY=PASSIVE") == 0);
  assert(strcmp(envp[expected_count + 2], SEEKDB_CUVS_OMP_MARKER "=1") == 0);
  assert(envp[expected_count + 3] == NULL);
  errno = ENOENT; return -1;
}
static void parser(const char *text, int expected, int value)
{
  int cpus = 77;
  assert(seekdb_cuvs_startup_cpu_max(text, &cpus) == expected);
  assert(cpus == (expected == 1 ? value : 77));
}
int main(void)
{
  parser("2400000 100000", 1, 24); parser("150000 100000\n", 1, 2);
  parser("1 100000", 1, 1); parser("max 100000", 0, 0);
  parser(" \tmax\t100000 \n", 0, 0);
  parser("18446744073709551615 1", 1, INT_MAX);
  parser("18446744073709551615 18446744073709551615", 1, 1);
  const char *bad[] = {"", "max", "max 0", "0 100", "1 0", "-1 100", "+1 100", "1 -1", "1 1 extra", "1.5 100", "1 100x", "18446744073709551616 1"};
  for (size_t i = 0; i < sizeof(bad)/sizeof(*bad); ++i) parser(bad[i], -1, 0);
  assert(seekdb_cuvs_startup_cpu_max(NULL, NULL) == -1);
  SeekdbCuvsStartupCpus cpu;
  cpu.online=128; cpu.affinity=128; cpu.quota=24; assert(seekdb_cuvs_startup_effective_cap(cpu)==24);
  cpu.online=128; cpu.affinity=16; cpu.quota=0; assert(seekdb_cuvs_startup_effective_cap(cpu)==16);
  cpu.online=128; cpu.affinity=16; cpu.quota=24; assert(seekdb_cuvs_startup_effective_cap(cpu)==16);
  cpu.online=24; cpu.affinity=24; cpu.quota=0; assert(seekdb_cuvs_startup_effective_cap(cpu)==0);
  cpu.online=24; cpu.affinity=23; cpu.quota=0; assert(seekdb_cuvs_startup_effective_cap(cpu)==0);
  cpu.online=24; cpu.affinity=18; cpu.quota=0; assert(seekdb_cuvs_startup_effective_cap(cpu)==18);
  cpu.online=0; cpu.affinity=16; cpu.quota=2; assert(seekdb_cuvs_startup_effective_cap(cpu)==0);
  size_t bytes=0, count=0;
  assert(seekdb_cuvs_env_allocation_bytes(0,&bytes) && bytes==4*sizeof(char *));
  assert(!seekdb_cuvs_env_allocation_bytes(SIZE_MAX,&bytes));
  assert(!seekdb_cuvs_env_allocation_bytes(SIZE_MAX/sizeof(char *),&bytes));
  assert(!seekdb_cuvs_env_allocation_bytes(1,NULL));
  char a0[]="seekdb",a1[]="--base-dir",a2[]="literal space $() ; ' \"",a3[]="--nodaemon";
  char *args[]={a0,a1,a2,a3,NULL};
  char e0[]="LD_PRELOAD=unmodified-profiler-value",e1[]="LD_LIBRARY_PATH=/keep exactly",e2[]="OTHER=spaces $() ; literal",e3[]="HIP_VISIBLE_DEVICES=3",e4[]="DUP=one",e5[]="DUP=two";
  char *env[]={e0,e1,e2,e3,e4,e5,NULL};
  assert(seekdb_cuvs_env_count(env,7,&count) && count==6);
  assert(!seekdb_cuvs_env_count(env,6,&count));
  assert(!seekdb_cuvs_env_count(NULL,7,&count));
  assert(!seekdb_cuvs_env_count(env,SEEKDB_CUVS_OMP_MAX_ENV+1U,&count));
  assert(seekdb_cuvs_env_present(env,"LD_PRELOAD"));
  assert(!seekdb_cuvs_env_present(env,"LD_PRE"));
  expected_argv=args;expected_env=env;expected_count=6;wanted_cap=24;
  cpu.online=128;cpu.affinity=128;cpu.quota=24;
  assert(seekdb_cuvs_startup_attempt(4,args,env,cpu,"/proc/self/exe",allocate,release,execute)==-1);
  assert(errno==ENOENT && allocations==1 && releases==1 && executions==1);
  assert(strcmp(e0,"LD_PRELOAD=unmodified-profiler-value")==0 && env[6]==NULL);
  fail_alloc=1;
  assert(seekdb_cuvs_startup_attempt(4,args,env,cpu,"/proc/self/exe",allocate,release,execute)==0);
  assert(allocations==2 && releases==1 && executions==1);fail_alloc=0;
  const char *skips[]={"OMP_NUM_THREADS=", "OMP_NUM_THREADS=99", "OMP_WAIT_POLICY=ACTIVE", "OMP_WAIT_POLICY=", "OMP_NUM_THREADS_ALL=", "OMP_NUM_THREADS_ALL=99", "OMP_WAIT_POLICY_ALL=", "OMP_WAIT_POLICY_ALL=ACTIVE", SEEKDB_CUVS_OMP_MARKER "=1", SEEKDB_CUVS_OMP_MARKER "=unexpected"};
  for(size_t i=0;i<sizeof(skips)/sizeof(*skips);++i) {
    char *explicit_env[]={(char *)skips[i],NULL};
    assert(seekdb_cuvs_startup_attempt(4,args,explicit_env,cpu,"/proc/self/exe",allocate,release,execute)==0);
  }
  /* Binding variables may have narrowed the main thread before this policy.
   * Presence always skips, including empty values and runtime-specific knobs. */
  const char *bindings[]={"OMP_PROC_BIND", "OMP_PROC_BIND_ALL", "OMP_PLACES", "GOMP_CPU_AFFINITY",
                          "KMP_AFFINITY", "KMP_HW_SUBSET", "KMP_PLACE_THREADS", "KMP_HIDDEN_HELPER_AFFINITY"};
  const char *binding_values[]={"TRUE", "TRUE", "threads", "0-7", "granularity=fine,compact",
                                "1s,4c,1t", "1s,4c,1t", "granularity=fine,compact"};
  for (size_t i = 0; i < sizeof(bindings)/sizeof(*bindings); ++i) {
    for (int empty = 0; empty < 2; ++empty) {
      char setting[128];
      assert(snprintf(setting, sizeof(setting), "%s=%s", bindings[i], empty ? "" : binding_values[i]) > 0);
      char *explicit_env[]={e0,setting,e3,NULL};
      SeekdbCuvsStartupCpus bound_cpu={128,1,24};
      int old_allocations=allocations,old_executions=executions;
      assert(seekdb_cuvs_startup_explicit_binding(explicit_env));
      assert(seekdb_cuvs_startup_attempt(4,args,explicit_env,bound_cpu,"/proc/self/exe",allocate,release,execute)==0);
      assert(allocations==old_allocations && executions==old_executions);
    }
  }
  char similar[]="OMP_PROC_BIND_SUFFIX=TRUE";
  char *similar_env[]={similar,NULL};
  assert(!seekdb_cuvs_startup_explicit_binding(similar_env));
  const char *info[]={"--version","-V","--help","-h"};
  for(size_t i=0;i<sizeof(info)/sizeof(*info);++i) {
    char *info_args[]={a0,(char *)info[i],NULL};
    assert(seekdb_cuvs_startup_attempt(2,info_args,env,cpu,"/proc/self/exe",allocate,release,execute)==0);
  }
  cpu.online=24;cpu.affinity=24;cpu.quota=0;
  assert(seekdb_cuvs_startup_attempt(4,args,env,cpu,"/proc/self/exe",allocate,release,execute)==0);
  assert(allocations==2 && releases==1 && executions==1);
#ifndef __linux__
  int saved_errno=EINTR;errno=saved_errno;
  seekdb_cuvs_maybe_restart_for_cpu_quota(4,args);assert(errno==saved_errno);
#endif
  puts("PASS startup policy: quota/affinity gates, parser boundaries, argv/env preservation, explicit settings/binding/marker/help skip, allocation overflow/OOM and failed-exec fallback");
}
