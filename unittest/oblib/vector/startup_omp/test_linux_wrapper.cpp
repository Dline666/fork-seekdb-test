/* Actual Linux entry-wrapper probe, compiled against the production header.
 * A Python driver restricts only this child process to a permitted CPU. */
#include <cassert>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <unistd.h>

#if defined(STARTUP_TEST_FORCE_EXEC_FAILURE)
static int fail_exec(const char *path, char *const[], char *const[])
{
  assert(std::strcmp(path, "/proc/self/exe") == 0);
  errno = EACCES;
  return -1;
}
#define execve fail_exec
#endif
#include "observer/seekdb_cuvs_startup_omp.h"
#if defined(STARTUP_TEST_FORCE_EXEC_FAILURE)
#undef execve
#endif

extern "C" int omp_get_max_threads(void);
extern "C" int omp_get_num_threads(void);
static int run_team()
{
  int observed = 0;
#pragma omp parallel
  {
#pragma omp single
    observed = omp_get_num_threads();
  }
  return observed;
}
int main(int argc, char **argv)
{
  errno = E2BIG;
  seekdb_cuvs_maybe_restart_for_cpu_quota(argc, argv);
  assert(errno == E2BIG);
  assert(argc >= 4);
  assert(std::strcmp(argv[argc-2], "literal spaces $() ; and ' quote") == 0);
  assert(std::strcmp(argv[argc-1], "argument-with-\"double-quote\"") == 0);
  const char *mode = argv[1];
  const char *marker = std::getenv(SEEKDB_CUVS_OMP_MARKER);
  const char *threads = std::getenv("OMP_NUM_THREADS");
  const char *policy = std::getenv("OMP_WAIT_POLICY");
  assert(std::strcmp(std::getenv("CUVS_TEST_PRESERVED"), "keep spaces = $ and ; exactly") == 0);
  int current = 0, child = 0;
  if (std::strcmp(mode, "restart") == 0) {
    assert(marker && std::strcmp(marker, "1") == 0);
    assert(threads && std::strcmp(threads, "1") == 0);
    assert(policy && std::strcmp(policy, "PASSIVE") == 0);
    current = run_team();
    std::thread thread([&] { child = run_team(); }); thread.join();
    assert(current == 1 && child == 1 && omp_get_max_threads() == 1);
  } else if (std::strcmp(mode, "explicit-threads") == 0) {
    assert(!marker && threads && std::strcmp(threads, "2") == 0 && !policy);
    current = run_team();
    std::thread thread([&] { child = run_team(); }); thread.join();
    assert(current == 2 && child == 2);
  } else if (std::strcmp(mode, "marker") == 0) {
    assert(marker && std::strcmp(marker, "externally-supplied-unknown") == 0);
    assert(!threads && !policy);
  } else if (std::strcmp(mode, "explicit-policy") == 0) {
    assert(!marker && !threads && policy && std::strcmp(policy, "ACTIVE") == 0);
  } else {
    // help/version, explicit affinity/binding, CPU-only build and failed exec.
    assert(!marker && !threads && !policy);
  }
  std::printf("{\"pid\":%ld,\"mode\":\"%s\",\"marker\":%s,\"current_team\":%d,\"new_thread_team\":%d,\"passed\":true}\n",
              (long)getpid(), mode, marker ? "true" : "false", current, child);
  return 0;
}
