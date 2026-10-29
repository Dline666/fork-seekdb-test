#include <pthread.h>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

static int creations = 0;
static bool fail_create = true;
static int create_thread(pthread_t *thread, const pthread_attr_t *attr,
                         void *(*function)(void *), void *argument)
{
  ++creations;
  if (fail_create) { fail_create = false; return EAGAIN; }
  return pthread_create(thread, attr, function, argument);
}
#define pthread_create create_thread
#include "lib/vector/cuvs_worker.h"
#undef pthread_create

struct Job {
  std::atomic<int> *active;
  int output = 0;
  pthread_t thread{};
};
static void *execute(void *arg)
{
  auto &job = *static_cast<Job *>(arg);
  assert(job.active->fetch_add(1) == 0);
  job.thread = pthread_self();
#ifdef __APPLE__
  assert(pthread_get_stacksize_np(job.thread) >= 32UL * 1024 * 1024);
#else
  pthread_attr_t attr;
  assert(pthread_getattr_np(job.thread, &attr) == 0);
  size_t size = 0;
  assert(pthread_attr_getstacksize(&attr, &size) == 0);
  assert(size >= 32UL * 1024 * 1024);
  pthread_attr_destroy(&attr);
#endif
  job.output = 42;
  assert(job.active->fetch_sub(1) == 1);
  return nullptr;
}
int main()
{
  { oceanbase::common::CuvsWorker unused; }
  std::atomic<int> active{0};
  oceanbase::common::CuvsWorker worker;
  Job first{&active};
  assert(!worker.run(nullptr, nullptr));
  assert(creations == 0);
  assert(!worker.run(execute, &first));
  assert(first.output == 0 && creations == 1);
  assert(worker.run(execute, &first));
  assert(first.output == 42 && creations == 2);
  std::vector<std::thread> callers;
  for (int thread = 0; thread < 8; ++thread) {
    callers.emplace_back([&] {
      for (int iteration = 0; iteration < 500; ++iteration) {
        Job job{&active};
        assert(worker.run(execute, &job));
        assert(job.output == 42);
        assert(pthread_equal(first.thread, job.thread));
      }
    });
  }
  for (auto &thread : callers) { thread.join(); }
  assert(creations == 2 && active == 0);
  std::puts("PASS worker: failed-start retry, 32 MB stack, 4000 concurrent submissions, same thread, synchronous visibility, shutdown");
}
