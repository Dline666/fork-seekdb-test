#pragma once

#include <pthread.h>
#include <condition_variable>
#include <mutex>

namespace oceanbase {
namespace common {

// cuVS needs more stack than an OB worker provides. Reuse one large-stack
// execution thread; run() is synchronous, so callers can submit stack arguments.
// The adaptor already serializes access to the GPU index registry.
class CuvsWorker {
public:
  CuvsWorker() = default;
  CuvsWorker(const CuvsWorker &) = delete;
  CuvsWorker &operator=(const CuvsWorker &) = delete;

  ~CuvsWorker()
  {
    std::lock_guard<std::mutex> submit(submit_mutex_);
    if (started_) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        stop_ = true;
      }
      ready_.notify_one();
      pthread_join(thread_, nullptr);
    }
  }

  bool run(void *(*function)(void *), void *argument)
  {
    if (function == nullptr) { return false; }
    std::lock_guard<std::mutex> submit(submit_mutex_);
    std::unique_lock<std::mutex> lock(mutex_);
    if (!started_) {
      pthread_attr_t attr;
      if (pthread_attr_init(&attr) != 0) { return false; }
      int rc = pthread_attr_setstacksize(&attr, 32UL * 1024 * 1024);
      if (rc == 0) { rc = pthread_create(&thread_, &attr, entry, this); }
      pthread_attr_destroy(&attr);
      if (rc != 0) { return false; }
      started_ = true;
    }
    function_ = function;
    argument_ = argument;
    completed_ = false;
    ready_.notify_one();
    done_.wait(lock, [this] { return completed_; });
    return true;
  }

private:
  static void *entry(void *argument)
  {
    static_cast<CuvsWorker *>(argument)->loop();
    return nullptr;
  }

  void loop()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    for (;;) {
      ready_.wait(lock, [this] { return stop_ || function_ != nullptr; });
      if (stop_) { return; }
      auto function = function_;
      void *argument = argument_;
      lock.unlock();
      function(argument);
      lock.lock();
      function_ = nullptr;
      argument_ = nullptr;
      completed_ = true;
      done_.notify_one();
    }
  }

  std::mutex submit_mutex_;
  std::mutex mutex_;
  std::condition_variable ready_;
  std::condition_variable done_;
  pthread_t thread_{};
  bool started_ = false;
  bool stop_ = false;
  bool completed_ = false;
  void *(*function_)(void *) = nullptr;
  void *argument_ = nullptr;
};

}  // namespace common
}  // namespace oceanbase
