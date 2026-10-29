#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <limits>

namespace oceanbase
{
namespace storage
{

// Cancellation never modifies the writer's staging buffer. A successful begin
// is the dispatch linearization point: an already active batch may finish, but
// cancellation permanently prevents another batch from starting until reset.
class HnswBatchDispatchGate
{
public:
  bool try_begin()
  {
    unsigned char expected = 0;
    return state_.compare_exchange_strong(expected, ACTIVE, std::memory_order_acq_rel);
  }
  void finish() { state_.fetch_and(CANCELED, std::memory_order_acq_rel); }
  void cancel() { state_.fetch_or(CANCELED, std::memory_order_acq_rel); }
  bool canceled() const { return (state_.load(std::memory_order_acquire) & CANCELED) != 0; }
  void reset() { state_.store(0, std::memory_order_release); }

private:
  static constexpr unsigned char ACTIVE = 1;
  static constexpr unsigned char CANCELED = 2;
  std::atomic<unsigned char> state_{0};
};

// Non-owning, bounded staging buffer for dense snapshot input. Callers retain
// the adaptor guard and execute flush() on the original OB worker thread.
class HnswVectorBatch
{
public:
  enum class AppendResult { SUCCESS, INVALID_INPUT, FULL };

  static int64_t row_capacity(const int64_t dim)
  {
    constexpr int64_t MAX_ROWS = 1024;
    constexpr int64_t TARGET_BYTES = 1024 * 1024;
    if (dim <= 0 || dim > std::numeric_limits<int64_t>::max() / static_cast<int64_t>(sizeof(float))) {
      return 0;
    }
    const int64_t rows = TARGET_BYTES / (dim * sizeof(float));
    return rows < 1 ? 1 : (rows < MAX_ROWS ? rows : MAX_ROWS);
  }

  bool init(float *vectors, int64_t *ids, const int64_t dim, const int64_t capacity)
  {
    if (vectors == nullptr || ids == nullptr || dim <= 0 || capacity <= 0 ||
        capacity > std::numeric_limits<int>::max() ||
        static_cast<uint64_t>(dim) > std::numeric_limits<size_t>::max() / sizeof(float) /
                                     static_cast<uint64_t>(capacity)) {
      return false;
    }
    vectors_ = vectors;
    ids_ = ids;
    dim_ = dim;
    capacity_ = capacity;
    count_ = 0;
    return true;
  }

  AppendResult append(const void *vector, const int64_t bytes, const int64_t id)
  {
    if (vectors_ == nullptr || ids_ == nullptr || vector == nullptr || bytes < 0 ||
        static_cast<uint64_t>(bytes) != static_cast<uint64_t>(dim_) * sizeof(float)) {
      return AppendResult::INVALID_INPUT;
    } else if (full()) {
      return AppendResult::FULL;
    }
    std::memcpy(vectors_ + count_ * dim_, vector, static_cast<size_t>(bytes));
    ids_[count_++] = id;
    return AppendResult::SUCCESS;
  }

  template <typename Writer>
  int flush(Writer &&writer)
  {
    int ret = 0;
    if (count_ > 0) {
      ret = writer(vectors_, ids_, static_cast<int>(count_));
      if (ret == 0) { count_ = 0; }
    }
    return ret;
  }

  bool full() const { return count_ == capacity_; }
  int64_t count() const { return count_; }
  void reset()
  {
    vectors_ = nullptr;
    ids_ = nullptr;
    dim_ = 0;
    capacity_ = 0;
    count_ = 0;
  }

private:
  float *vectors_ = nullptr;
  int64_t *ids_ = nullptr;
  int64_t dim_ = 0;
  int64_t capacity_ = 0;
  int64_t count_ = 0;
};

}  // namespace storage
}  // namespace oceanbase
