#include "storage/ddl/ob_hnsw_vector_batch.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <limits>
#include <vector>

using oceanbase::storage::HnswVectorBatch;

int main()
{
  using Result = HnswVectorBatch::AppendResult;
  assert(HnswVectorBatch::row_capacity(0) == 0);
  assert(HnswVectorBatch::row_capacity(-1) == 0);
  assert(HnswVectorBatch::row_capacity(std::numeric_limits<int64_t>::max()) == 0);
  assert(HnswVectorBatch::row_capacity(128) == 1024);
  assert(HnswVectorBatch::row_capacity(256) == 1024);
  assert(HnswVectorBatch::row_capacity(1024) == 256);
  float staging[3 * 4] = {};
  int64_t ids[3] = {};
  HnswVectorBatch batch;
  assert(!batch.init(nullptr, ids, 4, 3));
  assert(!batch.init(staging, nullptr, 4, 3));
  assert(!batch.init(staging, ids, 0, 3));
  assert(!batch.init(staging, ids, 4, 0));
  assert(!batch.init(staging, ids, std::numeric_limits<int64_t>::max(), 3));
  assert(batch.init(staging, ids, 4, 3));
  float value[4] = {1, -0.0f, 3, 4};
  assert(batch.append(nullptr, sizeof(value), 1) == Result::INVALID_INPUT);
  assert(batch.append(value, -1, 1) == Result::INVALID_INPUT);
  assert(batch.append(value, sizeof(value) - 1, 1) == Result::INVALID_INPUT);
  assert(batch.append(value, sizeof(value) + 1, 1) == Result::INVALID_INPUT);
  assert(batch.count() == 0);
  assert(batch.append(value, sizeof(value), -100) == Result::SUCCESS);
  value[0] = 99; // Source LOB/row storage may be reused immediately.
  assert(staging[0] == 1 && std::signbit(staging[1]));
  assert(batch.append(value, sizeof(value), 37) == Result::SUCCESS);
  assert(batch.append(value, sizeof(value), 100000000000LL) == Result::SUCCESS);
  assert(batch.full());
  assert(batch.append(value, sizeof(value), 999) == Result::FULL);
  assert(batch.count() == 3 && ids[2] == 100000000000LL);
  int calls = 0;
  const int injected_error = -4000;
  assert(batch.flush([&](float *vectors, int64_t *vids, int count) {
    ++calls;
    assert(count == 3 && vectors[0] == 1 && vids[0] == -100);
    return injected_error;
  }) == injected_error);
  assert(batch.count() == 3); // A failed writer is never reported as a successful flush.
  assert(batch.flush([&](float *, int64_t *, int count) { ++calls; return count == 3 ? 0 : -1; }) == 0);
  assert(batch.count() == 0 && calls == 2);
  assert(batch.flush([&](float *, int64_t *, int) { ++calls; return -1; }) == 0);
  assert(calls == 2);

  // Model batches containing NULL/empty rows and a final partial batch. Only
  // accepted vectors consume IDs, source storage is overwritten after each row.
  std::vector<int64_t> emitted_ids;
  std::vector<float> emitted_values;
  int flush_calls = 0;
  auto writer = [&](float *vectors, int64_t *vids, int count) {
    ++flush_calls;
    emitted_ids.insert(emitted_ids.end(), vids, vids + count);
    emitted_values.insert(emitted_values.end(), vectors, vectors + count * 4);
    return 0;
  };
  std::vector<int64_t> expected_ids;
  std::vector<float> expected_values;
  for (int64_t row = 0; row < 17; ++row) {
    if (row % 5 == 0 || row % 7 == 0) { continue; } // caller filters NULL/empty values
    const int64_t vid = -2000 + row * 37;
    float source[4] = {static_cast<float>(row), static_cast<float>(row + 1), -2.5f, 7.25f};
    expected_ids.push_back(vid);
    expected_values.insert(expected_values.end(), source, source + 4);
    assert(batch.append(source, sizeof(source), vid) == Result::SUCCESS);
    for (float &v : source) { v = -999; }
    if (batch.full()) { assert(batch.flush(writer) == 0); }
  }
  assert(batch.count() > 0 && batch.count() < 3);
  assert(batch.flush(writer) == 0);
  assert(emitted_ids == expected_ids && emitted_values == expected_values);
  assert(flush_calls == static_cast<int>((expected_ids.size() + 2) / 3));
  assert(batch.flush(writer) == 0); // close then serialize must not duplicate the tail.
  batch.reset();
  assert(batch.count() == 0);
  assert(batch.append(value, sizeof(value), 1) == Result::INVALID_INPUT);
  // cancel->close/serialize cannot dispatch a new pending tail. An already
  // dispatched add can finish, and its finish must preserve the cancel bit.
  oceanbase::storage::HnswBatchDispatchGate gate;
  assert(gate.try_begin());
  assert(!gate.try_begin());
  gate.finish();
  gate.cancel();
  assert(gate.canceled() && !gate.try_begin());
  gate.reset();
  assert(!gate.canceled());
  std::mutex mutex;
  std::condition_variable cv;
  bool in_flight = false;
  bool release = false;
  std::atomic<int> writes{0};
  std::thread writer_thread([&] {
    assert(gate.try_begin());
    {
      std::unique_lock<std::mutex> lock(mutex);
      in_flight = true;
      cv.notify_one();
      cv.wait(lock, [&] { return release; });
    }
    ++writes;  // accepted before cancellation: allowed to finish
    gate.finish();
    assert(!gate.try_begin());
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return in_flight; });
  }
  gate.cancel();
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  cv.notify_one();
  writer_thread.join();
  assert(writes == 1 && gate.canceled() && !gate.try_begin());
  assert(batch.init(staging, ids, 4, 3));
  assert(batch.append(value, sizeof(value), 123) == Result::SUCCESS);
  assert(batch.flush([&](float *, int64_t *, int) {
    if (!gate.try_begin()) { return injected_error; }
    ++writes;
    gate.finish();
    return 0;
  }) == injected_error);
  assert(batch.count() == 1 && writes == 1); // cancellation never mutates staging
  std::puts("PASS HNSW batch: dimensions, bounded capacity, FP32 copy/negative zero, sparse IDs, skipped-row alignment, full/tail/empty flush, writer error propagation, source lifetime, cross-thread cancel-before/while-dispatch");
}
