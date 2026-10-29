#include "lib/vector/cuvs_snapshot_file.h"
#include <cassert>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <vector>
#include <cstdio>

int main()
{
  std::vector<unsigned char> payload(1024 * 1024);
  for (size_t i = 0; i < payload.size(); ++i) payload[i] = (unsigned char)(i * 31 + i / 17);
  std::vector<unsigned char> result(payload.size());
  int closed_fd = -1;
  for (int pass = 0; pass < 100; ++pass) {
    {
      oceanbase::common::CuvsSnapshotFile graph;
      assert(graph.file_ && graph.path_[0]);
      closed_fd = fileno(graph.file_);
      struct stat original;
      assert(fstat(closed_fd, &original) == 0 && original.st_size == 0);
      // cuVS writes through a separately opened filename, not the FILE*.
      FILE *serializer = fopen(graph.path_, "wb");
      assert(serializer);
      struct stat reopened;
      assert(fstat(fileno(serializer), &reopened) == 0);
      assert(original.st_dev == reopened.st_dev && original.st_ino == reopened.st_ino);
      assert(fwrite(payload.data(), 1, payload.size(), serializer) == payload.size());
      assert(fclose(serializer) == 0);
      rewind(graph.file_);
      assert(fread(result.data(), 1, result.size(), graph.file_) == result.size());
      assert(result == payload);
      rewind(graph.file_);
      assert(fwrite(payload.data(), 1, payload.size(), graph.file_) == payload.size());
      assert(fflush(graph.file_) == 0);
      FILE *loader = fopen(graph.path_, "rb");
      assert(loader);
      // /dev/fd on macOS shares the file offset; Linux /proc opens at zero.
      rewind(loader);
      assert(fread(result.data(), 1, result.size(), loader) == result.size());
      assert(result == payload);
      assert(fclose(loader) == 0);
      // Two live snapshots must use separate files; no cross-round cache.
      oceanbase::common::CuvsSnapshotFile other;
      assert(other.file_);
      struct stat separate;
      assert(fstat(fileno(other.file_), &separate) == 0 && separate.st_size == 0);
      assert(original.st_dev != separate.st_dev || original.st_ino != separate.st_ino);
    }
    errno = 0;
    assert(fcntl(closed_fd, F_GETFD) == -1 && errno == EBADF);
  }
  std::puts("PASS snapshot scratch: 100 independent 1 MiB filename round trips, file identity, close, no cache");
}
