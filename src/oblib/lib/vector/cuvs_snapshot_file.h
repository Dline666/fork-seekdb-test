#pragma once

#include <cstdio>
#include <unistd.h>
#ifdef __linux__
#include <sys/syscall.h>
#include <linux/memfd.h>
#endif

namespace oceanbase {
namespace common {

// The hipVS serializer accepts a filename. An anonymous RAM-backed file avoids
// staging the graph on disk before copying it into/out of seekdb's snapshot.
// This is per-operation scratch, never a cache or a persistent index.
class CuvsSnapshotFile {
public:
  CuvsSnapshotFile()
  {
#if defined(__linux__) && defined(SYS_memfd_create)
    int fd = static_cast<int>(::syscall(SYS_memfd_create, "seekdb-cagra-snapshot", MFD_CLOEXEC));
    if (fd >= 0) {
      file_ = ::fdopen(fd, "w+b");
      if (file_ == nullptr) { ::close(fd); }
    }
#endif
    if (file_ == nullptr) { file_ = ::tmpfile(); }
    if (file_ != nullptr) {
#ifdef __APPLE__
      ::snprintf(path_, sizeof(path_), "/dev/fd/%d", ::fileno(file_));
#else
      ::snprintf(path_, sizeof(path_), "/proc/self/fd/%d", ::fileno(file_));
#endif
    }
  }

  ~CuvsSnapshotFile() { if (file_ != nullptr) { ::fclose(file_); } }
  CuvsSnapshotFile(const CuvsSnapshotFile &) = delete;
  CuvsSnapshotFile &operator=(const CuvsSnapshotFile &) = delete;

  FILE *file_ = nullptr;
  char path_[64] = {};
};

}  // namespace common
}  // namespace oceanbase
