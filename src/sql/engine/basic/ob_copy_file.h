/*
 * Copyright (c) 2026 OceanBase.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#ifndef _WIN32
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace oceanbase
{
namespace sql
{
namespace file_sql
{

// COPY alone uses this local publication protocol. The SQL formatter streams
// into temporary_path(); no destination entry exists until publish() succeeds.
class CopyFilePublication
{
public:
  enum Result { SUCCESS = 0, FAILED = -1, EXISTS = -2 };
  ~CopyFilePublication() { abort(); }
  CopyFilePublication() = default;
  CopyFilePublication(const CopyFilePublication &) = delete;
  CopyFilePublication &operator=(const CopyFilePublication &) = delete;

  int prepare(const std::string &path, const std::string &secure_directory, std::string &err)
  {
    namespace fs = std::filesystem;
    abort();
    err.clear();
    if (path.empty() || secure_directory.empty() || contains_nul(path) || contains_nul(secure_directory)
        || ascii_null(secure_directory)) {
      err = "COPY requires a nonempty path and secure_file_priv directory";
      return FAILED;
    }
    std::error_code ec;
    const fs::path requested(path);
    const fs::path name = requested.filename();
    if (name.empty() || name == "." || name == "..") {
      err = "COPY target must name a file";
      return FAILED;
    }
    const fs::path root = fs::canonical(fs::path(secure_directory), ec);
    if (ec || !fs::is_directory(root, ec) || ec) {
      err = "secure_file_priv must name an existing directory";
      return FAILED;
    }
    const fs::path parent = fs::canonical(requested.has_parent_path() ? requested.parent_path() : fs::path("."), ec);
    if (ec || !fs::is_directory(parent, ec) || ec || !inside(root, parent)) {
      err = "COPY target parent is outside secure_file_priv or does not exist";
      return FAILED;
    }
    final_path_ = (parent / name).string();
#ifndef _WIN32
    return prepare_anchored(root, parent, name, err);
#else
    const auto status = fs::symlink_status(fs::path(final_path_), ec);
    if (ec && ec != std::errc::no_such_file_or_directory) {
      err = "cannot inspect COPY target: " + ec.message();
      final_path_.clear();
      return FAILED;
    }
    if (fs::exists(status)) {
      err = "COPY target already exists";
      final_path_.clear();
      return EXISTS;
    }
    // A private, atomically-created sibling directory prevents an existing
    // .tmp file or symlink from being opened by the streaming file writer.
    static std::atomic<uint64_t> sequence{0};
    for (int attempt = 0; attempt < 128; ++attempt) {
      const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
      fs::path candidate = parent / (".seekdb-copy-" + std::to_string(stamp) + "-" +
          std::to_string(sequence.fetch_add(1, std::memory_order_relaxed)));
      ec.clear();
      if (fs::create_directory(candidate, ec)) {
        temporary_directory_ = candidate.string();
        fs::permissions(candidate, fs::perms::owner_all, fs::perm_options::replace, ec);
        if (ec) {
          err = "cannot protect COPY temporary directory: " + ec.message();
          abort();
          return FAILED;
        }
        temporary_path_ = (candidate / "data.csv").string();
        return SUCCESS;
      }
      if (ec && ec != std::errc::file_exists) {
        err = "cannot create COPY temporary directory: " + ec.message();
        final_path_.clear();
        return FAILED;
      }
    }
    err = "cannot reserve a unique COPY temporary directory";
    final_path_.clear();
    return FAILED;
#endif
  }

  const std::string &temporary_path() const { return temporary_path_; }
  const std::string &writer_path() const
  {
#ifndef _WIN32
    return writer_path_;
#else
    return temporary_path_;
#endif
  }
  bool has_reserved_file() const
  {
#ifndef _WIN32
    return data_fd_ >= 0;
#else
    return false;
#endif
  }
  const std::string &final_path() const { return final_path_; }

  int publish(std::string &err)
  {
    namespace fs = std::filesystem;
    if (temporary_path_.empty() || final_path_.empty()) {
      err = "COPY publication is not prepared";
      return FAILED;
    }
#ifndef _WIN32
    return publish_anchored(err);
#else
    std::error_code ec;
    // Same-filesystem link is atomic and refuses every existing destination,
    // including a dangling symlink. Never fall back to a replacing rename.
    fs::create_hard_link(fs::path(temporary_path_), fs::path(final_path_), ec);
    if (ec) {
      err = "cannot publish COPY output: " + ec.message();
      const int result = ec == std::errc::file_exists ? EXISTS : FAILED;
      abort();
      return result;
    }
    abort(); // The hard link keeps the successfully published file alive.
    return SUCCESS;
#endif
  }

  void abort()
  {
#ifndef _WIN32
    if (data_fd_ >= 0) { ::close(data_fd_); data_fd_ = -1; }
    if (temporary_fd_ >= 0) {
      (void)::unlinkat(temporary_fd_, "data.csv", 0);
      ::close(temporary_fd_);
      temporary_fd_ = -1;
    }
    if (parent_fd_ >= 0) {
      if (!temporary_name_.empty()) (void)::unlinkat(parent_fd_, temporary_name_.c_str(), AT_REMOVEDIR);
      ::close(parent_fd_);
      parent_fd_ = -1;
    }
    writer_path_.clear();
    parent_path_.clear();
    final_name_.clear();
    temporary_name_.clear();
#else
    std::error_code ec;
    if (!temporary_path_.empty()) std::filesystem::remove(std::filesystem::path(temporary_path_), ec);
    ec.clear();
    if (!temporary_directory_.empty()) std::filesystem::remove(std::filesystem::path(temporary_directory_), ec);
#endif
    temporary_path_.clear();
    temporary_directory_.clear();
    final_path_.clear();
  }

private:
#ifndef _WIN32
  // Every component is opened without following links. Later creation,
  // publication and cleanup are relative to the retained directory handles.
  static int open_directory(const std::filesystem::path &path)
  {
    int fd = ::open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return -1;
    for (const auto &part : path.relative_path()) {
      const std::string component = part.string();
      if (component.empty() || component == ".") continue;
      if (component == "..") { ::close(fd); errno = EINVAL; return -1; }
      const int next = ::openat(fd, component.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
      const int saved_errno = errno;
      ::close(fd);
      if (next < 0) { errno = saved_errno; return -1; }
      fd = next;
    }
    return fd;
  }
  int fail_anchored(std::string &err, const char *operation, int result = FAILED)
  {
    const int saved_errno = errno;
    err = std::string(operation) + ": " + std::strerror(saved_errno);
    abort();
    return result;
  }
  int prepare_anchored(const std::filesystem::path &root, const std::filesystem::path &parent,
                       const std::filesystem::path &name, std::string &err)
  {
    parent_fd_ = open_directory(root);
    if (parent_fd_ < 0) return fail_anchored(err, "cannot open COPY allowed directory");
    for (const auto &part : parent.lexically_relative(root)) {
      const std::string component = part.string();
      if (component.empty() || component == ".") continue;
      if (component == "..") { errno = EACCES; return fail_anchored(err, "COPY parent escaped allowed directory"); }
      const int next = ::openat(parent_fd_, component.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
      if (next < 0) return fail_anchored(err, "cannot open COPY parent component");
      ::close(parent_fd_);
      parent_fd_ = next;
    }
    parent_path_ = parent.string();
    final_name_ = name.string();
    struct stat status;
    if (::fstatat(parent_fd_, final_name_.c_str(), &status, AT_SYMLINK_NOFOLLOW) == 0) {
      errno = EEXIST;
      return fail_anchored(err, "COPY target already exists", EXISTS);
    }
    if (errno != ENOENT) return fail_anchored(err, "cannot inspect COPY target");
    static std::atomic<uint64_t> sequence{0};
    for (int attempt = 0; attempt < 128; ++attempt) {
      const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
      const std::string candidate = ".seekdb-copy-" + std::to_string(stamp) + "-" +
          std::to_string(sequence.fetch_add(1, std::memory_order_relaxed));
      if (::mkdirat(parent_fd_, candidate.c_str(), 0700) == 0) {
        temporary_name_ = candidate;
        temporary_directory_ = (parent / candidate).string();
        temporary_path_ = (parent / candidate / "data.csv").string();
        temporary_fd_ = ::openat(parent_fd_, candidate.c_str(), O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
        if (temporary_fd_ < 0) return fail_anchored(err, "cannot open COPY temporary directory");
        if (::fstat(temporary_fd_, &status) != 0) return fail_anchored(err, "cannot inspect COPY temporary directory");
        if (status.st_uid != ::geteuid() || (status.st_mode & 0077) != 0) {
          errno = EACCES;
          return fail_anchored(err, "COPY temporary directory is not private");
        }
        data_fd_ = ::openat(temporary_fd_, "data.csv", O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
        if (data_fd_ < 0) return fail_anchored(err, "cannot reserve COPY temporary file");
#ifdef __APPLE__
        writer_path_ = "/dev/fd/" + std::to_string(data_fd_);
#else
        writer_path_ = "/proc/self/fd/" + std::to_string(data_fd_);
#endif
        return SUCCESS;
      }
      if (errno != EEXIST) return fail_anchored(err, "cannot create COPY temporary directory");
    }
    errno = EEXIST;
    return fail_anchored(err, "cannot reserve COPY temporary directory");
  }
  int publish_anchored(std::string &err)
  {
    if (parent_fd_ < 0 || temporary_fd_ < 0 || data_fd_ < 0) {
      errno = EBADF;
      return fail_anchored(err, "COPY publication has no reserved file");
    }
    struct stat original_parent, current_parent, original_data, current_data;
    const int current_fd = open_directory(std::filesystem::path(parent_path_));
    if (current_fd < 0) return fail_anchored(err, "COPY parent changed during export");
    const bool same_parent = ::fstat(parent_fd_, &original_parent) == 0 && ::fstat(current_fd, &current_parent) == 0 &&
        original_parent.st_dev == current_parent.st_dev && original_parent.st_ino == current_parent.st_ino;
    ::close(current_fd);
    if (!same_parent) { errno = ESTALE; return fail_anchored(err, "COPY parent changed during export"); }
    if (::fstat(data_fd_, &original_data) != 0 ||
        ::fstatat(temporary_fd_, "data.csv", &current_data, AT_SYMLINK_NOFOLLOW) != 0) {
      return fail_anchored(err, "cannot inspect COPY reserved file");
    }
    if (!S_ISREG(current_data.st_mode) || original_data.st_dev != current_data.st_dev ||
        original_data.st_ino != current_data.st_ino) {
      errno = ESTALE;
      return fail_anchored(err, "COPY temporary file changed during export");
    }
    if (::fsync(data_fd_) != 0) return fail_anchored(err, "cannot flush COPY reserved file");
    if (::linkat(temporary_fd_, "data.csv", parent_fd_, final_name_.c_str(), 0) != 0) {
      return fail_anchored(err, "cannot publish COPY output", errno == EEXIST ? EXISTS : FAILED);
    }
    (void)::fsync(parent_fd_);
    abort();
    return SUCCESS;
  }
  int parent_fd_ = -1;
  int temporary_fd_ = -1;
  int data_fd_ = -1;
  std::string parent_path_;
  std::string final_name_;
  std::string temporary_name_;
  std::string writer_path_;
#endif
  static bool contains_nul(const std::string &value) { return value.find('\0') != std::string::npos; }
  static bool ascii_null(const std::string &value)
  {
    if (value.size() != 4) return false;
    const char *literal = "null";
    for (size_t i = 0; i < 4; ++i) {
      const char c = value[i] >= 'A' && value[i] <= 'Z' ? value[i] + ('a' - 'A') : value[i];
      if (c != literal[i]) return false;
    }
    return true;
  }
  static bool inside(const std::filesystem::path &root, const std::filesystem::path &path)
  {
    auto p = path.begin();
    for (auto r = root.begin(); r != root.end(); ++r, ++p) {
      if (p == path.end()) return false;
#ifdef _WIN32
      const std::wstring left = r->wstring();
      const std::wstring right = p->wstring();
      if (left.size() != right.size()) return false;
      for (size_t i = 0; i < left.size(); ++i) {
        const wchar_t l = left[i] >= L'A' && left[i] <= L'Z' ? left[i] + (L'a' - L'A') : left[i];
        const wchar_t q = right[i] >= L'A' && right[i] <= L'Z' ? right[i] + (L'a' - L'A') : right[i];
        if (l != q) return false;
      }
#else
      if (*r != *p) return false;
#endif
    }
    return true;
  }
  std::string final_path_;
  std::string temporary_directory_;
  std::string temporary_path_;
};

} // namespace file_sql
} // namespace sql
} // namespace oceanbase
