// Pure C++ COPY publication protocol regression. No database or user files used.
#include "sql/engine/basic/ob_copy_file.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

namespace fs = std::filesystem;
using oceanbase::sql::file_sql::CopyFilePublication;
#define CHECK(value) do { if (!(value)) { std::cerr << "FAIL line " << __LINE__ << ": " #value "\n"; return 1; } } while (0)
static void put(const fs::path &p, const std::string &v) { std::ofstream(p, std::ios::binary) << v; }
static std::string get(const fs::path &p) { std::ifstream f(p, std::ios::binary); std::ostringstream s; s << f.rdbuf(); return s.str(); }

int main(int argc, char **argv)
{
  CHECK(argc == 2);
  const fs::path requested_root = fs::absolute(argv[1]);
  CHECK(fs::create_directory(requested_root));
  const fs::path root = fs::canonical(requested_root);
  const fs::path allowed = root / "allowed";
  const fs::path outside = root / "outside";
  fs::create_directory(allowed);
  fs::create_directory(outside);
  std::string err;
  CopyFilePublication copy;
  CHECK(copy.prepare((allowed / "ok.csv").string(), allowed.string(), err) == 0);
  const fs::path temp = copy.temporary_path();
  CHECK(temp.parent_path().parent_path() == allowed);
  CHECK(!fs::exists(allowed / "ok.csv"));
  put(temp, "ready\n");
  CHECK(copy.publish(err) == 0);
  CHECK(get(allowed / "ok.csv") == "ready\n");
  CHECK(!fs::exists(temp.parent_path()));
  CHECK(copy.prepare((allowed / "ok.csv").string(), allowed.string(), err) == CopyFilePublication::EXISTS);
  CHECK(get(allowed / "ok.csv") == "ready\n");

  CHECK(copy.prepare((allowed / "raced.csv").string(), allowed.string(), err) == 0);
  const fs::path raced_temp = copy.temporary_path();
  put(raced_temp, "our-data\n");
  put(allowed / "raced.csv", "other-writer\n");
  CHECK(copy.publish(err) == CopyFilePublication::EXISTS);
  CHECK(get(allowed / "raced.csv") == "other-writer\n");
  CHECK(!fs::exists(raced_temp.parent_path()));

  CopyFilePublication second;
  CHECK(copy.prepare((allowed / "double.csv").string(), allowed.string(), err) == 0);
  CHECK(second.prepare((allowed / "double.csv").string(), allowed.string(), err) == 0);
  CHECK(copy.temporary_path() != second.temporary_path());
  put(copy.temporary_path(), "first\n");
  put(second.temporary_path(), "second\n");
  CHECK(copy.publish(err) == 0);
  CHECK(second.publish(err) == CopyFilePublication::EXISTS);
  CHECK(get(allowed / "double.csv") == "first\n");

  CHECK(copy.prepare((allowed / "abort.csv").string(), allowed.string(), err) == 0);
  const fs::path abandoned = copy.temporary_path();
  put(abandoned, "incomplete\n");
  copy.abort();
  CHECK(!fs::exists(allowed / "abort.csv"));
  CHECK(!fs::exists(abandoned.parent_path()));
  fs::create_symlink(outside / "missing.csv", allowed / "dangling.csv");
  CHECK(copy.prepare((allowed / "dangling.csv").string(), allowed.string(), err) == CopyFilePublication::EXISTS);
  CHECK(fs::is_symlink(allowed / "dangling.csv"));
  CHECK(copy.prepare((outside / "escape.csv").string(), allowed.string(), err) != 0);
  CHECK(copy.prepare((allowed / ".." / "outside" / "escape.csv").string(), allowed.string(), err) != 0);
  fs::create_directory_symlink(outside, allowed / "link");
  CHECK(copy.prepare((allowed / "link" / "escape.csv").string(), allowed.string(), err) != 0);
  CHECK(copy.prepare((allowed / "null.csv").string(), "NULL", err) != 0);
  CHECK(copy.prepare((allowed / "null.csv").string(), "", err) != 0);
  std::string nul = (allowed / "null.csv").string();
  nul.push_back('\0');
  nul += "ignored";
  CHECK(copy.prepare(nul, allowed.string(), err) != 0);
#ifndef _WIN32
  // Replacing a parent after validation must not redirect writes or cleanup.
  const fs::path parent = allowed / "swap-parent";
  const fs::path displaced = allowed / "displaced-parent";
  fs::create_directory(parent);
  CHECK(copy.prepare((parent / "output.csv").string(), allowed.string(), err) == 0);
  const fs::path original_temp = copy.temporary_path();
  const std::string reserved_writer = copy.writer_path();
  fs::rename(parent, displaced);
  fs::create_directory_symlink(outside, parent);
  fs::create_directory(outside / original_temp.parent_path().filename());
  const fs::path outside_decoy = outside / original_temp.parent_path().filename() / "data.csv";
  put(outside_decoy, "outside-sentinel");
  put(reserved_writer, "owned-output");
  CHECK(get(outside_decoy) == "outside-sentinel");
  CHECK(get(displaced / original_temp.parent_path().filename() / "data.csv") == "owned-output");
  CHECK(copy.publish(err) != 0);
  CHECK(!fs::exists(outside / "output.csv"));
  CHECK(get(outside_decoy) == "outside-sentinel");
  CHECK(!fs::exists(displaced / original_temp.parent_path().filename()));
  fs::remove(parent);

  CHECK(copy.prepare((allowed / "substituted.csv").string(), allowed.string(), err) == 0);
  const fs::path replaced_file = copy.temporary_path();
  const fs::path sentinel = outside / "sentinel.csv";
  put(sentinel, "do-not-touch");
  fs::remove(replaced_file);
  fs::create_symlink(sentinel, replaced_file);
  CHECK(copy.publish(err) != 0);
  CHECK(get(sentinel) == "do-not-touch");
  CHECK(!fs::exists(allowed / "substituted.csv"));

  const auto fd_count = []() {
    int count = 0;
    for (int fd = 0; fd < 1024; ++fd) if (::fcntl(fd, F_GETFD) >= 0) ++count;
    return count;
  };
  const int descriptors_before = fd_count();
  for (int i = 0; i < 100; ++i) {
    CHECK(copy.prepare((allowed / "discard.csv").string(), allowed.string(), err) == 0);
    copy.abort();
  }
  CHECK(fd_count() == descriptors_before);
#endif
  fs::remove_all(root);
  std::cout << "COPY publication regression: PASS\n";
}
