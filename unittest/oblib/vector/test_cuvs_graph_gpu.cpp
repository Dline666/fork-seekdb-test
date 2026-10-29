// Real GPU diagnostic for graph-only snapshots. Compile against the real bridge;
// it is not included in the host mock test runner or official scoring.
#include "seekdb_cuvs_bridge.h"
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>
#include <sys/stat.h>

struct IndexOwner {
  void *handle = nullptr;
  explicit IndexOwner(void *h = nullptr) : handle(h) {}
  ~IndexOwner() { seekdb_cuvs_free(handle); }
  IndexOwner(const IndexOwner &) = delete;
  IndexOwner &operator=(const IndexOwner &) = delete;
};
static bool same_file(const std::string &first, const std::string &second)
{
  std::ifstream a(first, std::ios::binary), b(second, std::ios::binary);
  return a.good() && b.good() &&
      std::equal(std::istreambuf_iterator<char>(a), std::istreambuf_iterator<char>(),
                 std::istreambuf_iterator<char>(b), std::istreambuf_iterator<char>());
}
int main(int argc, char **argv)
{
  if (argc != 2) { std::fprintf(stderr, "usage: %s <new-output-directory>\n", argv[0]); return 2; }
  const std::string dir = argv[1];
  if (::mkdir(dir.c_str(), 0700) != 0) { std::perror("output directory must be new"); return 2; }
  const long n = 10000, dim = 128, nq = 32, k = 100;
  std::vector<float> base(n * dim), queries(nq * dim);
  // Deterministic FP32 test data, independent of all competition datasets.
  for (long row = 0; row < n; ++row)
    for (long column = 0; column < dim; ++column)
      base[row * dim + column] = std::sin(float(row * 31 + column * 17)) +
                                std::cos(float(row * 7 + column * 13) * .019f);
  for (long q = 0; q < nq; ++q)
    std::copy_n(base.data() + (q * 307 % n) * dim, dim, queries.data() + q * dim);
  std::vector<unsigned> expected(nq * k), actual(nq * k);
  std::vector<float> expected_dist(nq * k), actual_dist(nq * k);
  const std::string graph = dir + "/graph.bin", graph_copy = dir + "/graph-copy.bin";
  const std::string legacy = dir + "/legacy.bin";
  {
    IndexOwner built(seekdb_cuvs_build(base.data(), n, dim));
    if (!built.handle) return 3;
    if (seekdb_cuvs_search(built.handle, queries.data(), nq, k, expected.data(), expected_dist.data()) ||
        seekdb_cuvs_serialize_graph(built.handle, graph.c_str()) ||
        seekdb_cuvs_serialize(built.handle, legacy.c_str())) return 4;
  }
  for (int format = 0; format < 2; ++format) {
    IndexOwner loaded(format == 0
        ? seekdb_cuvs_deserialize_graph(graph.c_str(), base.data(), n, dim)
        : seekdb_cuvs_deserialize(legacy.c_str(), n, dim));
    if (!loaded.handle) return 5;
    if (seekdb_cuvs_search(loaded.handle, queries.data(), nq, k, actual.data(), actual_dist.data())) return 6;
    if (actual != expected) { std::fprintf(stderr, "format %d changed neighbor IDs\n", format); return 7; }
    for (size_t i = 0; i < actual.size(); ++i)
      if (!std::isfinite(actual_dist[i]) || std::fabs(actual_dist[i] - expected_dist[i]) > 1e-5f) return 8;
    if (seekdb_cuvs_serialize_graph(loaded.handle, graph_copy.c_str()) || !same_file(graph, graph_copy)) return 9;
  }
  IndexOwner invalid(seekdb_cuvs_deserialize_graph(graph.c_str(), base.data(), n - 1, dim));
  if (invalid.handle) return 10;
  std::printf("PASS real GPU: n=%ld dim=%ld nq=%ld k=%ld; graph bytes unchanged, graph/legacy restore IDs and distances match\n", n, dim, nq, k);
  return 0;
}
