# V5 最低提测验证记录

## 目标与版本

本阶段目标是保留官方 V5 实现，打通源码编译、本地功能验证及正式提测流程，不声称获得性能优化分。

- 冻结 commit：`634888d9b037153fa143d2c913ba4b25503e1d62`
- 本地分支：`amd-cagra-submit`
- 算法及内核源码：未修改。
- 正式评测通过与否、得分均待平台确认。

## 本地环境

- AMD 平台远程 Linux 实例；ROCm 7.2.4，GPU 架构 gfx1100。
- GCC 13.3、CMake 3.31.10、Rust 1.97.1。
- CMake 默认选择冻结依赖内的 Clang 17.0.6，未额外强制替换编译器。
- 容器 cgroup 配额：16 核、55 GiB 内存；构建并发为 8。
- hipVS 安装路径 `/opt/hipvs`。尚未独立核验安装包对应的库源码 commit。
- 本地缓存依赖：`/opt/seekdb-deps-cache/ac032efc6b707fc0273e6b553db53011/3rd`，在源码的 `deps/3rd` 建立软链接。该链接仅用于本地环境，不应作为代码变更提交；正式评测使用组织方注入的冻结依赖。

## 编译复现

源码路径 `/workspace/seekdb-v5-src`；构建产物路径 `/workspace/seekdb-build-artifacts`。

```bash
export PATH=/opt/venv/bin:/opt/rocm/bin:$PATH
cd /workspace/seekdb-v5-src
HIPVS_ROOT=/opt/hipvs ROCM_ROOT=/opt/rocm \
  tools/hipvs_bridge/build.sh /workspace/seekdb-build-artifacts/libseekdb_cuvs_bridge.so
cmake -S . -B build_release -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DOB_ENABLE_UNITY=ON \
  -DOB_USE_LLD=ON \
  -DOB_BUILD_CUVS=ON \
  -DOB_BUILD_CUVS_TRACE=OFF \
  -DCUVS_BRIDGE_LIB=/workspace/seekdb-build-artifacts/libseekdb_cuvs_bridge.so
CARGO_NET_OFFLINE=true CMAKE_BUILD_PARALLEL_LEVEL=8 \
  cmake --build build_release --target seekdb
```

结果：bridge 和 `build_release/src/observer/seekdb` 均成功生成。日志见 `build-v5.log`。

## 独立服务

首次运行须使用新的、未被其他实例使用的数据目录；下列配置仅为本地验证配置，不是改动组织方评测配置。

```bash
export LD_LIBRARY_PATH=/workspace/seekdb-build-artifacts:/opt/hipvs/lib:/opt/rocm/lib
/workspace/seekdb-v5-src/build_release/src/observer/seekdb \
  --base-dir=/workspace/v5-validation --port=2881 --nodaemon \
  --parameter _memory_budget=16G \
  --parameter datafile_size=10G \
  --parameter datafile_maxsize=20G \
  --parameter log_disk_size=5G --log-level INFO
```

## CPU/GPU 本地对比方法

运行随附 `v5_local_smoke.py`，使用 Python `-I` 模式和 NumPy；通过已安装的 mysql CLI 连接服务，不需要额外安装 PyMySQL。

```bash
/opt/venv/bin/python -I /workspace/seekdb-build-artifacts/v5_local_smoke.py
```

脚本预期数据库 `contest_v5_smoke` 不存在；重复执行应选择新的数据库名，不应删除未知数据库。

- 输入：预装 `/seekdb_workspace/datasets/base.f32` 和 `query.f32`。
- 10,000 条 FP32、128 维向量；使用前 20 条查询。
- 在 NumPy 中对全部基础向量计算 FP32 平方 L2，得到本地验证用精确近邻；CPU ground truth 不参与被测 SQL 构建或近似检索。
- 先分别导入无索引表，再执行 `CREATE VECTOR INDEX`。
- 参数均为 `distance=l2,type=hnsw,m=16,ef_construction=200,ef_search=64`，仅切换 `lib=vsag/cuvs`。
- 构建时间包含 CREATE INDEX 和第一条 Top-10 SQL。
- 每组检查 Top-10/100 返回数量、ID 范围、无重复，并计算 Recall。
- QPS 和 P95 包含逐条启动 mysql 客户端的开销，仅用于诊断，不可替代官方持续连接 nq=4096 的性能测试。
- CPU 先运行，GPU 后运行，仅一轮，未做冷热状态配对；不可视为等质量、公平的正式性能结论。

## 实测结果

| 路径 | 构建含首查（秒） | Recall@10 | Recall@100 | Top-10 诊断 QPS | Top-100 诊断 QPS |
|---|---:|---:|---:|---:|---:|
| CPU VSAG | 2.666454 | 0.610 | 0.6475 | 166.803 | 159.078 |
| GPU CAGRA | 6.553624 | 1.000 | 1.000 | 76.477 | 75.736 |

GPU 构建在本轮小数据集上更慢；CPU/GPU Recall 不一致，不将耗时比解释为等质量加速比。原始记录见 `v5-smoke-report.json`。

日志观测到离线 DDL 阶段：

```text
serialized GPU CAGRA snapshot(n=10000, dim32=128, built=true, build_us=5863779, graph_bytes=10241713)
```

进程打开 `/dev/kfd` 和 `/dev/dri/renderD130`。日志与设备句柄仅为本地路径证据，不等同于正式 GPU profiler 核验。

## 重启验证状态

首次重启编排失败：旧进程收到 TERM 后已退出，但留下僵尸进程，`tail --pid` 等待超时（exit 124），因此后续启动步骤未执行；紧接着的 SQL 探测返回连接拒绝。检查旧进程为 `Z` 状态后，单独重新启动服务，恢复验证通过，未修改数据或索引。

恢复结果（同一 10K 演示数据、20 条查询）：

- CPU/GPU 两表均保留 10,000 行，Top-10/100 返回数量、ID 范围与唯一性全部通过。
- GPU Recall@10/100 均为 1.000；CPU 为 0.610/0.6475，与重启前一致。
- 重启后的日志观测到 `deserialized GPU CAGRA snapshot`，未观测到新的 GPU 快照序列化，也未观测到 `lazily built VSAG fallback`。
- 这些日志证明本次测试观测到快照重载；未做 profiler 级别的无重新建图证明，不将日志证据扩展为正式 GPU 抽检结论。
- 原始结果见 `v5-restart-report.json`，日志摘录见 `v5-restart-gpu-evidence.log`。

## 尚未覆盖

- SIFT250K 和 Hidden250K 正式数据集；不得据 10K 演示数据推断正式 Recall 达标。
- nq=4096、同轮冻结基线配对的 QPS/Recall 门槛。
- 外部 profiler 的构建、查询 GPU 抽检。
- 正式显存相对基线 1.25 倍及全轮 15 分钟资源限制。
- 组织方完整无网络编译容器；本地仅强制 Cargo 离线，没有做全环境网络隔离。
- 正式平台提交与评分。

## 提测前清单

1. 参赛分支保留冻结 V5 的祖先关系，不使用当前不继承 V5 的远端 master。
2. 经用户确认后才提交和推送分支；提测指定分支或完整 commit，避免误用 master。
3. 当前答题仓库公开，赛题要求私有；需用户解决此项后再正式提测。不得在公开文档记录令牌。
4. 用户确认已配置只读令牌，但尚未验证平台可拉取；令牌仅登记至比赛平台。
5. 保留本报告、测试脚本和真实结果。最低目标为全部硬门槛通过；未优化基线理论得分 4 分，实际以平台结果为准。
