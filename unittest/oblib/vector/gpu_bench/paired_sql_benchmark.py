#!/usr/bin/env python3
"""Public FP32/L2 SQL comparison; not the organizer's official score.

Preparation uses only Python's standard library. Actual SQL runs need PyMySQL,
Linux, taskset, and two independently built seekdb binaries/bridge libraries.
No input is downloaded, no ground truth is calculated during measurements,
and no existing output/data directory is reused or removed.
"""
from __future__ import annotations

import argparse
import array
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import time
from typing import Iterable

TOPKS = (10, 100)
BASELINE_COMMIT = "634888d9b037153fa143d2c913ba4b25503e1d62"
INDEX_SQL = (
    "CREATE VECTOR INDEX idx_items ON items(v) "
    "WITH (distance=l2, type=hnsw, lib=cuvs, m=16, "
    "ef_construction=200, ef_search=64)"
)
SCOPE = "public_data_local_SQL_comparison_not_official_score"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, value: object) -> None:
    """Atomic checkpoint in a directory created for this invocation."""
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def vector_records(path: Path, kind: str) -> Iterable[array.array]:
    if kind not in ("f", "i"):
        raise ValueError("vector kind must be f or i")
    if array.array(kind).itemsize != 4:
        raise RuntimeError("this interpreter does not provide 32-bit array elements")
    width = None
    with path.open("rb") as source:
        row = 0
        while True:
            header = source.read(4)
            if not header:
                break
            if len(header) != 4:
                raise ValueError(f"{path}: truncated dimension at row {row}")
            dimension = struct.unpack("<i", header)[0]
            if not 0 < dimension <= 1048576:
                raise ValueError(f"{path}: invalid width {dimension} at row {row}")
            if width is None:
                width = dimension
            if dimension != width:
                raise ValueError(f"{path}: inconsistent dimensions at row {row}")
            payload = source.read(dimension * 4)
            if len(payload) != dimension * 4:
                raise ValueError(f"{path}: truncated values at row {row}")
            values = array.array(kind)
            values.frombytes(payload)
            if sys.byteorder != "little":
                values.byteswap()
            if kind == "f" and not all(math.isfinite(value) for value in values):
                raise ValueError(f"{path}: non-finite FP32 input at row {row}")
            yield values
            row += 1
    if width is None:
        raise ValueError(f"{path}: empty vector file")


def vector_literal(values: Iterable[float]) -> str:
    # Nine significant decimal digits round-trip every finite binary32 value.
    return "[" + ",".join("-0.0" if value == 0 and math.copysign(1.0, value) < 0
                          else format(value, ".9g") for value in values) + "]"


@dataclass(frozen=True)
class IDMapping:
    count: int
    offset: int
    stride: int

    def __post_init__(self) -> None:
        if self.count < 1 or self.stride < 1:
            raise ValueError("positive row count and ID stride required")
        last = self.offset + (self.count - 1) * self.stride
        if self.offset < -(1 << 63) or last >= (1 << 63):
            raise ValueError("external ID mapping exceeds signed BIGINT")

    def external(self, row: int) -> int:
        if row < 0 or row >= self.count:
            raise ValueError("ground truth row index is outside the input base")
        return self.offset + row * self.stride

    def contains(self, value: object) -> bool:
        if not isinstance(value, int) or isinstance(value, bool):
            return False
        delta = value - self.offset
        return delta >= 0 and delta % self.stride == 0 and delta // self.stride < self.count


def prepare_inputs(args: argparse.Namespace, directory: Path) -> dict:
    directory.mkdir(exist_ok=False)
    mapping = IDMapping(args.expected_count, args.id_offset, args.id_stride)
    count = 0
    tsv = directory / "base.tsv"
    with tsv.open("x") as destination:
        for row in vector_records(args.base, "f"):
            if len(row) != args.expected_dim:
                raise ValueError("base dimension differs from --expected-dim")
            if count >= args.expected_count:
                raise ValueError("base contains more rows than --expected-count; never silently truncate")
            destination.write(f"{mapping.external(count)}\t{vector_literal(row)}\n")
            count += 1
    if count != args.expected_count:
        raise ValueError(f"base count {count} differs from expected {args.expected_count}")
    if count < max(TOPKS):
        raise ValueError("at least 100 base rows are required")

    literals = []
    query_rows = 0
    for row in vector_records(args.queries, "f"):
        if len(row) != args.expected_dim:
            raise ValueError("query dimension does not match base dimension")
        if query_rows < args.nq:
            literals.append(vector_literal(row))
        query_rows += 1
    if query_rows < args.nq:
        raise ValueError("query file contains fewer rows than --nq")

    truths = {str(k): [] for k in TOPKS}
    truth_rows = 0
    for row in vector_records(args.ground_truth, "i"):
        if len(row) < max(TOPKS):
            raise ValueError("ground truth must contain at least Top-100 for every query")
        # Validate every supplied truth row, including unused query suffixes.
        relevant = row[:max(TOPKS)]
        if len(set(relevant)) != max(TOPKS):
            raise ValueError(f"duplicate IDs in ground truth row {truth_rows}")
        external = [mapping.external(index) for index in relevant]
        if truth_rows < args.nq:
            for k in TOPKS:
                truths[str(k)].append(external[:k])
        truth_rows += 1
    if truth_rows != query_rows:
        raise ValueError("ground truth and query files must contain the same number of rows")

    sql = {str(k): [
        f"SELECT id FROM items ORDER BY l2_distance(v, '{literal}') APPROXIMATE LIMIT {k}"
        for literal in literals
    ] for k in TOPKS}
    write_json(directory / "query-sql.json", sql)
    write_json(directory / "ground-truth-external-ids.json", truths)
    manifest = {
        "count": count, "dimension": args.expected_dim, "nq": args.nq,
        "available_queries": query_rows, "query_selection": f"first {args.nq}, unchanged order",
        "topk": list(TOPKS), "format": "little-endian fvecs / 0-based row-index ivecs",
        "id_mapping": {"offset": args.id_offset, "stride": args.id_stride},
        "base": {"path": str(args.base), "sha256": file_sha256(args.base)},
        "queries": {"path": str(args.queries), "sha256": file_sha256(args.queries)},
        "ground_truth": {"path": str(args.ground_truth), "sha256": file_sha256(args.ground_truth)},
        "tsv_sha256": file_sha256(tsv),
        "ground_truth_mathematical_correctness": "caller responsibility; structural checks only",
        "input_semantics": "FP32 values preserved through 9-significant-digit SQL serialization; L2",
    }
    write_json(directory / "input-manifest.json", manifest)
    return manifest


def nearest_rank_p95(values: list[float]) -> float:
    if not values or any(not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("finite nonnegative latency observations required")
    return sorted(values)[math.ceil(0.95 * len(values)) - 1]


def assess_results(predictions: list[list[int]], truth: list[list[int]], k: int,
                   mapping: IDMapping, latencies: list[float], wall_seconds: float) -> dict:
    if len(predictions) != len(truth) or len(predictions) != len(latencies) or not predictions:
        raise ValueError("prediction, truth and latency row counts must match")
    if not math.isfinite(wall_seconds) or wall_seconds <= 0:
        raise ValueError("invalid elapsed query time")
    failures = []
    hits = 0
    for row, (found, expected) in enumerate(zip(predictions, truth)):
        if len(expected) != k or len(set(expected)) != k:
            raise ValueError("ground truth contains malformed Top-k rows")
        valid = len(found) == k and len(set(found)) == k and all(mapping.contains(v) for v in found)
        if not valid:
            failures.append(row)
        # Count real set hits even on failed integrity rows; do not silently pad IDs.
        hits += len(set(found).intersection(expected))
    return {
        "nq": len(predictions), "k": k, "wall_seconds": wall_seconds,
        "qps": len(predictions) / wall_seconds,
        "p95_ms": nearest_rank_p95(latencies) * 1000,
        "mean_latency_ms": statistics.mean(latencies) * 1000,
        "recall": hits / (len(predictions) * k),
        "result_integrity": not failures,
        "integrity_failure_count": len(failures), "integrity_failure_rows_first20": failures[:20],
    }


def summarize_pairs(runs: list[dict]) -> dict:
    pairs = []
    all_repetitions = sorted({run["repetition"] for run in runs})
    for repetition in all_repetitions:
        subset = {run["version"]: run for run in runs if run["repetition"] == repetition}
        if set(subset) != {"baseline", "candidate"}:
            continue
        base, candidate = subset["baseline"], subset["candidate"]
        if base.get("status") != "complete" or candidate.get("status") != "complete":
            continue
        ratios = {str(k): candidate["queries"][str(k)]["qps"] / base["queries"][str(k)]["qps"]
                  for k in TOPKS}
        query_ratio = math.sqrt(ratios["10"] * ratios["100"])
        recall_checks = {str(k): {
            "candidate": candidate["queries"][str(k)]["recall"],
            "baseline": base["queries"][str(k)]["recall"],
            "absolute_minimum_0_95": candidate["queries"][str(k)]["recall"] >= 0.95,
            "baseline_delta_at_least_minus_0_005":
                candidate["queries"][str(k)]["recall"] + 0.005 + 1e-12 >= base["queries"][str(k)]["recall"],
        } for k in TOPKS}
        integrity = all(run["queries"][str(k)]["result_integrity"]
                        for run in (base, candidate) for k in TOPKS)
        checks = {"result_integrity_both_versions": integrity,
                  "candidate_query_ratio_at_least_0_90": query_ratio >= 0.90,
                  "candidate_recall": recall_checks}
        pairs.append({
            "repetition": repetition,
            "build_speedup": base["build_plus_first_query_seconds"] / candidate["build_plus_first_query_seconds"],
            "query_speedup_geomean": query_ratio, "qps_ratios": ratios,
            "local_observed_checks": checks,
            "local_observed_checks_passed": integrity and query_ratio >= 0.90 and all(
                v["absolute_minimum_0_95"] and v["baseline_delta_at_least_minus_0_005"]
                for v in recall_checks.values()),
        })
    return {
        "completed_pairs": len(pairs), "pairs": pairs,
        "median_build_speedup": statistics.median(p["build_speedup"] for p in pairs) if pairs else None,
        "median_query_speedup": statistics.median(p["query_speedup_geomean"] for p in pairs) if pairs else None,
        "official_gates_verified": False, "official_score": None,
        "unverified": ["organizer input identity", "Hidden250K", "external GPU profiler",
                       "peak VRAM and relative VRAM limit", "formal host memory and resource quotas"],
    }


def process_owns_tcp_port(pid: int, port: int) -> bool:
    """Avoid connecting to an unrelated server that raced us for the SQL port."""
    try:
        inodes = set()
        for descriptor in Path(f"/proc/{pid}/fd").iterdir():
            try:
                target = os.readlink(descriptor)
                if target.startswith("socket:["):
                    inodes.add(target[8:-1])
            except FileNotFoundError:
                continue
        for table in ("tcp", "tcp6"):
            for line in Path(f"/proc/{pid}/net/{table}").read_text().splitlines()[1:]:
                fields = line.split()
                if len(fields) > 9 and fields[3] == "0A" and int(fields[1].split(":")[1], 16) == port:
                    if fields[9] in inodes:
                        return True
    except (FileNotFoundError, ProcessLookupError):
        pass
    return False


class Instance:
    def __init__(self, binary: Path, bridge: Path, directory: Path, args: argparse.Namespace):
        self.binary, self.bridge, self.directory, self.args = binary, bridge, directory, args
        self.process = None
        self.console = None
        self.env = os.environ.copy()
        self.env["LD_LIBRARY_PATH"] = str(bridge.parent) + ":/opt/hipvs/lib:/opt/rocm/lib"
        self.env["OMP_NUM_THREADS"] = str(args.cpu_count)
        self.env["OPENBLAS_NUM_THREADS"] = "1"
        self.env.pop("OB_VSAG_USE_CUVS", None)
        self.env.pop("OB_VSAG_TRACE", None)
        self.env.pop("OB_VSAG_TRACE_FILE", None)

    def connect(self):
        import pymysql
        return pymysql.connect(host="127.0.0.1", port=self.args.port, user="root", autocommit=True,
                               local_infile=True, connect_timeout=3,
                               read_timeout=self.args.query_timeout, write_timeout=self.args.query_timeout)

    def start(self) -> None:
        import pymysql
        self.directory.mkdir(exist_ok=False)
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", self.args.port)) == 0:
                raise RuntimeError("SQL port already in use; refusing to attach to an existing service")
        cpus = self.args.cpu_affinity
        command = ["taskset", "-c", ",".join(map(str, cpus)), str(self.binary), "--nodaemon",
                   "--base-dir", str(self.directory / "data"), "--port", str(self.args.port),
                   "--parameter", "memory_budget=" + self.args.memory_budget,
                   "--parameter", "datafile_size=" + self.args.datafile_size,
                   "--parameter", "datafile_maxsize=" + self.args.datafile_maxsize,
                   "--parameter", "log_disk_size=" + self.args.log_disk_size,
                   "--parameter", "cpu_count=" + str(self.args.cpu_count),
                   "--parameter", "local_ip=127.0.0.1"]
        write_json(self.directory / "command.json", command)
        self.console = (self.directory / "console.log").open("xb")
        self.process = subprocess.Popen(command, env=self.env, stdout=self.console, stderr=subprocess.STDOUT)
        write_json(self.directory / "owned-process.json", {"pid": self.process.pid, "binary": str(self.binary)})
        deadline = time.monotonic() + self.args.startup_timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("owned seekdb process exited before readiness; see console.log")
            if process_owns_tcp_port(self.process.pid, self.args.port):
                try:
                    connection = self.connect()
                    connection.close()
                    actual_binary = Path(os.readlink(f"/proc/{self.process.pid}/exe")).resolve()
                    if actual_binary != self.binary:
                        raise RuntimeError("owned PID is not executing the requested seekdb binary")
                    loaded_paths = []
                    for line in Path(f"/proc/{self.process.pid}/maps").read_text().splitlines():
                        fields = line.split(maxsplit=5)
                        if len(fields) == 6 and fields[5].startswith("/"):
                            loaded_paths.append(str(Path(fields[5]).resolve()))
                    if str(self.bridge) not in loaded_paths:
                        raise RuntimeError("requested bridge is not loaded by the owned seekdb PID")
                    write_json(self.directory / "loaded-runtime.json", {
                        "binary": str(actual_binary), "bridge": str(self.bridge),
                        "bridge_seen_in_proc_maps": True,
                    })
                    return
                except pymysql.Error:
                    pass
            time.sleep(1)
        raise TimeoutError("owned seekdb process did not become SQL-ready before timeout")

    def stop(self) -> None:
        # Popen is the sole authority: never pkill/killall, process name scans,
        # or termination of a service that was not started by this run.
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=15)
            self.process = None
        if self.console is not None:
            self.console.close()
            self.console = None


def execute(connection, statement: str):
    with connection.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def validate_first(rows, mapping: IDMapping) -> None:
    ids = [row[0] for row in rows]
    if len(ids) != 10 or len(set(ids)) != 10 or not all(mapping.contains(value) for value in ids):
        raise RuntimeError("first-query result integrity failed")


def run_one(args, prepared: Path, version: str, repetition: int, output: Path) -> dict:
    binary = getattr(args, version + "_binary")
    bridge = getattr(args, version + "_bridge")
    directory = output / f"pair-{repetition:02d}-{version}"
    query_sql = json.loads((prepared / "query-sql.json").read_text())
    truths = json.loads((prepared / "ground-truth-external-ids.json").read_text())
    mapping = IDMapping(args.expected_count, args.id_offset, args.id_stride)
    report = {"version": version, "repetition": repetition, "status": "running", "queries": {}}
    instance = Instance(binary, bridge, directory, args)
    connection = None
    try:
        instance.start()
        connection = instance.connect()
        execute(connection, "set ob_query_timeout=" + str(args.query_timeout * 1000000))
        # Fresh data directory and fresh process mean no reuse of graphs, process
        # caches or old SQL state. Only immutable TSV/query/truth files are shared.
        execute(connection, "CREATE DATABASE amd_public_pair")
        execute(connection, "USE amd_public_pair")
        execute(connection, f"CREATE TABLE items(id BIGINT PRIMARY KEY, v VECTOR({args.expected_dim}))")
        base_path = str(prepared / "base.tsv")
        with connection.cursor() as cursor:
            cursor.execute("LOAD DATA LOCAL INFILE %s INTO TABLE items "
                           "FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (id,v)", (base_path,))
        actual_rows = execute(connection, "SELECT COUNT(*) FROM items")[0][0]
        if actual_rows != args.expected_count:
            raise RuntimeError(f"LOAD DATA row count mismatch: {actual_rows}")
        indexes = execute(connection, "SHOW INDEX FROM items")
        if any(row[2] != "PRIMARY" for row in indexes):
            raise RuntimeError("unexpected secondary index exists before timed DDL")
        first_query = query_sql["10"][0]
        tick = time.perf_counter()
        execute(connection, INDEX_SQL)
        first_rows = execute(connection, first_query)
        report["build_plus_first_query_seconds"] = time.perf_counter() - tick
        validate_first(first_rows, mapping)
        report["first_query_integrity"] = True
        write_json(directory / "report.json", report)

        for k in TOPKS:
            predictions = []
            latencies = []
            # SQL text and ground truth are already prepared. Do no validation,
            # FP32 formatting, exact search or Recall work inside this interval.
            started = time.perf_counter()
            for statement in query_sql[str(k)]:
                before = time.perf_counter()
                rows = execute(connection, statement)
                elapsed = time.perf_counter() - before
                latencies.append(elapsed)
                predictions.append(rows)
            wall_seconds = time.perf_counter() - started
            plain_ids = [[row[0] for row in rows] for rows in predictions]
            del predictions
            report["queries"][str(k)] = assess_results(
                plain_ids, truths[str(k)], k, mapping, latencies, wall_seconds)
            write_json(directory / f"top{k}-ids.json", plain_ids)
            write_json(directory / f"top{k}-latency-seconds.json", latencies)
            write_json(directory / "report.json", report)
            print(json.dumps({"version": version, "pair": repetition, "k": k,
                              **report["queries"][str(k)]}), flush=True)
        report["status"] = "complete"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                instance.stop()
            finally:
                if directory.exists():
                    write_json(directory / "report.json", report)
    return report


def executable_info(binary: Path, bridge: Path, commit: str | None) -> dict:
    if not binary.is_file() or not os.access(binary, os.X_OK) or not bridge.is_file():
        raise ValueError("binary must be executable and bridge must exist")
    environment = os.environ.copy()
    environment["LD_LIBRARY_PATH"] = str(bridge.parent) + ":/opt/hipvs/lib:/opt/rocm/lib"
    result = subprocess.run([str(binary), "--version"], env=environment, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30, check=True)
    if commit and commit not in result.stdout:
        raise ValueError(f"binary does not report expected commit {commit}")
    return {"binary": str(binary), "binary_sha256": file_sha256(binary),
            "bridge": str(bridge), "bridge_sha256": file_sha256(bridge),
            "version_output": result.stdout, "expected_commit": commit}


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True, help="must not already exist")
    parser.add_argument("--baseline-binary", type=Path)
    parser.add_argument("--baseline-bridge", type=Path)
    parser.add_argument("--candidate-binary", type=Path)
    parser.add_argument("--candidate-bridge", type=Path)
    parser.add_argument("--baseline-commit", default=BASELINE_COMMIT)
    parser.add_argument("--candidate-commit", help="expected full commit in candidate --version")
    parser.add_argument("--expected-count", type=int, default=250000)
    parser.add_argument("--expected-dim", type=int, default=128)
    parser.add_argument("--nq", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=2, help="pairs; default yields B/C then C/B")
    parser.add_argument("--id-offset", type=int, default=-130000)
    parser.add_argument("--id-stride", type=int, default=37)
    parser.add_argument("--port", type=int, default=2883)
    parser.add_argument("--cpu-count", type=int, default=16)
    parser.add_argument("--memory-budget", default="16G")
    parser.add_argument("--datafile-size", default="4G")
    parser.add_argument("--datafile-maxsize", default="12G")
    parser.add_argument("--log-disk-size", default="2G")
    parser.add_argument("--startup-timeout", type=int, default=240)
    parser.add_argument("--query-timeout", type=int, default=600)
    parser.add_argument("--prepare-only", action="store_true", help="validate/serialize input without GPU or PyMySQL")
    args = parser.parse_args(argv)
    for name in ("expected_count", "expected_dim", "nq", "repeats", "cpu_count", "startup_timeout", "query_timeout"):
        if getattr(args, name) <= 0:
            parser.error(name + " must be positive")
    if args.repeats < 2 and not args.prepare_only:
        parser.error("at least two pairs required to exchange baseline/candidate order")
    if not 1024 <= args.port <= 65535:
        parser.error("port must be between 1024 and 65535")
    for name in ("memory_budget", "datafile_size", "datafile_maxsize", "log_disk_size"):
        if not re.fullmatch(r"[1-9][0-9]*[MG]", getattr(args, name)):
            parser.error(name + " must use a positive integer followed by M or G")
    for name in ("base", "queries", "ground_truth", "output_dir", "baseline_binary", "baseline_bridge", "candidate_binary", "candidate_bridge"):
        value = getattr(args, name)
        if value is not None:
            setattr(args, name, value.resolve())
    if not args.prepare_only:
        for name in ("baseline_binary", "baseline_bridge", "candidate_binary", "candidate_bridge", "candidate_commit"):
            if not getattr(args, name):
                parser.error("--" + name.replace("_", "-") + " is required for SQL runs")
        if not re.fullmatch(r"[0-9a-f]{40}", args.candidate_commit):
            parser.error("candidate commit must be a full 40-character Git SHA")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.output_dir.exists():
        raise ValueError("output directory already exists; no files or SQL state will be overwritten")
    args.output_dir.mkdir(parents=True, exist_ok=False)
    prepared = args.output_dir / "prepared-inputs"
    report = {"scope": SCOPE, "official_score": None, "status": "preparing", "runs": []}
    write_json(args.output_dir / "summary.json", report)
    try:
        report["input"] = prepare_inputs(args, prepared)
        if args.prepare_only:
            report["status"] = "input_prepared"
            print("PASS FP32/ivecs input preparation:", prepared, flush=True)
            return 0
        if not sys.platform.startswith("linux") or not shutil.which("taskset"):
            raise RuntimeError("actual benchmark requires Linux with taskset")
        import pymysql  # noqa: F401 -- fail before starting any server
        allowed_cpus = sorted(os.sched_getaffinity(0))
        if len(allowed_cpus) < args.cpu_count:
            raise ValueError("requested CPU count exceeds current allowed CPU affinity")
        args.cpu_affinity = allowed_cpus[:args.cpu_count]
        report["resources"] = {name: getattr(args, name) for name in (
            "cpu_count", "cpu_affinity", "memory_budget", "datafile_size", "datafile_maxsize", "log_disk_size", "port")}
        report["resource_limit_notes"] = "CPU affinity enforced; memory_budget is a seekdb setting, not a cgroup hard limit"
        report["binaries"] = {version: executable_info(getattr(args, version + "_binary"),
                                                     getattr(args, version + "_bridge"),
                                                     getattr(args, version + "_commit"))
                              for version in ("baseline", "candidate")}
        report["index_sql"] = INDEX_SQL
        report["status"] = "running"
        write_json(args.output_dir / "summary.json", report)
        for repetition in range(args.repeats):
            order = ("baseline", "candidate") if repetition % 2 == 0 else ("candidate", "baseline")
            for version in order:
                print(f"Starting pair {repetition}: {version}", flush=True)
                try:
                    run = run_one(args, prepared, version, repetition, args.output_dir)
                except BaseException:
                    path = args.output_dir / f"pair-{repetition:02d}-{version}" / "report.json"
                    if path.is_file():
                        report["runs"].append(json.loads(path.read_text()))
                    raise
                report["runs"].append(run)
                report["comparison"] = summarize_pairs(report["runs"])
                write_json(args.output_dir / "summary.json", report)
        report["status"] = "complete"
        print(json.dumps(report["comparison"], indent=2), flush=True)
        return 0 if all(p["local_observed_checks_passed"] for p in report["comparison"]["pairs"]) else 1
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        write_json(args.output_dir / "summary.json", report)


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except (Exception, KeyboardInterrupt) as error:
        print(f"FAILED: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(2)
