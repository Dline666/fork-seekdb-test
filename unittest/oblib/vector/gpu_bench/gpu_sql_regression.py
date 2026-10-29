#!/usr/bin/env python3
"""Isolated GPU/CPU SQL edge regression, not formal scoring or a performance test.

Uses paired_sql_benchmark.Instance for owned-PID startup/cleanup and exact runtime
identity checking. Requires PyMySQL only when running SQL. No downloads.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
import random
import signal
import struct
import sys
import time

import paired_sql_benchmark as paired


@dataclass(frozen=True)
class Fixture:
    name: str
    valid_rows: tuple[tuple[int, str], ...]
    input_rows: tuple[tuple[int, str | None], ...]
    probes: tuple[str, ...]


def make_fixture(valid_count: int, dimension: int = 128) -> Fixture:
    if valid_count not in (0, 1023, 1024, 1025):
        raise ValueError('fixture supports only zero or requested batch-boundary sizes')
    rng = random.Random(73145)
    valid_rows = []
    input_rows = []
    for row in range(valid_count):
        # Distinct signed/noncontiguous SQL primary keys. All values are rounded
        # to binary32 before SQL serialization so exact SQL compares the same data.
        values = [struct.unpack('<f', struct.pack('<f', rng.gauss(0, 1)))[0]
                  for _ in range(dimension)]
        literal = paired.vector_literal(values)
        external_id = -130000 + row * 37
        valid_rows.append((external_id, literal))
        if row % 7 == 0:
            input_rows.append((900000000000 + row, None))
        input_rows.append((external_id, literal))
    if valid_count:
        # Leading/trailing NULLs ensure slots are compressed around file and
        # append-batch boundaries, not only in the middle of one contiguous run.
        input_rows.insert(0, (899999999999, None))
        input_rows.append((999999999999, None))
    positions = sorted({0, valid_count // 2, valid_count - 1}) if valid_count else []
    probes = tuple(valid_rows[i][1] for i in positions)
    return Fixture(f'mixed_{valid_count}', tuple(valid_rows), tuple(input_rows), probes)


def fixture_plan() -> tuple[Fixture, ...]:
    mixed = tuple(make_fixture(count) for count in (1023, 1024, 1025))
    return mixed + (
        Fixture('all_null', (), ((-9001, None), (0, None), (9001, None)), ()),
        Fixture('empty', (), (), ()),
    )


def query_sql(table: str, literal: str, k: int, approximate: bool) -> str:
    return (f"SELECT id FROM {table} WHERE v IS NOT NULL "
            f"ORDER BY l2_distance(v, '{literal}') "
            + ('APPROXIMATE ' if approximate else '') + f'LIMIT {k}')


def assert_ids(rows, allowed_ids: set[int], k: int) -> list[int]:
    ids = [row[0] for row in rows]
    expected_count = min(k, len(allowed_ids))
    if len(ids) != expected_count or len(set(ids)) != expected_count:
        raise RuntimeError(f'expected {expected_count} distinct IDs, got {ids}')
    if not set(ids).issubset(allowed_ids):
        raise RuntimeError('returned ID belongs to NULL input or another row')
    return ids


def exercise_table(connection, library: str, fixture: Fixture, report: dict, output: Path) -> dict:
    table = f'edge_reg.{library}_{fixture.name}'
    ef_search = 64 if library == 'cuvs' else 200
    valid_ids = {row[0] for row in fixture.valid_rows}
    paired.execute(connection, f'CREATE TABLE {table}(id BIGINT PRIMARY KEY, v VECTOR(128))')
    with connection.cursor() as cursor:
        for start in range(0, len(fixture.input_rows), 100):
            cursor.executemany(f'INSERT INTO {table}(id,v) VALUES(%s,%s)',
                               fixture.input_rows[start:start + 100])
    counts = paired.execute(connection, f'SELECT COUNT(*), COUNT(v) FROM {table}')[0]
    if counts != (len(fixture.input_rows), len(fixture.valid_rows)):
        raise RuntimeError('fixture row/NULL count mismatch')

    empty_probe = paired.vector_literal([0.0] * 128)
    probes = fixture.probes if fixture.probes else (empty_probe,)
    truth = {}
    for probe_index, literal in enumerate(probes):
        for k in (10, 100):
            truth[(probe_index, k)] = assert_ids(
                paired.execute(connection, query_sql(table, literal, k, False)), valid_ids, k)

    # No exceptions are accepted as a successful edge case. If the original
    # baseline rejects an input, that must be inspected explicitly, not swallowed.
    paired.execute(connection,
        f'CREATE VECTOR INDEX idx_{library}_{fixture.name} ON {table}(v) '
        f'WITH (distance=l2, type=hnsw, lib={library}, m=16, '
        f'ef_construction=200, ef_search={ef_search})')
    details = {'passed': False, 'input_rows': len(fixture.input_rows),
               'valid_vectors': len(fixture.valid_rows), 'library': library,
               'checks': [], 'first_and_last_vector_found': not fixture.valid_rows}
    for probe_index, literal in enumerate(probes):
        for k in (10, 100):
            got = assert_ids(paired.execute(connection, query_sql(table, literal, k, True)), valid_ids, k)
            expected = truth[(probe_index, k)]
            recall = len(set(got).intersection(expected)) / len(expected) if expected else 1.0
            if recall < 0.95:
                raise RuntimeError(f'{table} probe {probe_index} top{k} recall={recall}')
            if got and got[0] != expected[0]:
                raise RuntimeError(f'{table}: exact self probe nearest ID missing, possible dropped tail')
            details['checks'].append({'probe': probe_index, 'k': k, 'recall': recall,
                                      'result_integrity': True, 'ids': got})
    details['first_and_last_vector_found'] = True
    # Create/drop/recreate on the same table exercises a second DDL lifecycle;
    # it may expose a stale staging count even if the initial flush looked valid.
    paired.execute(connection, f'DROP INDEX idx_{library}_{fixture.name} ON {table}')
    paired.execute(connection,
        f'CREATE VECTOR INDEX idx_{library}_{fixture.name} ON {table}(v) '
        f'WITH (distance=l2, type=hnsw, lib={library}, m=16, '
        f'ef_construction=200, ef_search={ef_search})')
    for probe_index, literal in enumerate(probes):
        got = assert_ids(paired.execute(connection, query_sql(table, literal, 10, True)), valid_ids, 10)
        expected = truth[(probe_index, 10)]
        if expected and (got[0] != expected[0] or len(set(got).intersection(expected)) / len(expected) < .95):
            raise RuntimeError(f'{table}: recreation lost or mismapped vectors')
    details['recreate_index_passed'] = True
    details['passed'] = True
    report['cases'][f'{library}/{fixture.name}'] = details
    paired.write_json(output / 'summary.json', report)
    print(f'PASS {library}/{fixture.name}: valid={len(fixture.valid_rows)} total={len(fixture.input_rows)}', flush=True)
    return details


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--bridge', type=Path, required=True)
    p.add_argument('--commit', required=True)
    p.add_argument('--output-dir', type=Path, required=True)
    p.add_argument('--port', type=int, default=2983)
    p.add_argument('--cpu-count', type=int, default=8)
    p.add_argument('--memory-budget', default='8G')
    p.add_argument('--datafile-size', default='2G')
    p.add_argument('--datafile-maxsize', default='4G')
    p.add_argument('--log-disk-size', default='2G')
    p.add_argument('--startup-timeout', type=int, default=180)
    p.add_argument('--query-timeout', type=int, default=180)
    args = p.parse_args(argv)
    for name in ('binary', 'bridge', 'output_dir'):
        setattr(args, name, getattr(args, name).resolve())
    if not 1024 <= args.port <= 65535 or args.cpu_count < 1:
        p.error('invalid SQL port or CPU count')
    return args


def main(argv=None):
    args = parse_args(argv)
    if not sys.platform.startswith('linux'):
        raise RuntimeError('actual SQL regression requires Linux; use host tests for fixture validation')
    cpus = sorted(os.sched_getaffinity(0))
    if len(cpus) < args.cpu_count:
        raise RuntimeError('requested CPUs unavailable')
    args.cpu_affinity = cpus[:args.cpu_count]
    args.output_dir.mkdir(parents=True, exist_ok=False)
    report = {'scope': 'batch_input_edge_SQL_regression_not_official_score',
              'passed': False, 'cases': {}, 'binary': paired.executable_info(args.binary, args.bridge, args.commit),
              'notes': ['WHERE v IS NOT NULL intentionally tests NULL filtering; CPU fallback is not a scoring gate in this edge test.',
                        'Internal NOP markers and both C++ caller branches cannot be forced or proven solely through this public SQL runner.',
                        'No GPU profiler or build speedup claim is made.']}
    paired.write_json(args.output_dir / 'summary.json', report)
    instance = paired.Instance(args.binary, args.bridge, args.output_dir / 'instance', args)
    connection = None
    try:
        instance.start()
        connection = instance.connect()
        paired.execute(connection, f'SET ob_query_timeout={args.query_timeout * 1000000}')
        paired.execute(connection, 'CREATE DATABASE edge_reg')
        fixtures = fixture_plan()
        for library in ('cuvs', 'vsag'):
            for fixture in fixtures:
                exercise_table(connection, library, fixture, report, args.output_dir)
        for fixture in fixtures:
            gpu = report['cases']['cuvs/' + fixture.name]
            cpu = report['cases']['vsag/' + fixture.name]
            if gpu['valid_vectors'] != cpu['valid_vectors'] or not gpu['passed'] or not cpu['passed']:
                raise RuntimeError('CPU/GPU case coverage mismatch')
        report['passed'] = True
        return 0
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        try:
            if connection is not None:
                connection.close()
        finally:
            try:
                instance.stop()
            finally:
                paired.write_json(args.output_dir / 'summary.json', report)


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except (Exception, KeyboardInterrupt) as error:
        print(f'FAIL: {type(error).__name__}: {error}', file=sys.stderr)
        sys.exit(2)
