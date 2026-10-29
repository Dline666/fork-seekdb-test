#!/usr/bin/env python3
"""GPU bridge diagnostic, not the organizer's end-to-end SQL score.

No Python dependencies. Each library runs in a fresh child process. Input vectors
remain FP32; provide matching fvecs/ivecs files for meaningful Recall measurements.
"""
import argparse
import array
import ctypes
import json
import math
from pathlib import Path
import statistics
import struct
import subprocess
import sys
import tempfile
import time


def read_vectors(path, kind, limit=None):
    values = array.array(kind)
    rows = 0
    dimension = None
    with open(path, 'rb') as source:
        while limit is None or rows < limit:
            header = source.read(4)
            if not header:
                break
            if len(header) != 4:
                raise ValueError('truncated vector dimension')
            width = struct.unpack('<i', header)[0]
            if width <= 0 or width > 1048576 or (dimension is not None and width != dimension):
                raise ValueError('invalid or inconsistent vector dimensions')
            dimension = width
            payload = source.read(width * 4)
            if len(payload) != width * 4:
                raise ValueError('truncated vector data')
            values.frombytes(payload)
            rows += 1
    if not rows:
        raise ValueError('empty vector file')
    if sys.byteorder != 'little':
        values.byteswap()
    return values, rows, dimension


def worker(args):
    if bool(args.base) != bool(args.queries):
        raise ValueError('--base and --queries must be supplied together')
    if args.base:
        base, count, dimension = read_vectors(args.base, 'f')
        queries, available, query_dim = read_vectors(args.queries, 'f', args.nq)
        if dimension != query_dim or available < args.nq:
            raise ValueError('query shape mismatch or insufficient query vectors')
    else:
        count, dimension = 4096, 32
        base = array.array('f', (math.sin(row * 0.71 + column * 1.13) +
                                math.cos(row * 0.019 + column * 0.37)
                                for row in range(count) for column in range(dimension)))
        queries = array.array('f', (base[(row % count) * dimension + column]
                                   for row in range(args.nq) for column in range(dimension)))
    truth = None
    if args.ground_truth:
        truth, truth_rows, truth_width = read_vectors(args.ground_truth, 'i', args.nq)
        if truth_rows < args.nq or truth_width < max(args.topk):
            raise ValueError('ground truth is too small')
        for row in range(args.nq):
            ids = truth[row * truth_width:row * truth_width + max(args.topk)]
            if len(set(ids)) != len(ids) or any(index < 0 or index >= count for index in ids):
                raise ValueError('ground truth must match this exact base dataset')
    if max(args.topk) > count:
        raise ValueError('topk exceeds base size')
    library = ctypes.CDLL(str(Path(args.worker_library).resolve()))
    fp = ctypes.POINTER(ctypes.c_float)
    ui = ctypes.POINTER(ctypes.c_uint)
    library.seekdb_cuvs_build.argtypes = [fp, ctypes.c_long, ctypes.c_long]
    library.seekdb_cuvs_build.restype = ctypes.c_void_p
    library.seekdb_cuvs_search.argtypes = [ctypes.c_void_p, fp, ctypes.c_long, ctypes.c_long, ui, fp]
    library.seekdb_cuvs_search.restype = ctypes.c_int
    library.seekdb_cuvs_serialize.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
    library.seekdb_cuvs_serialize.restype = ctypes.c_int
    library.seekdb_cuvs_deserialize.argtypes = [ctypes.c_char_p, ctypes.c_long, ctypes.c_long]
    library.seekdb_cuvs_deserialize.restype = ctypes.c_void_p
    library.seekdb_cuvs_free.argtypes = [ctypes.c_void_p]
    library.seekdb_cuvs_free.restype = None
    base_buffer = (ctypes.c_float * len(base)).from_buffer(base)
    query_buffer = (ctypes.c_float * len(queries)).from_buffer(queries)
    neighbors = (ctypes.c_uint * max(args.topk))()
    distances = (ctypes.c_float * max(args.topk))()
    result = {'scope': 'bridge_only_not_SQL_score', 'count': count, 'dimension': dimension,
              'nq': args.nq, 'library': str(Path(args.worker_library).resolve()),
              'ground_truth_provided': truth is not None, 'queries': {}}
    handle = None
    try:
        with tempfile.TemporaryDirectory(prefix='seekdb-cagra-gpu-') as directory:
            snapshot = str(Path(directory) / 'index.bin').encode()
            begin = time.perf_counter()
            handle = library.seekdb_cuvs_build(base_buffer, count, dimension)
            if not handle:
                raise RuntimeError('GPU build failed')
            built = time.perf_counter()
            if library.seekdb_cuvs_serialize(handle, snapshot):
                raise RuntimeError('GPU serialization failed')
            library.seekdb_cuvs_free(handle)
            handle = None
            handle = library.seekdb_cuvs_deserialize(snapshot, count, dimension)
            if not handle:
                raise RuntimeError('GPU snapshot reload failed')
            first_k = args.topk[0]
            if library.seekdb_cuvs_search(handle, query_buffer, 1, first_k, neighbors, distances):
                raise RuntimeError('first GPU query failed')
            if len(set(neighbors[:first_k])) != first_k or any(i >= count for i in neighbors[:first_k]):
                raise RuntimeError('first query result integrity failed')
            result['gpu_build_seconds'] = built - begin
            result['bridge_build_serialize_reload_first_seconds'] = time.perf_counter() - begin
            for k in args.topk:
                latencies = []
                hits = 0
                started = time.perf_counter()
                for row in range(args.nq):
                    query = ctypes.cast(ctypes.byref(query_buffer, row * dimension * 4), fp)
                    tick = time.perf_counter()
                    if library.seekdb_cuvs_search(handle, query, 1, k, neighbors, distances):
                        raise RuntimeError('GPU search failed')
                    latencies.append(time.perf_counter() - tick)
                    found = neighbors[:k]
                    if len(set(found)) != k or any(index >= count for index in found):
                        raise RuntimeError('result integrity failed')
                    if any(not math.isfinite(distance) for distance in distances[:k]):
                        raise RuntimeError('non-finite distance')
                    if truth is not None:
                        expected = set(truth[row * truth_width:row * truth_width + k])
                        hits += len(expected.intersection(found))
                elapsed = time.perf_counter() - started
                result['queries'][str(k)] = {'qps': args.nq / elapsed,
                    'p95_ms': sorted(latencies)[math.ceil(0.95 * args.nq) - 1] * 1000,
                    'recall': hits / (args.nq * k) if truth is not None else None,
                    'result_integrity': True}
    finally:
        if handle:
            library.seekdb_cuvs_free(handle)
    print('CUVS_RESULT ' + json.dumps(result), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-lib')
    parser.add_argument('--candidate-lib')
    parser.add_argument('--base', help='FP32 .fvecs file')
    parser.add_argument('--queries', help='FP32 .fvecs query file')
    parser.add_argument('--ground-truth', help='.ivecs for this exact base and query order')
    parser.add_argument('--nq', type=int, default=4096)
    parser.add_argument('--topk', type=int, nargs='+', default=[10, 100])
    parser.add_argument('--repeats', type=int, default=2)
    parser.add_argument('--output', type=Path, default=Path('cuvs-gpu-diagnostic.json'))
    parser.add_argument('--worker-library', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.nq <= 0 or args.repeats <= 0 or not args.topk or min(args.topk) <= 0:
        parser.error('nq, repeats and topk must be positive')
    if args.worker_library:
        worker(args)
        return
    if not args.baseline_lib or not args.candidate_lib:
        parser.error('--baseline-lib and --candidate-lib are required')
    report = {'scope': 'bridge_only_not_SQL_score', 'official_gates_verified': False, 'runs': []}
    for repetition in range(args.repeats):
        order = [('baseline', args.baseline_lib), ('candidate', args.candidate_lib)]
        if repetition % 2:
            order.reverse()
        for label, library in order:
            command = [sys.executable, str(Path(__file__).resolve()), '--worker-library', library,
                       '--nq', str(args.nq), '--topk', *map(str, args.topk)]
            for option in ['base', 'queries', 'ground_truth']:
                if getattr(args, option):
                    command.extend(['--' + option.replace('_', '-'), getattr(args, option)])
            process = subprocess.run(command, text=True, stdout=subprocess.PIPE, check=True)
            lines = [line for line in process.stdout.splitlines() if line.startswith('CUVS_RESULT ')]
            if len(lines) != 1:
                raise RuntimeError('missing diagnostic result')
            run = json.loads(lines[0][len('CUVS_RESULT '):])
            run.update(version=label, repetition=repetition)
            report['runs'].append(run)
            print(label, repetition, run['bridge_build_serialize_reload_first_seconds'], flush=True)
    report['median_bridge_seconds'] = {
        label: statistics.median(run['bridge_build_serialize_reload_first_seconds']
                                 for run in report['runs'] if run['version'] == label)
        for label in ['baseline', 'candidate']}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print('Saved bridge diagnostic:', args.output)


if __name__ == '__main__':
    main()
