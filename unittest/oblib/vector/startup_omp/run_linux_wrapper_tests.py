#!/usr/bin/env python3
"""Test the actual Linux wrapper in child-only restricted-affinity processes."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--probe', type=Path, required=True)
    parser.add_argument('--failure-probe', type=Path, required=True)
    parser.add_argument('--cpu-only-probe', type=Path, required=True)
    args = parser.parse_args()
    if not sys.platform.startswith('linux'):
        raise SystemExit('This runner requires Linux; it changes only subprocess affinity.')
    taskset = shutil.which('taskset')
    if not taskset:
        raise SystemExit('taskset is required')
    original_environment = dict(os.environ)
    permitted = sorted(os.sched_getaffinity(0))
    if not permitted:
        raise SystemExit('No permitted CPU')
    if os.sysconf('SC_NPROCESSORS_ONLN') < 2:
        raise SystemExit('At least two online CPUs are needed to exercise the real restriction gate')
    # Build a private environment for each child; never change this process.
    binding_names = {'GOMP_CPU_AFFINITY', 'KMP_AFFINITY', 'KMP_HW_SUBSET',
                     'KMP_PLACE_THREADS', 'KMP_HIDDEN_HELPER_AFFINITY'}
    clean = {key: value for key, value in os.environ.items()
             if not key.startswith('OMP_') and key not in binding_names
             and key != 'SEEKDB_CUVS_OMP_REEXEC'}
    clean['CUVS_TEST_PRESERVED'] = 'keep spaces = $ and ; exactly'
    literal = ['literal spaces $() ; and \' quote', 'argument-with-"double-quote"']
    cases = [
        ('restart', args.probe, [], {}),
        ('help', args.probe, ['--help'], {}),
        ('help-short', args.probe, ['-h'], {}),
        ('version', args.probe, ['--version'], {}),
        ('version-short', args.probe, ['-V'], {}),
        ('explicit-threads', args.probe, [], {'OMP_NUM_THREADS': '2'}),
        ('explicit-policy', args.probe, [], {'OMP_WAIT_POLICY': 'ACTIVE'}),
        ('explicit-bind', args.probe, [], {'OMP_PROC_BIND': 'false'}),
        ('explicit-bind-active', args.probe, [], {'OMP_PROC_BIND': 'true'}),
        ('explicit-places', args.probe, [], {'OMP_PLACES': 'threads'}),
        ('explicit-gomp-affinity', args.probe, [], {'GOMP_CPU_AFFINITY': str(permitted[0])}),
        ('marker', args.probe, [], {'SEEKDB_CUVS_OMP_REEXEC': 'externally-supplied-unknown'}),
        ('failed-exec', args.failure_probe, [], {}),
        ('CPU-only', args.cpu_only_probe, [], {}),
    ]
    for label, binary, options, overrides in cases:
        command = [taskset, '-c', str(permitted[0]), str(binary.resolve()), label, *options, *literal]
        process = subprocess.Popen(command, env={**clean, **overrides}, text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            output, errors = process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            output, errors = process.communicate()
            raise RuntimeError(f'{label}: timed out; only the owned child was stopped')
        if process.returncode != 0:
            raise RuntimeError(f'{label}: exit={process.returncode}\n{output}\n{errors}')
        result = json.loads(output.strip())
        assert result['passed'] and result['mode'] == label
        assert result['pid'] == process.pid, (result, process.pid)
        if label == 'restart':
            assert result['marker'] and result['current_team'] == result['new_thread_team'] == 1
        print('PASS Linux production wrapper:', json.dumps(result), flush=True)
    assert sorted(os.sched_getaffinity(0)) == permitted
    assert dict(os.environ) == original_environment
    print(f'PASS {len(cases)} actual Linux wrapper cases; parent affinity and environment unchanged', flush=True)


if __name__ == '__main__':
    main()
