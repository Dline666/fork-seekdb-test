import json
import pathlib
import subprocess
import time
import numpy as np

OUT = pathlib.Path('/workspace/seekdb-build-artifacts/smoke')
MYSQL = ['mysql', '-h127.0.0.1', '-P2881', '-uroot', '-N', '-B']

def sql(text):
    p = subprocess.run(MYSQL + ['-e', 'set ob_query_timeout=300000000; ' + text], capture_output=True, text=True, timeout=320)
    if p.returncode:
        raise RuntimeError(p.stderr)
    return p.stdout

base = np.fromfile('/seekdb_workspace/datasets/base.f32', dtype=np.float32).reshape(-1, 128)
queries = np.fromfile('/seekdb_workspace/datasets/query.f32', dtype=np.float32).reshape(-1, 128)[:20]
report = {'scope': 'restart recovery of isolated V5 instance; local 10K deployment dataset', 'results': {}}
for lib in ['cuvs', 'vsag']:
    table = 'contest_v5_smoke.items_' + lib
    assert int(sql(f'select count(*) from {table}')) == len(base)
    recalls = {}
    for k in [10, 100]:
        values = []
        for query in queries:
            vector = '[' + ','.join(format(float(x), '.9g') for x in query) + ']'
            ids = [int(x) for x in sql(f"select id from {table} order by l2_distance(v,'{vector}') approximate limit {k}").split()]
            assert len(ids) == len(set(ids)) == k
            assert all(0 <= i < len(base) for i in ids)
            truth = set(np.argsort(np.sum((base-query)**2, axis=1))[:k].tolist())
            values.append(len(set(ids) & truth)/k)
        recalls[str(k)] = float(np.mean(values))
    report['results'][lib] = {'rows': len(base), 'recall': recalls, 'id_integrity': 'pass'}
log = pathlib.Path('/workspace/v5-validation/log/seekdb.log').read_bytes()
offset = int((OUT / 'restart-log-offset.txt').read_text())
recent = log[offset:].decode(errors='replace')
evidence = [line for line in recent.splitlines() if 'deserialized GPU CAGRA snapshot' in line or 'serialized GPU CAGRA snapshot' in line or 'lazily built VSAG fallback' in line]
(OUT / 'restart-gpu-evidence.log').write_text('\n'.join(evidence)+'\n')
report['gpu_snapshot_reload_seen'] = any('deserialized GPU CAGRA snapshot' in line and 'graph_bytes=' in line for line in evidence)
report['lazy_vsag_fallback_seen'] = 'lazily built VSAG fallback' in recent
report['gpu_snapshot_serialized_after_restart'] = any('serialized GPU CAGRA snapshot' in line and 'deserialized' not in line for line in evidence)
(OUT / 'restart-report.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
assert report['gpu_snapshot_reload_seen'], 'GPU graph reload not observed'
assert not report['lazy_vsag_fallback_seen'], 'unexpected CPU fallback observed'
print('RESTART SMOKE PASS')
