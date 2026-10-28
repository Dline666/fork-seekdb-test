import json
import pathlib
import subprocess
import time
import numpy as np

OUT = pathlib.Path('/workspace/seekdb-build-artifacts/smoke')
OUT.mkdir(exist_ok=True)
MYSQL = ['mysql', '--local-infile=1', '-h127.0.0.1', '-P2881', '-uroot', '-N', '-B']

def sql(text):
    result = subprocess.run(MYSQL + ['-e', 'set ob_query_timeout=300000000; ' + text], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout

base = np.fromfile('/seekdb_workspace/datasets/base.f32', dtype=np.float32).reshape(-1, 128)
queries = np.fromfile('/seekdb_workspace/datasets/query.f32', dtype=np.float32).reshape(-1, 128)[:20]
assert len(base) >= 10000

def vector(row):
    return '[' + ','.join(format(float(x), '.9g') for x in row) + ']'

tsv = OUT / 'base.tsv'
with tsv.open('w') as stream:
    for i, row in enumerate(base):
        stream.write(str(i) + '\t' + vector(row) + '\n')
sql('create database contest_v5_smoke')
report = {'commit': '634888d9b037153fa143d2c913ba4b25503e1d62', 'rows': len(base), 'dim': 128, 'nq': len(queries), 'note': 'Local deployment dataset only; not official SIFT250K or Hidden250K. Sequential CPU/GPU diagnostic, not official paired scoring.', 'results': {}}
for lib in ['vsag', 'cuvs']:
    table = 'contest_v5_smoke.items_' + lib
    sql(f'create table {table}(id bigint primary key, v vector(128))')
    sql(f"load data local infile '{tsv}' into table {table} fields terminated by '\\t' lines terminated by '\\n' (id,v)")
    assert int(sql(f'select count(*) from {table}')) == len(base)
    start = time.perf_counter()
    sql(f'create vector index idx_items on {table}(v) with (distance=l2,type=hnsw,lib={lib},m=16,ef_construction=200,ef_search=64)')
    ids = sql(f'select id from {table} order by l2_distance(v,\'{vector(queries[0])}\') approximate limit 10').split()
    build = time.perf_counter() - start
    assert len(ids) == len(set(ids)) == 10
    measurements = {}
    for k in [10, 100]:
        recall = []
        elapsed = []
        for query in queries:
            truth = set(np.argsort(np.sum((base - query) ** 2, axis=1))[:k].tolist())
            start = time.perf_counter()
            ids = [int(x) for x in sql(f'select id from {table} order by l2_distance(v,\'{vector(query)}\') approximate limit {k}').split()]
            elapsed.append(time.perf_counter() - start)
            assert len(ids) == len(set(ids)) == k
            assert all(0 <= x < len(base) for x in ids)
            recall.append(len(set(ids) & truth) / k)
        measurements[str(k)] = {'recall': float(np.mean(recall)), 'qps_including_mysql_client_startup': len(queries) / sum(elapsed), 'p95_seconds_including_mysql_client_startup': float(np.percentile(elapsed, 95))}
    report['results'][lib] = {'build_seconds_including_first_query': build, 'queries': measurements}
    (OUT / 'report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps({lib: report['results'][lib]}), flush=True)
print('SMOKE PASS: SQL CPU/GPU build, query result cardinality, ID range and uniqueness', flush=True)
