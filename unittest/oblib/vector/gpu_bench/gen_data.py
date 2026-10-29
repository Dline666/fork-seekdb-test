"""Generate paired-benchmark datasets on the cloud host (numpy only).

hard256 / mid256: 250K x 256 clustered latent + noise (stand-ins for a 256-D set;
NOT the Hidden data). sift250k: first 250K/4096 of public SIFT1M if the original
files are given. Truth = exact top-100 (float64).
"""
import sys, time
from pathlib import Path
import numpy as np

OUT = Path(sys.argv[1]); OUT.mkdir(parents=True, exist_ok=True)

def write_vecs(p, x, code):
    a = np.empty((x.shape[0], x.shape[1] + 1), dtype='<i4'); a[:, 0] = x.shape[1]
    a[:, 1:] = x.view('<i4') if code == 'f' else x; a.tofile(p)
def read_fvecs(p, rows=None):
    raw = np.fromfile(p, dtype='<i4'); d = int(raw[0]); m = raw.reshape(-1, d + 1)
    if rows: m = m[:rows]
    return m[:, 1:].view('<f4').copy()
def truth(base, query, k=100):
    b = base.astype(np.float64); nb = (b ** 2).sum(1); out = np.empty((query.shape[0], k), np.int32)
    for s in range(0, query.shape[0], 256):
        d = nb[None, :] - 2.0 * (query[s:s + 256].astype(np.float64) @ b.T)
        p = np.argpartition(d, k, axis=1)[:, :k]; pd = np.take_along_axis(d, p, axis=1)
        out[s:s + 256] = np.take_along_axis(p, np.argsort(pd, axis=1, kind='stable'), axis=1)
    return out
def emit(name, base, query):
    d = OUT / name; d.mkdir(exist_ok=True); t = time.time()
    write_vecs(d / 'base.fvecs', base, 'f'); write_vecs(d / 'query.fvecs', query, 'f')
    write_vecs(d / 'truth.ivecs', truth(base, query), 'i'); print(name, base.shape, f'{time.time() - t:.1f}s', flush=True)

rng = np.random.default_rng(20261030)
def clustered(n, nq, dim, lat, c, sep, noise):
    centers = rng.normal(size=(c, lat)) * sep; proj = rng.normal(size=(lat, dim)) / np.sqrt(lat)
    def s(m):
        lab = rng.integers(0, c, m); z = centers[lab] + rng.normal(size=(m, lat))
        return (z @ proj + noise * rng.normal(size=(m, dim))).astype(np.float32)
    return s(n), s(nq)
emit('hard256', *clustered(250000, 2048, 256, 64, 200, 3.0, 0.35))
emit('mid256', *clustered(250000, 2048, 256, 128, 500, 2.0, 0.5))
src = Path(sys.argv[2]) if len(sys.argv) > 2 else None
if src and (src / 'sift_base.fvecs').exists():
    emit('sift250k', read_fvecs(src / 'sift_base.fvecs', 250000), read_fvecs(src / 'sift_query.fvecs', 4096))
