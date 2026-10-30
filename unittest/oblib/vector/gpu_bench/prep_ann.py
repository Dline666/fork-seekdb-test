"""Convert public ann-benchmarks HDF5 sets to fvecs subsets with exact L2 truth.

usage: prep_ann.py <hdf5> <outdir> <n_base> <n_query>
Takes the first n_base train rows and first n_query test rows (FP32 unchanged)
and computes exact float64 squared-L2 top-100 over the subset. Not Hidden data.
"""
import sys, time
from pathlib import Path
import numpy as np
import h5py

src, out, nb, nq = sys.argv[1], Path(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
out.mkdir(parents=True, exist_ok=True)
with h5py.File(src, 'r') as f:
    base = np.asarray(f['train'][:nb], dtype=np.float32)
    query = np.asarray(f['test'][:nq], dtype=np.float32)
assert np.isfinite(base).all() and np.isfinite(query).all()
def w(p, x, code):
    a = np.empty((x.shape[0], x.shape[1] + 1), '<i4'); a[:, 0] = x.shape[1]
    a[:, 1:] = x.view('<i4') if code == 'f' else x; a.tofile(p)
w(out / 'base.fvecs', base, 'f'); w(out / 'query.fvecs', query, 'f')
t = time.time(); b = base.astype(np.float64); n2 = (b ** 2).sum(1)
truth = np.empty((nq, 100), np.int32)
for i in range(0, nq, 256):
    d = n2[None, :] - 2.0 * (query[i:i + 256].astype(np.float64) @ b.T)
    p = np.argpartition(d, 100, axis=1)[:, :100]; pd = np.take_along_axis(d, p, 1)
    truth[i:i + 256] = np.take_along_axis(p, np.argsort(pd, 1, kind='stable'), 1)
w(out / 'truth.ivecs', truth, 'i')
print(out.name, base.shape, query.shape, f'{time.time() - t:.1f}s', flush=True)
