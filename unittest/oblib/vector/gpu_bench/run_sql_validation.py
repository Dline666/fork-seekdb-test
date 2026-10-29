#!/usr/bin/env python3
"""Isolated SQL/GPU persistence smoke; local diagnostic, not official scoring."""
import argparse,json,os,subprocess,time,signal,socket,hashlib
from pathlib import Path
import numpy as np
import pymysql
p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--bridge',required=True);p.add_argument('--run-dir',required=True);p.add_argument('--port',type=int,default=2883);a=p.parse_args()
run=Path(a.run_dir).resolve();run.mkdir(parents=True,exist_ok=True)
assert not (run/'data').exists(),'use a fresh run directory'
with socket.socket() as s:assert s.connect_ex(('127.0.0.1',a.port))!=0,'SQL port in use'
binary=Path(a.binary).resolve();bridge=Path(a.bridge).resolve();env=os.environ.copy();env['LD_LIBRARY_PATH']=str(bridge.parent)+':/opt/hipvs/lib:/opt/rocm/lib';env['OPENBLAS_NUM_THREADS']='1';env['OMP_NUM_THREADS']='16'
process=None;output=None
report={'scope':'isolated_SQL_smoke_not_official_score','passed':False,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'bridge_sha256':hashlib.sha256(bridge.read_bytes()).hexdigest(),'cases':{}}
def connect():return pymysql.connect(host='127.0.0.1',port=a.port,user='root',autocommit=True,local_infile=True,connect_timeout=3,read_timeout=180,write_timeout=180)
def stop():
 global process,output
 if process and process.poll() is None:
  process.terminate()
  try:process.wait(timeout=60)
  except subprocess.TimeoutExpired:process.kill();process.wait(timeout=15)
 if output:output.close();output=None
 process=None
def start():
 global process,output
 output=open(run/'observer-console.log','ab')
 process=subprocess.Popen([str(binary),'--nodaemon','--base-dir',str(run/'data'),'--port',str(a.port),'--parameter','memory_budget=8G','--parameter','datafile_size=2G','--parameter','datafile_maxsize=4G','--parameter','log_disk_size=2G','--parameter','cpu_count=16','--parameter','local_ip=127.0.0.1'],env=env,stdout=output,stderr=subprocess.STDOUT)
 deadline=time.monotonic()+180
 while time.monotonic()<deadline:
  if process.poll() is not None:raise RuntimeError('seekdb exited during startup; see observer-console.log')
  try:
   conn=connect();conn.close();return
  except pymysql.Error:time.sleep(1)
 raise RuntimeError('SQL readiness timeout')
def sql(conn,text):
 with conn.cursor() as c:c.execute(text);return c.fetchall()
def query(conn,vector,k,table='items',approx=True):
 literal='['+','.join(format(float(v),'.9g') for v in vector)+']'
 rows=sql(conn,f"select id from amd_smoke.{table} order by l2_distance(v, '{literal}') {'approximate' if approx else ''} limit {k}")
 ids=[r[0] for r in rows]
 assert len(ids)==k and len(set(ids))==k,'result integrity'
 return ids
try:
 rng=np.random.default_rng(73145);n,dim,nq=10000,128,100
 base=rng.normal(size=(n,dim)).astype(np.float32);base/=np.linalg.norm(base,axis=1,keepdims=True)
 queries=(base[np.linspace(0,n-1,nq,dtype=int)]+rng.normal(0,.002,size=(nq,dim))).astype(np.float32)
 ext=np.arange(n,dtype=np.int64)*37-130000
 data=run/'base.tsv'
 with data.open('w') as f:
  for row,vid in zip(base,ext):f.write(str(int(vid))+'\t['+','.join(format(float(v),'.9g') for v in row)+']\n')
 start();report['cases']['SQL_ready']=True
 conn=connect();sql(conn,'set ob_query_timeout=180000000');sql(conn,'create database amd_smoke')
 sql(conn,'create table amd_smoke.items(id bigint primary key,v vector(128))')
 sql(conn,"load data local infile '"+str(data)+"' into table amd_smoke.items fields terminated by '\\t' lines terminated by '\\n' (id,v)")
 assert sql(conn,'select count(*) from amd_smoke.items')[0][0]==n
 tick=time.perf_counter();sql(conn,'create vector index idx_items on amd_smoke.items(v) with (distance=l2,type=hnsw,lib=cuvs,m=16,ef_construction=200,ef_search=64)')
 first=query(conn,queries[0],10);report['GPU_DDL_plus_first_query_seconds']=time.perf_counter()-tick
 report['cases']['offline_GPU_DDL_and_first_query']=True
 report['recall']={};report['query_qps']={};saved={}
 for k in [10,100]:
  hits=0;latencies=[]
  for q,vector in enumerate(queries):
   distances=np.sum((base-vector)**2,axis=1);truth=set(ext[np.argsort(distances)[:k]].tolist())
   tick=time.perf_counter();found=query(conn,vector,k);latencies.append(time.perf_counter()-tick)
   assert all(x in set(ext.tolist()) for x in found)
   hits+=len(truth.intersection(found))
   if q<3:saved[(k,q)]=found
  report['recall'][str(k)]=hits/(nq*k);report['query_qps'][str(k)]=nq/sum(latencies)
  assert report['recall'][str(k)]>=.95,'local recall smoke threshold'
 report['cases']['GPU_recall_ID_mapping_integrity']=True
 sql(conn,'create table amd_smoke.cpu_items(id bigint primary key,v vector(128))')
 sql(conn,'insert into amd_smoke.cpu_items select * from amd_smoke.items')
 sql(conn,'create vector index idx_cpu on amd_smoke.cpu_items(v) with (distance=l2,type=hnsw,lib=vsag,m=16,ef_construction=200,ef_search=200)')
 cpu=query(conn,queries[0],10,table='cpu_items');assert set(cpu)==set(query(conn,queries[0],10,table='cpu_items',approx=False))
 report['cases']['VSAG_CPU_DDL_and_query']=True
 conn.close();stop();start();conn=connect();sql(conn,'set ob_query_timeout=180000000')
 for (k,q),expected in saved.items():assert query(conn,queries[q],k)==expected,'snapshot reload changed IDs'
 assert set(query(conn,queries[0],10,table='cpu_items'))==set(cpu)
 report['cases']['GPU_and_CPU_restart_persistence']=True
 conn.close();stop()
 logs='\n'.join(path.read_text(errors='replace') for path in (run/'data/log').glob('seekdb.log*') if path.is_file() and not path.name.endswith('.wf'))
 assert 'serialized GPU CAGRA snapshot' in logs,'GPU snapshot not observed'
 assert 'deserialized GPU CAGRA snapshot' in logs,'GPU snapshot reload not observed'
 assert 'lazily built VSAG fallback' not in logs,'unexpected CPU fallback'
 report['cases']['GPU_snapshot_logs_no_lazy_CPU_fallback']=True
 report['passed']=True
 print(json.dumps(report,indent=2),flush=True)
finally:
 stop();(run/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
