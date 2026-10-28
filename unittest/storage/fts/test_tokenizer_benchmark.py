import argparse,json,pathlib,tempfile,subprocess,time,sys,random,statistics
def main():
 # Requires PyMySQL. Run baseline and candidate sequentially on an idle machine.
 import pymysql
 p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True);p.add_argument('--compare');a=p.parse_args()
 run=pathlib.Path(tempfile.mkdtemp(prefix='seekdb-fts-',dir='/private/tmp'))
 f=open(run/'console.log','w');server=subprocess.Popen([a.binary,'--nodaemon','--base-dir='+str(run),'--parameter','mysql_port_mode=disabled','--parameter','enable_rpc_service=false','--parameter','memory_budget=2G','--parameter','log_disk_size=2G','--parameter','cpu_count=2','--parameter','vector_memory_limit=128M'],stdout=f,stderr=subprocess.STDOUT)
 c=None
 try:
  for i in range(100):
   if server.poll() is not None:raise RuntimeError('startup exited '+str(run))
   try:c=pymysql.connect(unix_socket=str(run/'run/sql.sock'),user='root',autocommit=True,charset='utf8mb4',read_timeout=30);break
   except pymysql.MySQLError:time.sleep(.2)
  if c is None:raise RuntimeError('startup timeout')
  cur=c.cursor()
  def execute(text,parser,detail=False):
   sql='select tokenize(%s,%s'+(",json_array(json_object('output','all')))" if detail else ')')+' as tokens'
   cur.execute(sql,(text,parser));r=cur.fetchone()[0]
   return json.loads(r) if r is not None else None
  def canonical(value):
   if isinstance(value,list):return sorted([canonical(x) for x in value],key=lambda x:json.dumps(x,sort_keys=True,ensure_ascii=False))
   if isinstance(value,dict):return {k:canonical(v) for k,v in value.items()}
   return value
  corpus=['',' ','a','a b c','Hello WORLD','Database Database database PERFORMANCE! audit-log 2026','abc'+''.join(chr(i) for i in range(32))+'XYZ', '中文 English 混合检索 OceanBase seekdb 内核 123!','Café naïve Straße İSTANBUL Ελληνικά 日本語 한국어 😀','abcdefghijklmnopqrstuvwx'*10,'end...', 'éabcé xyz', 'foo_bar foo-bar a.b:c', 'ABC\x00DEF']
  rng=random.Random(20261028)
  alphabet='abcXYZ019 ,.;\n\t中文éÄİ😀'
  corpus+=[''.join(rng.choice(alphabet) for _ in range(rng.randrange(1,160))) for _ in range(100)]
  outputs={}
  for parser in ['beng','ik']:
   for i,text in enumerate(corpus):
    for detail in [False,True]:outputs[f'{parser}/{i}/{detail}']=canonical(execute(text,parser,detail))
  if a.compare:
   old=json.loads(pathlib.Path(a.compare).read_text())
   differences={key:{'before':old['outputs'].get(key),'after':value} for key,value in outputs.items() if old['outputs'].get(key)!=value}
   if differences:
    pathlib.Path(a.output+'.mismatch.json').write_text(json.dumps(differences,ensure_ascii=False,indent=2))
    raise AssertionError('tokenizer output mismatch: '+str(len(differences)))
  workloads={'beng_600':('beng',('database performance audit STREAMING search 123 AbCd ')*12),'beng_8000':('beng',('database performance audit STREAMING search 123 AbCd ')*160),'ik_mixed':('ik',('数据库全文索引 OceanBase search performance 2026 ')*12)}
  timings={}
  for name,(parser,text) in workloads.items():
   for _ in range(30):execute(text,parser)
   samples=[]
   for _ in range(5):
    start=time.perf_counter_ns()
    for _ in range(500):execute(text,parser)
    samples.append((time.perf_counter_ns()-start)/500/1e6)
   timings[name]={'samples_ms':samples,'median_ms':statistics.median(samples),'mean_ms':statistics.mean(samples)}
  result={'binary':a.binary,'runtime':str(run),'correctness_cases':len(outputs),'outputs':outputs,'timings':timings}
  pathlib.Path(a.output).write_text(json.dumps(result,ensure_ascii=False,indent=2));print(json.dumps({k:v for k,v in result.items() if k!='outputs'},ensure_ascii=False,indent=2),flush=True)
 finally:
  if c:c.close()
  server.terminate()
  try:server.wait(timeout=10)
  except subprocess.TimeoutExpired:server.kill();server.wait()
  f.close()

if __name__ == "__main__":
 main()
