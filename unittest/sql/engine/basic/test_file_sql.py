"""File SQL regression on an isolated disposable native seekdb instance."""
import argparse,sys,pathlib,tempfile,subprocess,time,json,base64
def main():
 # Requires PyMySQL; always creates a fresh disposable server instance.
 import pymysql
 ap=argparse.ArgumentParser();ap.add_argument('--binary',required=True);a=ap.parse_args()
 run=pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-functions-',dir='/tmp'))
 fixtures=run/'fixtures';fixtures.mkdir()
 ROOT=pathlib.Path(__file__).resolve().parents[4]
 out=open(run/'console.log','w')
 server=subprocess.Popen([a.binary,'--nodaemon','--base-dir='+str(run),'--parameter','mysql_port_mode=disabled','--parameter','enable_rpc_service=false','--parameter','memory_budget=2G','--parameter','log_disk_size=2G','--parameter','cpu_count=2','--parameter','vector_memory_limit=128M','--variable','secure_file_priv='+str(fixtures)],stdout=out,stderr=subprocess.STDOUT)
 conn=None;passed=failed=0
 try:
  for _ in range(100):
   if server.poll() is not None:raise RuntimeError('server exited: '+str(run))
   try:conn=pymysql.connect(unix_socket=str(run/'run/sql.sock'),user='root',password='',autocommit=True,charset='utf8mb4',connect_timeout=1,read_timeout=30);break
   except pymysql.MySQLError:time.sleep(.2)
  if conn is None:raise RuntimeError('startup timeout')
  cur=conn.cursor()
  def q(sql,args=None):cur.execute(sql,args);return cur.fetchall()
  def eq(x,y):
   if x!=y:raise AssertionError((x,y))
  def scalar(sql,args=None):return q(sql,args)[0][0]
  def j(sql,args=None):return json.loads(scalar(sql,args))
  def err(sql,args=None):
   try:q(sql,args)
   except pymysql.MySQLError:return
   raise AssertionError('expected SQL error')
  def check(name,f):
   nonlocal passed,failed
   try:f();passed+=1;print('PASS',name,flush=True)
   except Exception as e:failed+=1;print('FAIL',name,type(e).__name__,str(e),flush=True)
  q('create database file_sql_validation');q('use file_sql_validation')
  check('ordinary SQL alive',lambda:eq(scalar('select 1'),1))
  q('set session enable_file_sql=on')
  def file(name,text):
   path=fixtures/name;path.write_text(text,encoding='utf-8');return conn.escape(str(path))
  csv=file('data.csv','id,name,score,day\n1,Alice,1.5,2026-10-01\n2,Bob,2.5,2026-10-02\n3,Carol,3.5,2026-10-03\n')
  js=file('teams.jsonl','{"id":1,"team":"a"}\n{"id":3,"team":"b","extra":true}\n')
  check('file CSV aggregates',lambda:eq(q('select count(*),sum(id),sum(score) from read_csv('+csv+')'),((3,6,7.5),)))
  check('file grouped aggregate/having',lambda:eq(q('select id%2 as k,count(*) as n from read_csv('+csv+') group by k having count(*)>1'),((1,2),)))
  check('file inner cross-format join',lambda:eq(q('select c.id,t.team from read_csv('+csv+') c join read_ndjson('+js+') t on c.id=t.id order by c.id'),((1,'a'),(3,'b'))))
  check('file JSON missing union',lambda:eq(q('select id,extra from read_json('+js+') order by id'),((1,None),(3,1))))
  check('file DESCRIBE',lambda:eq(len(q('describe select * from read_csv('+csv+')')),4))
  check('file glob',lambda:eq(len(q('select file from glob('+conn.escape(str(fixtures/'*.csv'))+')')),1))
  malformed=file('malformed.csv','id,name\n1,"abc"garbage\n')
  short=file('short.csv','a,b\n1\n');long=file('long.csv','a,b\n1,2,3\n')
  check('file rejects trailing CSV data',lambda:err('select * from read_csv('+malformed+')'))
  check('file rejects short row',lambda:err('select * from read_csv('+short+')'))
  check('file rejects long row',lambda:err('select * from read_csv('+long+')'))
  nested=file('nested.jsonl','{"a":{"b":1}}\n');array=file('nested-array.jsonl','{"a":[1]}\n')
  check('file rejects nested object',lambda:err('select * from read_json('+nested+')'))
  check('file rejects nested array',lambda:err('select * from read_json('+array+')'))
  outside=run/'outside.csv';outside.write_text('id\n77\n');(fixtures/'escape.csv').symlink_to(outside)
  check('file rejects parent traversal',lambda:err('select * from read_csv('+conn.escape(str(fixtures/'..'/'outside.csv'))+')'))
  check('file rejects symlink escape',lambda:err('select * from read_csv('+conn.escape(str(fixtures/'escape.csv'))+')'))
  check('file rejects glob traversal',lambda:err('select * from glob('+conn.escape(str(fixtures/'*'/'..'/'*.csv'))+')'))
  def parquet():
   src=ROOT/'tools/deploy/mysql_test/test_suite/file_sql/data/simple.parquet.b64'
   path=fixtures/'fixture.parquet';path.write_bytes(base64.b64decode(src.read_bytes()))
   eq(q('select count(*),sum(c1) from read_parquet('+conn.escape(str(path))+')'),((7,21),))
  check('file Parquet native aggregate',parquet)
  target=fixtures/'export.csv';quoted=conn.escape(str(target))
  def copy_roundtrip():
   q('copy (select id,name from read_csv('+csv+') order by id) to '+quoted+' (format csv,header)')
   eq(q('select id,name from read_csv('+quoted+') order by id'),((1,'Alice'),(2,'Bob'),(3,'Carol')))
  check('COPY query roundtrip',copy_roundtrip)
  check('COPY existing target rejected',lambda:err('copy (select 9 as id) to '+quoted+' (format csv,header)'))
  def copy_quotes():
   dest=fixtures/'quotes.csv';q('copy (select %s as value) to '+conn.escape(str(dest))+' (format csv,header)',('comma,quote"line\r\nNULL',))
   eq(q('select value from read_csv('+conn.escape(str(dest))+')'),(('comma,quote"line\r\nNULL',),))
  check('COPY quote/CRLF roundtrip',copy_quotes)
  check('server alive after file errors',lambda:eq(scalar('select 1'),1))

  if passed+failed != 20:raise AssertionError(('expected 20 file checks',passed,failed))
  print('RUNTIME',run,flush=True);print('SUMMARY',json.dumps({'passed':passed,'failed':failed}),flush=True)
 finally:
  if conn:conn.close()
  server.terminate()
  try:server.wait(timeout=10)
  except subprocess.TimeoutExpired:server.kill();server.wait()
  out.close()
 sys.exit(bool(failed))

if __name__ == "__main__":
 main()
