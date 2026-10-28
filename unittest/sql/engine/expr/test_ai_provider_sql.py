"""AI function/provider SQL regression: 31 checks on an isolated disposable instance."""
import argparse,sys,pathlib,tempfile,subprocess,time,json,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
def main():
 # Requires PyMySQL; always creates a fresh disposable server instance.
 import pymysql
 ap=argparse.ArgumentParser();ap.add_argument('--binary',required=True);a=ap.parse_args()
 run=pathlib.Path(tempfile.mkdtemp(prefix='seekdb-ai-functions-',dir='/tmp'))
 requests=[];attempts={}
 class Mock(BaseHTTPRequestHandler):
  protocol_version='HTTP/1.1'
  def log_message(self,*args):pass
  def do_POST(self):
   body=json.loads(self.rfile.read(int(self.headers['Content-Length'])));requests.append((self.path,body,dict(self.headers)))
   mode=self.path.split('/')[1];attempts[self.path]=attempts.get(self.path,0)+1
   if mode=='retry' and attempts[self.path]==1:
    self.send_response(503);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'{}');return
   if mode=='partial' and attempts[self.path]==1:
    self.send_response(200);self.send_header('Content-Length','300');self.end_headers();self.wfile.write(b'{"partial":');self.wfile.flush();self.close_connection=True;return
   if self.path.endswith('embeddings') or self.path.endswith('/embed'):
    inputs=body.get('input',[]);inputs=[inputs] if isinstance(inputs,str) else inputs
    dim=body.get('dimensions',4)
    data=[{'index':i,'embedding':[float(len(v))]+[0.25]*(dim-1)} for i,v in reversed(list(enumerate(inputs)))]
    if mode=='duplicate' and len(data)>1:data[0]['index']=data[1]['index']
    if mode=='dimension':data[0]['embedding']=[1.0]
    if mode=='element':data[0]['embedding']=['invalid']
    response={'embeddings':[d['embedding'] for d in sorted(data,key=lambda d:d['index'])]} if mode=='ollama' else {'data':data}
   elif self.path.endswith('/rerank'):
    docs=body.get('documents',[]);data=[{'index':i,'relevance_score':0.5 if mode=='ties' else float(len(v))} for i,v in reversed(list(enumerate(docs)))]
    if mode=='rerankdup' and len(data)>1:data[0]['index']=data[1]['index']
    if mode=='rerankshape':response={'results':{}}
    else:response={'results':data}
   elif mode=='ollama':response={'response':'ollama-ok','done':True}
   else:
    prompt=''.join(m.get('content','') for m in body.get('messages',[]) if m.get('role')=='user')
    response={'choices':[{'message':{'content':prompt}}]}
    if mode=='empty':response={'choices':[{'message':{'content':''}}]}
    if mode=='wrongtype':response={'choices':[{'message':{'content':42}}]}
   if mode=='array':response=[]
   if mode=='null':response=None
   payload=json.dumps(response,ensure_ascii=False).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
 mock=ThreadingHTTPServer(('127.0.0.1',0),Mock);threading.Thread(target=mock.serve_forever,daemon=True).start();port=mock.server_address[1]
 out=open(run/'console.log','w')
 server=subprocess.Popen([a.binary,'--nodaemon','--base-dir='+str(run),'--parameter','mysql_port_mode=disabled','--parameter','enable_rpc_service=false','--parameter','memory_budget=2G','--parameter','log_disk_size=2G','--parameter','cpu_count=2','--parameter','vector_memory_limit=128M'],stdout=out,stderr=subprocess.STDOUT)
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
  def model(name,kind,mode,provider='openai',suffix=None):
   suffix=suffix or {'completion':'chat/completions','dense_embedding':'embeddings','rerank':'rerank'}[kind]
   q('call DBMS_AI_SERVICE.CREATE_AI_MODEL(%s,%s)',(name,json.dumps({'type':kind,'model_name':'mock'})))
   q('call DBMS_AI_SERVICE.CREATE_AI_MODEL_ENDPOINT(%s,%s)',(name+'_ep',json.dumps({'ai_model_name':name,'url':f'http://127.0.0.1:{port}/{mode}/{suffix}','provider':provider,'access_key':'' if provider.lower()=='ollama' else 'synthetic','request_model_name':'mock'})))
  q('create database extension_tests');q('use extension_tests')
  for n,m in [('complete','ok'),('empty','empty'),('wrongtype','wrongtype'),('array','array'),('nullresponse','null'),('retry','retry'),('partial','partial')]:model(n,'completion',m)
  for n,m in [('embed','ok'),('dimension','dimension'),('element','element'),('dup','duplicate')]:model(n,'dense_embedding',m)
  model('rank','rerank','ok','siliconflow');model('ties','rerank','ties','siliconflow');model('rankdup','rerank','rerankdup','siliconflow');model('rankshape','rerank','rerankshape','siliconflow')
  model('ollcomplete','completion','ollama','Ollama','generate');model('ollembed','dense_embedding','ollama','ollama','embed')
  check('ordinary SQL alive',lambda:eq(scalar('select 1'),1))
  check('completion text',lambda:eq(scalar("select ai_complete('complete','hello')"),'hello'))
  check('completion native config values',lambda:(eq(scalar("select ai_complete('complete','config',json_object('temperature',0.2,'max_tokens',16))"),'config'),eq(requests[-1][1]['temperature'],.2),eq(requests[-1][1]['max_tokens'],16)))
  check('completion NULL config',lambda:eq(scalar("select ai_complete('complete','null-config',NULL)"),'null-config'))
  check('prompt repeats/nested/Unicode',lambda:eq(scalar("select ai_complete('complete',ai_prompt('中{0}/{0}/{1}',ai_prompt('内{0}','文'),json_array(1,true)))"),'中内文/内文/[1, true]'))
  check('prompt invalid numeric index',lambda:err("select ai_complete('complete',ai_prompt('{9}','x'))"))
  check('prompt overflow numeric index',lambda:err("select ai_complete('complete',ai_prompt('{99999999999999999999999}','x'))"))
  q('create table lob(t longtext,a longtext)');q('insert into lob values(%s,%s)',('{0}/{0}','界'*5000))
  check('prompt/completion LONGTEXT',lambda:eq(len(scalar("select ai_complete('complete',ai_prompt(t,a)) from lob")),10001))
  for name in ['empty','wrongtype','array','nullresponse']:
   check('completion rejects '+name,lambda name=name:err('select ai_complete(%s,%s)',(name,'text')))
  check('HTTP 503 retry',lambda:eq(scalar("select ai_complete('retry','retry')"),'retry'))
  check('HTTP partial transfer retry body reset',lambda:eq(scalar("select ai_complete('partial','partial')"),'partial'))
  check('Ollama no key native completion',lambda:(eq(scalar("select ai_complete('ollcomplete','hello')"),'ollama-ok'),eq(requests[-1][1].get('stream'),False),eq(requests[-1][2].get('Authorization'),None)))
  check('Ollama native embed',lambda:eq(j("select ai_embed('ollembed','hello',4)"),[5.0,.25,.25,.25]))
  check('embed explicit dimension',lambda:eq(j("select ai_embed('embed','你好',4)"),[2.0,.25,.25,.25]))
  for name in ['dimension','element']:
   check('embed rejects '+name,lambda name=name:err('select ai_embed(%s,%s,4)',(name,'text')))
  for dim in ['0','-1','1.5','NULL']:
   check('embed dimension '+dim,lambda dim=dim:eq(scalar("select ai_embed('embed','x',NULL)"),None) if dim=='NULL' else err("select ai_embed('embed','x',"+dim+")"))
  q('create table inputs(id int primary key,content text)');cur.executemany('insert into inputs values(%s,%s)',[(i,None if i%11==0 else 'x'*((i%10)+1)) for i in range(150)])
  def batch():
   start=len(requests);rows=q("select id,ai_embed('embed',content,4) from inputs order by id");eq([(i,None if v is None else json.loads(v)[0]) for i,v in rows],[(i,None if i%11==0 else float(i%10+1)) for i in range(150)]);sizes=[len(b['input']) for _,b,_ in requests[start:]]
   if not any(n>1 for n in sizes):raise AssertionError(('no cross-row batch requests',sizes))
  check('embed 150 rows reverse response/index/NULL/batching',batch)
  check('embed filter limit mapping',lambda:eq([(i,json.loads(v)[0]) for i,v in q("select id,ai_embed('embed',content,4) from inputs where id between 20 and 25 and content is not null order by id limit 3")],[(20,1.0),(21,2.0),(23,4.0)]))
  docs=['x'*(i+1) for i in range(45)]
  check('rerank global mapping across 3 batches',lambda:eq([e['index'] for e in j("select ai_rerank('rank','q',%s,NULL)",(json.dumps(docs),))],list(reversed(range(45)))))
  check('rerank stable ties across batches',lambda:eq([e['index'] for e in j("select ai_rerank('ties','q',%s,NULL)",(json.dumps(docs),))],list(range(45))))
  check('rerank object IDs preserved',lambda:eq([e['id'] for e in j("select ai_rerank('rank','q',%s,'text')",(json.dumps([{'id':i,'text':d} for i,d in enumerate(docs)]),))],list(reversed(range(45)))))
  check('rerank rejects duplicate indexes',lambda:err("select ai_rerank('rankdup','q','[\"a\",\"bb\"]',NULL)"))
  check('rerank rejects object in results',lambda:err("select ai_rerank('rankshape','q','[\"a\"]',NULL)"))
  check('server alive after malformed responses',lambda:eq(scalar('select 10+2'),12))
  if passed+failed != 31:raise AssertionError(('expected 31 AI checks',passed,failed))
  print('RUNTIME',run,flush=True);print('SUMMARY',json.dumps({'passed':passed,'failed':failed}),flush=True)
 finally:
  if conn:conn.close()
  mock.shutdown();mock.server_close();server.terminate()
  try:server.wait(timeout=10)
  except subprocess.TimeoutExpired:server.kill();server.wait()
  out.close()
 sys.exit(bool(failed))

if __name__ == "__main__":
 main()
