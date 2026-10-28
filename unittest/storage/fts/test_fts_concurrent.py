"""Exercise independent per-document accumulators under concurrent SQL work."""
import argparse,concurrent.futures,json,os,pathlib
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);a=p.parse_args()
def connect():return pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60,write_timeout=60)
root=connect();cur=root.cursor();database='fts_parallel_'+str(os.getpid());cur.execute('create database '+database);cur.execute('use '+database)
cur.execute('create table docs(id int primary key,content text,fulltext index ft(content) with parser beng)')
texts=['Database database PERFORMANCE audit repeated repeated', '数据库全文索引 OceanBase English 123 中文中英混合', 'Café naïve Straße İSTANBUL Ελληνικά 😀', 'aa\0bb repeat REPEAT repeated', '中华人民共和国南京市长江大桥 数据库性能优化。'*40, '', None]
expected={}
def canonical(value):
 if isinstance(value,list):return sorted((canonical(x) for x in value),key=lambda x:json.dumps(x,sort_keys=True,ensure_ascii=False))
 if isinstance(value,dict):return {k:canonical(v) for k,v in value.items()}
 return value
for parser in ['beng','ik','space','ngram','ngram2']:
 for i,text in enumerate(texts):
  cur.execute("select tokenize(convert(%s using utf8mb4),%s,json_array(json_object('output','all'))) as tokens",(text,parser));expected[(parser,i)]=canonical(json.loads(cur.fetchone()[0]))
def worker(index):
 c=connect();q=c.cursor();q.execute('use '+database);checks=0;offset=index*1000
 try:
  for repeat in range(5):
   for parser in ['beng','ik','space','ngram','ngram2']:
    for i,text in enumerate(texts):
     q.execute("select tokenize(convert(%s using utf8mb4),%s,json_array(json_object('output','all'))) as tokens",(text,parser));actual=canonical(json.loads(q.fetchone()[0]));assert actual==expected[(parser,i)],(index,repeat,parser,i);checks+=1
  q.executemany('insert into docs values(%s,%s)',[(offset+i,'database engine performance audit repeated') for i in range(20)])
  for sql in ["select count(*) from docs where id>=%s and id<%s and match(content) against('database performance')", "select count(*) from docs where id>=%s and id<%s and match(content) against('database performance')+0>0"]:
   q.execute(sql,(offset,offset+20));assert q.fetchone()[0]==20;checks+=1
  q.execute('start transaction');q.execute("update docs set content='unrelated words' where id>=%s and id<%s",(offset,offset+20));q.execute("select count(*) from docs where id>=%s and id<%s and match(content) against('database performance')",(offset,offset+20));assert q.fetchone()[0]==0;checks+=1;q.execute('rollback')
  q.execute("select count(*) from docs where id>=%s and id<%s and match(content) against('database performance')",(offset,offset+20));assert q.fetchone()[0]==20;checks+=1
  q.execute('delete from docs where id>=%s and id<%s',(offset,offset+20));checks+=1
  return checks
 finally:c.rollback();c.close()
try:
 with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:counts=list(pool.map(worker,range(1,9)))
 print('PASS concurrent FTS:',sum(counts),'checks across 8 clients',flush=True)
finally:
 root.rollback();cur.execute('drop database if exists '+database);root.close()
