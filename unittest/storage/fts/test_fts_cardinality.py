"""Capture cardinality-only and value-producing FTS queries for differential checking."""
import argparse,json,pathlib,random,os
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);p.add_argument('--output',required=True,type=pathlib.Path);p.add_argument('--compare',type=pathlib.Path);a=p.parse_args();c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60);cur=c.cursor();db='fts_cardinality_'+str(os.getpid());results={};rng=random.Random(20261029)
def query(sql,args=None):cur.execute(sql,args);return cur.fetchall()
def capture(name,sql,args=None):
 try:results[name]=query(sql,args)
 except Exception:print('FAILED QUERY',name,sql,repr(args),flush=True);raise
try:
 query('create database '+db);query('use '+db);query('create table docs(id int primary key,category int,content text,fulltext index ft(content) with parser beng)')
 rows=[(i,i%5,' '.join(w for w in ['alpha','bravo','charlie','delta','echo'] if rng.randrange(4)!=0)) for i in range(800)];rows[0]=(0,0,None);rows[1]=(1,1,'');rows[2]=(2,2,'term0000 term0001 term0299');cur.executemany('insert into docs values(%s,%s,%s)',rows)
 queries=['select count(*) from docs where {m}', 'select count(1),sum(1),avg(1),min(1),max(1) from docs where {m}', 'select count(null),count(distinct 1) from docs where {m}', 'select count(*)+sum(1) from docs where {m}', 'select count(*) from docs where {m} having count(*)>100', 'select count(*) from docs where {m} and id between 77 and 510', 'select count(*) from docs where {m} and category=3', 'select category,count(*) from docs where {m} group by category order by category', 'select sum(id),min(id),max(id),count(id) from docs where {m}', "select 'constant' as k from docs where {m}", 'select count(*) over() from docs where {m}', 'select count(*) from(select 1 as k from docs where {m})t', 'select count(*) from(select id from docs where {m} limit 17)t', 'select count(*) from(select distinct category from docs where {m})t', 'select count(*) from docs d join docs e on d.id=e.id where match(d.content) against(%s)>0', 'select exists(select 1 from docs where {m})', 'select count(*) from docs where not({m})', 'select count(*) from docs where {m} or id<8']
 terms=['alpha','alpha bravo','alpha bravo charlie','alpha bravo charlie delta','alpha alpha bravo','missingterm','missingterm alpha','the and','',None,' '.join('term%04d'%i for i in range(300))]
 for text in terms:
  for i,sql in enumerate(queries):
   for hint in ['', '/*+ index(docs ft) */ ', '/*+ no_index(docs ft) */ ']:
    if 'docs d join' in sql and hint:continue
    sql_to_run=sql.format(m='match(content) against(%s)>0').replace('select ', 'select '+hint,1)
    capture(str((text,i,hint)),sql_to_run,(text,))
 query("prepare card_stmt from 'select count(*) from docs where match(content) against(?)>?'")
 for text,threshold in [('alpha',0),('alpha bravo charlie',0),('alpha',1000),('alpha',0),('missingterm',0),('alpha bravo charlie delta',0)]:
  query('set @text=%s,@threshold=%s',(text,threshold));capture(str(('prepared',text,threshold)),'execute card_stmt using @text,@threshold')
 query('deallocate prepare card_stmt')
 query('set session transaction isolation level repeatable read');query('start transaction with consistent snapshot');capture('snapshot before',"select count(*) from docs where match(content) against('alpha bravo')>0")
 other=pymysql.connect(unix_socket=a.socket,user='root',database=db,autocommit=True,charset='utf8mb4')
 try:
  other.cursor().execute("insert into docs values(1000,1,'alpha bravo')");capture('snapshot after concurrent insert',"select count(*) from docs where match(content) against('alpha bravo')>0");assert results['snapshot before']==results['snapshot after concurrent insert']
 finally:other.close()
 query('commit');capture('snapshot committed',"select count(*) from docs where match(content) against('alpha bravo')>0")
 query('start transaction');query("update docs set content='changed' where id<40");capture('uncommitted own writes',"select count(*) from docs where match(content) against('alpha bravo')>0");query('rollback');capture('after rollback',"select count(*) from docs where match(content) against('alpha bravo')>0")
 query('delete from docs where id<40');capture('after delete',"select count(*) from docs where match(content) against('alpha bravo')>0")
 record={'checks':len(results),'results':results};a.output.write_text(json.dumps(record,ensure_ascii=False,indent=2,default=str)+'\n')
 if a.compare:assert json.loads(a.compare.read_text())==json.loads(a.output.read_text()),'cardinality results differ'
 print('PASS FTS cardinality:',len(results),'query results;', 'baseline equivalent' if a.compare else 'baseline recorded',flush=True)
finally:c.rollback();query('drop database if exists '+db);c.close()
