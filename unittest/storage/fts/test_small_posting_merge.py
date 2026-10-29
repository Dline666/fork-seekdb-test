import argparse,random,os
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);a=p.parse_args();c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60);cur=c.cursor();db='small_merge_'+str(os.getpid());checks=0
r=random.Random(20261029)
def query(sql,args=None):cur.execute(sql,args);return cur.fetchall()
def compare(table,text):
 global checks
 ids=query('select /*+ index('+table+' ft) */ k from '+table+' where match(content) against(%s)>0 order by k',(text,));scored=query('select /*+ index('+table+' ft) */ k from '+table+' where match(content) against(%s)+0>0 order by k',(text,));assert ids==scored,(table,text,ids,scored);checks+=1
 for cap in [1,7,17,40,900]:
  # Explicit ID order retains deterministic LIMIT semantics on both paths.
  got=query('select k from '+table+' where match(content) against(%s)>0 order by k limit '+str(cap),(text,));assert got==scored[:cap];checks+=1
try:
 query('create database '+db);query('use '+db)
 for table,key_type,key in [('int_docs','bigint',lambda i:i),('str_docs','varchar(100)',lambda i:'key-%04d'%i),('binary_docs','varbinary(100)',lambda i:bytes([i%256])+('%04d'%i).encode()+b'\0\xff')]:
  query('create table '+table+'(k '+key_type+' primary key,content text,fulltext index ft(content) with parser beng)')
  rows=[]
  for i in range(400):
   words=[w for w in ['alpha','bravo','charlie','delta','echo','foxtrot'] if r.randrange(4)!=0];words+=['repeated']*r.randrange(4);rows.append((key(i),' '.join(words)))
  rows[0]=(key(0),'');rows[1]=(key(1),None);cur.executemany('insert into '+table+' values(%s,%s)',rows)
  for text in ['alpha','bravo','charlie','alpha bravo','alpha charlie','alpha bravo charlie','alpha alpha bravo','bravo missingterm','missingterm alpha bravo','alpha bravo charlie delta','missingterm','',None]:compare(table,text)
  query('prepare p from '+"'select k from "+table+" where match(content) against(?)>0 order by k'")
  for text in ['alpha','alpha bravo charlie','bravo','alpha bravo charlie delta','alpha']:
   query('set @text=%s',(text,));actual=query('execute p using @text');expect=query('select k from '+table+' where match(content) against(%s)+0>0 order by k',(text,));assert actual==expect;checks+=1
  query('deallocate prepare p')
  query('start transaction');query('update '+table+" set content='differentwords' where k=%s",(key(7),));query('delete from '+table+' where k=%s',(key(8),));compare(table,'alpha bravo charlie');query('rollback');compare(table,'alpha bravo charlie')
 print('PASS small posting merge:',checks,'checks',flush=True)
finally:
 c.rollback();query('drop database if exists '+db);c.close()
