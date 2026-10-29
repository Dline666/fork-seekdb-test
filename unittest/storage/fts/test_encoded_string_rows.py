"""Validate stored string bytes before and after native compaction."""
import argparse,hashlib,os,random,time,json,pathlib
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);p.add_argument('--output',type=pathlib.Path);a=p.parse_args();c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=120);cur=c.cursor();db='encoded_string_'+str(os.getpid());rng=random.Random(20261029);checks=0

def query(sql,args=None):cur.execute(sql,args);return cur.fetchall()
def check_rows(stage):
 global checks
 for hint in ['/*+ no_use_column_store(docs) */','/*+ use_column_store(docs) */']:
  for order in ['asc','desc']:
   got=query('select '+hint+'id,hex(v),hex(w),hex(u) from docs order by id '+order)
   expected=sorted(rows,key=lambda row:row[0],reverse=order=='desc');expected=tuple((i,None if v is None else v.hex().upper(),None if w is None else w.hex().upper(),None if u is None else u.encode().hex().upper()) for i,v,w,u in expected);assert got==expected,(stage,hint,order,len(got));checks+=1
  for lower in [0,1,15,255,256,499,500,998,1023,1200,1500]:
   upper=lower+257;got=query('select '+hint+'id,hex(v),hex(w),hex(u) from docs where id>=%s and id<%s order by id',(lower,upper));expected=tuple((i,None if v is None else v.hex().upper(),None if w is None else w.hex().upper(),None if u is None else u.encode().hex().upper()) for i,v,w,u in rows if lower<=i<upper);assert got==expected,(stage,hint,lower);checks+=1
  for cap in [1,17,255,256,257,512,1500]:
   got=query('select '+hint+'id,hex(v),hex(w) from docs order by v,id limit '+str(cap));compare=query('select id,hex(v),hex(w) from docs order by v,id limit '+str(cap));assert got==compare;checks+=1
try:
 query('create database '+db);query('use '+db);query("create table docs(id int primary key,v varbinary(1000),w varbinary(1000),u varchar(1000)) row_format=compressed")
 rows=[]
 for i in range(2000):
  # Fixed and variable strings, shared prefixes/suffixes, hex-friendly bytes,
  # long common runs, embedded zero, and NULL patterns across batch boundaries.
  v=(b'\x00prefix-common-'+i.to_bytes(4,'big')+b'-suffix\xff')
  w=(b'leading shared bytes'*10+bytes([i%256])*rng.randrange(1,40)+b'trailer')
  u='相同前缀数据库-%08d-尾部'%i
  if i%11==0:v=None
  if i%13==0:w=None
  if i%17==0:u=None
  if i%19==0:v=b''
  rows.append((i,v,w,u))
 for start in range(0,len(rows),200):cur.executemany('insert into docs values(%s,%s,%s,%s)',rows[start:start+200])
 check_rows('memtable')
 table_id=query("select table_id from oceanbase.__all_table where database_id=(select database_id from oceanbase.__all_database where database_name=%s) and table_name='docs'",(db,))[0][0]
 query("alter system set merger_check_interval='10s'");query("alter system set ob_compaction_schedule_interval='3s'")
 query('alter system major freeze')
 deadline=time.monotonic()+120;compactions=[]
 while time.monotonic()<deadline:
  compactions=query("select type,tablet_id from oceanbase.__all_virtual_tablet_compaction_history where tablet_id in(select tablet_id from oceanbase.__all_tablet_to_table where table_id=%s) and type='MAJOR_MERGE'",(table_id,))
  if compactions:break
  time.sleep(.5)
 if not compactions:raise RuntimeError('native major compaction did not complete')
 check_rows('major SSTable')
 for i in [1,15,256,512,1000,1999]:
  query('update docs set v=%s,w=%s,u=%s where id=%s',(b'new\0'+i.to_bytes(4,'little'),b'\xffnew','更新数据',i));rows[i]=(i,b'new\0'+i.to_bytes(4,'little'),b'\xffnew','更新数据')
 check_rows('mixed SSTable and memtable')
 print('PASS encoded string data:',checks,'checks; major compaction',compactions,flush=True)
 if a.output:a.output.write_text(json.dumps({'passed':True,'checks':checks,'native_major_compactions':compactions,'table_id':table_id},indent=2)+'\n')
finally:
 c.rollback();query('drop database if exists '+db);c.close()
