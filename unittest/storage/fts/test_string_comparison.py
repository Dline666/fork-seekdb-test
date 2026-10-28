"""Compare native string ordering across candidate and baseline servers."""
import pathlib,argparse,json,random,os
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);p.add_argument('--output',required=True,type=pathlib.Path);p.add_argument('--compare',type=pathlib.Path);a=p.parse_args()
c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60);cur=c.cursor();db='cmp_regression_'+str(os.getpid());outputs={}
def query(sql,args=None):cur.execute(sql,args);return cur.fetchall()
def capture(key,sql,args=None):outputs[key]=query(sql,args)
try:
 query('create database '+db);query('use '+db)
 texts=[None,'',' ','  ','a','a ','a  ','A','a\0','a\0b','abc','ABC','AbC','abc\t','abc\n','é','e','É','e\u0301','Straße','STRASSE','中文','中文 ','数据库','数据库'*50,'😀','😃','𠀀','İ','ı','i','I','ß','ss','０','0','Å','A\u030a','a'*4000]
 rng=random.Random(20261029);alphabet='aaAÁée中中文01 \t\n\0😀ß'
 texts+=[''.join(rng.choice(alphabet) for _ in range(rng.randrange(1,120))) for _ in range(100)]
 # Duplicate keys force equal-byte comparisons in grouping and sort; IDs make
 # ties deterministic in complete output comparisons.
 texts*=3
 query('create table input_text(id int primary key,value varchar(16000))')
 cur.executemany('insert into input_text values(%s,%s)',enumerate(texts))
 for encoding,collations in [('utf8mb4',['utf8mb4_general_ci','utf8mb4_bin','utf8mb4_unicode_ci']),('utf8',['utf8_general_ci','utf8_bin','utf8_unicode_ci']),('binary',['binary'])]:
  for collation in collations:
   expr=('cast(value as binary)' if encoding=='binary' else 'convert(value using '+encoding+') collate '+collation)
   query('drop table if exists cmp_values')
   query('create table cmp_values as select id,'+expr+' as v from input_text')
   query('create index v_idx on cmp_values(v(1000))')
   for hint in ['', '/*+ no_index(cmp_values v_idx) */ ', '/*+ index(cmp_values v_idx) */ ']:
    for direction in ['asc','desc']:
     capture(collation+hint+direction,'select '+hint+'id,hex(v) from cmp_values order by v '+direction+',id')
     capture(collation+hint+'limit'+direction,'select '+hint+'id,hex(v) from cmp_values order by v '+direction+',id limit 83 offset 7')
   capture(collation+'group','select min(id),count(*) from cmp_values group by v order by min(id)')
   capture(collation+'window','select id,dense_rank() over(order by v),rank() over(order by v) from cmp_values order by id')
   capture(collation+'self join','select a.id,count(b.id) from cmp_values a left join cmp_values b on a.v=b.v group by a.id order by a.id')
   capture(collation+'nullsafe join','select a.id,count(b.id) from cmp_values a left join cmp_values b on a.v<=>b.v group by a.id order by a.id')
   capture(collation+'prefix','select id from cmp_values order by left(v,1),v,id')
   # CHAR padding/empty comparisons exercise both stored and expression paths.
   capture(collation+'char','select id from cmp_values order by cast(v as char(20)),id')
   capture(collation+'comparisons','select a.id,b.id,strcmp(a.v,b.v),a.v=b.v,a.v<=>b.v,a.v<b.v from cmp_values a join cmp_values b on a.id<39 and b.id<39 order by a.id,b.id')
 query('create table binary_values(id int primary key,v varbinary(300))')
 binary_values=[None,b'',b'\0',b'\xff',b'\x7f',b'\x80',b' ',b'  ',b'\0 ',b'\0\0',b'a',b'a ',b'a\0',b'a\xff',bytes(range(256)),b'\0'*256,b'\xff'*256]
 binary_values += [bytes([i]) for i in range(256)]
 binary_values += [bytes(rng.randrange(256) for _ in range(rng.randrange(1,100))) for _ in range(128)]
 cur.executemany('insert into binary_values values(%s,%s)',enumerate(binary_values*2))
 query('create index binary_idx on binary_values(v)')
 for hint in ['', '/*+ no_index(binary_values binary_idx) */ ', '/*+ index(binary_values binary_idx) */ ']:
  for direction in ['asc','desc']:
   capture('raw binary '+hint+direction,'select '+hint+'id,hex(v) from binary_values order by v '+direction+',id')
   capture('raw binary top '+hint+direction,'select '+hint+'id,hex(v) from binary_values order by v '+direction+',id limit 51 offset 11')
 capture('raw binary groups','select min(id),count(*) from binary_values group by v order by min(id)')
 capture('raw binary window','select id,dense_rank() over(order by v),rank() over(order by v) from binary_values order by id')
 capture('raw binary joins','select a.id,count(b.id) from binary_values a left join binary_values b on a.v<=>b.v group by a.id order by a.id')
 capture('raw binary comparisons','select a.id,b.id,strcmp(a.v,b.v),a.v=b.v,a.v<=>b.v,a.v<b.v from binary_values a join binary_values b on a.id<40 and b.id<40 order by a.id,b.id')
 record={'checks':len(outputs),'outputs':outputs};a.output.write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
 if a.compare:
  before=json.loads(a.compare.read_text());actual=json.loads(a.output.read_text());differences=[k for k,v in actual['outputs'].items() if before['outputs'].get(k)!=v]
  if set(before['outputs'])!=set(actual['outputs']) or differences:raise AssertionError(differences)
 print('PASS string comparison',len(outputs),'query results;', 'baseline equivalent' if a.compare else 'baseline recorded',flush=True)
finally:
 c.rollback();query('drop database if exists '+db);c.close()
