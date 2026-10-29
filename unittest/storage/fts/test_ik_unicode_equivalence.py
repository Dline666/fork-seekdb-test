"""Compare IK outputs over Unicode classification boundaries and random inputs."""
import argparse,json,pathlib,random,hashlib
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);p.add_argument('--output',required=True,type=pathlib.Path);p.add_argument('--compare',type=pathlib.Path);a=p.parse_args();rng=random.Random(20261029)
points=list(range(0,256));boundaries=[0x1100,0x11ff,0x3040,0x309f,0x30a0,0x30ff,0x3130,0x318f,0x31f0,0x31ff,0x3400,0x4dbf,0x4e00,0x9fff,0xac00,0xd7af,0xd800,0xdfff,0xf900,0xfaff,0xff00,0xff10,0xff19,0xff21,0xff3a,0xff41,0xff5a,0xffef,0x10000,0x20000,0x2fa1f,0x10ffff]
for boundary in boundaries:points.extend(range(max(0,boundary-3),min(0x110000,boundary+4)))
points += [rng.randrange(0x110000) for _ in range(2048)];points=[point for point in points if not(0xd800<=point<=0xdfff)]
texts=[''.join(chr(point) for point in points[i:i+24]) for i in range(0,len(points),24)]
texts += ['１２３.４５万 ＡＢＣ ａｂｃ 全角数字','一二三四五六七八九十百千万亿 两个人 1.2亿','南京市长江大桥 研究生命起源 中华人民共和国','Café İSTANBUL Straße Ελληνικά 𠀀😀',None,'','数据数据库'*300]
results={}
def canonical(value):
 if isinstance(value,list):return sorted((canonical(x) for x in value),key=lambda x:json.dumps(x,ensure_ascii=False,sort_keys=True))
 if isinstance(value,dict):return {k:canonical(v) for k,v in value.items()}
 return value
c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60);cur=c.cursor()
try:
 for i,text in enumerate(texts):
  for mode in ['smart','max_word']:
   for coll in ['utf8mb4_general_ci','utf8mb4_bin','utf8mb4_unicode_ci']:
    options=json.dumps([{'additional_args':[{'ik_mode':mode}]},{'output':'all'}])
    cur.execute('select tokenize(convert(%s using utf8mb4) collate '+coll+',%s,%s) as tokens',(text,'ik',options));results[str((i,mode,coll))]=canonical(json.loads(cur.fetchone()[0]))
 record={'checks':len(results),'points':len(points),'text_sha256':hashlib.sha256(json.dumps(texts,ensure_ascii=False).encode()).hexdigest(),'results':results};a.output.write_text(json.dumps(record,ensure_ascii=False,indent=2)+'\n')
 if a.compare:
  old=json.loads(a.compare.read_text());new=json.loads(a.output.read_text());assert old==new,'Unicode token outputs differ'
 print('PASS IK Unicode:',len(results),'queries over',len(points),'boundary/random code points;', 'baseline equivalent' if a.compare else 'baseline recorded',flush=True)
finally:c.close()
