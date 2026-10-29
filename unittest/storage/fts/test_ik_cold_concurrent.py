"""Race first dictionary acquisition, then reuse loaders on independent sessions."""
import argparse,concurrent.futures,threading,json
import pymysql
p=argparse.ArgumentParser();p.add_argument('--socket',required=True);a=p.parse_args();barrier=threading.Barrier(12)
texts=['中华人民共和国中央人民政府 数据库全文索引 OceanBase 123', '南京市长江大桥 研究生命起源 Café 😀','数据库数据库 数据库 全文检索性能优化。'*35,'',None]
def canonical(value):
 if isinstance(value,list):return sorted((canonical(item) for item in value),key=lambda item:json.dumps(item,ensure_ascii=False,sort_keys=True))
 if isinstance(value,dict):return {key:canonical(item) for key,item in value.items()}
 return value
def worker(index):
 c=pymysql.connect(unix_socket=a.socket,user='root',autocommit=True,charset='utf8mb4',read_timeout=60);q=c.cursor();results=[]
 try:
  barrier.wait(timeout=15)
  for repeat in range(12):
   for mode in ['smart','max_word']:
    options=json.dumps([{'additional_args':[{'ik_mode':mode}]},{'output':'all'}])
    for text in texts:
     q.execute('select tokenize(convert(%s using utf8mb4),%s,%s) as tokens',(text,'ik' if repeat%2==0 else '  IK ',options));results.append(canonical(json.loads(q.fetchone()[0])))
  return results
 finally:c.close()
with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:outputs=list(pool.map(worker,range(12)))
for index,result in enumerate(outputs):assert result==outputs[0],index
assert all(outputs[0][i]==outputs[0][i%10] for i in range(len(outputs[0])))
print('PASS IK cold/concurrent loaders:',len(outputs)*len(outputs[0]),'checks across 12 clients',flush=True)
