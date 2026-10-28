"""Local deterministic protocol mock, not the organizer's hosted service."""
import json,threading
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
class Handler(BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def do_POST(self):
  body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  if self.path.endswith('/rerank'):
   docs=body['documents'];query=body['query'];result={'results':[{'index':i,'relevance_score':float(query.lower() in v.lower())} for i,v in reversed(list(enumerate(docs)))]}
  elif self.path.endswith('/embeddings') or self.path.endswith('/embed'):
   values=body.get('input',[]);values=[values] if isinstance(values,str) else values;dim=body.get('dimensions',768)
   data=[{'index':i,'embedding':[float(len(v))]+[.25]*(dim-1)} for i,v in reversed(list(enumerate(values)))]
   result={'embeddings':[v['embedding'] for v in reversed(data)]} if self.path.endswith('/embed') else {'data':data}
  elif self.path.endswith('/generate'):result={'response':'mock completion','done':True}
  else:result={'choices':[{'message':{'role':'assistant','content':'mock completion'}}]}
  payload=json.dumps(result).encode();self.send_response(200);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)

def start():
 server=ThreadingHTTPServer(('127.0.0.1',8000),Handler);threading.Thread(target=server.serve_forever,daemon=True).start();return server


if __name__ == '__main__':
 import argparse
 parser=argparse.ArgumentParser()
 parser.add_argument('--port',type=int,default=8000)
 args=parser.parse_args()
 server=ThreadingHTTPServer(('127.0.0.1',args.port),Handler)
 try:server.serve_forever()
 finally:server.server_close()
