#!/usr/bin/env python3
import json, os, subprocess, sys, time, unittest, select
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
MCP=ROOT/'mcp-calculator-main'
FAKE=ROOT/'tests'/'fake_ddg_child.py'

class ProxyStdioTests(unittest.TestCase):
    def setUp(self):
        env=os.environ.copy()
        env.pop('TAVILY_API_KEY',None)
        env.pop('SERPAPI_API_KEY',None)
        env.pop('ATLAS_CACHE_ENABLED',None)
        env['RESEARCH_CACHE_URL']='https://example.invalid/old-cache.json'
        env['ATLAS_DDG_CHILD_COMMAND']=sys.executable
        env['ATLAS_DDG_CHILD_ARGS_JSON']=json.dumps([str(FAKE)])
        self.p=subprocess.Popen(
            [sys.executable,str(MCP/'atlas_cache_mcp_proxy.py')],
            cwd=MCP,env=env,text=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            bufsize=1,
        )

    def tearDown(self):
        if self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait(timeout=3)
        for stream in (self.p.stdin, self.p.stdout, self.p.stderr):
            try:
                stream.close()
            except Exception:
                pass

    def rpc(self,msg,timeout=5):
        self.p.stdin.write(json.dumps(msg,separators=(',',':'))+'\n'); self.p.stdin.flush()
        deadline=time.time()+timeout
        while time.time()<deadline:
            r,_,_=select.select([self.p.stdout],[],[],0.2)
            if r:
                line=self.p.stdout.readline().strip()
                if line:
                    return json.loads(line)
            if self.p.poll() is not None:
                err=self.p.stderr.read()
                self.fail('proxy exited early: '+err)
        self.fail('timeout waiting for proxy response')

    def test_36_initialize_passthrough(self):
        out=self.rpc({'jsonrpc':'2.0','id':1,'method':'initialize','params':{'protocolVersion':'2024-11-05','capabilities':{},'clientInfo':{'name':'audit','version':'1'}}})
        self.assertEqual(out['result']['serverInfo']['name'],'fake-ddg')

    def test_37_tools_list_passthrough(self):
        out=self.rpc({'jsonrpc':'2.0','id':2,'method':'tools/list','params':{}})
        names=[x['name'] for x in out['result']['tools']]
        self.assertEqual(names,['search','fetch_content'])

    def test_38_search_falls_through_to_ddg_when_paid_sources_unavailable(self):
        out=self.rpc({'jsonrpc':'2.0','id':3,'method':'tools/call','params':{'name':'search','arguments':{'query':'hoa giấy'}}})
        self.assertEqual(out['result']['content'][0]['text'],'FAKE_DDG_RESULT:search')

    def test_39_fetch_falls_through_to_ddg_when_tavily_unavailable(self):
        out=self.rpc({'jsonrpc':'2.0','id':4,'method':'tools/call','params':{'name':'fetch_content','arguments':{'url':'https://example.com'}}})
        self.assertEqual(out['result']['content'][0]['text'],'FAKE_DDG_RESULT:fetch_content')

    def test_40_old_cache_url_does_not_trigger_http_or_block(self):
        out=self.rpc({'jsonrpc':'2.0','id':5,'method':'tools/call','params':{'name':'search','arguments':{'query':'du lịch'}}})
        self.assertEqual(out['result']['content'][0]['text'],'FAKE_DDG_RESULT:search')
        time.sleep(0.1)
        err=''
        while True:
            r,_,_=select.select([self.p.stderr],[],[],0)
            if not r:break
            line=self.p.stderr.readline()
            if not line:break
            err+=line
        self.assertNotIn('REFRESH FAIL',err)
        self.assertNotIn('HTTP 404',err)

if __name__=='__main__':
    unittest.main(verbosity=2)
