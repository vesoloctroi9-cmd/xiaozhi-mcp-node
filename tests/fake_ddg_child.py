#!/usr/bin/env python3
import json, sys
for line in sys.stdin:
    line=line.strip()
    if not line:
        continue
    try:
        msg=json.loads(line)
    except Exception:
        continue
    if 'id' not in msg:
        continue
    mid=msg['id']
    method=msg.get('method','')
    if method=='initialize':
        result={'protocolVersion':'2024-11-05','capabilities':{'tools':{}},'serverInfo':{'name':'fake-ddg','version':'1'}}
    elif method=='tools/list':
        result={'tools':[{'name':'search','description':'fake search','inputSchema':{'type':'object'}},{'name':'fetch_content','description':'fake fetch','inputSchema':{'type':'object'}}]}
    elif method=='tools/call':
        name=(msg.get('params') or {}).get('name','')
        result={'content':[{'type':'text','text':f'FAKE_DDG_RESULT:{name}'}],'isError':False}
    else:
        result={'ok':True,'method':method}
    print(json.dumps({'jsonrpc':'2.0','id':mid,'result':result},separators=(',',':')),flush=True)
