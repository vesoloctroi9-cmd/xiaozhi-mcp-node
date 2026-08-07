#!/usr/bin/env python3
import asyncio
import json
import os
import sys
import unittest
from pathlib import Path
from aiohttp import web

ROOT = Path(__file__).resolve().parents[1]
MCP = ROOT / 'mcp-calculator-main'
sys.path.insert(0, str(MCP))
import atlas_search_router as r


class ProviderContractTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.requests = []
        self.app = web.Application()
        self.app.router.add_post('/tavily/search', self.tavily_search)
        self.app.router.add_post('/tavily/extract', self.tavily_extract)
        self.app.router.add_get('/serpapi/search.json', self.serpapi_search)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, '127.0.0.1', 0)
        await self.site.start()
        port = self.site._server.sockets[0].getsockname()[1]
        self.base = f'http://127.0.0.1:{port}'
        self.old_urls = (r.TAVILY_SEARCH_URL, r.TAVILY_EXTRACT_URL, r.SERPAPI_SEARCH_URL)
        r.TAVILY_SEARCH_URL = self.base + '/tavily/search'
        r.TAVILY_EXTRACT_URL = self.base + '/tavily/extract'
        r.SERPAPI_SEARCH_URL = self.base + '/serpapi/search.json'
        os.environ['TAVILY_API_KEY'] = 'synthetic-tavily-key'
        os.environ['SERPAPI_API_KEY'] = 'synthetic-serpapi-key'
        self.router = r.AtlasSearchRouter()

    async def asyncTearDown(self):
        r.TAVILY_SEARCH_URL, r.TAVILY_EXTRACT_URL, r.SERPAPI_SEARCH_URL = self.old_urls
        os.environ.pop('TAVILY_API_KEY', None)
        os.environ.pop('SERPAPI_API_KEY', None)
        await self.runner.cleanup()

    async def tavily_search(self, request):
        body = await request.json()
        self.requests.append(('tavily_search', body, dict(request.headers)))
        return web.json_response({'results': [
            {'title':'TikTok public result', 'url':'https://www.tiktok.com/@demo/video/1', 'content':'du lịch #vungtau', 'published_date':'2026-08-07'},
            {'title':'Facebook public result', 'url':'https://www.facebook.com/reel/2', 'content':'du lịch #vungtau', 'published_date':'2026-08-07'},
        ], 'usage': {'credits': 1}})

    async def tavily_extract(self, request):
        body = await request.json()
        self.requests.append(('tavily_extract', body, dict(request.headers)))
        return web.json_response({'results': [{'url': body['urls'][0], 'raw_content': 'sample extracted public page text'}]})

    async def serpapi_search(self, request):
        params = dict(request.query)
        self.requests.append(('serpapi', params, dict(request.headers)))
        if params.get('engine') == 'youtube':
            return web.json_response({'video_results': [
                {'title':'YouTube Shorts Vũng Tàu', 'link':'https://www.youtube.com/watch?v=abc', 'channel':{'name':'Travel VN'}, 'published_date':'1 hour ago', 'views':123456, 'description':'du lịch #vungtau'}
            ]})
        return web.json_response({'organic_results': [
            {'title':'Public result', 'link':'https://example.com/a', 'snippet':'sample result', 'date':'Aug 7, 2026'}
        ]})

    async def test_31_tavily_domain_contract(self):
        out = await self.router._tavily_search('du lịch viral', max_results=5, domains=('tiktok.com','facebook.com'))
        self.assertEqual(len(out), 2)
        _, body, headers = self.requests[-1]
        self.assertEqual(body['search_depth'], 'basic')
        self.assertEqual(body['include_domains'], ['tiktok.com','facebook.com'])
        self.assertEqual(body['topic'], 'general')
        self.assertTrue(headers['Authorization'].startswith('Bearer '))

    async def test_32_serpapi_youtube_contract(self):
        out = await self.router._serpapi_youtube('Vũng Tàu shorts', max_results=5)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].platform, 'youtube')
        self.assertEqual(out[0].views, 123456)
        _, params, _ = self.requests[-1]
        self.assertEqual(params['engine'], 'youtube')
        self.assertEqual(params['search_query'], 'Vũng Tàu shorts')
        self.assertEqual(params['hl'], 'vi')
        self.assertEqual(params['gl'], 'vn')

    async def test_33_serpapi_google_contract(self):
        out = await self.router._serpapi_google('site:tiktok.com du lịch', max_results=5)
        self.assertEqual(len(out), 1)
        _, params, _ = self.requests[-1]
        self.assertEqual(params['engine'], 'google')
        self.assertTrue(params['q'].startswith('site:tiktok.com'))
        self.assertEqual(params['safe'], 'active')

    async def test_34_tavily_extract_contract(self):
        original = r.is_public_http_url
        r.is_public_http_url = lambda _u: True
        try:
            out = await self.router.fetch_text('https://example.com/article', 0, 200)
        finally:
            r.is_public_http_url = original
        self.assertIn('sample extracted public page text', out)
        _, body, _ = self.requests[-1]
        self.assertEqual(body['urls'], ['https://example.com/article'])
        self.assertEqual(body['extract_depth'], 'basic')
        self.assertEqual(body['format'], 'text')

    async def test_35_viral_route_uses_two_paid_calls_max(self):
        out = await self.router.search_text('hashtag du lịch Vũng Tàu viral', 8)
        self.assertIn('public/discoverable', out)
        names = [x[0] for x in self.requests]
        self.assertEqual(names.count('tavily_search'), 1)
        self.assertEqual(names.count('serpapi'), 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
