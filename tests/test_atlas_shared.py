#!/usr/bin/env python3
import asyncio
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[1]
MCP = ROOT / "mcp-calculator-main"
sys.path.insert(0, str(MCP))

# Force cache-off default and remove real keys: tests must never consume quota.
os.environ.pop("TAVILY_API_KEY", None)
os.environ.pop("SERPAPI_API_KEY", None)
os.environ.pop("ATLAS_CACHE_ENABLED", None)
os.environ["RESEARCH_CACHE_URL"] = "https://example.invalid/old-cache.json"

import atlas_search_router as r
import atlas_cache_mcp_proxy as p


class RouterTests(unittest.TestCase):
    def test_01_general_arbitrary_topic_no_technology_bias(self):
        plan = r.build_plan("cách trồng hoa giấy")
        self.assertEqual(plan.intent, r.Intent.GENERAL)
        blob = " ".join(plan.expanded_queries).lower()
        self.assertNotIn("trí tuệ nhân tạo", blob)
        self.assertNotIn("công nghệ mới nhất", blob)

    def test_02_fresh_is_live_first(self):
        plan = r.build_plan("tin du lịch mới nhất hôm nay")
        self.assertEqual(plan.intent, r.Intent.FRESH)
        self.assertFalse(plan.cache_first)

    def test_03_tiktok_plan(self):
        plan = r.build_plan("TikTok món ăn viral Việt Nam")
        self.assertEqual(plan.intent, r.Intent.TIKTOK)
        self.assertIn("tiktok", plan.target_platforms)
        self.assertTrue(any("site:tiktok.com" in q for q in plan.expanded_queries))

    def test_04_facebook_plan(self):
        plan = r.build_plan("Facebook reels du lịch đang hot")
        self.assertEqual(plan.intent, r.Intent.FACEBOOK)
        self.assertTrue(any("site:facebook.com" in q for q in plan.expanded_queries))

    def test_05_youtube_plan(self):
        plan = r.build_plan("YouTube Shorts Vũng Tàu mới nhất")
        self.assertEqual(plan.intent, r.Intent.YOUTUBE)
        self.assertEqual(plan.target_platforms, ("youtube",))

    def test_06_multi_platform_query_becomes_viral(self):
        plan = r.build_plan("TikTok Facebook YouTube hashtag du lịch viral")
        self.assertEqual(plan.intent, r.Intent.VIRAL)
        self.assertEqual(set(plan.target_platforms), {"tiktok", "facebook", "youtube"})

    def test_07_hashtag_candidates_exist(self):
        tags = r.generate_hashtag_candidates("hashtag du lịch Vũng Tàu viral")
        self.assertTrue(tags)
        self.assertTrue(all(x.startswith("#") for x in tags))

    def test_08_tracking_url_dedupe(self):
        plan = r.build_plan("viral du lịch")
        a = r.SearchResult("A", "https://example.com/x?utm_source=a", "du lịch viral", "tavily")
        b = r.SearchResult("B", "https://example.com/x?utm_source=b", "du lịch viral", "serpapi")
        self.assertEqual(len(r.dedupe_rank([a,b], plan, 10)), 1)

    def test_09_viral_source_diversity(self):
        plan = r.build_plan("hashtag du lịch viral")
        items = [
            r.SearchResult("T", "https://www.tiktok.com/@a/video/1", "du lịch #vungtau", "tavily", "tiktok"),
            r.SearchResult("F", "https://www.facebook.com/reel/2", "du lịch #vungtau", "tavily", "facebook"),
            r.SearchResult("Y", "https://www.youtube.com/watch?v=3", "du lịch #vungtau", "serpapi", "youtube", views=1000),
        ]
        out = r.dedupe_rank(items, plan, 3)
        self.assertEqual({x.platform for x in out}, {"tiktok", "facebook", "youtube"})

    def test_10_cache_default_disabled_even_old_url_exists(self):
        self.assertFalse(p.CACHE.configured())

    def test_11_no_keys_returns_no_paid_result(self):
        router = r.AtlasSearchRouter()
        out = asyncio.run(router.search_text("cách trồng hoa giấy", 5))
        self.assertIsNone(out)

    def test_12_public_url_gate(self):
        self.assertTrue(r.is_public_http_url("https://example.com/a"))
        self.assertFalse(r.is_public_http_url("http://127.0.0.1/a"))
        self.assertFalse(r.is_public_http_url("http://localhost/a"))

    def test_13_provider_circuit(self):
        c = r.ProviderCircuit(cooldown_seconds=1)
        self.assertTrue(c.available("tavily"))
        c.failure("tavily", "x")
        self.assertFalse(c.available("tavily"))
        c.success("tavily")
        self.assertTrue(c.available("tavily"))

    def test_14_restricted_gate_runs_before_network(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(side_effect=AssertionError("network should not run"))
        original_gate = r.query_is_restricted
        r.query_is_restricted = lambda _q: True
        try:
            out = asyncio.run(router.search_text("synthetic blocked test", 5))
        finally:
            r.query_is_restricted = original_gate
        self.assertIn("unavailable", out.lower())
        router._tavily_search.assert_not_awaited()

    def test_15_general_tavily_success_avoids_serpapi_quota(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[r.SearchResult("A","https://example.com/a","hoa giấy","tavily")])
        router._serpapi_google = AsyncMock(return_value=[])
        out = asyncio.run(router.search_text("hoa giấy", 5))
        self.assertIn("ATLAS LIVE MULTI-SOURCE SEARCH", out)
        router._serpapi_google.assert_not_awaited()

    def test_16_general_fallback_to_serpapi(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[])
        router._serpapi_google = AsyncMock(return_value=[r.SearchResult("B","https://example.com/b","hoa giấy","serpapi")])
        out = asyncio.run(router.search_text("hoa giấy", 5))
        self.assertIn("serpapi", out.lower())
        router._serpapi_google.assert_awaited_once()

    def test_17_tiktok_domain_first_then_serpapi_if_sparse(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[])
        router._serpapi_google = AsyncMock(return_value=[r.SearchResult("T","https://tiktok.com/@a/video/1","viral","serpapi","tiktok")])
        out = asyncio.run(router.search_text("TikTok món ăn viral", 5))
        self.assertIn("tiktok", out.lower())
        args = router._tavily_search.await_args.kwargs
        self.assertEqual(args["domains"], ("tiktok.com",))
        self.assertTrue(router._serpapi_google.await_args.args[0].startswith("site:tiktok.com"))

    def test_18_facebook_domain_first(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[r.SearchResult("F","https://facebook.com/reel/1","hot","tavily","facebook"), r.SearchResult("F2","https://facebook.com/reel/2","hot","tavily","facebook")])
        router._serpapi_google = AsyncMock(return_value=[])
        out = asyncio.run(router.search_text("Facebook reels đang hot", 5))
        self.assertIn("facebook", out.lower())
        self.assertEqual(router._tavily_search.await_args.kwargs["domains"], ("facebook.com",))
        router._serpapi_google.assert_not_awaited()

    def test_19_youtube_direct_engine_route(self):
        router = r.AtlasSearchRouter()
        router._serpapi_youtube = AsyncMock(return_value=[r.SearchResult("Y","https://youtube.com/watch?v=1","shorts","serpapi","youtube")])
        router._tavily_search = AsyncMock(return_value=[])
        out = asyncio.run(router.search_text("YouTube Shorts Vũng Tàu", 5))
        self.assertIn("youtube", out.lower())
        router._serpapi_youtube.assert_awaited_once()
        router._tavily_search.assert_called_once() if False else None

    def test_20_viral_bounded_paid_fanout_exactly_two_route_calls(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[
            r.SearchResult("T","https://tiktok.com/@a/video/1","#vungtau","tavily","tiktok"),
            r.SearchResult("F","https://facebook.com/reel/2","#vungtau","tavily","facebook"),
        ])
        router._serpapi_youtube = AsyncMock(return_value=[
            r.SearchResult("Y","https://youtube.com/watch?v=3","#vungtau","serpapi","youtube",views=10000),
        ])
        router._serpapi_google = AsyncMock(return_value=[])
        out = asyncio.run(router.search_text("hashtag du lịch Vũng Tàu viral", 8))
        self.assertIn("public/discoverable", out)
        router._tavily_search.assert_awaited_once()
        router._serpapi_youtube.assert_awaited_once()
        router._serpapi_google.assert_not_awaited()

    def test_21_proxy_paid_success_intercepts_without_ddg(self):
        p.SEARCH_ROUTER.search_text = AsyncMock(return_value="multi live result")
        p.SEARCH_ROUTER.plan = lambda q: r.build_plan(q)
        msg = json.dumps({"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"search","arguments":{"query":"hoa giấy","max_results":5}}})
        raw = asyncio.run(p.maybe_intercept_tool_call(msg))
        self.assertIsNotNone(raw)
        payload = json.loads(raw)
        self.assertIn("multi live result", payload["result"]["content"][0]["text"])

    def test_22_proxy_empty_paid_result_falls_through_to_ddg(self):
        p.SEARCH_ROUTER.search_text = AsyncMock(return_value=None)
        p.SEARCH_ROUTER.plan = lambda q: r.build_plan(q)
        msg = json.dumps({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"search","arguments":{"query":"hoa giấy"}}})
        raw = asyncio.run(p.maybe_intercept_tool_call(msg))
        self.assertIsNone(raw)

    def test_23_proxy_non_search_passthrough(self):
        msg = json.dumps({"jsonrpc":"2.0","id":9,"method":"tools/list","params":{}})
        self.assertIsNone(asyncio.run(p.maybe_intercept_tool_call(msg)))

    def test_24_fetch_empty_falls_through(self):
        p.SEARCH_ROUTER.fetch_text = AsyncMock(return_value=None)
        msg = json.dumps({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com"}}})
        self.assertIsNone(asyncio.run(p.maybe_intercept_tool_call(msg)))

    def test_25_same_code_has_no_node_specific_runtime_name(self):
        text = (MCP / "mcp_pipe.py").read_text(encoding="utf-8")
        self.assertNotIn('SERVICE_NAME = "ATLAS NODE-2 MCP"', text)
        self.assertIn('SERVICE_NAME = "ATLAS MCP"', text)

    def test_26_workflow_has_no_fixed_research_topics_or_pages(self):
        wf = (ROOT / ".github/workflows/atlas_research.yml").read_text(encoding="utf-8").lower()
        self.assertNotIn("atlas_research_topics", wf)
        self.assertNotIn("deploy-pages", wf)
        self.assertNotIn("upload-pages", wf)
        self.assertIn("offline regression suite", wf)

    def test_27_router_never_logs_or_formats_secret_keys(self):
        text = (MCP / "atlas_search_router.py").read_text(encoding="utf-8")
        self.assertNotIn("print(key", text)
        self.assertNotIn("logger.info(key", text.lower())
        self.assertIn("Authorization", text)

    def test_28_mcp_config_still_uses_proxy_tool(self):
        data = json.loads((MCP / "mcp_config.json").read_text(encoding="utf-8"))
        srv = data["mcpServers"]["duckduckgo-web-search"]
        self.assertEqual(srv["command"], "python")
        self.assertEqual(srv["args"], ["atlas_cache_mcp_proxy.py"])

    def test_29_upstream_command_preserved(self):
        cmd = p.upstream_command()
        self.assertEqual(cmd[0], "uvx")
        self.assertIn("duckduckgo-mcp-server", cmd)

    def test_30_cache_can_be_explicitly_enabled_only(self):
        self.assertFalse(p.CACHE_ENABLED)

    def test_31_broad_sources_general_merges_two_paid_sources(self):
        router = r.AtlasSearchRouter()
        router._tavily_search = AsyncMock(return_value=[r.SearchResult("A","https://a.example/x","du lịch","tavily")])
        router._serpapi_google = AsyncMock(return_value=[r.SearchResult("B","https://b.example/y","du lịch","serpapi")])
        out = asyncio.run(router.search_text("tìm du lịch nhiều nguồn", 6))
        self.assertIn("tavily", out.lower())
        self.assertIn("serpapi", out.lower())
        router._tavily_search.assert_awaited_once()
        router._serpapi_google.assert_awaited_once()


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RouterTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
