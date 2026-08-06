import asyncio
import json
import os
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import atlas_multi_source_final_v1 as m


class FakeRouter:
    def __init__(self, ddg_text="", fetch_text="DDG FETCH"):
        self.ddg_text = ddg_text
        self.fetch_text = fetch_text
        self.calls = []

    async def call_tool(self, name, arguments, timeout):
        self.calls.append((name, arguments, timeout))
        if name == "search":
            return {
                "jsonrpc": "2.0",
                "id": "internal",
                "result": {
                    "content": [{"type": "text", "text": self.ddg_text}],
                    "isError": False,
                },
            }
        if name == "fetch_content":
            return {
                "jsonrpc": "2.0",
                "id": "internal",
                "result": {
                    "content": [{"type": "text", "text": self.fetch_text}],
                    "isError": False,
                },
            }
        return {"error": "unknown"}


def ev(provider, url, title="atlas", snippet="atlas research", query="atlas", score=0.0):
    return m.Evidence(
        title=title,
        url=url,
        snippet=snippet,
        provider=provider,
        source_query=query,
        provider_score=score,
    ).finalize()


class CoreSelfTest(unittest.TestCase):
    def test_001_version(self):
        self.assertEqual(m.VERSION, "1.0.0")

    def test_002_normalize_vietnamese(self):
        self.assertEqual(m.normalize_text("Tin MỚI Việt Nam"), "tin moi viet nam")

    def test_003_tokens_remove_generic(self):
        self.assertEqual(m.tokens("tin mới ATLAS hôm nay"), ["atlas"])

    def test_004_tokens_dedup(self):
        self.assertEqual(m.tokens("atlas atlas research"), ["atlas", "research"])

    def test_005_news_vi(self):
        self.assertTrue(m.has_news_intent("tin mới AI hôm nay"))

    def test_006_news_en(self):
        self.assertTrue(m.has_news_intent("latest AI news"))

    def test_007_history_not_news(self):
        self.assertFalse(m.has_news_intent("lịch sử máy tính"))

    def test_008_thoi_su_news(self):
        self.assertTrue(m.has_news_intent("thời sự Việt Nam"))

    def test_009_restricted_gambling(self):
        self.assertTrue(m.query_is_restricted("casino gambling hôm nay"))

    def test_010_normal_query_not_restricted(self):
        self.assertFalse(m.query_is_restricted("tin công nghệ Việt Nam"))

    def test_011_parse_iso_z(self):
        self.assertIsNotNone(m.parse_iso("2026-08-07T10:00:00Z"))

    def test_012_parse_iso_invalid(self):
        self.assertIsNone(m.parse_iso("invalid"))

    def test_013_public_https(self):
        self.assertTrue(m.is_public_http_url("https://example.com/a"))

    def test_014_private_ip_block(self):
        self.assertFalse(m.is_public_http_url("http://127.0.0.1/x"))

    def test_015_localhost_block(self):
        self.assertFalse(m.is_public_http_url("http://localhost/x"))

    def test_016_non_http_block(self):
        self.assertFalse(m.is_public_http_url("file:///tmp/a"))

    def test_017_canonical_fragment(self):
        self.assertEqual(m.canonicalize_url("HTTPS://Example.COM/a/#x"), "https://example.com/a")

    def test_018_canonical_tracking(self):
        self.assertEqual(
            m.canonicalize_url("https://example.com/a?id=1&utm_source=x&fbclid=y"),
            "https://example.com/a?id=1",
        )

    def test_019_canonical_semantic_query(self):
        self.assertEqual(
            m.canonicalize_url("https://example.com/s?q=atlas&page=2"),
            "https://example.com/s?q=atlas&page=2",
        )

    def test_020_platform_fb(self):
        self.assertEqual(m.platform_of("https://www.facebook.com/x"), "facebook")

    def test_021_platform_tiktok(self):
        self.assertEqual(m.platform_of("https://www.tiktok.com/@x/video/1"), "tiktok")

    def test_022_platform_youtube(self):
        self.assertEqual(m.platform_of("https://youtu.be/abc"), "youtube")

    def test_023_platform_web(self):
        self.assertEqual(m.platform_of("https://example.com"), "web")

    def test_024_query_plan_social(self):
        with patch.object(m, "SOCIAL_DISCOVERY_ENABLED", True):
            queries = [x.query for x in m.build_query_plan("AI Việt Nam")]
        self.assertEqual(len(queries), 4)
        self.assertIn("site:facebook.com AI Việt Nam", queries)
        self.assertIn("site:tiktok.com AI Việt Nam", queries)
        self.assertIn("site:youtube.com AI Việt Nam", queries)

    def test_025_query_plan_social_off(self):
        with patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            queries = [x.query for x in m.build_query_plan("AI")]
        self.assertEqual(queries, ["AI"])

    def test_026_query_plan_empty(self):
        with self.assertRaises(ValueError):
            m.build_query_plan("  ")

    def test_027_mcp_text_roundtrip(self):
        raw = m.mcp_text_response(7, "hello")
        obj = json.loads(raw)
        self.assertEqual(obj["id"], 7)
        self.assertEqual(m.extract_mcp_text(obj), "hello")

    def test_028_ddg_markdown_parser(self):
        rows = m.parse_ddg_evidence(
            "[ATLAS News](https://example.com/a) useful snippet",
            m.PlannedQuery("atlas", "base"),
            5,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "ATLAS News")

    def test_029_ddg_plain_url_parser(self):
        rows = m.parse_ddg_evidence(
            "ATLAS result\nhttps://example.com/a\nmore detail",
            m.PlannedQuery("atlas", "base"),
            5,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "ATLAS result")

    def test_030_ddg_parser_dedup(self):
        text = "[A](https://example.com/a)\nhttps://example.com/a"
        rows = m.parse_ddg_evidence(text, m.PlannedQuery("a", "base"), 10)
        self.assertEqual(len(rows), 1)

    def test_031_dedup_provider_merge(self):
        rows = m.deduplicate([
            ev("cache", "https://same.test/a?utm_source=x"),
            ev("tavily", "https://same.test/a", snippet="longer atlas research snippet"),
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(set(rows[0].providers), {"cache", "tavily"})

    def test_032_dedup_query_preserved(self):
        rows = m.deduplicate([
            ev("cache", "https://same.test/s?q=a&page=1"),
            ev("cache", "https://same.test/s?q=a&page=2"),
        ])
        self.assertEqual(len(rows), 2)

    def test_033_relevance_ranking(self):
        good = ev("x", "https://good.test", title="atlas research", snippet="atlas research")
        bad = ev("x", "https://bad.test", title="cooking", snippet="recipe")
        self.assertGreater(m.evidence_rank_score(good, "atlas research"), m.evidence_rank_score(bad, "atlas research"))

    def test_034_multi_provider_bonus(self):
        a = ev("cache", "https://a.test")
        b = ev("cache", "https://b.test")
        m.merge_evidence(a, ev("tavily", "https://a.test"))
        self.assertGreater(m.evidence_rank_score(a, "atlas"), m.evidence_rank_score(b, "atlas"))

    def test_035_domain_limit(self):
        rows = [ev("x", f"https://same.test/{i}") for i in range(8)]
        ranked = m.rank_and_diversify(rows, "atlas", limit=20, max_per_domain=2)
        self.assertEqual(len(ranked), 2)

    def test_036_global_limit(self):
        rows = [ev("x", f"https://d{i}.test/a") for i in range(8)]
        ranked = m.rank_and_diversify(rows, "atlas", limit=3, max_per_domain=5)
        self.assertEqual(len(ranked), 3)

    def test_037_format_has_cache_policy(self):
        states = {x: m.ProviderState(x, True, ok=1, rows=1) for x in ("cache","tavily","serpapi","duckduckgo")}
        text = m.format_search_response("atlas", [ev("tavily", "https://a.test")], states)
        self.assertIn("Cache policy: supplemental", text)

    def test_038_format_social_label(self):
        states = {x: m.ProviderState(x, False) for x in ("cache","tavily","serpapi","duckduckgo")}
        text = m.format_search_response("atlas", [ev("serpapi", "https://facebook.com/a")], states)
        self.assertIn("public/search-indexed", text)

    def test_039_upstream_default_command(self):
        with patch.dict(os.environ, {"ATLAS_DDG_CHILD_COMMAND": "", "ATLAS_DDG_CHILD_ARGS_JSON": ""}, clear=False):
            cmd = m.upstream_command()
        self.assertEqual(cmd[0], "uvx")
        self.assertIn("duckduckgo-mcp-server", cmd)

    def test_040_upstream_json_args(self):
        with patch.dict(os.environ, {"ATLAS_DDG_CHILD_COMMAND": "python", "ATLAS_DDG_CHILD_ARGS_JSON": '["-m","x"]'}, clear=False):
            self.assertEqual(m.upstream_command(), ["python", "-m", "x"])


class AggregateFallbackSimulation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.router = FakeRouter("[DDG](https://ddg.test/a) atlas research")
        self.cache_rows = [ev("cache", "https://cache.test/a")]
        self.tavily_rows = [ev("tavily", "https://tavily.test/a")]
        self.serp_rows = [ev("serpapi", "https://serp.test/a")]

    async def _run(self, cache=None, tavily=None, serp=None, ddg_text=None, social=False):
        if ddg_text is not None:
            self.router.ddg_text = ddg_text
        async def fake_cache(planned, max_results):
            return list(self.cache_rows if cache is None else cache)
        async def fake_tavily(planned, max_results, news):
            return list(self.tavily_rows if tavily is None else tavily)
        async def fake_serp(planned, max_results, news):
            return list(self.serp_rows if serp is None else serp)
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", side_effect=fake_cache), \
             patch.object(m, "tavily_search", side_effect=fake_tavily), \
             patch.object(m, "serpapi_search", side_effect=fake_serp), \
             patch.dict(os.environ, {"TAVILY_API_KEY": "t", "SERPAPI_API_KEY": "s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", social):
            return await m.aggregate_search("atlas", 16, self.router)

    async def test_101_all_four_sources_contribute(self):
        results, states = await self._run()
        self.assertEqual(len(results), 4)
        self.assertEqual(states["cache"].ok, 1)
        self.assertEqual(states["tavily"].ok, 1)
        self.assertEqual(states["serpapi"].ok, 1)
        self.assertEqual(states["duckduckgo"].ok, 1)

    async def test_102_cache_hit_does_not_stop_live(self):
        results, states = await self._run()
        self.assertTrue(any("cache" in x.providers for x in results))
        self.assertTrue(any("tavily" in x.providers for x in results))
        self.assertTrue(any("serpapi" in x.providers for x in results))
        self.assertEqual(len(self.router.calls), 1)

    async def test_103_cache_failure_isolated(self):
        async def fail_cache(planned, max_results):
            raise RuntimeError("cache fail")
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", side_effect=fail_cache), \
             patch.object(m, "tavily_search", return_value=self.tavily_rows), \
             patch.object(m, "serpapi_search", return_value=self.serp_rows), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, self.router)
        self.assertGreaterEqual(len(results), 3)
        self.assertEqual(states["cache"].failed, 1)

    async def test_104_tavily_failure_isolated(self):
        async def fail(*args):
            raise RuntimeError("tavily fail")
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", return_value=self.cache_rows), \
             patch.object(m, "tavily_search", side_effect=fail), \
             patch.object(m, "serpapi_search", return_value=self.serp_rows), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, self.router)
        self.assertGreaterEqual(len(results), 3)
        self.assertEqual(states["tavily"].failed, 1)

    async def test_105_serp_failure_isolated(self):
        async def fail(*args):
            raise RuntimeError("serp fail")
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", return_value=self.cache_rows), \
             patch.object(m, "tavily_search", return_value=self.tavily_rows), \
             patch.object(m, "serpapi_search", side_effect=fail), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, self.router)
        self.assertGreaterEqual(len(results), 3)
        self.assertEqual(states["serpapi"].failed, 1)

    async def test_106_ddg_failure_isolated(self):
        class BadRouter:
            async def call_tool(self, *args, **kwargs):
                raise RuntimeError("ddg fail")
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", return_value=self.cache_rows), \
             patch.object(m, "tavily_search", return_value=self.tavily_rows), \
             patch.object(m, "serpapi_search", return_value=self.serp_rows), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, BadRouter())
        self.assertEqual(len(results), 3)
        self.assertEqual(states["duckduckgo"].failed, 1)

    async def test_107_three_fail_one_survives(self):
        async def fail_cache(*args): raise RuntimeError("x")
        async def fail_live(*args): raise RuntimeError("x")
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "search", side_effect=fail_cache), \
             patch.object(m, "tavily_search", side_effect=fail_live), \
             patch.object(m, "serpapi_search", side_effect=fail_live), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, self.router)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].provider, "duckduckgo")

    async def test_108_duplicate_cross_provider_merges(self):
        same_cache = [ev("cache", "https://same.test/a?utm_source=x")]
        same_tavily = [ev("tavily", "https://same.test/a", snippet="longer atlas research")]
        results, _ = await self._run(cache=same_cache, tavily=same_tavily, serp=[] , ddg_text="")
        self.assertEqual(len(results), 1)
        self.assertEqual(set(results[0].providers), {"cache", "tavily"})

    async def test_109_social_queries_expand_live_calls(self):
        tavily_calls=[]; serp_calls=[]
        async def ft(planned, max_results, news): tavily_calls.append(planned.query); return []
        async def fs(planned, max_results, news): serp_calls.append(planned.query); return []
        with patch.object(m.CACHE, "configured", return_value=False), \
             patch.object(m, "tavily_search", side_effect=ft), \
             patch.object(m, "serpapi_search", side_effect=fs), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t", "SERPAPI_API_KEY":"s"}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", True):
            await m.aggregate_search("atlas", 16, self.router)
        self.assertEqual(len(tavily_calls), 4)
        self.assertEqual(len(serp_calls), 4)
        self.assertTrue(any(q.startswith("site:facebook.com ") for q in tavily_calls))
        self.assertTrue(any(q.startswith("site:tiktok.com ") for q in tavily_calls))
        self.assertTrue(any(q.startswith("site:youtube.com ") for q in tavily_calls))

    async def test_110_ddg_only_base_query(self):
        await self._run(social=True)
        ddg_calls = [x for x in self.router.calls if x[0] == "search"]
        self.assertEqual(len(ddg_calls), 1)
        self.assertEqual(ddg_calls[0][1]["query"], "atlas")

    async def test_111_result_limit(self):
        many = [ev("tavily", f"https://d{i}.test/a") for i in range(30)]
        results, _ = await self._run(cache=[], tavily=many, serp=[], ddg_text="")
        self.assertLessEqual(len(results), 16)

    async def test_112_domain_diversity(self):
        many = [ev("tavily", f"https://same.test/{i}") for i in range(20)]
        results, _ = await self._run(cache=[], tavily=many, serp=[], ddg_text="")
        self.assertLessEqual(len(results), m.MAX_PER_DOMAIN)

    async def test_113_missing_api_keys_disable_live_api_sources(self):
        with patch.object(m.CACHE, "configured", return_value=False), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"", "SERPAPI_API_KEY":""}, clear=False), \
             patch.object(m, "SOCIAL_DISCOVERY_ENABLED", False):
            results, states = await m.aggregate_search("atlas", 16, self.router)
        self.assertFalse(states["tavily"].enabled)
        self.assertFalse(states["serpapi"].enabled)
        self.assertEqual(len(results), 1)

    async def test_114_empty_sources_do_not_count_failure(self):
        results, states = await self._run(cache=[], tavily=[], serp=[], ddg_text="")
        self.assertEqual(results, [])
        self.assertEqual(states["cache"].failed, 0)
        self.assertEqual(states["tavily"].failed, 0)
        self.assertEqual(states["serpapi"].failed, 0)
        self.assertEqual(states["duckduckgo"].failed, 0)


class InterceptIntegrationSimulation(unittest.IsolatedAsyncioTestCase):
    async def test_201_non_tool_call_passthrough(self):
        router = FakeRouter()
        self.assertIsNone(await m.maybe_intercept_tool_call(json.dumps({"method":"tools/list","id":1}), router))

    async def test_202_unknown_tool_passthrough(self):
        router = FakeRouter()
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"other","arguments":{}}}
        self.assertIsNone(await m.maybe_intercept_tool_call(json.dumps(msg), router))

    async def test_203_restricted_search_intercepted(self):
        router = FakeRouter()
        msg = {"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"search","arguments":{"query":"casino gambling"}}}
        out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["id"], 9)
        self.assertFalse(out["result"]["isError"])
        self.assertEqual(router.calls, [])

    async def test_204_search_original_id_preserved(self):
        router = FakeRouter("[DDG](https://ddg.test/a) atlas")
        async def fake_aggregate(q, n, r):
            states = {x:m.ProviderState(x, x=="duckduckgo", ok=1 if x=="duckduckgo" else 0, rows=1 if x=="duckduckgo" else 0) for x in ("cache","tavily","serpapi","duckduckgo")}
            return [ev("duckduckgo", "https://ddg.test/a")], states
        msg = {"jsonrpc":"2.0","id":"abc","method":"tools/call","params":{"name":"search","arguments":{"query":"atlas"}}}
        with patch.object(m, "aggregate_search", side_effect=fake_aggregate):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["id"], "abc")
        self.assertIn("ATLAS MULTI-SOURCE SEARCH", out["result"]["content"][0]["text"])

    async def test_205_fetch_live_wins_over_cache(self):
        router = FakeRouter(fetch_text="ddg")
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com/a"}}}
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "fetch_exact", new=AsyncMock(return_value="CACHE")), \
             patch.object(m, "tavily_extract", new=AsyncMock(return_value="LIVE")), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t"}, clear=False):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["result"]["content"][0]["text"], "LIVE")
        self.assertEqual(router.calls, [])

    async def test_206_fetch_cache_fallback(self):
        router = FakeRouter(fetch_text="ddg")
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com/a"}}}
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "fetch_exact", new=AsyncMock(return_value="CACHE")), \
             patch.object(m, "tavily_extract", new=AsyncMock(return_value=None)), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t"}, clear=False):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["result"]["content"][0]["text"], "CACHE")
        self.assertEqual(router.calls, [])

    async def test_207_fetch_ddg_final_fallback(self):
        router = FakeRouter(fetch_text="DDG FETCH CONTENT")
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com/a"}}}
        with patch.object(m.CACHE, "configured", return_value=False), \
             patch.dict(os.environ, {"TAVILY_API_KEY":""}, clear=False):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["result"]["content"][0]["text"], "DDG FETCH CONTENT")
        self.assertEqual(router.calls[0][0], "fetch_content")

    async def test_208_fetch_private_url_passthrough(self):
        router = FakeRouter()
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"http://127.0.0.1/a"}}}
        self.assertIsNone(await m.maybe_intercept_tool_call(json.dumps(msg), router))

    async def test_209_fetch_exceptions_fall_to_ddg(self):
        router = FakeRouter(fetch_text="DDG")
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com/a"}}}
        with patch.object(m.CACHE, "configured", return_value=True), \
             patch.object(m.CACHE, "fetch_exact", new=AsyncMock(side_effect=RuntimeError("cache fail"))), \
             patch.object(m, "tavily_extract", new=AsyncMock(side_effect=RuntimeError("live fail"))), \
             patch.dict(os.environ, {"TAVILY_API_KEY":"t"}, clear=False):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), router))
        self.assertEqual(out["result"]["content"][0]["text"], "DDG")

    async def test_210_fetch_ddg_failure_returns_message_not_crash(self):
        class BadRouter:
            async def call_tool(self, *args, **kwargs): raise RuntimeError("ddg fail")
        msg = {"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"fetch_content","arguments":{"url":"https://example.com/a"}}}
        with patch.object(m.CACHE, "configured", return_value=False), patch.dict(os.environ, {"TAVILY_API_KEY":""}, clear=False):
            out = json.loads(await m.maybe_intercept_tool_call(json.dumps(msg), BadRouter()))
        self.assertIn("Unable to fetch", out["result"]["content"][0]["text"])


class RouterSimulation(unittest.IsolatedAsyncioTestCase):
    async def test_301_fail_waiters(self):
        proc = SimpleNamespace(stdin=None)
        r = m.UpstreamRouter(proc)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        r._waiters["x"] = fut
        r.fail_waiters(RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            await fut

    async def test_302_internal_prefix(self):
        self.assertTrue(m.UpstreamRouter.INTERNAL_PREFIX.startswith("__atlas_internal"))

    async def test_303_route_internal_not_forwarded(self):
        proc = SimpleNamespace(stdin=None)
        r = m.UpstreamRouter(proc)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        rid = "__atlas_internal__1"
        r._waiters[rid] = fut
        raw = (json.dumps({"jsonrpc":"2.0","id":rid,"result":{"content":[]}})+"\n").encode()
        with patch.object(m, "write_stdout", new=AsyncMock()) as writer:
            await r.route_child_line(raw)
        self.assertTrue(fut.done())
        writer.assert_not_called()

    async def test_304_route_external_forwarded(self):
        proc = SimpleNamespace(stdin=None)
        r = m.UpstreamRouter(proc)
        raw = (json.dumps({"jsonrpc":"2.0","id":5,"result":{"content":[]}})+"\n").encode()
        with patch.object(m, "write_stdout", new=AsyncMock()) as writer:
            await r.route_child_line(raw)
        writer.assert_awaited_once()

    async def test_305_route_non_json_forwarded(self):
        proc = SimpleNamespace(stdin=None)
        r = m.UpstreamRouter(proc)
        with patch.object(m, "write_stdout", new=AsyncMock()) as writer:
            await r.route_child_line(b"not-json\n")
        writer.assert_awaited_once_with("not-json")


if __name__ == "__main__":
    unittest.main(verbosity=2)
