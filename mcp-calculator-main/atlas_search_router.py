#!/usr/bin/env python3
"""
ATLAS SHARED MULTI-SOURCE SEARCH ROUTER
Version: 3.0.0

One shared search layer for both ATLAS nodes. The production nodes should use
identical files and settings; only MCP_ENDPOINT differs at deployment.

Public-web routes
-----------------
- General/fresh web: Tavily first, SerpApi Google as fallback, then upstream DDG.
- TikTok public/discoverable: Tavily domain-filtered, SerpApi Google site fallback.
- Facebook public/discoverable: Tavily domain-filtered, SerpApi Google site fallback.
- YouTube: SerpApi YouTube engine, Tavily YouTube-domain fallback.
- Viral/hashtag: bounded fan-out to TikTok/Facebook via Tavily plus YouTube via
  SerpApi, then deterministic dedupe/ranking/source-diversity.

No login bypass, private-content access, or closed-group access is attempted.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import math
import os
import re
import time
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout

VERSION = "3.0.0"
LOGGER = logging.getLogger("ATLAS_SEARCH_ROUTER")

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
TAVILY_EXTRACT_URL = "https://api.tavily.com/extract"
SERPAPI_SEARCH_URL = "https://serpapi.com/search.json"


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        value = default
    return max(minimum, min(maximum, value))


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


WEB_TIMEOUT_SECONDS = env_int("ATLAS_WEB_TIMEOUT_SECONDS", 12, 3, 30)
TAVILY_MAX_RESULTS = env_int("ATLAS_TAVILY_MAX_RESULTS", 6, 1, 10)
SERPAPI_MAX_RESULTS = env_int("ATLAS_SERPAPI_MAX_RESULTS", 6, 1, 10)
PROVIDER_COOLDOWN_SECONDS = env_int("ATLAS_PROVIDER_COOLDOWN_SECONDS", 120, 15, 3600)
MAX_RESULT_TEXT_CHARS = env_int("ATLAS_SEARCH_MAX_TEXT_CHARS", 12000, 2000, 24000)


# Narrow safety gate: informational public-web queries remain available; only
# clear requests to locate/access restricted material are blocked.
_RESTRICTED_PATTERNS = [
    re.compile(r"\b(?:weapon|illegal drug|gambling|adult content)\b", re.I),
]


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFD", value or "")
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9#]+", " ", text)
    return " ".join(text.split())


def tokens(value: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in normalize_text(value).replace("#", " ").split():
        if len(token) < 2:
            continue
        if token not in seen:
            seen.add(token)
            out.append(token)
    return out


def query_is_restricted(query: str) -> bool:
    n = normalize_text(query)
    return any(p.search(n) for p in _RESTRICTED_PATTERNS)


def canonical_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        p = urlsplit(raw)
    except Exception:
        return raw.rstrip("/")
    if not p.scheme or not p.netloc:
        return raw.rstrip("/")
    path = p.path or "/"
    if path != "/":
        path = path.rstrip("/")
    pairs = []
    for k, v in parse_qsl(p.query, keep_blank_values=True):
        lk = k.lower()
        if lk.startswith("utm_") or lk in {"fbclid", "gclid"}:
            continue
        pairs.append((k, v))
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), path, urlencode(pairs), ""))


def is_public_http_url(value: str) -> bool:
    try:
        parts = urlsplit((value or "").strip())
    except Exception:
        return False
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        return False
    host = parts.hostname.lower().rstrip(".")
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return True
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
        or ip.is_reserved or ip.is_unspecified
    )


def platform_from_url(url: str) -> str:
    try:
        host = urlsplit(url or "").netloc.lower()
    except Exception:
        return "web"
    if "tiktok.com" in host:
        return "tiktok"
    if "facebook.com" in host or "fb.watch" in host:
        return "facebook"
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube"
    return "web"


def extract_hashtags(text: str) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in re.findall(r"#[A-Za-z0-9_À-ỹ]+", text or "", re.UNICODE):
        tag = normalize_text(raw).replace(" ", "")
        if not tag.startswith("#"):
            tag = "#" + tag
        if len(tag) > 2 and tag not in seen:
            seen.add(tag)
            out.append(tag)
    return out


class Intent(str, Enum):
    GENERAL = "general"
    FRESH = "fresh"
    TIKTOK = "tiktok"
    FACEBOOK = "facebook"
    YOUTUBE = "youtube"
    VIRAL = "viral"


@dataclass(frozen=True)
class QueryPlan:
    intent: Intent
    original_query: str
    expanded_queries: Tuple[str, ...]
    target_platforms: Tuple[str, ...]
    freshness: bool
    hashtag_mode: bool
    cache_first: bool
    broad_sources: bool


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str = ""
    provider: str = ""
    platform: str = "web"
    published_at: str = ""
    views: Optional[int] = None
    hashtags: List[str] = field(default_factory=list)
    score: float = 0.0

    def key(self) -> str:
        return canonical_url(self.url) or normalize_text(self.title + " " + self.snippet)[:240]


@dataclass
class ProviderState:
    failures: int = 0
    disabled_until: float = 0.0
    last_error: str = ""


class ProviderCircuit:
    def __init__(self, cooldown_seconds: int = PROVIDER_COOLDOWN_SECONDS) -> None:
        self.cooldown_seconds = cooldown_seconds
        self.states: Dict[str, ProviderState] = {
            "tavily": ProviderState(),
            "serpapi": ProviderState(),
        }

    def available(self, provider: str) -> bool:
        return time.monotonic() >= self.states[provider].disabled_until

    def success(self, provider: str) -> None:
        st = self.states[provider]
        st.failures = 0
        st.disabled_until = 0.0
        st.last_error = ""

    def failure(self, provider: str, error: str, *, long: bool = False) -> None:
        st = self.states[provider]
        st.failures += 1
        st.last_error = error[:240]
        multiplier = 5 if long else min(4, st.failures)
        st.disabled_until = time.monotonic() + self.cooldown_seconds * multiplier


def _mentions(query: str) -> List[str]:
    n = normalize_text(query)
    found = []
    if "tiktok" in n:
        found.append("tiktok")
    if "facebook" in n or re.search(r"\\bfb\\b", n) or "reels" in n:
        found.append("facebook")
    if "youtube" in n or "shorts" in n:
        found.append("youtube")
    return found


def detect_intent(query: str) -> Intent:
    n = normalize_text(query)
    mentions = _mentions(query)
    viral_hints = ("viral", "trending", "trend", "hashtag", "xu huong", "dang hot")
    viral = "#" in query or any(x in n for x in viral_hints)
    if len(mentions) >= 2:
        return Intent.VIRAL
    if len(mentions) == 1:
        return {"tiktok": Intent.TIKTOK, "facebook": Intent.FACEBOOK, "youtube": Intent.YOUTUBE}[mentions[0]]
    if viral:
        return Intent.VIRAL
    fresh_hints = ("hom nay", "moi nhat", "latest", "today", "recent", "news", "tin moi", "cap nhat", "hien nay")
    if any(x in n for x in fresh_hints):
        return Intent.FRESH
    return Intent.GENERAL


def _topic_words(query: str, max_words: int = 4) -> List[str]:
    stop = {
        "tim", "kiem", "cho", "toi", "ve", "dang", "la", "nhung", "cac", "hom", "nay", "moi", "nhat",
        "viral", "trending", "trend", "hashtag", "tren", "tiktok", "facebook", "youtube", "shorts", "reels",
        "find", "search", "latest", "today", "about", "xu", "huong", "hot",
    }
    words = [w for w in normalize_text(query).replace("#", " ").split() if w not in stop and len(w) > 1]
    return words[:max_words]


def generate_hashtag_candidates(query: str) -> List[str]:
    explicit = extract_hashtags(query)
    words = _topic_words(query, 4)
    generated: List[str] = []
    if words:
        generated.append("#" + "".join(words))
        if len(words) >= 2:
            generated.append("#" + "".join(words[:2]))
        generated.extend("#" + w for w in words[:3])
    out: List[str] = []
    seen = set()
    for tag in [*explicit, *generated]:
        tag = normalize_text(tag).replace(" ", "")
        if not tag.startswith("#"):
            tag = "#" + tag
        if len(tag) > 2 and tag not in seen:
            seen.add(tag)
            out.append(tag)
    return out[:6]


def build_plan(query: str) -> QueryPlan:
    original = (query or "").strip()
    if not original:
        raise ValueError("query is empty")
    intent = detect_intent(original)
    tags = generate_hashtag_candidates(original)
    freshness = intent in {Intent.FRESH, Intent.VIRAL}
    hashtag_mode = intent == Intent.VIRAL or bool(extract_hashtags(original))
    queries: List[str] = [original]

    def add(q: str) -> None:
        q = " ".join(q.split())
        if q and q not in queries:
            queries.append(q)

    if intent == Intent.TIKTOK:
        targets = ("tiktok",)
        add(f"site:tiktok.com {original}")
        if tags: add("site:tiktok.com " + " ".join(tags[:3]))
    elif intent == Intent.FACEBOOK:
        targets = ("facebook",)
        add(f"site:facebook.com {original}")
        if tags: add("site:facebook.com " + " ".join(tags[:3]))
    elif intent == Intent.YOUTUBE:
        targets = ("youtube",)
        add(f"site:youtube.com {original}")
    elif intent == Intent.VIRAL:
        targets = ("tiktok", "facebook", "youtube")
        add(f"site:tiktok.com {original}")
        add(f"site:facebook.com {original}")
        add(f"site:youtube.com {original}")
        if tags:
            blob = " ".join(tags[:4])
            add(f"site:tiktok.com {blob}")
            add(f"site:facebook.com {blob}")
            add(f"site:youtube.com {blob}")
    else:
        targets = ("web",)
        if intent == Intent.FRESH:
            add(original + " mới nhất")

    broad_hints = ("nhieu nguon", "da nguon", "tong hop nguon", "nhieu trang", "multiple sources", "many sources")
    broad_sources = any(x in normalize_text(original) for x in broad_hints)

    return QueryPlan(
        intent=intent,
        original_query=original,
        expanded_queries=tuple(queries),
        target_platforms=targets,
        freshness=freshness,
        hashtag_mode=hashtag_mode,
        cache_first=(intent == Intent.GENERAL),
        broad_sources=broad_sources,
    )


def _score_result(result: SearchResult, plan: QueryPlan) -> float:
    q = set(tokens(plan.original_query))
    blob = set(tokens(result.title + " " + result.snippet + " " + " ".join(result.hashtags)))
    score = len(q & blob) * 3.0
    if result.platform in plan.target_platforms:
        score += 4.0
    elif plan.target_platforms == ("web",) and result.platform == "web":
        score += 2.0
    if plan.freshness and result.published_at:
        score += 1.5
    if plan.hashtag_mode and result.hashtags:
        score += min(2.0, 0.5 * len(result.hashtags))
    if result.views and result.views > 0:
        score += min(3.0, math.log10(result.views + 1) / 2.0)
    score += {"tavily": 0.8, "serpapi": 0.7}.get(result.provider, 0.0)
    return round(score, 3)


def dedupe_rank(results: Iterable[SearchResult], plan: QueryPlan, max_results: int) -> List[SearchResult]:
    best: Dict[str, SearchResult] = {}
    for result in results:
        result.platform = result.platform or platform_from_url(result.url)
        result.score = _score_result(result, plan)
        key = result.key()
        if not key:
            continue
        old = best.get(key)
        if old is None or result.score > old.score:
            best[key] = result
    ranked = sorted(best.values(), key=lambda r: (r.score, r.views or 0, r.published_at), reverse=True)
    if plan.intent != Intent.VIRAL:
        return ranked[:max_results]

    # For cross-platform trend discovery, force source diversity when available.
    diverse: List[SearchResult] = []
    seen = set()
    for platform in ("tiktok", "facebook", "youtube"):
        item = next((r for r in ranked if r.platform == platform), None)
        if item:
            diverse.append(item)
            seen.add(item.key())
    for item in ranked:
        if item.key() in seen:
            continue
        diverse.append(item)
        seen.add(item.key())
        if len(diverse) >= max_results:
            break
    return diverse[:max_results]


def _format_results(plan: QueryPlan, results: Sequence[SearchResult], trace: Sequence[str]) -> Optional[str]:
    if not results:
        return None
    heading = "ATLAS LIVE MULTI-SOURCE SEARCH"
    if plan.intent == Intent.VIRAL:
        heading += " — public/discoverable trend discovery"
    parts = [heading, f"Query: {plan.original_query}", f"Intent: {plan.intent.value}", ""]
    for idx, r in enumerate(results, 1):
        parts.append(f"[{idx}] {r.title or 'Result'}")
        parts.append(f"Source: {r.provider} | Platform: {r.platform}")
        if r.published_at:
            parts.append(f"Published: {r.published_at}")
        if r.views is not None:
            parts.append(f"Views: {r.views}")
        if r.hashtags:
            parts.append("Hashtags: " + " ".join(r.hashtags[:8]))
        if r.url:
            parts.append(f"URL: {r.url}")
        if r.snippet:
            parts.append("Snippet: " + r.snippet[:1600])
        parts.append("")
    if plan.intent == Intent.VIRAL:
        parts.append("Note: ranking is based on available public search metadata; it is not a claim of an official platform-wide viral rank.")
    if trace and env_bool("ATLAS_SEARCH_TRACE", False):
        parts.extend(["", "Route trace: " + " -> ".join(trace)])
    return "\n".join(parts).strip()[:MAX_RESULT_TEXT_CHARS]


class AtlasSearchRouter:
    def __init__(self) -> None:
        self.circuit = ProviderCircuit()

    def plan(self, query: str) -> QueryPlan:
        return build_plan(query)

    def _tavily_key(self) -> str:
        return os.environ.get("TAVILY_API_KEY", "").strip()

    def _serpapi_key(self) -> str:
        return os.environ.get("SERPAPI_API_KEY", "").strip()

    async def _tavily_search(self, query: str, *, max_results: int, topic: str = "general", domains: Sequence[str] = ()) -> List[SearchResult]:
        key = self._tavily_key()
        if not key or not self.circuit.available("tavily"):
            return []
        body: Dict[str, Any] = {
            "query": query,
            "search_depth": "basic",
            "topic": topic,
            "max_results": max(1, min(TAVILY_MAX_RESULTS, max_results)),
            "include_answer": False,
            "include_raw_content": False,
            "include_images": False,
        }
        if domains:
            body["include_domains"] = list(domains)
        timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.post(
                    TAVILY_SEARCH_URL,
                    json=body,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                        "User-Agent": f"ATLAS-Search-Router/{VERSION}",
                    },
                ) as response:
                    text = await response.text()
                    if response.status != 200:
                        long = response.status in {401, 403, 429}
                        raise RuntimeError(f"HTTP {response.status}: {text[:240]}|long={int(long)}")
                    data = json.loads(text)
            if not isinstance(data, dict):
                return []
            items = data.get("results", [])
            if not isinstance(items, list):
                return []
            out: List[SearchResult] = []
            for item in items[:max_results]:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url", "")).strip()
                title = str(item.get("title", "")).strip()
                snippet = str(item.get("content", "")).strip()
                if not url and not snippet:
                    continue
                out.append(SearchResult(
                    title=title or "Result",
                    url=url,
                    snippet=snippet,
                    provider="tavily",
                    platform=platform_from_url(url),
                    published_at=str(item.get("published_date", "")).strip(),
                    hashtags=extract_hashtags(title + " " + snippet),
                ))
            self.circuit.success("tavily")
            return out
        except Exception as exc:
            msg = str(exc)
            self.circuit.failure("tavily", msg, long="|long=1" in msg)
            LOGGER.warning("[TAVILY] search fail | %s", msg[:240])
            return []

    async def _serpapi_google(self, query: str, *, max_results: int, news: bool = False) -> List[SearchResult]:
        key = self._serpapi_key()
        if not key or not self.circuit.available("serpapi"):
            return []
        params: Dict[str, str] = {
            "engine": "google", "q": query, "api_key": key,
            "num": str(max(1, min(SERPAPI_MAX_RESULTS, max_results))),
            "hl": "vi", "gl": "vn", "safe": "active", "output": "json",
        }
        if news:
            params["tbm"] = "nws"
        return await self._serpapi_request(params, max_results=max_results, platform_hint="web", news=news)

    async def _serpapi_youtube(self, query: str, *, max_results: int) -> List[SearchResult]:
        key = self._serpapi_key()
        if not key or not self.circuit.available("serpapi"):
            return []
        params = {
            "engine": "youtube", "search_query": query, "api_key": key,
            "hl": "vi", "gl": "vn", "output": "json",
        }
        return await self._serpapi_request(params, max_results=max_results, platform_hint="youtube", news=False)

    async def _serpapi_request(self, params: Mapping[str, str], *, max_results: int, platform_hint: str, news: bool) -> List[SearchResult]:
        timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(
                    SERPAPI_SEARCH_URL,
                    params=dict(params),
                    headers={"User-Agent": f"ATLAS-Search-Router/{VERSION}"},
                ) as response:
                    text = await response.text()
                    if response.status != 200:
                        long = response.status in {401, 403, 429}
                        raise RuntimeError(f"HTTP {response.status}: {text[:240]}|long={int(long)}")
                    data = json.loads(text)
            if not isinstance(data, dict):
                return []
            if data.get("error"):
                raise RuntimeError(str(data.get("error")) + "|long=1")

            if params.get("engine") == "youtube":
                items = data.get("video_results", [])
                if not isinstance(items, list):
                    items = []
                out: List[SearchResult] = []
                for item in items[:max_results]:
                    if not isinstance(item, dict):
                        continue
                    url = str(item.get("link", "")).strip()
                    title = str(item.get("title", "")).strip()
                    description = str(item.get("description", "")).strip()
                    channel = item.get("channel", "")
                    if isinstance(channel, dict):
                        channel = channel.get("name", "")
                    snippet = " | ".join(x for x in [str(channel or "").strip(), description, str(item.get("published_date", "")).strip()] if x)
                    views = item.get("views")
                    if isinstance(views, str):
                        views = None
                    try:
                        views = int(views) if views is not None else None
                    except (TypeError, ValueError):
                        views = None
                    out.append(SearchResult(
                        title=title or "YouTube result", url=url, snippet=snippet,
                        provider="serpapi", platform="youtube",
                        published_at=str(item.get("published_date", "")).strip(),
                        views=views, hashtags=extract_hashtags(title + " " + description),
                    ))
                self.circuit.success("serpapi")
                return out

            items = data.get("news_results") if news else data.get("organic_results")
            if not isinstance(items, list):
                items = data.get("organic_results", [])
            if not isinstance(items, list):
                items = []
            out = []
            for item in items[:max_results]:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("link", "")).strip()
                title = str(item.get("title", "")).strip()
                snippet = str(item.get("snippet", "")).strip()
                if not url and not snippet:
                    continue
                out.append(SearchResult(
                    title=title or "Google result", url=url, snippet=snippet,
                    provider="serpapi", platform=platform_from_url(url) if platform_hint == "web" else platform_hint,
                    published_at=str(item.get("date", "")).strip(),
                    hashtags=extract_hashtags(title + " " + snippet),
                ))
            self.circuit.success("serpapi")
            return out
        except Exception as exc:
            msg = str(exc)
            self.circuit.failure("serpapi", msg, long="|long=1" in msg)
            LOGGER.warning("[SERPAPI] search fail | %s", msg[:240])
            return []

    async def search_text(self, query: str, max_results: int = 10) -> Optional[str]:
        if query_is_restricted(query):
            return "Web search is unavailable for this request. I can still help with safe, factual information."

        plan = build_plan(query)
        limit = max(1, min(10, int(max_results or 10)))
        collected: List[SearchResult] = []
        trace: List[str] = []

        if plan.intent == Intent.GENERAL:
            got = await self._tavily_search(query, max_results=limit, topic="general")
            trace.append(f"tavily:{len(got)}")
            collected.extend(got)
            if plan.broad_sources or not collected:
                got = await self._serpapi_google(query, max_results=limit)
                trace.append(f"serpapi-google:{len(got)}")
                collected.extend(got)

        elif plan.intent == Intent.FRESH:
            got = await self._tavily_search(query, max_results=limit, topic="news")
            trace.append(f"tavily-news:{len(got)}")
            collected.extend(got)
            if plan.broad_sources or len(collected) < 2:
                got = await self._serpapi_google(query, max_results=limit, news=True)
                trace.append(f"serpapi-news:{len(got)}")
                collected.extend(got)

        elif plan.intent == Intent.TIKTOK:
            got = await self._tavily_search(query, max_results=limit, domains=("tiktok.com",))
            trace.append(f"tavily-tiktok:{len(got)}")
            collected.extend(got)
            if len(collected) < 2:
                got = await self._serpapi_google(f"site:tiktok.com {query}", max_results=limit)
                trace.append(f"serpapi-tiktok:{len(got)}")
                collected.extend(got)

        elif plan.intent == Intent.FACEBOOK:
            got = await self._tavily_search(query, max_results=limit, domains=("facebook.com",))
            trace.append(f"tavily-facebook:{len(got)}")
            collected.extend(got)
            if len(collected) < 2:
                got = await self._serpapi_google(f"site:facebook.com {query}", max_results=limit)
                trace.append(f"serpapi-facebook:{len(got)}")
                collected.extend(got)

        elif plan.intent == Intent.YOUTUBE:
            got = await self._serpapi_youtube(query, max_results=limit)
            trace.append(f"serpapi-youtube:{len(got)}")
            collected.extend(got)
            if len(collected) < 2:
                got = await self._tavily_search(query, max_results=limit, domains=("youtube.com",))
                trace.append(f"tavily-youtube:{len(got)}")
                collected.extend(got)

        else:  # VIRAL / hashtag / multi-platform
            # Bounded paid fan-out: at most one Tavily request + one SerpApi request.
            social = await self._tavily_search(query, max_results=limit, domains=("tiktok.com", "facebook.com"))
            trace.append(f"tavily-social:{len(social)}")
            collected.extend(social)
            yt = await self._serpapi_youtube(query, max_results=limit)
            trace.append(f"serpapi-youtube:{len(yt)}")
            collected.extend(yt)

        ranked = dedupe_rank(collected, plan, max_results=limit)
        return _format_results(plan, ranked, trace)

    async def fetch_text(self, url: str, start_index: int = 0, max_length: int = 8000) -> Optional[str]:
        key = self._tavily_key()
        if not key or not is_public_http_url(url) or not self.circuit.available("tavily"):
            return None
        body = {"urls": [url], "extract_depth": "basic", "include_images": False, "format": "text"}
        timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.post(
                    TAVILY_EXTRACT_URL,
                    json=body,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                        "User-Agent": f"ATLAS-Search-Router/{VERSION}",
                    },
                ) as response:
                    text = await response.text()
                    if response.status != 200:
                        long = response.status in {401, 403, 429}
                        raise RuntimeError(f"HTTP {response.status}: {text[:240]}|long={int(long)}")
                    data = json.loads(text)
            results = data.get("results", []) if isinstance(data, dict) else []
            if not isinstance(results, list) or not results or not isinstance(results[0], dict):
                return None
            content = str(results[0].get("raw_content", "")).strip()
            if not content:
                return None
            start = max(0, int(start_index or 0))
            length = max(1, min(12000, int(max_length or 8000)))
            sliced = content[start:start + length]
            if not sliced:
                return None
            self.circuit.success("tavily")
            return f"ATLAS LIVE FETCH — Tavily\nURL: {url}\n\n{sliced}"
        except Exception as exc:
            msg = str(exc)
            self.circuit.failure("tavily", msg, long="|long=1" in msg)
            LOGGER.warning("[TAVILY] extract fail | %s", msg[:240])
            return None
