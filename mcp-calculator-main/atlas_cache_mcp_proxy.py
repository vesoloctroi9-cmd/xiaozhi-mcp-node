#!/usr/bin/env python3
"""
ATLAS MULTI-SOURCE MCP PROXY
Version: 2.0.0

Route order for search:
  1) Fresh GitHub Pages research cache
  2) Tavily Search API (if TAVILY_API_KEY is configured)
  3) SerpApi Google Search (if SERPAPI_API_KEY is configured)
  4) Upstream DuckDuckGo MCP server

Route order for fetch_content:
  1) Fresh GitHub Pages cached fetched_text
  2) Tavily Extract API (if TAVILY_API_KEY is configured)
  3) Upstream DuckDuckGo MCP fetch_content

This proxy preserves the upstream MCP server for initialize/tools/list and for all
unhandled requests. It does not create a keep-alive mechanism.
"""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout

VERSION = "2.0.0"
LOGGER = logging.getLogger("ATLAS_MULTI_PROXY")
logging.basicConfig(
    level=os.environ.get("MCP_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        LOGGER.warning("%s=%r invalid; using %s", name, raw, default)
        value = default
    return max(minimum, min(maximum, value))


CACHE_REFRESH_SECONDS = env_int("ATLAS_CACHE_REFRESH_SECONDS", 60, 15, 600)
CACHE_MAX_AGE_SECONDS = env_int("ATLAS_CACHE_MAX_AGE_SECONDS", 3600, 60, 86400)
CACHE_TIMEOUT_SECONDS = env_int("ATLAS_CACHE_TIMEOUT_SECONDS", 3, 1, 15)
CACHE_MIN_SCORE = env_int("ATLAS_CACHE_MIN_SCORE", 1, 1, 10)
CACHE_MAX_SEARCH_CHARS = env_int("ATLAS_CACHE_MAX_SEARCH_CHARS", 9000, 1000, 20000)
CACHE_MAX_FETCH_CHARS = env_int("ATLAS_CACHE_MAX_FETCH_CHARS", 12000, 1000, 30000)
WEB_TIMEOUT_SECONDS = env_int("ATLAS_WEB_TIMEOUT_SECONDS", 12, 3, 30)
TAVILY_MAX_RESULTS = env_int("ATLAS_TAVILY_MAX_RESULTS", 5, 1, 10)
SERPAPI_MAX_RESULTS = env_int("ATLAS_SERPAPI_MAX_RESULTS", 5, 1, 10)

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
TAVILY_EXTRACT_URL = "https://api.tavily.com/extract"
SERPAPI_SEARCH_URL = "https://serpapi.com/search.json"

GENERIC_TOKENS = {
    "tin", "tuc", "moi", "nhat", "hom", "nay", "bay", "gio", "doc", "cho",
    "toi", "ve", "la", "gi", "co", "nhung", "cac", "va", "cua", "tren", "the",
    "nao", "cap", "nhat", "thoi", "su", "xem", "tim", "kiem", "noi", "dung",
    "news", "latest", "today", "read", "search", "find", "about", "what", "the",
    "and", "for", "from", "update", "updates", "current", "recent",
}
NEWS_HINTS = {
    "tin", "tuc", "news", "latest", "moi", "nhat", "hom", "nay", "today",
    "thoi", "su", "update", "updates", "recent", "current",
}

# Keep web lookup appropriate for a general assistant. Informational/news queries are
# not blanket-blocked; only clear requests to locate/access restricted material are stopped.
RESTRICTED_PATTERNS = [
    re.compile(r"\b(porn|pornography|xxx|sex\s*video|adult\s*video)\b", re.I),
    re.compile(r"\b(buy|mua|order|dat mua)\b.{0,35}\b(gun|firearm|ammo|ammunition|sung|dao bam|switchblade)\b", re.I),
    re.compile(r"\b(buy|mua|order|dat mua|how to use|cach dung)\b.{0,35}\b(cocaine|heroin|meth|fentanyl|ma tuy|can sa|marijuana|thc)\b", re.I),
    re.compile(r"\b(bet|betting|sportsbook|casino|gambling|ca cuoc|danh bac|keo cuoc)\b", re.I),
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFD", value or "")
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def tokens(value: str, *, remove_generic: bool = True) -> List[str]:
    out: List[str] = []
    seen = set()
    for tok in normalize_text(value).split():
        if len(tok) <= 1 and tok != "ai":
            continue
        if remove_generic and tok in GENERIC_TOKENS:
            continue
        if tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def has_news_intent(query: str) -> bool:
    raw = set(normalize_text(query).split())
    return bool(raw & NEWS_HINTS)


def query_is_restricted(query: str) -> bool:
    normalized = normalize_text(query)
    return any(pattern.search(normalized) for pattern in RESTRICTED_PATTERNS)


def parse_iso(value: str) -> Optional[datetime]:
    value = (value or "").strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def cache_bypass_enabled() -> bool:
    raw = os.environ.get("ATLAS_CACHE_BYPASS", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    return os.environ.get("GITHUB_ACTIONS", "").strip().lower() == "true"


def derive_cache_url() -> str:
    explicit = os.environ.get("ATLAS_RESEARCH_CACHE_URL", "").strip()
    if explicit:
        return explicit
    legacy = os.environ.get("RESEARCH_CACHE_URL", "").strip()
    if legacy:
        return legacy
    repo = (
        os.environ.get("ATLAS_GITHUB_REPOSITORY", "").strip()
        or os.environ.get("GITHUB_REPOSITORY", "").strip()
    )
    if "/" not in repo:
        return ""
    owner, name = repo.split("/", 1)
    return f"https://{owner}.github.io/{name}/atlas_research.json"


def canonical_url_for_cache(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw.rstrip("/")
    if not parts.scheme or not parts.netloc:
        return raw.rstrip("/")
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


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
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def upstream_command() -> List[str]:
    raw_json = os.environ.get("ATLAS_DDG_CHILD_ARGS_JSON", "").strip()
    command = os.environ.get("ATLAS_DDG_CHILD_COMMAND", "uvx").strip() or "uvx"
    if raw_json:
        try:
            args = json.loads(raw_json)
            if not isinstance(args, list) or not all(isinstance(x, str) for x in args):
                raise ValueError("must be a JSON array of strings")
            return [command, *args]
        except Exception as exc:
            raise RuntimeError(f"ATLAS_DDG_CHILD_ARGS_JSON invalid: {exc}") from exc
    return [
        command,
        "--with",
        "duckduckgo-mcp-server[browser]",
        "duckduckgo-mcp-server",
        "--fetch-backend",
        "auto",
    ]


@dataclass
class CacheSnapshot:
    payload: Optional[Dict[str, Any]] = None
    last_attempt_monotonic: float = 0.0
    source_url: str = ""
    last_error: str = ""
    hits_search: int = 0
    hits_fetch: int = 0
    misses: int = 0


class ResearchCache:
    def __init__(self) -> None:
        self.state = CacheSnapshot(source_url=derive_cache_url())
        self._lock = asyncio.Lock()

    def configured(self) -> bool:
        return bool(self.state.source_url) and not cache_bypass_enabled()

    async def refresh(self, *, force: bool = False) -> Optional[Dict[str, Any]]:
        if not self.configured():
            return None
        loop = asyncio.get_running_loop()
        now = loop.time()
        if (
            not force
            and self.state.last_attempt_monotonic > 0
            and now - self.state.last_attempt_monotonic < CACHE_REFRESH_SECONDS
        ):
            return self.state.payload
        async with self._lock:
            now = loop.time()
            if (
                not force
                and self.state.last_attempt_monotonic > 0
                and now - self.state.last_attempt_monotonic < CACHE_REFRESH_SECONDS
            ):
                return self.state.payload
            self.state.last_attempt_monotonic = now
            timeout = ClientTimeout(total=CACHE_TIMEOUT_SECONDS)
            try:
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(
                        self.state.source_url,
                        headers={
                            "User-Agent": f"ATLAS-Multi-Proxy/{VERSION}",
                            "Accept": "application/json",
                            "Cache-Control": "no-cache",
                        },
                        allow_redirects=True,
                    ) as response:
                        if response.status != 200:
                            raise RuntimeError(f"HTTP {response.status}")
                        payload = await response.json(content_type=None)
                if not isinstance(payload, dict):
                    raise RuntimeError("cache root is not a JSON object")
                if not isinstance(payload.get("items", []), list):
                    raise RuntimeError("cache items is not a list")
                self.state.payload = payload
                self.state.last_error = ""
                LOGGER.info(
                    "[CACHE] REFRESH PASS | status=%s | items=%s | generated_at=%s",
                    payload.get("status"),
                    len(payload.get("items", []) or []),
                    payload.get("generated_at", ""),
                )
                return payload
            except Exception as exc:
                self.state.last_error = str(exc)
                LOGGER.warning(
                    "[CACHE] REFRESH FAIL | %s | fallback=WEB | retry_after=%ss",
                    exc,
                    CACHE_REFRESH_SECONDS,
                )
                return self.state.payload

    def fresh(self, payload: Optional[Dict[str, Any]]) -> bool:
        if not payload:
            return False
        generated = parse_iso(str(payload.get("generated_at", "")))
        if generated is None:
            return False
        age = (utc_now() - generated).total_seconds()
        return -300 <= age <= CACHE_MAX_AGE_SECONDS

    @staticmethod
    def _item_blob(item: Dict[str, Any]) -> str:
        urls = item.get("urls", []) or []
        if not isinstance(urls, list):
            urls = []
        return " ".join(
            [
                str(item.get("topic", "")),
                str(item.get("search_text", "")),
                str(item.get("fetched_text", "")),
                str(item.get("fetched_url", "")),
                " ".join(str(x) for x in urls),
            ]
        )

    def search_items(
        self, payload: Dict[str, Any], query: str, max_results: int
    ) -> List[Tuple[int, Dict[str, Any]]]:
        q_tokens = set(tokens(query))
        news_intent = has_news_intent(query)
        ranked: List[Tuple[int, str, Dict[str, Any]]] = []
        for raw in payload.get("items", []) or []:
            if not isinstance(raw, dict):
                continue
            blob_tokens = set(tokens(self._item_blob(raw), remove_generic=False))
            topic_tokens = set(tokens(str(raw.get("topic", "")), remove_generic=False))
            overlap = len(q_tokens & blob_tokens)
            topic_overlap = len(q_tokens & topic_tokens)
            score = overlap + (topic_overlap * 2)
            if not q_tokens and news_intent:
                score = 1
            elif score < CACHE_MIN_SCORE:
                continue
            discovered = str(raw.get("discovered_at", ""))
            ranked.append((score, discovered, raw))
        ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
        limit = max(1, min(10, max_results))
        return [(score, item) for score, _discovered, item in ranked[:limit]]

    async def search_hit(self, query: str, max_results: int) -> Optional[str]:
        payload = await self.refresh()
        if not self.fresh(payload):
            self.state.misses += 1
            if payload:
                LOGGER.info("[CACHE] SEARCH MISS | stale cache | query=%r", query[:120])
            return None
        assert payload is not None
        ranked = self.search_items(payload, query, max_results)
        if not ranked:
            self.state.misses += 1
            LOGGER.info("[CACHE] SEARCH MISS | no relevant item | query=%r", query[:120])
            return None
        parts = [
            "ATLAS CACHE HIT — public GitHub Pages research cache",
            f"Cache generated_at: {payload.get('generated_at', '')}",
            f"Query: {query}",
            "",
        ]
        for idx, (score, item) in enumerate(ranked, 1):
            urls = item.get("urls", []) or []
            if not isinstance(urls, list):
                urls = []
            parts.extend(
                [
                    f"[{idx}] Topic: {item.get('topic', '')}",
                    f"Relevance score: {score}",
                    f"Discovered: {item.get('discovered_at', '')}",
                    "URLs:",
                    *(f"- {url}" for url in urls[:5]),
                ]
            )
            search_text = str(item.get("search_text", "")).strip()
            fetched_text = str(item.get("fetched_text", "")).strip()
            fetched_url = str(item.get("fetched_url", "")).strip()
            if search_text:
                parts.append("Search result text:\n" + search_text[:2600])
            if fetched_text:
                parts.append(
                    "Cached fetched content"
                    + (f" ({fetched_url})" if fetched_url else "")
                    + ":\n"
                    + fetched_text[:2200]
                )
            parts.append("")
        text = "\n".join(parts).strip()
        self.state.hits_search += 1
        LOGGER.info(
            "[CACHE] SEARCH HIT | query=%r | results=%s | hit_count=%s",
            query[:120], len(ranked), self.state.hits_search,
        )
        return text[:CACHE_MAX_SEARCH_CHARS]

    async def fetch_hit(self, url: str, start_index: int, max_length: int) -> Optional[str]:
        payload = await self.refresh()
        if not self.fresh(payload):
            self.state.misses += 1
            return None
        assert payload is not None
        target = (url or "").strip()
        target_key = canonical_url_for_cache(target)
        if not target_key:
            return None
        for raw in payload.get("items", []) or []:
            if not isinstance(raw, dict):
                continue
            fetched_url = str(raw.get("fetched_url", "")).strip()
            if not fetched_url:
                continue
            if canonical_url_for_cache(fetched_url) != target_key:
                continue
            text = str(raw.get("fetched_text", "")).strip()
            if not text:
                continue
            start = max(0, start_index)
            length = max(1, min(CACHE_MAX_FETCH_CHARS, max_length))
            sliced = text[start:start + length]
            if not sliced:
                return None
            self.state.hits_fetch += 1
            LOGGER.info(
                "[CACHE] FETCH HIT | url=%s | chars=%s | hit_count=%s",
                target[:240], len(sliced), self.state.hits_fetch,
            )
            return (
                "ATLAS CACHE HIT — cached fetched content\n"
                f"URL: {fetched_url}\n"
                f"Cache generated_at: {payload.get('generated_at', '')}\n\n"
                f"{sliced}"
            )
        self.state.misses += 1
        LOGGER.info("[CACHE] FETCH MISS | url=%s", target[:240])
        return None


CACHE = ResearchCache()
STDOUT_LOCK: Optional[asyncio.Lock] = None


def mcp_text_response(request_id: Any, text: str, *, is_error: bool = False) -> str:
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "content": [{"type": "text", "text": text}],
            "isError": bool(is_error),
        },
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


async def write_stdout(line: str) -> None:
    global STDOUT_LOCK
    if STDOUT_LOCK is None:
        STDOUT_LOCK = asyncio.Lock()
    async with STDOUT_LOCK:
        sys.stdout.write(line.rstrip("\r\n") + "\n")
        sys.stdout.flush()


async def tavily_search(query: str, max_results: int) -> Optional[str]:
    api_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not api_key:
        return None
    limit = max(1, min(TAVILY_MAX_RESULTS, max_results))
    body: Dict[str, Any] = {
        "query": query,
        "search_depth": "basic",
        "topic": "news" if has_news_intent(query) else "general",
        "max_results": limit,
        "include_answer": False,
        "include_raw_content": False,
        "include_images": False,
    }
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.post(
                TAVILY_SEARCH_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "User-Agent": f"ATLAS-Multi-Proxy/{VERSION}",
                },
                json=body,
            ) as response:
                raw_text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {raw_text[:300]}")
                data = json.loads(raw_text)
        results = data.get("results", []) if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            LOGGER.warning("[TAVILY] SEARCH EMPTY | query=%r", query[:120])
            return None
        parts = ["ATLAS LIVE SEARCH — Tavily", f"Query: {query}", ""]
        count = 0
        for item in results[:limit]:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            title = str(item.get("title", "")).strip()
            content = str(item.get("content", "")).strip()
            published = str(item.get("published_date", "")).strip()
            score = item.get("score")
            if not url and not content:
                continue
            count += 1
            parts.append(f"[{count}] {title or 'Result'}")
            if url:
                parts.append(f"URL: {url}")
            if published:
                parts.append(f"Published: {published}")
            if score is not None:
                parts.append(f"Score: {score}")
            if content:
                parts.append("Snippet: " + content[:1800])
            parts.append("")
        if count == 0:
            return None
        text = "\n".join(parts).strip()
        LOGGER.info("[TAVILY] SEARCH PASS | query=%r | results=%s", query[:120], count)
        return text[:CACHE_MAX_SEARCH_CHARS]
    except Exception as exc:
        LOGGER.warning("[TAVILY] SEARCH FAIL | query=%r | %s", query[:120], exc)
        return None


async def serpapi_search(query: str, max_results: int) -> Optional[str]:
    api_key = os.environ.get("SERPAPI_API_KEY", "").strip()
    if not api_key:
        return None
    limit = max(1, min(SERPAPI_MAX_RESULTS, max_results))
    params: Dict[str, str] = {
        "engine": "google",
        "q": query,
        "api_key": api_key,
        "num": str(limit),
        "hl": "vi",
        "gl": "vn",
        "safe": "active",
        "output": "json",
    }
    if has_news_intent(query):
        params["tbm"] = "nws"
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.get(
                SERPAPI_SEARCH_URL,
                params=params,
                headers={"User-Agent": f"ATLAS-Multi-Proxy/{VERSION}"},
            ) as response:
                raw_text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {raw_text[:300]}")
                data = json.loads(raw_text)
        if not isinstance(data, dict):
            return None
        if data.get("error"):
            raise RuntimeError(str(data.get("error")))
        results = data.get("news_results") if has_news_intent(query) else data.get("organic_results")
        if not isinstance(results, list) or not results:
            # Google sometimes gives only an answer box/knowledge graph.
            results = data.get("organic_results", [])
        parts = ["ATLAS LIVE SEARCH — SerpApi / Google", f"Query: {query}", ""]
        count = 0
        if isinstance(results, list):
            for item in results[:limit]:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                url = str(item.get("link", "")).strip()
                snippet = str(item.get("snippet", "")).strip()
                source = item.get("source")
                date = str(item.get("date", "")).strip()
                if isinstance(source, dict):
                    source = source.get("name") or source.get("title") or ""
                source = str(source or "").strip()
                if not url and not snippet:
                    continue
                count += 1
                parts.append(f"[{count}] {title or 'Result'}")
                if source:
                    parts.append(f"Source: {source}")
                if date:
                    parts.append(f"Date: {date}")
                if url:
                    parts.append(f"URL: {url}")
                if snippet:
                    parts.append("Snippet: " + snippet[:1800])
                parts.append("")
        if count == 0:
            answer_box = data.get("answer_box")
            if isinstance(answer_box, dict):
                text_bits = []
                for key in ("answer", "result", "snippet", "title"):
                    value = answer_box.get(key)
                    if value:
                        text_bits.append(str(value))
                link = answer_box.get("link")
                if text_bits:
                    count = 1
                    parts.append("[1] Google answer")
                    if link:
                        parts.append(f"URL: {link}")
                    parts.append("Snippet: " + " — ".join(text_bits)[:2500])
        if count == 0:
            LOGGER.warning("[SERPAPI] SEARCH EMPTY | query=%r", query[:120])
            return None
        text = "\n".join(parts).strip()
        LOGGER.info("[SERPAPI] SEARCH PASS | query=%r | results=%s", query[:120], count)
        return text[:CACHE_MAX_SEARCH_CHARS]
    except Exception as exc:
        LOGGER.warning("[SERPAPI] SEARCH FAIL | query=%r | %s", query[:120], exc)
        return None


async def tavily_extract(url: str, start_index: int, max_length: int) -> Optional[str]:
    api_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not api_key or not is_public_http_url(url):
        return None
    body = {"urls": [url], "extract_depth": "basic"}
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    try:
        async with ClientSession(timeout=timeout) as session:
            async with session.post(
                TAVILY_EXTRACT_URL,
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "User-Agent": f"ATLAS-Multi-Proxy/{VERSION}",
                },
                json=body,
            ) as response:
                raw_text = await response.text()
                if response.status != 200:
                    raise RuntimeError(f"HTTP {response.status}: {raw_text[:300]}")
                data = json.loads(raw_text)
        results = data.get("results", []) if isinstance(data, dict) else []
        if not isinstance(results, list) or not results:
            LOGGER.warning("[TAVILY] EXTRACT EMPTY | url=%s", url[:240])
            return None
        first = results[0] if isinstance(results[0], dict) else {}
        content = str(first.get("raw_content", "")).strip()
        if not content:
            return None
        start = max(0, start_index)
        length = max(1, min(CACHE_MAX_FETCH_CHARS, max_length))
        sliced = content[start:start + length]
        if not sliced:
            return None
        LOGGER.info("[TAVILY] EXTRACT PASS | url=%s | chars=%s", url[:240], len(sliced))
        return f"ATLAS LIVE FETCH — Tavily Extract\nURL: {url}\n\n{sliced}"
    except Exception as exc:
        LOGGER.warning("[TAVILY] EXTRACT FAIL | url=%s | %s", url[:240], exc)
        return None


async def maybe_intercept_tool_call(message: str) -> Optional[str]:
    try:
        payload = json.loads(message)
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("method") != "tools/call":
        return None
    request_id = payload.get("id")
    if request_id is None:
        return None
    params = payload.get("params")
    if not isinstance(params, dict):
        return None
    name = str(params.get("name", ""))
    arguments = params.get("arguments") or {}
    if not isinstance(arguments, dict):
        arguments = {}

    if name == "search":
        query = str(arguments.get("query", "")).strip()
        if not query:
            return None
        if query_is_restricted(query):
            LOGGER.warning("[SAFE] SEARCH BLOCKED | query=%r", query[:120])
            return mcp_text_response(
                request_id,
                "Web search blocked for this request. I can still help with safe, factual, age-appropriate information.",
                is_error=False,
            )
        try:
            max_results = int(arguments.get("max_results", 10))
        except (TypeError, ValueError):
            max_results = 10

        if CACHE.configured():
            hit = await CACHE.search_hit(query, max_results)
            if hit:
                LOGGER.info("[WEB ROUTE] tool=search | source=CACHE")
                return mcp_text_response(request_id, hit)

        live = await tavily_search(query, max_results)
        if live:
            LOGGER.info("[WEB ROUTE] tool=search | source=TAVILY")
            return mcp_text_response(request_id, live)

        live = await serpapi_search(query, max_results)
        if live:
            LOGGER.info("[WEB ROUTE] tool=search | source=SERPAPI")
            return mcp_text_response(request_id, live)

        LOGGER.info("[WEB ROUTE] tool=search | source=DUCKDUCKGO")
        return None

    if name == "fetch_content":
        url = str(arguments.get("url", "")).strip()
        if not url:
            return None
        try:
            start_index = int(arguments.get("start_index", 0))
        except (TypeError, ValueError):
            start_index = 0
        try:
            max_length = int(arguments.get("max_length", 8000))
        except (TypeError, ValueError):
            max_length = 8000

        if CACHE.configured():
            hit = await CACHE.fetch_hit(url, start_index, max_length)
            if hit:
                LOGGER.info("[WEB ROUTE] tool=fetch_content | source=CACHE")
                return mcp_text_response(request_id, hit)

        live = await tavily_extract(url, start_index, max_length)
        if live:
            LOGGER.info("[WEB ROUTE] tool=fetch_content | source=TAVILY")
            return mcp_text_response(request_id, live)

        LOGGER.info("[WEB ROUTE] tool=fetch_content | source=DUCKDUCKGO")
        return None

    return None


async def warm_cache_loop() -> None:
    if cache_bypass_enabled():
        LOGGER.info("[CACHE] BYPASS | environment requests LIVE-only mode")
        return
    if not CACHE.configured():
        LOGGER.warning(
            "[CACHE] DISABLED | set ATLAS_RESEARCH_CACHE_URL (or RESEARCH_CACHE_URL) "
            "or ATLAS_GITHUB_REPOSITORY"
        )
        return
    LOGGER.info("[CACHE] ENABLED | source=%s", CACHE.state.source_url)
    while True:
        try:
            await CACHE.refresh(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("[CACHE] warm loop error: %s", exc)
        await asyncio.sleep(CACHE_REFRESH_SECONDS)


async def parent_to_child(process: asyncio.subprocess.Process) -> None:
    if process.stdin is None:
        raise RuntimeError("upstream MCP stdin unavailable")
    while True:
        line = await asyncio.to_thread(sys.stdin.readline)
        if line == "":
            try:
                process.stdin.close()
            except Exception:
                pass
            return
        message = line.rstrip("\r\n")
        if not message:
            continue
        intercepted = await maybe_intercept_tool_call(message)
        if intercepted is not None:
            await write_stdout(intercepted)
            continue
        process.stdin.write((message + "\n").encode("utf-8"))
        await process.stdin.drain()


async def child_to_parent(process: asyncio.subprocess.Process) -> None:
    if process.stdout is None:
        raise RuntimeError("upstream MCP stdout unavailable")
    while True:
        raw = await process.stdout.readline()
        if not raw:
            return
        await write_stdout(raw.decode("utf-8", errors="replace"))


async def child_stderr(process: asyncio.subprocess.Process) -> None:
    if process.stderr is None:
        return
    while True:
        raw = await process.stderr.readline()
        if not raw:
            return
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        print(f"[UPSTREAM_DDG] {text}", file=sys.stderr, flush=True)


async def main() -> int:
    command = upstream_command()
    LOGGER.info("ATLAS Multi-Source Proxy v%s starting", VERSION)
    LOGGER.info(
        "Sources | cache=%s | tavily=%s | serpapi=%s | ddg=enabled",
        "enabled" if CACHE.configured() else "disabled",
        "enabled" if os.environ.get("TAVILY_API_KEY", "").strip() else "disabled",
        "enabled" if os.environ.get("SERPAPI_API_KEY", "").strip() else "disabled",
    )
    LOGGER.info("Upstream MCP: %s", " ".join(command))

    child_env = os.environ.copy()
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=child_env,
    )
    cache_task = asyncio.create_task(warm_cache_loop(), name="atlas-cache-warm")
    p2c = asyncio.create_task(parent_to_child(process), name="parent-to-ddg")
    c2p = asyncio.create_task(child_to_parent(process), name="ddg-to-parent")
    cerr = asyncio.create_task(child_stderr(process), name="ddg-stderr")
    wait = asyncio.create_task(process.wait(), name="ddg-exit")

    done, pending = await asyncio.wait({p2c, c2p, wait}, return_when=asyncio.FIRST_COMPLETED)
    exit_code = process.returncode
    if wait in done:
        exit_code = wait.result()
        LOGGER.warning("Upstream MCP exited with code %s", exit_code)

    for task in pending:
        task.cancel()
    if not cache_task.done():
        cache_task.cancel()
    if not cerr.done():
        cerr.cancel()

    await asyncio.gather(*pending, cache_task, cerr, return_exceptions=True)
    if process.returncode is None:
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
    return int(exit_code or 0)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        LOGGER.exception("ATLAS Multi-Source Proxy FAIL: %s", exc)
        raise SystemExit(1)
