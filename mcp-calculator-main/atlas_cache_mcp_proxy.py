#!/usr/bin/env python3
"""
ATLAS MULTI-SOURCE MCP PROXY — FINAL V1
Version: 1.0.0
Deployment model: ONE CODEBASE / TWO IDENTICAL NODES

Design goals
------------
- Preserve the existing Xiaozhi <-> MCP stdio bridge contract.
- Preserve ONE persistent upstream DuckDuckGo MCP child process.
- Aggregate search evidence from cache + Tavily + SerpApi/Google + DuckDuckGo.
- A cache hit NEVER short-circuits live search.
- De-duplicate, merge provenance, rank and diversify results before returning them.
- Discover public/search-indexed Facebook, TikTok and YouTube pages via site queries.
- Provider failure/quota/timeout must not crash the whole search.
- Never log in to social platforms or bypass private/restricted content.

Environment compatibility
-------------------------
Current Node-2 / Node-1 keys are supported:
  RESEARCH_CACHE_URL
  TAVILY_API_KEY
  SERPAPI_API_KEY
  MCP_ENDPOINT                 # bridge-owned; not consumed here
  ATLAS_RESEARCH_SERVER        # bridge/research-worker-owned; not consumed here
  ATLAS_RESEARCH_OUTPUT        # bridge/research-worker-owned; not consumed here

Preferred aliases are also supported:
  ATLAS_RESEARCH_CACHE_URL
  ATLAS_GITHUB_REPOSITORY
  ATLAS_DDG_CHILD_COMMAND
  ATLAS_DDG_CHILD_ARGS_JSON

Important: this file does not deploy itself and does not create a keep-alive mechanism.
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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from aiohttp import ClientSession, ClientTimeout

VERSION = "1.0.0"
LOGGER = logging.getLogger("ATLAS_MULTI_SOURCE_FINAL")
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


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    LOGGER.warning("%s=%r invalid; using %s", name, raw, default)
    return default


CACHE_REFRESH_SECONDS = env_int("ATLAS_CACHE_REFRESH_SECONDS", 60, 15, 600)
CACHE_NEGATIVE_TTL_SECONDS = env_int("ATLAS_CACHE_NEGATIVE_TTL_SECONDS", 300, 30, 3600)
CACHE_MAX_AGE_SECONDS = env_int("ATLAS_CACHE_MAX_AGE_SECONDS", 3600, 60, 86400)
CACHE_TIMEOUT_SECONDS = env_int("ATLAS_CACHE_TIMEOUT_SECONDS", 3, 1, 15)
WEB_TIMEOUT_SECONDS = env_int("ATLAS_WEB_TIMEOUT_SECONDS", 12, 3, 30)
AGGREGATE_TIMEOUT_SECONDS = env_int("ATLAS_AGGREGATE_TIMEOUT_SECONDS", 18, 5, 45)
TAVILY_MAX_RESULTS = env_int("ATLAS_TAVILY_MAX_RESULTS", 6, 1, 20)
SERPAPI_MAX_RESULTS = env_int("ATLAS_SERPAPI_MAX_RESULTS", 6, 1, 20)
DDG_MAX_RESULTS = env_int("ATLAS_DDG_MAX_RESULTS", 8, 1, 20)
FINAL_MAX_RESULTS = env_int("ATLAS_FINAL_MAX_RESULTS", 16, 3, 40)
MAX_PER_DOMAIN = env_int("ATLAS_MAX_PER_DOMAIN", 4, 1, 10)
MAX_SEARCH_CHARS = env_int("ATLAS_MAX_SEARCH_CHARS", 18000, 3000, 30000)
MAX_FETCH_CHARS = env_int("ATLAS_MAX_FETCH_CHARS", 16000, 2000, 30000)
SOCIAL_DISCOVERY_ENABLED = env_bool("ATLAS_SOCIAL_DISCOVERY_ENABLED", True)
CACHE_WARM_ENABLED = env_bool("ATLAS_CACHE_WARM_ENABLED", False)

TAVILY_SEARCH_URL = "https://api.tavily.com/search"
TAVILY_EXTRACT_URL = "https://api.tavily.com/extract"
SERPAPI_SEARCH_URL = "https://serpapi.com/search.json"

TRACKING_KEYS = {
    "fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid",
    "ref", "ref_src", "spm",
}
TRACKING_PREFIXES = ("utm_",)

PLATFORM_HOSTS = {
    "facebook": ("facebook.com",),
    "tiktok": ("tiktok.com",),
    "youtube": ("youtube.com", "youtu.be"),
}

GENERIC_TOKENS = {
    "tin", "tuc", "moi", "nhat", "hom", "nay", "bay", "gio", "doc", "cho",
    "toi", "ve", "la", "gi", "co", "nhung", "cac", "va", "cua", "tren",
    "the", "nao", "cap", "thoi", "xem", "tim", "kiem", "noi", "dung",
    "news", "latest", "today", "read", "search", "find", "about", "what",
    "the", "and", "for", "from", "update", "updates", "current", "recent",
}
NEWS_HINTS = {
    "tin", "tuc", "news", "latest", "moi", "nhat", "hom", "nay", "today",
    "update", "updates", "recent", "current",
}

# General-assistant safety gate carried forward from Node 2. It blocks only clear
# requests to locate/access restricted material, not ordinary informational/news queries.
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
    normalized = normalize_text(query)
    raw = set(normalized.split())
    # "su" alone is ambiguous in Vietnamese (e.g. "lịch sử").
    return bool(raw & NEWS_HINTS) or "thoi su" in normalized


def query_is_restricted(query: str) -> bool:
    normalized = normalize_text(query)
    return any(pattern.search(normalized) for pattern in RESTRICTED_PATTERNS)


def parse_iso(value: str) -> Optional[datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
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


def canonicalize_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw.rstrip("/")
    if not parts.scheme or not parts.netloc:
        return raw.rstrip("/")
    scheme = parts.scheme.lower()
    host = parts.netloc.lower()
    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")
    cleaned = []
    for key, val in parse_qsl(parts.query, keep_blank_values=True):
        lower_key = key.lower()
        if lower_key in TRACKING_KEYS or any(lower_key.startswith(p) for p in TRACKING_PREFIXES):
            continue
        cleaned.append((key, val))
    return urlunsplit((scheme, host, path, urlencode(cleaned, doseq=True), ""))


def domain_of(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").lower()
    except Exception:
        return ""


def platform_of(url: str) -> str:
    host = domain_of(url)
    for platform, roots in PLATFORM_HOSTS.items():
        for root in roots:
            if host == root or host.endswith("." + root):
                return platform
    return "web"


@dataclass(frozen=True)
class PlannedQuery:
    query: str
    kind: str
    expected_platform: str = "web"


@dataclass
class Evidence:
    title: str
    url: str
    snippet: str = ""
    provider: str = ""
    source_query: str = ""
    query_kind: str = "base"
    expected_platform: str = "web"
    provider_score: float = 0.0
    published_at: str = ""
    canonical_url: str = ""
    platform: str = "web"
    providers: List[str] = field(default_factory=list)
    source_queries: List[str] = field(default_factory=list)
    query_kinds: List[str] = field(default_factory=list)
    rank_score: float = 0.0

    def finalize(self) -> "Evidence":
        self.title = (self.title or "").strip()
        self.url = (self.url or "").strip()
        self.snippet = (self.snippet or "").strip()
        self.canonical_url = canonicalize_url(self.url)
        self.platform = platform_of(self.url)
        if self.provider and self.provider not in self.providers:
            self.providers.append(self.provider)
        if self.source_query and self.source_query not in self.source_queries:
            self.source_queries.append(self.source_query)
        if self.query_kind and self.query_kind not in self.query_kinds:
            self.query_kinds.append(self.query_kind)
        return self


@dataclass
class ProviderState:
    name: str
    enabled: bool
    ok: int = 0
    empty: int = 0
    failed: int = 0
    rows: int = 0
    last_error: str = ""


def build_query_plan(query: str) -> List[PlannedQuery]:
    base = " ".join((query or "").split()).strip()
    if not base:
        raise ValueError("query must not be empty")
    plan = [PlannedQuery(base, "base", "web")]
    if SOCIAL_DISCOVERY_ENABLED:
        plan.extend([
            PlannedQuery(f"site:facebook.com {base}", "social_site", "facebook"),
            PlannedQuery(f"site:tiktok.com {base}", "social_site", "tiktok"),
            PlannedQuery(f"site:youtube.com {base}", "social_site", "youtube"),
        ])
    return plan


@dataclass
class CacheSnapshot:
    payload: Optional[Dict[str, Any]] = None
    last_attempt_monotonic: float = 0.0
    last_success_monotonic: float = 0.0
    source_url: str = ""
    last_error: str = ""


class ResearchCache:
    def __init__(self) -> None:
        self.state = CacheSnapshot(source_url=derive_cache_url())
        self._lock = asyncio.Lock()

    def configured(self) -> bool:
        return bool(self.state.source_url) and not cache_bypass_enabled()

    def _retry_window(self) -> int:
        return CACHE_NEGATIVE_TTL_SECONDS if self.state.last_error else CACHE_REFRESH_SECONDS

    async def refresh(self, *, force: bool = False) -> Optional[Dict[str, Any]]:
        if not self.configured():
            return None
        loop = asyncio.get_running_loop()
        now = loop.time()
        retry_window = self._retry_window()
        if (
            not force
            and self.state.last_attempt_monotonic > 0
            and now - self.state.last_attempt_monotonic < retry_window
        ):
            return self.state.payload

        async with self._lock:
            now = loop.time()
            retry_window = self._retry_window()
            if (
                not force
                and self.state.last_attempt_monotonic > 0
                and now - self.state.last_attempt_monotonic < retry_window
            ):
                return self.state.payload
            self.state.last_attempt_monotonic = now
            timeout = ClientTimeout(total=CACHE_TIMEOUT_SECONDS)
            try:
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(
                        self.state.source_url,
                        headers={
                            "User-Agent": f"ATLAS-Multi-Source-Final/{VERSION}",
                            "Accept": "application/json",
                            "Cache-Control": "no-cache",
                        },
                        allow_redirects=True,
                    ) as response:
                        if response.status != 200:
                            raise RuntimeError(f"HTTP {response.status}")
                        payload = await response.json(content_type=None)
                if not isinstance(payload, dict) or not isinstance(payload.get("items", []), list):
                    raise RuntimeError("invalid cache schema")
                self.state.payload = payload
                self.state.last_success_monotonic = loop.time()
                self.state.last_error = ""
                LOGGER.info(
                    "[CACHE] REFRESH PASS | items=%s | generated_at=%s",
                    len(payload.get("items", []) or []),
                    payload.get("generated_at", ""),
                )
                return payload
            except Exception as exc:
                self.state.last_error = str(exc)
                LOGGER.warning(
                    "[CACHE] REFRESH FAIL | %s | cache=supplemental | retry_after=%ss",
                    exc,
                    CACHE_NEGATIVE_TTL_SECONDS,
                )
                return self.state.payload

    @staticmethod
    def fresh(payload: Optional[Dict[str, Any]]) -> bool:
        if not payload:
            return False
        generated = parse_iso(str(payload.get("generated_at", "")))
        if generated is None:
            return False
        age = (utc_now() - generated).total_seconds()
        return -300 <= age <= CACHE_MAX_AGE_SECONDS

    async def search(self, planned: PlannedQuery, max_results: int) -> List[Evidence]:
        payload = await self.refresh()
        if not self.fresh(payload):
            return []
        assert payload is not None
        q_tokens = set(tokens(planned.query))
        ranked: List[Tuple[float, str, Dict[str, Any]]] = []
        for raw in payload.get("items", []) or []:
            if not isinstance(raw, dict):
                continue
            urls = raw.get("urls", []) or []
            if not isinstance(urls, list):
                urls = []
            blob = " ".join([
                str(raw.get("topic", "")),
                str(raw.get("search_text", "")),
                str(raw.get("fetched_url", "")),
                " ".join(str(x) for x in urls),
            ])
            blob_tokens = set(tokens(blob, remove_generic=False))
            overlap = len(q_tokens & blob_tokens)
            if q_tokens and overlap <= 0:
                continue
            score = overlap / max(1, len(q_tokens))
            ranked.append((score, str(raw.get("discovered_at", "")), raw))
        ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)

        out: List[Evidence] = []
        for score, _date, raw in ranked:
            urls = raw.get("urls", []) or []
            if not isinstance(urls, list):
                urls = []
            fetched_url = str(raw.get("fetched_url", "")).strip()
            if fetched_url and fetched_url not in urls:
                urls = [fetched_url, *urls]
            local_seen = set()
            for url in urls:
                url = str(url).strip()
                if not is_public_http_url(url):
                    continue
                key = canonicalize_url(url)
                if not key or key in local_seen:
                    continue
                local_seen.add(key)
                if fetched_url and canonicalize_url(url) == canonicalize_url(fetched_url):
                    snippet = str(raw.get("fetched_text", "") or raw.get("search_text", "")).strip()
                else:
                    snippet = str(raw.get("search_text", "")).strip()
                out.append(Evidence(
                    title=str(raw.get("topic", "") or url),
                    url=url,
                    snippet=snippet[:1800],
                    provider="cache",
                    source_query=planned.query,
                    query_kind=planned.kind,
                    expected_platform=planned.expected_platform,
                    provider_score=score,
                    published_at=str(raw.get("discovered_at", "")),
                ).finalize())
                if len(out) >= max_results:
                    return out
        return out

    async def fetch_exact(self, url: str, start_index: int, max_length: int) -> Optional[str]:
        payload = await self.refresh()
        if not self.fresh(payload):
            return None
        assert payload is not None
        target = canonicalize_url(url)
        for raw in payload.get("items", []) or []:
            if not isinstance(raw, dict):
                continue
            fetched_url = str(raw.get("fetched_url", "")).strip()
            if not fetched_url or canonicalize_url(fetched_url) != target:
                continue
            text = str(raw.get("fetched_text", "")).strip()
            if not text:
                continue
            start = max(0, start_index)
            length = max(1, min(MAX_FETCH_CHARS, max_length))
            chunk = text[start:start + length]
            if chunk:
                return f"ATLAS CACHE SUPPORT\nURL: {fetched_url}\n\n{chunk}"
        return None


CACHE = ResearchCache()
STDOUT_LOCK: Optional[asyncio.Lock] = None


async def write_stdout(line: str) -> None:
    global STDOUT_LOCK
    if STDOUT_LOCK is None:
        STDOUT_LOCK = asyncio.Lock()
    async with STDOUT_LOCK:
        sys.stdout.write(line.rstrip("\r\n") + "\n")
        sys.stdout.flush()


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


def extract_mcp_text(message: Dict[str, Any]) -> str:
    if not isinstance(message, dict):
        return ""
    result = message.get("result", {}) or {}
    if not isinstance(result, dict):
        return ""
    parts = []
    for item in result.get("content", []) or []:
        if isinstance(item, dict) and item.get("type") == "text":
            parts.append(str(item.get("text", "")))
    return "\n".join(parts)


def parse_ddg_evidence(text: str, planned: PlannedQuery, max_results: int) -> List[Evidence]:
    """Parse common Markdown/plain-text search output conservatively.

    We never invent a URL. Titles/snippets are best-effort; URLs are the source of truth.
    """
    out: List[Evidence] = []
    seen = set()

    # First prefer Markdown links, which preserve titles.
    for match in re.finditer(r"\[([^\]]{1,300})\]\((https?://[^)\s]+)\)", text or ""):
        title = match.group(1).strip()
        url = match.group(2).strip().rstrip(".,;:")
        key = canonicalize_url(url)
        if not key or key in seen or not is_public_http_url(url):
            continue
        seen.add(key)
        around = (text[max(0, match.start() - 240):match.end() + 500] or "").strip()
        out.append(Evidence(
            title=title or url,
            url=url,
            snippet=around[:1000],
            provider="duckduckgo",
            source_query=planned.query,
            query_kind=planned.kind,
            expected_platform=planned.expected_platform,
        ).finalize())
        if len(out) >= max_results:
            return out

    lines = [line.strip() for line in (text or "").splitlines()]
    for idx, line in enumerate(lines):
        for raw_url in re.findall(r"https?://[^\s<>\]})\"']+", line):
            url = raw_url.rstrip(".,;:")
            key = canonicalize_url(url)
            if not key or key in seen or not is_public_http_url(url):
                continue
            seen.add(key)
            title = ""
            for j in range(idx - 1, max(-1, idx - 4), -1):
                candidate = lines[j].strip(" -*#\t")
                if candidate and not candidate.startswith("http"):
                    title = candidate[:300]
                    break
            snippet_lines = []
            for j in range(idx + 1, min(len(lines), idx + 4)):
                candidate = lines[j]
                if candidate and "http://" not in candidate and "https://" not in candidate:
                    snippet_lines.append(candidate)
            out.append(Evidence(
                title=title or url,
                url=url,
                snippet=" ".join(snippet_lines)[:1000],
                provider="duckduckgo",
                source_query=planned.query,
                query_kind=planned.kind,
                expected_platform=planned.expected_platform,
            ).finalize())
            if len(out) >= max_results:
                return out
    return out


class UpstreamRouter:
    """One persistent upstream DDG MCP process shared by pass-through and ATLAS calls.

    This avoids spawning a second DDG server and keeps the Node-2 process model intact.
    Internal aggregate calls use private request IDs; child responses are routed to
    internal futures instead of being exposed to Xiaozhi.
    """

    INTERNAL_PREFIX = "__atlas_internal__"

    def __init__(self, process: asyncio.subprocess.Process):
        self.process = process
        self._write_lock = asyncio.Lock()
        self._call_lock = asyncio.Lock()
        self._waiters: Dict[Any, asyncio.Future] = {}
        self._counter = 0

    async def send_raw(self, line: str) -> None:
        if self.process.stdin is None:
            raise RuntimeError("upstream MCP stdin unavailable")
        async with self._write_lock:
            self.process.stdin.write((line.rstrip("\r\n") + "\n").encode("utf-8"))
            await self.process.stdin.drain()

    async def call_tool(self, name: str, arguments: Dict[str, Any], timeout: int) -> Dict[str, Any]:
        # Serialize internal DDG tool calls for conservative compatibility.
        async with self._call_lock:
            loop = asyncio.get_running_loop()
            self._counter += 1
            request_id = f"{self.INTERNAL_PREFIX}{self._counter}"
            future = loop.create_future()
            self._waiters[request_id] = future
            payload = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
            try:
                await self.send_raw(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                result = await asyncio.wait_for(future, timeout=timeout)
                if not isinstance(result, dict):
                    raise RuntimeError("invalid upstream MCP response")
                return result
            finally:
                self._waiters.pop(request_id, None)

    async def route_child_line(self, raw: bytes) -> None:
        text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        try:
            payload = json.loads(text)
        except Exception:
            await write_stdout(text)
            return
        if isinstance(payload, dict):
            request_id = payload.get("id")
            future = self._waiters.get(request_id)
            if future is not None and not future.done():
                future.set_result(payload)
                return
        await write_stdout(text)

    def fail_waiters(self, exc: BaseException) -> None:
        for future in list(self._waiters.values()):
            if not future.done():
                future.set_exception(exc)
        self._waiters.clear()


async def tavily_search(planned: PlannedQuery, max_results: int, news_intent: bool) -> List[Evidence]:
    api_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not api_key:
        return []
    limit = max(1, min(TAVILY_MAX_RESULTS, max_results))
    body: Dict[str, Any] = {
        "query": planned.query,
        "search_depth": "basic",
        "topic": "news" if (news_intent and planned.kind == "base") else "general",
        "max_results": limit,
        "include_answer": False,
        "include_raw_content": False,
        "include_images": False,
    }
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    async with ClientSession(timeout=timeout) as session:
        async with session.post(
            TAVILY_SEARCH_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "User-Agent": f"ATLAS-Multi-Source-Final/{VERSION}",
            },
            json=body,
        ) as response:
            raw_text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {raw_text[:200]}")
            data = json.loads(raw_text)
    results = data.get("results", []) if isinstance(data, dict) else []
    out = []
    for raw in results if isinstance(results, list) else []:
        if not isinstance(raw, dict):
            continue
        url = str(raw.get("url", "")).strip()
        if not is_public_http_url(url):
            continue
        out.append(Evidence(
            title=str(raw.get("title", "")),
            url=url,
            snippet=str(raw.get("content", ""))[:1800],
            provider="tavily",
            source_query=planned.query,
            query_kind=planned.kind,
            expected_platform=planned.expected_platform,
            provider_score=float(raw.get("score") or 0.0),
            published_at=str(raw.get("published_date", "") or ""),
        ).finalize())
    return out


async def serpapi_search(planned: PlannedQuery, max_results: int, news_intent: bool) -> List[Evidence]:
    api_key = os.environ.get("SERPAPI_API_KEY", "").strip()
    if not api_key:
        return []
    limit = max(1, min(SERPAPI_MAX_RESULTS, max_results))
    params: Dict[str, str] = {
        "engine": "google",
        "q": planned.query,
        "api_key": api_key,
        "num": str(limit),
        "hl": "vi",
        "gl": "vn",
        "safe": "active",
        "output": "json",
    }
    if news_intent and planned.kind == "base":
        params["tbm"] = "nws"
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    async with ClientSession(timeout=timeout) as session:
        async with session.get(
            SERPAPI_SEARCH_URL,
            params=params,
            headers={"User-Agent": f"ATLAS-Multi-Source-Final/{VERSION}"},
        ) as response:
            raw_text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {raw_text[:200]}")
            data = json.loads(raw_text)
    if not isinstance(data, dict):
        return []
    if data.get("error"):
        raise RuntimeError(str(data.get("error")))
    rows = data.get("news_results") if (news_intent and planned.kind == "base") else data.get("organic_results")
    if not isinstance(rows, list):
        rows = data.get("organic_results", [])
    out = []
    for raw in rows[:limit] if isinstance(rows, list) else []:
        if not isinstance(raw, dict):
            continue
        url = str(raw.get("link", "")).strip()
        if not is_public_http_url(url):
            continue
        source = raw.get("source")
        if isinstance(source, dict):
            source = source.get("name") or source.get("title") or ""
        source = str(source or "").strip()
        snippet = str(raw.get("snippet", "")).strip()
        if source:
            snippet = f"{source}: {snippet}" if snippet else source
        out.append(Evidence(
            title=str(raw.get("title", "")),
            url=url,
            snippet=snippet[:1800],
            provider="serpapi",
            source_query=planned.query,
            query_kind=planned.kind,
            expected_platform=planned.expected_platform,
            published_at=str(raw.get("date", "") or ""),
        ).finalize())
    return out


async def ddg_search(router: UpstreamRouter, planned: PlannedQuery, max_results: int) -> List[Evidence]:
    # Base query always uses DDG. Social site queries are handled by Tavily/Google to
    # avoid serializing four upstream DDG calls on small/free instances.
    if planned.kind != "base":
        return []
    response = await router.call_tool(
        "search",
        {"query": planned.query, "max_results": max(1, min(DDG_MAX_RESULTS, max_results))},
        timeout=WEB_TIMEOUT_SECONDS,
    )
    if response.get("error"):
        raise RuntimeError(str(response.get("error")))
    return parse_ddg_evidence(extract_mcp_text(response), planned, max_results)


async def tavily_extract(url: str, start_index: int, max_length: int) -> Optional[str]:
    api_key = os.environ.get("TAVILY_API_KEY", "").strip()
    if not api_key or not is_public_http_url(url):
        return None
    body = {"urls": [url], "extract_depth": "basic"}
    timeout = ClientTimeout(total=WEB_TIMEOUT_SECONDS)
    async with ClientSession(timeout=timeout) as session:
        async with session.post(
            TAVILY_EXTRACT_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "User-Agent": f"ATLAS-Multi-Source-Final/{VERSION}",
            },
            json=body,
        ) as response:
            raw_text = await response.text()
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {raw_text[:200]}")
            data = json.loads(raw_text)
    results = data.get("results", []) if isinstance(data, dict) else []
    if not isinstance(results, list) or not results:
        return None
    first = results[0] if isinstance(results[0], dict) else {}
    content = str(first.get("raw_content", "")).strip()
    if not content:
        return None
    start = max(0, start_index)
    length = max(1, min(MAX_FETCH_CHARS, max_length))
    chunk = content[start:start + length]
    return f"ATLAS LIVE FETCH — Tavily Extract\nURL: {url}\n\n{chunk}" if chunk else None


def merge_evidence(existing: Evidence, incoming: Evidence) -> Evidence:
    for provider in incoming.providers or ([incoming.provider] if incoming.provider else []):
        if provider and provider not in existing.providers:
            existing.providers.append(provider)
    for query in incoming.source_queries or ([incoming.source_query] if incoming.source_query else []):
        if query and query not in existing.source_queries:
            existing.source_queries.append(query)
    for kind in incoming.query_kinds or ([incoming.query_kind] if incoming.query_kind else []):
        if kind and kind not in existing.query_kinds:
            existing.query_kinds.append(kind)
    if incoming.title and (not existing.title or existing.title == existing.url):
        existing.title = incoming.title
    if len(incoming.snippet) > len(existing.snippet):
        existing.snippet = incoming.snippet
    if incoming.provider_score > existing.provider_score:
        existing.provider_score = incoming.provider_score
    if incoming.published_at and not existing.published_at:
        existing.published_at = incoming.published_at
    return existing


def deduplicate(results: Iterable[Evidence]) -> List[Evidence]:
    merged: Dict[str, Evidence] = {}
    for result in results:
        result.finalize()
        key = result.canonical_url or result.url
        if not key:
            continue
        if key in merged:
            merge_evidence(merged[key], result)
        else:
            merged[key] = result
    return list(merged.values())


def evidence_rank_score(result: Evidence, original_query: str) -> float:
    q = set(tokens(original_query))
    blob = set(tokens(f"{result.title} {result.snippet} {result.url}", remove_generic=False))
    overlap = len(q & blob)
    relevance = overlap / max(1, len(q))
    provider_score = max(0.0, min(1.0, float(result.provider_score or 0.0)))
    provider_diversity = min(1.0, max(0, len(result.providers) - 1) / 3.0)
    query_diversity = min(1.0, max(0, len(result.source_queries) - 1) / 3.0)
    social_bonus = 1.0 if result.platform in {"facebook", "tiktok", "youtube"} else 0.0
    return round(
        relevance * 0.64
        + provider_score * 0.14
        + provider_diversity * 0.14
        + query_diversity * 0.03
        + social_bonus * 0.05,
        6,
    )


def rank_and_diversify(
    results: Iterable[Evidence],
    original_query: str,
    *,
    limit: int = FINAL_MAX_RESULTS,
    max_per_domain: int = MAX_PER_DOMAIN,
) -> List[Evidence]:
    items = list(results)
    for item in items:
        item.rank_score = evidence_rank_score(item, original_query)
    items.sort(
        key=lambda x: (
            x.rank_score,
            len(x.providers),
            len(x.source_queries),
            len(x.snippet),
            x.canonical_url,
        ),
        reverse=True,
    )
    selected: List[Evidence] = []
    domain_counts: Dict[str, int] = {}
    for item in items:
        domain = domain_of(item.url) or "_unknown"
        if domain_counts.get(domain, 0) >= max_per_domain:
            continue
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        selected.append(item)
        if len(selected) >= limit:
            break
    return selected


def provider_configured(name: str) -> bool:
    if name == "cache":
        return CACHE.configured()
    if name == "tavily":
        return bool(os.environ.get("TAVILY_API_KEY", "").strip())
    if name == "serpapi":
        return bool(os.environ.get("SERPAPI_API_KEY", "").strip())
    if name == "duckduckgo":
        return True
    return False


async def aggregate_search(query: str, max_results: int, router: UpstreamRouter) -> Tuple[List[Evidence], Dict[str, ProviderState]]:
    plan = build_query_plan(query)
    news = has_news_intent(query)
    states = {
        name: ProviderState(name=name, enabled=provider_configured(name))
        for name in ("cache", "tavily", "serpapi", "duckduckgo")
    }

    async def run_one(name: str, planned: PlannedQuery) -> List[Evidence]:
        state = states[name]
        if not state.enabled:
            return []
        try:
            if name == "cache":
                rows = await CACHE.search(planned, max_results)
            elif name == "tavily":
                rows = await tavily_search(planned, max_results, news)
            elif name == "serpapi":
                rows = await serpapi_search(planned, max_results, news)
            elif name == "duckduckgo":
                rows = await ddg_search(router, planned, max_results)
            else:
                rows = []
            if rows:
                state.ok += 1
                state.rows += len(rows)
            else:
                state.empty += 1
            return rows
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            state.failed += 1
            state.last_error = f"{type(exc).__name__}: {exc}"
            LOGGER.warning("[%s] SEARCH FAIL | query=%r | %s", name.upper(), planned.query[:140], exc)
            return []

    tasks: List[asyncio.Task] = []
    for planned in plan:
        for name in ("cache", "tavily", "serpapi", "duckduckgo"):
            # DDG only receives base query; public social discovery still runs through
            # Tavily/SerpApi/cache. This bounds latency while retaining DDG evidence.
            if name == "duckduckgo" and planned.kind != "base":
                continue
            if states[name].enabled:
                tasks.append(asyncio.create_task(run_one(name, planned)))

    if not tasks:
        return [], states

    done, pending = await asyncio.wait(tasks, timeout=AGGREGATE_TIMEOUT_SECONDS)
    batches: List[List[Evidence]] = []
    for task in done:
        try:
            batches.append(task.result())
        except Exception as exc:
            LOGGER.warning("[AGGREGATE] task result error: %s", exc)
    if pending:
        LOGGER.warning("[AGGREGATE] TIMEOUT | cancelling=%s", len(pending))
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    raw = [row for batch in batches for row in batch]
    return rank_and_diversify(
        deduplicate(raw),
        query,
        limit=max(3, min(FINAL_MAX_RESULTS, max_results if max_results > 0 else FINAL_MAX_RESULTS)),
        max_per_domain=MAX_PER_DOMAIN,
    ), states


def format_search_response(query: str, results: Sequence[Evidence], states: Dict[str, ProviderState]) -> str:
    provider_summary = []
    for name in ("cache", "tavily", "serpapi", "duckduckgo"):
        state = states[name]
        if not state.enabled:
            provider_summary.append(f"{name}=disabled")
        elif state.failed and not state.ok:
            provider_summary.append(f"{name}=failed")
        elif state.ok:
            provider_summary.append(f"{name}=ok({state.rows})")
        else:
            provider_summary.append(f"{name}=empty")

    parts = [
        "ATLAS MULTI-SOURCE SEARCH — ONE CODEBASE / TWO IDENTICAL NODES",
        f"Query: {query}",
        "Sources: " + " | ".join(provider_summary),
        "Cache policy: supplemental; cache hits do not stop live search.",
        "",
    ]
    if not results:
        parts.append("No usable public search results were collected in this request. Provider failures are isolated; retrying later may succeed.")
        return "\n".join(parts)[:MAX_SEARCH_CHARS]

    for idx, item in enumerate(results, 1):
        title = item.title or item.url
        parts.append(f"[{idx}] {title}")
        parts.append(f"URL: {item.url}")
        parts.append("Providers: " + ", ".join(item.providers or [item.provider]))
        if item.platform != "web":
            parts.append(f"Platform: {item.platform} (public/search-indexed)")
        if item.published_at:
            parts.append(f"Published/Discovered: {item.published_at}")
        if item.snippet:
            parts.append("Snippet: " + item.snippet[:1000])
        parts.append("")
    return "\n".join(parts).strip()[:MAX_SEARCH_CHARS]


async def maybe_intercept_tool_call(message: str, router: UpstreamRouter) -> Optional[str]:
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
            return mcp_text_response(
                request_id,
                "Web search blocked for this request. I can still help with safe, factual, age-appropriate information.",
                is_error=False,
            )
        try:
            requested = int(arguments.get("max_results", FINAL_MAX_RESULTS))
        except (TypeError, ValueError):
            requested = FINAL_MAX_RESULTS
        requested = max(3, min(40, requested))
        results, states = await aggregate_search(query, requested, router)
        LOGGER.info(
            "[SEARCH] AGGREGATE | query=%r | returned=%s | providers=%s",
            query[:140],
            len(results),
            {k: (v.ok, v.empty, v.failed, v.rows) for k, v in states.items()},
        )
        return mcp_text_response(request_id, format_search_response(query, results, states))

    if name == "fetch_content":
        url = str(arguments.get("url", "")).strip()
        if not url or not is_public_http_url(url):
            return None
        try:
            start_index = int(arguments.get("start_index", 0))
        except (TypeError, ValueError):
            start_index = 0
        try:
            max_length = int(arguments.get("max_length", 8000))
        except (TypeError, ValueError):
            max_length = 8000

        # Cache and live extraction are attempted concurrently. Live wins when it
        # succeeds; cache is a support/fallback and never blocks the live attempt.
        cache_task = asyncio.create_task(CACHE.fetch_exact(url, start_index, max_length)) if CACHE.configured() else None
        live_task = asyncio.create_task(tavily_extract(url, start_index, max_length)) if os.environ.get("TAVILY_API_KEY", "").strip() else None
        cache_text: Optional[str] = None
        live_text: Optional[str] = None
        tasks = [t for t in (cache_task, live_task) if t is not None]
        if tasks:
            values = await asyncio.gather(*tasks, return_exceptions=True)
            cursor = 0
            if cache_task is not None:
                value = values[cursor]; cursor += 1
                if isinstance(value, str):
                    cache_text = value
                elif isinstance(value, BaseException):
                    LOGGER.warning("[CACHE] FETCH FAIL | %s", value)
            if live_task is not None:
                value = values[cursor]
                if isinstance(value, str):
                    live_text = value
                elif isinstance(value, BaseException):
                    LOGGER.warning("[TAVILY] EXTRACT FAIL | %s", value)
        if live_text:
            return mcp_text_response(request_id, live_text)
        if cache_text:
            return mcp_text_response(request_id, cache_text)

        # Final fallback uses the SAME persistent upstream DDG process.
        try:
            response = await router.call_tool(
                "fetch_content",
                {"url": url, "start_index": start_index, "max_length": max_length},
                timeout=WEB_TIMEOUT_SECONDS,
            )
            if response.get("error"):
                raise RuntimeError(str(response.get("error")))
            # Return the upstream result untouched except for the original request id.
            text = extract_mcp_text(response)
            if text:
                return mcp_text_response(request_id, text)
        except Exception as exc:
            LOGGER.warning("[DUCKDUCKGO] FETCH FAIL | url=%s | %s", url[:240], exc)
        return mcp_text_response(request_id, "Unable to fetch public content from this URL right now.")

    return None


async def warm_cache_loop() -> None:
    if not CACHE_WARM_ENABLED:
        LOGGER.info("[CACHE] warm loop disabled; cache refreshes on demand")
        return
    if not CACHE.configured():
        LOGGER.info("[CACHE] disabled/unconfigured")
        return
    while True:
        try:
            await CACHE.refresh(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.warning("[CACHE] warm loop error: %s", exc)
        await asyncio.sleep(max(CACHE_REFRESH_SECONDS, CACHE_NEGATIVE_TTL_SECONDS if CACHE.state.last_error else CACHE_REFRESH_SECONDS))


async def parent_to_child(router: UpstreamRouter) -> None:
    process = router.process
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
        intercepted = await maybe_intercept_tool_call(message, router)
        if intercepted is not None:
            await write_stdout(intercepted)
            continue
        await router.send_raw(message)


async def child_to_parent(router: UpstreamRouter) -> None:
    process = router.process
    if process.stdout is None:
        raise RuntimeError("upstream MCP stdout unavailable")
    try:
        while True:
            raw = await process.stdout.readline()
            if not raw:
                router.fail_waiters(RuntimeError("upstream MCP stdout closed"))
                return
            await router.route_child_line(raw)
    except Exception as exc:
        router.fail_waiters(exc)
        raise


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
    LOGGER.info("ATLAS Multi-Source Final v%s starting", VERSION)
    LOGGER.info(
        "Sources | cache=%s | tavily=%s | serpapi=%s | ddg=enabled | social_discovery=%s",
        "enabled" if CACHE.configured() else "disabled",
        "enabled" if os.environ.get("TAVILY_API_KEY", "").strip() else "disabled",
        "enabled" if os.environ.get("SERPAPI_API_KEY", "").strip() else "disabled",
        "enabled" if SOCIAL_DISCOVERY_ENABLED else "disabled",
    )
    LOGGER.info("Architecture | one persistent upstream DDG process | aggregate search | cache=no-short-circuit")
    LOGGER.info("Upstream MCP: %s", " ".join(command))

    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=os.environ.copy(),
    )
    router = UpstreamRouter(process)

    cache_task = asyncio.create_task(warm_cache_loop(), name="atlas-cache-warm")
    p2c = asyncio.create_task(parent_to_child(router), name="parent-to-ddg")
    c2p = asyncio.create_task(child_to_parent(router), name="ddg-to-parent")
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
    router.fail_waiters(RuntimeError("ATLAS proxy shutting down"))
    return int(exit_code or 0)


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(130)
    except Exception as exc:
        LOGGER.exception("ATLAS Multi-Source Final FAIL: %s", exc)
        raise SystemExit(1)
