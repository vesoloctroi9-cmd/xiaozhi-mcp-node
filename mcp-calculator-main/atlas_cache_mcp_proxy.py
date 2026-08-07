ATLAS CACHE-FIRST MCP PROXYVersion: 1.1.0

Purpose

Sits between Xiaozhi's MCP bridge and duckduckgo-mcp-server.

Preserves the same MCP tools exposed by the upstream server.

Intercepts only tools/call for:

search        -> use fresh GitHub Pages research cache when relevant.

fetch_content -> use cached fetched_text when URL already exists.

Falls back transparently to the live DuckDuckGo MCP server on cache miss,stale cache, disabled cache, or cache/network errors.

Does NOT create a keep-alive mechanism and does NOT replace Render's bridge.

Render/GitHub cache configuration

Preferred:ATLAS_RESEARCH_CACHE_URL=https://OWNER.github.io/REPO/atlas_research.json

Alternative:ATLAS_GITHUB_REPOSITORY=OWNER/REPO

Optional:ATLAS_CACHE_REFRESH_SECONDS=60ATLAS_CACHE_MAX_AGE_SECONDS=3600ATLAS_CACHE_TIMEOUT_SECONDS=3ATLAS_CACHE_MIN_SCORE=1ATLAS_CACHE_MAX_SEARCH_CHARS=9000ATLAS_CACHE_MAX_FETCH_CHARS=12000

The upstream DuckDuckGo MCP command defaults to:uvx --with duckduckgo-mcp-server[browser] duckduckgo-mcp-server--fetch-backend auto"""

from future import annotations

import asyncioimport jsonimport loggingimport osimport reimport sysimport unicodedatafrom urllib.parse import urlsplit, urlunsplitfrom dataclasses import dataclassfrom datetime import datetime, timezonefrom typing import Any, Dict, Iterable, List, Optional, Tuple

from aiohttp import ClientSession, ClientTimeout

VERSION = "1.1.0"LOGGER = logging.getLogger("ATLAS_CACHE_PROXY")logging.basicConfig(level=os.environ.get("MCP_LOG_LEVEL", "INFO").upper(),format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",stream=sys.stderr,)

def env_int(name: str, default: int, minimum: int, maximum: int) -> int:raw = os.environ.get(name, "").strip()try:value = int(raw) if raw else defaultexcept ValueError:LOGGER.warning("%s=%r invalid; using %s", name, raw, default)value = defaultreturn max(minimum, min(maximum, value))

CACHE_REFRESH_SECONDS = env_int("ATLAS_CACHE_REFRESH_SECONDS", 60, 15, 600)CACHE_MAX_AGE_SECONDS = env_int("ATLAS_CACHE_MAX_AGE_SECONDS", 3600, 60, 86400)CACHE_TIMEOUT_SECONDS = env_int("ATLAS_CACHE_TIMEOUT_SECONDS", 3, 1, 15)CACHE_MIN_SCORE = env_int("ATLAS_CACHE_MIN_SCORE", 1, 1, 10)CACHE_MAX_SEARCH_CHARS = env_int("ATLAS_CACHE_MAX_SEARCH_CHARS", 9000, 1000, 20000)CACHE_MAX_FETCH_CHARS = env_int("ATLAS_CACHE_MAX_FETCH_CHARS", 12000, 1000, 30000)

GENERIC_TOKENS = {# Vietnamese question/news/freshness glue."tin", "tuc", "moi", "nhat", "hom", "nay", "bay", "gio", "doc", "cho","toi", "ve", "la", "gi", "co", "nhung", "cac", "va", "cua", "tren", "the","nao", "cap", "nhat", "thoi", "su", "xem", "tim", "kiem", "noi", "dung",# English equivalents."news", "latest", "today", "read", "search", "find", "about", "what", "the","and", "for", "from", "update", "updates", "current", "recent",}NEWS_HINTS = {"tin", "tuc", "news", "latest", "moi", "nhat", "hom", "nay", "today","thoi", "su", "update", "updates", "recent", "current",}

def utc_now() -> datetime:return datetime.now(timezone.utc)

def utc_now_iso() -> str:return utc_now().isoformat()

def normalize_text(value: str) -> str:text = unicodedata.normalize("NFD", value or "")text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")text = text.lower()text = re.sub(r"[^a-z0-9]+", " ", text)return " ".join(text.split())

def tokens(value: str, *, remove_generic: bool = True) -> List[str]:out: List[str] = []seen = set()for tok in normalize_text(value).split():if len(tok) <= 1 and tok != "ai":continueif remove_generic and tok in GENERIC_TOKENS:continueif tok not in seen:seen.add(tok)out.append(tok)return out

def has_news_intent(query: str) -> bool:raw = set(normalize_text(query).split())return bool(raw & NEWS_HINTS)

def parse_iso(value: str) -> Optional[datetime]:value = (value or "").strip()if not value:return Noneif value.endswith("Z"):value = value[:-1] + "+00:00"try:dt = datetime.fromisoformat(value)except ValueError:return Noneif dt.tzinfo is None:dt = dt.replace(tzinfo=timezone.utc)return dt.astimezone(timezone.utc)

def cache_bypass_enabled() -> bool:# GitHub Research Worker must always use LIVE DuckDuckGo so the cache does not# feed itself. Explicit ATLAS_CACHE_BYPASS is also supported for diagnostics.raw = os.environ.get("ATLAS_CACHE_BYPASS", "").strip().lower()if raw in {"1", "true", "yes", "on"}:return Truereturn os.environ.get("GITHUB_ACTIONS", "").strip().lower() == "true"

def derive_cache_url() -> str:explicit = os.environ.get("ATLAS_RESEARCH_CACHE_URL", "").strip()if explicit:return explicit

# Backward-compatible alias: the current Render environment used this key.
legacy = os.environ.get("RESEARCH_CACHE_URL", "").strip()
if legacy:
    LOGGER.warning(
        "RESEARCH_CACHE_URL is deprecated; prefer ATLAS_RESEARCH_CACHE_URL"
    )
    return legacy

repo = (
    os.environ.get("ATLAS_GITHUB_REPOSITORY", "").strip()
    or os.environ.get("GITHUB_REPOSITORY", "").strip()
)
if "/" not in repo:
    return ""
owner, name = repo.split("/", 1)
return f"https://{owner}.github.io/{name}/atlas_research.json"

def canonical_url_for_cache(value: str) -> str:"""Conservative URL equality for cached fetched_text.

Keep query parameters because they can select different content. Ignore only
fragments, host/scheme case and a trailing slash. Correctness is preferred
over a cache hit: a non-identical article must fall back to LIVE fetch.
"""
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
return urlunsplit(
    (parts.scheme.lower(), parts.netloc.lower(), path, parts.query, "")
)

def upstream_command() -> List[str]:raw_json = os.environ.get("ATLAS_DDG_CHILD_ARGS_JSON", "").strip()command = os.environ.get("ATLAS_DDG_CHILD_COMMAND", "uvx").strip() or "uvx"if raw_json:try:args = json.loads(raw_json)if not isinstance(args, list) or not all(isinstance(x, str) for x in args):raise ValueError("must be a JSON array of strings")return [command, *args]except Exception as exc:raise RuntimeError(f"ATLAS_DDG_CHILD_ARGS_JSON invalid: {exc}") from exc

return [
    command,
    "--with",
    "duckduckgo-mcp-server[browser]",
    "duckduckgo-mcp-server",
    "--fetch-backend",
    "auto",
]

@dataclassclass CacheSnapshot:payload: Optional[Dict[str, Any]] = Nonefetched_at_monotonic: float = 0.0last_attempt_monotonic: float = 0.0source_url: str = ""last_error: str = ""hits_search: int = 0hits_fetch: int = 0misses: int = 0

class ResearchCache:def init(self) -> None:self.state = CacheSnapshot(source_url=derive_cache_url())self._lock = asyncio.Lock()

def configured(self) -> bool:
    return bool(self.state.source_url) and not cache_bypass_enabled()

async def refresh(self, *, force: bool = False) -> Optional[Dict[str, Any]]:
    if not self.configured():
        return None

    loop = asyncio.get_running_loop()
    now = loop.time()

    # Fail-fast throttle applies to both PASS and FAIL attempts. This prevents
    # every tool call from waiting on the same broken/404 cache URL.
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
                        "User-Agent": f"ATLAS-Cache-Proxy/{VERSION}",
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
            self.state.fetched_at_monotonic = loop.time()
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
                "[CACHE] REFRESH FAIL | %s | fallback=LIVE | retry_after=%ss",
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
    self,
    payload: Dict[str, Any],
    query: str,
    max_results: int,
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
        query[:120],
        len(ranked),
        self.state.hits_search,
    )
    return text[:CACHE_MAX_SEARCH_CHARS]

async def fetch_hit(self, url: str, start_index: int, max_length: int) -> Optional[str]:
    payload = await self.refresh()
    if not self.fresh(payload):
        self.state.misses += 1
        LOGGER.info("[CACHE] FETCH MISS | stale/unavailable cache | fallback=LIVE")
        return None

    assert payload is not None
    target = url.strip()
    target_key = canonical_url_for_cache(target)

    for raw in payload.get("items", []) or []:
        if not isinstance(raw, dict):
            continue

        # IMPORTANT CORRECTNESS RULE:
        # fetched_text belongs ONLY to fetched_url. The item's other search
        # URLs may point to different articles and must never reuse this text.
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
        sliced = text[start : start + length]
        if not sliced:
            return None

        self.state.hits_fetch += 1
        LOGGER.info(
            "[CACHE] FETCH HIT | url=%s | chars=%s | hit_count=%s",
            target[:240],
            len(sliced),
            self.state.hits_fetch,
        )
        return (
            "ATLAS CACHE HIT — cached fetched content\n"
            f"URL: {fetched_url}\n"
            f"Cache generated_at: {payload.get('generated_at', '')}\n\n"
            f"{sliced}"
        )

    self.state.misses += 1
    LOGGER.info("[CACHE] FETCH MISS | url=%s | fallback=LIVE", target[:240])
    return None

CACHE = ResearchCache()STDOUT_LOCK: Optional[asyncio.Lock] = None

def mcp_text_response(request_id: Any, text: str) -> str:payload = {"jsonrpc": "2.0","id": request_id,"result": {"content": [{"type": "text","text": text,}],"isError": False,},}return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))

async def write_stdout(line: str) -> None:global STDOUT_LOCKif STDOUT_LOCK is None:STDOUT_LOCK = asyncio.Lock()async with STDOUT_LOCK:sys.stdout.write(line.rstrip("\r\n") + "\n")sys.stdout.flush()

async def maybe_intercept_tool_call(message: str) -> Optional[str]:if not CACHE.configured():return None

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
    try:
        max_results = int(arguments.get("max_results", 10))
    except (TypeError, ValueError):
        max_results = 10
    hit = await CACHE.search_hit(query, max_results)
    if hit:
        LOGGER.info("[CACHE] ROUTE | tool=search | source=CACHE")
        return mcp_text_response(request_id, hit)
    LOGGER.info("[CACHE] ROUTE | tool=search | source=LIVE")
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
    hit = await CACHE.fetch_hit(url, start_index, max_length)
    if hit:
        LOGGER.info("[CACHE] ROUTE | tool=fetch_content | source=CACHE")
        return mcp_text_response(request_id, hit)
    LOGGER.info("[CACHE] ROUTE | tool=fetch_content | source=LIVE")
    return None

return None

async def warm_cache_loop() -> None:if cache_bypass_enabled():LOGGER.info("[CACHE] BYPASS | environment requests LIVE-only mode")returnif not CACHE.configured():LOGGER.warning("[CACHE] DISABLED | set ATLAS_RESEARCH_CACHE_URL (or legacy RESEARCH_CACHE_URL) ""or ATLAS_GITHUB_REPOSITORY")return

LOGGER.info("[CACHE] ENABLED | source=%s", CACHE.state.source_url)
while True:
    try:
        await CACHE.refresh(force=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        LOGGER.warning("[CACHE] warm loop error: %s", exc)
    await asyncio.sleep(CACHE_REFRESH_SECONDS)

async def parent_to_child(process: asyncio.subprocess.Process) -> None:if process.stdin is None:raise RuntimeError("upstream MCP stdin unavailable")

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

async def child_to_parent(process: asyncio.subprocess.Process) -> None:if process.stdout is None:raise RuntimeError("upstream MCP stdout unavailable")

while True:
    raw = await process.stdout.readline()
    if not raw:
        return
    await write_stdout(raw.decode("utf-8", errors="replace"))

async def child_stderr(process: asyncio.subprocess.Process) -> None:if process.stderr is None:returnwhile True:raw = await process.stderr.readline()if not raw:returntext = raw.decode("utf-8", errors="replace").rstrip("\r\n")# Keep upstream diagnostics visible in Render logs via mcp_pipe stderr reader.print(f"[UPSTREAM_DDG] {text}", file=sys.stderr, flush=True)

async def main() -> int:command = upstream_command()LOGGER.info("ATLAS Cache Proxy v%s starting", VERSION)LOGGER.info("Upstream MCP: %s", " ".join(command))

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

done, pending = await asyncio.wait(
    {p2c, c2p, wait},
    return_when=asyncio.FIRST_COMPLETED,
)

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

if name == "main":try:raise SystemExit(asyncio.run(main()))except KeyboardInterrupt:raise SystemExit(130)except Exception as exc:LOGGER.exception("ATLAS Cache Proxy FAIL: %s", exc)raise SystemExit(1)
