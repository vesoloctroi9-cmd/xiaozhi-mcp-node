#!/usr/bin/env python3
"""
ATLAS Research Worker
One-shot GitHub Actions worker that:
- Uses the same DuckDuckGo MCP server from mcp_config.json.
- Searches/fetches public web content.
- Merges the previous public GitHub Pages cache.
- Deduplicates repeated URLs.
- Writes site/atlas_research.json for GitHub Pages.
- Does NOT depend on Render, PC, or USB.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

VERSION = "1.0.0"
LOGGER = logging.getLogger("ATLAS_RESEARCH")

DEFAULT_CONFIG = "mcp-calculator-main/mcp_config.json"
DEFAULT_SERVER = "duckduckgo-web-search"
DEFAULT_TOPICS = "trí tuệ nhân tạo mới nhất,công nghệ mới nhất,tin Việt Nam mới nhất"
DEFAULT_OUTPUT = "site/atlas_research.json"
URL_RE = re.compile(r"https?://[^\s<>\]\[()\"']+", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "").strip()
    try:
        value = int(raw) if raw else default
    except ValueError:
        LOGGER.warning("%s=%r invalid; using %s", name, raw, default)
        value = default
    return max(minimum, min(maximum, value))


def parse_topics() -> List[str]:
    raw = os.environ.get("ATLAS_RESEARCH_TOPICS", DEFAULT_TOPICS)
    topics: List[str] = []
    for part in raw.split(","):
        topic = part.strip()
        if topic and topic not in topics:
            topics.append(topic)
    return topics


def result_to_text(result: Any) -> str:
    parts: List[str] = []
    for content in getattr(result, "content", []) or []:
        text = getattr(content, "text", None)
        if text:
            parts.append(str(text))
    return "\n".join(parts).strip()


def extract_urls(text: str) -> List[str]:
    urls: List[str] = []
    seen: set[str] = set()
    for match in URL_RE.findall(text or ""):
        url = match.rstrip(".,;:!?")
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def derive_pages_cache_url() -> str:
    explicit = os.environ.get("ATLAS_EXISTING_CACHE_URL", "").strip()
    if explicit:
        return explicit

    repository = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if "/" not in repository:
        return ""

    owner, repo = repository.split("/", 1)
    return f"https://{owner}.github.io/{repo}/atlas_research.json"


def load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else None
    except Exception as exc:
        LOGGER.warning("Local cache unreadable: %s", exc)
        return None


def load_json_url(url: str, timeout_seconds: int) -> Optional[Dict[str, Any]]:
    if not url:
        return None

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "ATLAS-Research-Worker/1.0",
            "Accept": "application/json",
            "Cache-Control": "no-cache",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            if response.status != 200:
                return None
            payload = response.read(2_000_000)
            data = json.loads(payload.decode("utf-8"))
            return data if isinstance(data, dict) else None
    except urllib.error.HTTPError as exc:
        LOGGER.info("Previous Pages cache unavailable: HTTP %s", exc.code)
        return None
    except Exception as exc:
        LOGGER.info("Previous Pages cache unavailable: %s", exc)
        return None


@dataclass
class ResearchItem:
    topic: str
    urls: List[str]
    search_text: str
    fetched_url: str
    fetched_text: str
    discovered_at: str

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "ResearchItem":
        return cls(
            topic=str(raw.get("topic", "")),
            urls=[str(x) for x in raw.get("urls", []) if x],
            search_text=str(raw.get("search_text", "")),
            fetched_url=str(raw.get("fetched_url", "")),
            fetched_text=str(raw.get("fetched_text", "")),
            discovered_at=str(raw.get("discovered_at", "")),
        )


def item_key(item: ResearchItem) -> str:
    if item.urls:
        return "url:" + item.urls[0].strip().lower()
    return "text:" + item.topic.strip().lower() + "|" + item.search_text[:300].strip().lower()


def merge_existing_items(
    sources: Iterable[Optional[Dict[str, Any]]],
    max_items: int,
) -> List[ResearchItem]:
    merged: List[ResearchItem] = []
    seen: set[str] = set()

    for source in sources:
        if not source:
            continue
        for raw in source.get("items", []) or []:
            if not isinstance(raw, dict):
                continue
            item = ResearchItem.from_dict(raw)
            key = item_key(item)
            if key in seen:
                continue
            seen.add(key)
            merged.append(item)
            if len(merged) >= max_items:
                return merged

    return merged


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise RuntimeError(f"MCP config not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise RuntimeError("mcp_config.json must be a JSON object")
    return data


def build_stdio_parameters(
    config: Dict[str, Any],
    server_name: str,
) -> StdioServerParameters:
    servers = config.get("mcpServers", {})
    if not isinstance(servers, dict) or server_name not in servers:
        raise RuntimeError(f"MCP server '{server_name}' not found")

    entry = servers[server_name]
    if not isinstance(entry, dict):
        raise RuntimeError(f"MCP server '{server_name}' config invalid")
    if entry.get("disabled"):
        raise RuntimeError(f"MCP server '{server_name}' disabled")

    transport = str(entry.get("type") or entry.get("transportType") or "stdio").lower()
    if transport != "stdio":
        raise RuntimeError(f"Research Worker requires stdio; got '{transport}'")

    command = entry.get("command")
    args = entry.get("args") or []
    if not command:
        raise RuntimeError(f"MCP server '{server_name}' missing command")

    child_env = os.environ.copy()
    for key, value in (entry.get("env") or {}).items():
        child_env[str(key)] = str(value)

    return StdioServerParameters(
        command=str(command),
        args=[str(arg) for arg in args],
        env=child_env,
    )


async def call_tool(
    session: ClientSession,
    tool_name: str,
    arguments: Dict[str, Any],
    timeout_seconds: int,
) -> Any:
    return await asyncio.wait_for(
        session.call_tool(tool_name, arguments=arguments),
        timeout=timeout_seconds,
    )


async def scan_topics(
    params: StdioServerParameters,
    topics: List[str],
    results_per_topic: int,
    fetch_top: int,
    fetch_max_length: int,
    timeout_seconds: int,
) -> Tuple[List[ResearchItem], List[str], int]:
    new_items: List[ResearchItem] = []
    errors: List[str] = []
    successful_topics = 0

    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await asyncio.wait_for(session.initialize(), timeout=timeout_seconds)
            tools = await asyncio.wait_for(session.list_tools(), timeout=timeout_seconds)
            tool_names = {tool.name for tool in tools.tools}

            LOGGER.info("MCP tools: %s", ", ".join(sorted(tool_names)))

            if "search" not in tool_names:
                raise RuntimeError("MCP server does not expose 'search'")

            can_fetch = "fetch_content" in tool_names

            for topic in topics:
                try:
                    LOGGER.info("[SCAN] %s", topic)
                    result = await call_tool(
                        session,
                        "search",
                        {"query": topic, "max_results": results_per_topic},
                        timeout_seconds,
                    )

                    search_text = result_to_text(result)
                    if getattr(result, "isError", False):
                        raise RuntimeError(search_text or "search tool error")
                    if not search_text:
                        raise RuntimeError("search returned empty text")

                    urls = extract_urls(search_text)[:results_per_topic]
                    successful_topics += 1
                    LOGGER.info("[SCAN] %s -> %s URL(s)", topic, len(urls))

                    fetched_url = ""
                    fetched_text = ""

                    if can_fetch and fetch_top > 0:
                        for candidate in urls[:fetch_top]:
                            try:
                                fetched = await call_tool(
                                    session,
                                    "fetch_content",
                                    {
                                        "url": candidate,
                                        "start_index": 0,
                                        "max_length": fetch_max_length,
                                        "backend": "auto",
                                    },
                                    timeout_seconds,
                                )
                                text = result_to_text(fetched)
                                if getattr(fetched, "isError", False) or not text:
                                    continue
                                fetched_url = candidate
                                fetched_text = text[:fetch_max_length]
                                LOGGER.info("[FETCH] PASS %s", candidate)
                                break
                            except Exception as exc:
                                LOGGER.warning("[FETCH] FAIL %s: %s", candidate, exc)

                    new_items.append(
                        ResearchItem(
                            topic=topic,
                            urls=urls,
                            search_text=search_text[:12000],
                            fetched_url=fetched_url,
                            fetched_text=fetched_text,
                            discovered_at=utc_now_iso(),
                        )
                    )

                except Exception as exc:
                    message = f"{topic}: {exc}"
                    errors.append(message)
                    LOGGER.error("[SCAN] FAIL %s", message)

    return new_items, errors, successful_topics


def merge_new_items(
    new_items: List[ResearchItem],
    old_items: List[ResearchItem],
    max_items: int,
) -> Tuple[List[ResearchItem], int]:
    merged: List[ResearchItem] = []
    seen: set[str] = set()
    new_keys = {item_key(item) for item in new_items}
    added = 0

    for item in [*new_items, *old_items]:
        key = item_key(item)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
        if key in new_keys:
            added += 1
        if len(merged) >= max_items:
            break

    return merged, added


def write_site(output_path: Path, payload: Dict[str, Any]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)

    (output_path.parent / ".nojekyll").write_text("", encoding="utf-8")

    index_html = f"""<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ATLAS Research Cache</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,sans-serif;max-width:760px;margin:40px auto;padding:0 18px;line-height:1.55}}
code{{background:#f2f2f2;padding:2px 6px;border-radius:4px}}
</style>
</head>
<body>
<h1>ATLAS Research Cache</h1>
<p>Trạng thái: <strong>{html.escape(str(payload.get("status", "unknown")))}</strong></p>
<p>Cập nhật UTC: <code>{html.escape(str(payload.get("generated_at", "")))}</code></p>
<p>Số mục đang lưu: <strong>{html.escape(str(payload.get("item_count", 0)))}</strong></p>
<p><a href="{html.escape(output_path.name)}">Mở atlas_research.json</a></p>
</body>
</html>
"""
    (output_path.parent / "index.html").write_text(index_html, encoding="utf-8")


async def async_main() -> int:
    topics = parse_topics()
    if not topics:
        raise RuntimeError("ATLAS_RESEARCH_TOPICS is empty")

    config_path = Path(os.environ.get("ATLAS_MCP_CONFIG", DEFAULT_CONFIG))
    server_name = os.environ.get("ATLAS_RESEARCH_SERVER", DEFAULT_SERVER).strip() or DEFAULT_SERVER
    output_path = Path(os.environ.get("ATLAS_RESEARCH_OUTPUT", DEFAULT_OUTPUT))

    results_per_topic = env_int("ATLAS_RESEARCH_RESULTS_PER_TOPIC", 3, 1, 10)
    fetch_top = env_int("ATLAS_RESEARCH_FETCH_TOP", 1, 0, 3)
    max_items = env_int("ATLAS_RESEARCH_MAX_ITEMS", 100, 10, 1000)
    timeout_seconds = env_int("ATLAS_TOOL_TIMEOUT_SECONDS", 45, 10, 120)
    remote_timeout = env_int("ATLAS_REMOTE_CACHE_TIMEOUT_SECONDS", 12, 3, 30)
    fetch_max_length = env_int("ATLAS_FETCH_MAX_LENGTH", 8000, 1000, 20000)

    cache_url = derive_pages_cache_url()
    local_cache = load_json_file(output_path)
    remote_cache = load_json_url(cache_url, remote_timeout)
    old_items = merge_existing_items([local_cache, remote_cache], max_items)

    LOGGER.info(
        "ATLAS Research Worker %s | server=%s | topics=%s | old_items=%s",
        VERSION,
        server_name,
        len(topics),
        len(old_items),
    )
    if cache_url:
        LOGGER.info("Previous public cache: %s", cache_url)

    config = load_config(config_path)
    params = build_stdio_parameters(config, server_name)

    scan_started_at = utc_now_iso()
    new_items, errors, successful_topics = await scan_topics(
        params,
        topics,
        results_per_topic,
        fetch_top,
        fetch_max_length,
        timeout_seconds,
    )
    scan_finished_at = utc_now_iso()

    # Preserve the previously deployed Pages site when the entire scan fails.
    if successful_topics == 0:
        LOGGER.error("All topics failed; refusing to deploy a broken cache.")
        return 2

    merged_items, added_items = merge_new_items(new_items, old_items, max_items)
    status = "ok" if not errors else "degraded"

    payload: Dict[str, Any] = {
        "service": "ATLAS Research Worker",
        "version": VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "scan_started_at": scan_started_at,
        "scan_finished_at": scan_finished_at,
        "research_server": server_name,
        "topics": topics,
        "results_per_topic": results_per_topic,
        "fetch_top": fetch_top,
        "successful_topics": successful_topics,
        "failed_topics": len(errors),
        "errors": errors,
        "new_unique_items": added_items,
        "item_count": len(merged_items),
        "items": [asdict(item) for item in merged_items],
    }

    write_site(output_path, payload)

    LOGGER.info(
        "PASS | status=%s | successful=%s/%s | new_unique=%s | total=%s | output=%s",
        status,
        successful_topics,
        len(topics),
        added_items,
        len(merged_items),
        output_path,
    )
    return 0


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("ATLAS_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    try:
        return asyncio.run(async_main())
    except KeyboardInterrupt:
        LOGGER.warning("Worker interrupted")
        return 130
    except Exception as exc:
        LOGGER.exception("Research Worker FAIL: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
