"""
ATLAS MCP PIPE + RESEARCH SCANNER
Version: 1.0.0

Purpose
-------
1) Keep the existing Xiaozhi <-> MCP bridge working.
2) Auto-reconnect the Xiaozhi WebSocket.
3) Auto-restart the MCP child process after disconnect/crash.
4) Run a real research scanner on a schedule.
5) The scanner uses a SEPARATE MCP stdio client process, so it never competes
   with Xiaozhi for the same stdin/stdout stream.
6) Save scanner results to a JSON cache.

Required environment
--------------------
MCP_ENDPOINT=<xiaozhi websocket endpoint>

Optional environment
--------------------
MCP_CONFIG=/path/to/mcp_config.json
MCP_LOG_LEVEL=INFO

ATLAS_RESEARCH_TOPICS=trí tuệ nhân tạo mới nhất,công nghệ mới nhất,tin Việt Nam mới nhất
ATLAS_RESEARCH_INTERVAL_SECONDS=600
ATLAS_RESEARCH_RESULTS_PER_TOPIC=3
ATLAS_RESEARCH_FETCH_TOP=1
ATLAS_RESEARCH_OUTPUT=/tmp/atlas_research.json
ATLAS_RESEARCH_SERVER=duckduckgo-web-search
ATLAS_RESEARCH_MAX_ITEMS=100
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import websockets
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


load_dotenv()

LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("MCP_PIPE")


INITIAL_BACKOFF = 1
MAX_BACKOFF = 30
STABLE_CONNECTION_SECONDS = 60

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_CLOSE_TIMEOUT = 10
WS_OPEN_TIMEOUT = 30


DEFAULT_RESEARCH_SERVER = "duckduckgo-web-search"
DEFAULT_RESEARCH_INTERVAL_SECONDS = 600
MIN_RESEARCH_INTERVAL_SECONDS = 300
DEFAULT_RESEARCH_RESULTS_PER_TOPIC = 3
DEFAULT_RESEARCH_FETCH_TOP = 1
DEFAULT_RESEARCH_MAX_ITEMS = 100
DEFAULT_RESEARCH_OUTPUT = "/tmp/atlas_research.json"

URL_RE = re.compile(r"https?://[^\s<>\]\[()\"']+", re.IGNORECASE)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(
    name: str,
    default: int,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw = os.environ.get(name, "").strip()

    try:
        value = int(raw) if raw else default
    except ValueError:
        logger.warning("%s=%r is invalid; using %s", name, raw, default)
        value = default

    if minimum is not None and value < minimum:
        logger.warning("%s=%s is below minimum %s; clamping", name, value, minimum)
        value = minimum

    if maximum is not None and value > maximum:
        logger.warning("%s=%s is above maximum %s; clamping", name, value, maximum)
        value = maximum

    return value


def parse_topics() -> List[str]:
    raw = os.environ.get("ATLAS_RESEARCH_TOPICS", "").strip()
    if not raw:
        return []

    topics: List[str] = []
    for part in raw.split(","):
        topic = part.strip()
        if topic and topic not in topics:
            topics.append(topic)
    return topics


def load_config() -> Dict[str, Any]:
    path = os.environ.get("MCP_CONFIG") or os.path.join(os.getcwd(), "mcp_config.json")

    if not os.path.exists(path):
        logger.warning("MCP config not found: %s", path)
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        logger.info("Loaded MCP config: %s", path)
        return cfg if isinstance(cfg, dict) else {}
    except Exception as exc:
        logger.error("Failed to load MCP config %s: %s", path, exc)
        return {}


def configured_servers() -> Dict[str, Dict[str, Any]]:
    cfg = load_config()
    raw = cfg.get("mcpServers", {}) if isinstance(cfg, dict) else {}
    return raw if isinstance(raw, dict) else {}


def build_server_command(target: str) -> Tuple[List[str], Dict[str, str]]:
    servers = configured_servers()

    if target in servers:
        entry = servers[target] or {}

        if entry.get("disabled"):
            raise RuntimeError(f"Server '{target}' is disabled in mcp_config.json")

        transport_type = (
            entry.get("type")
            or entry.get("transportType")
            or "stdio"
        ).lower()

        child_env = os.environ.copy()
        for key, value in (entry.get("env") or {}).items():
            child_env[str(key)] = str(value)

        if transport_type == "stdio":
            command = entry.get("command")
            args = entry.get("args") or []

            if not command:
                raise RuntimeError(f"Server '{target}' is missing 'command'")

            return [str(command), *[str(x) for x in args]], child_env

        if transport_type in ("sse", "http", "streamablehttp"):
            url = entry.get("url")
            if not url:
                raise RuntimeError(
                    f"Server '{target}' (type {transport_type}) is missing 'url'"
                )

            cmd = [sys.executable, "-m", "mcp_proxy"]

            if transport_type in ("http", "streamablehttp"):
                cmd += ["--transport", "streamablehttp"]

            for header_name, header_value in (entry.get("headers") or {}).items():
                cmd += ["-H", str(header_name), str(header_value)]

            cmd.append(str(url))
            return cmd, child_env

        raise RuntimeError(f"Unsupported MCP transport: {transport_type}")

    if os.path.exists(target):
        return [sys.executable, target], os.environ.copy()

    raise RuntimeError(
        f"'{target}' is neither a configured MCP server nor an existing script"
    )


async def pipe_websocket_to_process(
    websocket: Any,
    process: subprocess.Popen,
    target: str,
) -> None:
    while True:
        message = await websocket.recv()

        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="replace")

        logger.debug("[%s] << %s", target, message[:500])

        if process.poll() is not None:
            raise RuntimeError(f"MCP process exited with code {process.returncode}")

        if process.stdin is None or process.stdin.closed:
            raise RuntimeError("MCP process stdin unavailable/closed")

        process.stdin.write(message + "\n")
        process.stdin.flush()


async def pipe_process_to_websocket(
    process: subprocess.Popen,
    websocket: Any,
    target: str,
) -> None:
    while True:
        if process.stdout is None:
            raise RuntimeError("MCP process stdout unavailable")

        data = await asyncio.to_thread(process.stdout.readline)

        if not data:
            raise RuntimeError(
                f"MCP process stdout ended (exit_code={process.poll()})"
            )

        logger.debug("[%s] >> %s", target, data[:500])
        await websocket.send(data)


async def pipe_process_stderr_to_terminal(
    process: subprocess.Popen,
    target: str,
) -> None:
    if process.stderr is None:
        return

    while True:
        data = await asyncio.to_thread(process.stderr.readline)
        if not data:
            return

        sys.stderr.write(data)
        sys.stderr.flush()


async def wait_for_process_exit(
    process: subprocess.Popen,
    target: str,
) -> None:
    exit_code = await asyncio.to_thread(process.wait)
    raise RuntimeError(f"[{target}] MCP process exited with code {exit_code}")


async def terminate_process(process: Optional[subprocess.Popen], target: str) -> None:
    if process is None:
        return

    if process.poll() is not None:
        logger.info("[%s] Server process already stopped (%s)", target, process.returncode)
        return

    logger.info("[%s] Terminating server process", target)

    try:
        process.terminate()
        await asyncio.to_thread(process.wait, 5)
    except subprocess.TimeoutExpired:
        logger.warning("[%s] MCP process did not stop in 5s; killing it", target)
        process.kill()
        try:
            await asyncio.to_thread(process.wait, 5)
        except Exception:
            pass
    except Exception as exc:
        logger.warning("[%s] Error while terminating process: %s", target, exc)

    logger.info("[%s] Server process terminated", target)


async def connect_to_server(uri: str, target: str) -> None:
    process: Optional[subprocess.Popen] = None
    bridge_tasks: List[asyncio.Task] = []

    try:
        logger.info("[%s] Connecting to WebSocket server...", target)

        async with websockets.connect(
            uri,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
            close_timeout=WS_CLOSE_TIMEOUT,
            open_timeout=WS_OPEN_TIMEOUT,
        ) as websocket:
            logger.info("[%s] Successfully connected to WebSocket server", target)

            command, child_env = build_server_command(target)

            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                encoding="utf-8",
                text=True,
                env=child_env,
                bufsize=1,
            )

            logger.info("[%s] Started server process: %s", target, " ".join(command))

            ws_to_proc = asyncio.create_task(
                pipe_websocket_to_process(websocket, process, target),
                name=f"{target}-ws-to-proc",
            )
            proc_to_ws = asyncio.create_task(
                pipe_process_to_websocket(process, websocket, target),
                name=f"{target}-proc-to-ws",
            )
            proc_stderr = asyncio.create_task(
                pipe_process_stderr_to_terminal(process, target),
                name=f"{target}-stderr",
            )
            proc_exit = asyncio.create_task(
                wait_for_process_exit(process, target),
                name=f"{target}-exit",
            )

            bridge_tasks = [ws_to_proc, proc_to_ws, proc_stderr, proc_exit]
            critical_tasks = {ws_to_proc, proc_to_ws, proc_exit}

            done, _ = await asyncio.wait(
                critical_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                if task.cancelled():
                    continue
                exc = task.exception()
                if exc:
                    raise exc

            raise RuntimeError(f"[{target}] MCP bridge ended unexpectedly")

    except asyncio.CancelledError:
        raise
    except websockets.exceptions.ConnectionClosed as exc:
        logger.warning("[%s] WebSocket connection closed: %s", target, exc)
        raise
    except Exception as exc:
        logger.error("[%s] Connection error: %s", target, exc)
        raise
    finally:
        for task in bridge_tasks:
            if not task.done():
                task.cancel()

        if bridge_tasks:
            await asyncio.gather(*bridge_tasks, return_exceptions=True)

        await terminate_process(process, target)


async def connect_with_retry(uri: str, target: str) -> None:
    reconnect_attempt = 0
    backoff = INITIAL_BACKOFF

    while True:
        started_at = asyncio.get_running_loop().time()

        try:
            await connect_to_server(uri, target)
            raise RuntimeError("MCP connection ended unexpectedly")

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            lifetime = asyncio.get_running_loop().time() - started_at

            if lifetime >= STABLE_CONNECTION_SECONDS:
                reconnect_attempt = 0
                backoff = INITIAL_BACKOFF
                logger.info(
                    "[%s] Previous connection was stable for %.1fs; backoff reset",
                    target,
                    lifetime,
                )

            reconnect_attempt += 1

            logger.warning(
                "[%s] Connection closed (attempt %s, lifetime=%.1fs): %s",
                target,
                reconnect_attempt,
                lifetime,
                exc,
            )

            logger.info("[%s] Reconnecting in %ss...", target, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


@dataclass
class ResearchItem:
    topic: str
    search_text: str
    urls: List[str]
    fetched_url: str = ""
    fetched_text: str = ""
    discovered_at: str = ""


class ResearchCache:
    def __init__(self, output_path: str, max_items: int) -> None:
        self.output_path = Path(output_path)
        self.max_items = max_items
        self.items: List[ResearchItem] = []
        self.seen_fingerprints: set[str] = set()

        self.last_scan_started_at = ""
        self.last_scan_finished_at = ""
        self.last_scan_status = "not_started"
        self.last_error = ""

        self._load_existing()

    def _load_existing(self) -> None:
        if not self.output_path.exists():
            return

        try:
            with self.output_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)

            for raw in payload.get("items", []):
                if not isinstance(raw, dict):
                    continue

                item = ResearchItem(
                    topic=str(raw.get("topic", "")),
                    search_text=str(raw.get("search_text", "")),
                    urls=[str(x) for x in raw.get("urls", []) if x],
                    fetched_url=str(raw.get("fetched_url", "")),
                    fetched_text=str(raw.get("fetched_text", "")),
                    discovered_at=str(raw.get("discovered_at", "")),
                )

                self.items.append(item)
                self.seen_fingerprints.add(self._fingerprint(item))

            self.items = self.items[: self.max_items]

            logger.info(
                "[RESEARCH] Loaded %s cached item(s) from %s",
                len(self.items),
                self.output_path,
            )

        except Exception as exc:
            logger.warning(
                "[RESEARCH] Existing cache could not be loaded: %s",
                exc,
            )

    @staticmethod
    def _fingerprint(item: ResearchItem) -> str:
        first_url = item.urls[0] if item.urls else ""
        return f"{item.topic}\n{first_url}\n{item.search_text[:300]}"

    def add(self, item: ResearchItem) -> bool:
        fp = self._fingerprint(item)

        if fp in self.seen_fingerprints:
            return False

        self.seen_fingerprints.add(fp)
        self.items.insert(0, item)
        self.items = self.items[: self.max_items]
        return True

    def write_atomic(
        self,
        topics: List[str],
        interval_seconds: int,
        research_server: str,
    ) -> None:
        payload = {
            "service": "ATLAS Research Scanner",
            "version": "1.0.0",
            "generated_at": utc_now_iso(),
            "research_server": research_server,
            "topics": topics,
            "interval_seconds": interval_seconds,
            "last_scan_started_at": self.last_scan_started_at,
            "last_scan_finished_at": self.last_scan_finished_at,
            "last_scan_status": self.last_scan_status,
            "last_error": self.last_error,
            "item_count": len(self.items),
            "items": [asdict(item) for item in self.items],
        }

        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        fd, temp_path = tempfile.mkstemp(
            prefix=self.output_path.name + ".",
            suffix=".tmp",
            dir=str(self.output_path.parent),
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            os.replace(temp_path, self.output_path)

        finally:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass


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

    for match in URL_RE.findall(text):
        url = match.rstrip(".,;:!?")
        if url not in seen:
            seen.add(url)
            urls.append(url)

    return urls


def stdio_parameters_for_server(server_name: str) -> StdioServerParameters:
    servers = configured_servers()

    if server_name not in servers:
        raise RuntimeError(
            f"Research server '{server_name}' not found in mcp_config.json"
        )

    entry = servers[server_name] or {}

    if entry.get("disabled"):
        raise RuntimeError(f"Research server '{server_name}' is disabled")

    transport_type = (
        entry.get("type")
        or entry.get("transportType")
        or "stdio"
    ).lower()

    if transport_type != "stdio":
        raise RuntimeError(
            "ATLAS Research Scanner currently requires a stdio MCP server; "
            f"'{server_name}' is type '{transport_type}'"
        )

    command = entry.get("command")
    args = entry.get("args") or []

    if not command:
        raise RuntimeError(
            f"Research server '{server_name}' is missing 'command'"
        )

    child_env = os.environ.copy()
    for key, value in (entry.get("env") or {}).items():
        child_env[str(key)] = str(value)

    return StdioServerParameters(
        command=str(command),
        args=[str(x) for x in args],
        env=child_env,
    )


async def run_research_via_mcp(
    research_server: str,
    topics: List[str],
    results_per_topic: int,
    fetch_top: int,
    cache: ResearchCache,
) -> int:
    params = stdio_parameters_for_server(research_server)
    new_items = 0

    logger.info(
        "[RESEARCH] Opening dedicated MCP client: %s",
        research_server,
    )

    async with stdio_client(params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            tools_response = await session.list_tools()
            tool_names = {tool.name for tool in tools_response.tools}

            logger.info(
                "[RESEARCH] MCP tools available: %s",
                ", ".join(sorted(tool_names)),
            )

            if "search" not in tool_names:
                raise RuntimeError(
                    f"Research MCP '{research_server}' does not expose tool 'search'"
                )

            can_fetch = "fetch_content" in tool_names

            for topic in topics:
                logger.info("[RESEARCH] Searching via MCP: %s", topic)

                search_result = await session.call_tool(
                    "search",
                    arguments={
                        "query": topic,
                        "max_results": results_per_topic,
                    },
                )

                search_text = result_to_text(search_result)

                if getattr(search_result, "isError", False):
                    logger.warning(
                        "[RESEARCH] Search tool returned error for %r: %s",
                        topic,
                        search_text[:500],
                    )
                    continue

                urls = extract_urls(search_text)

                logger.info(
                    "[RESEARCH] Search returned %s URL(s) for: %s",
                    len(urls),
                    topic,
                )

                if not search_text:
                    logger.warning(
                        "[RESEARCH] Search returned empty text for: %s",
                        topic,
                    )
                    continue

                fetched_url = ""
                fetched_text = ""

                if can_fetch and fetch_top > 0 and urls:
                    for candidate in urls[:fetch_top]:
                        try:
                            logger.info(
                                "[RESEARCH] Fetching via MCP: %s",
                                candidate,
                            )

                            fetch_result = await session.call_tool(
                                "fetch_content",
                                arguments={"url": candidate},
                            )

                            text = result_to_text(fetch_result)

                            if getattr(fetch_result, "isError", False) or not text:
                                logger.info(
                                    "[RESEARCH] Fetch produced no usable content: %s",
                                    candidate,
                                )
                                continue

                            fetched_url = candidate
                            fetched_text = text[:8000]
                            break

                        except Exception as exc:
                            logger.info(
                                "[RESEARCH] Fetch failed for %s: %s",
                                candidate,
                                exc,
                            )

                item = ResearchItem(
                    topic=topic,
                    search_text=search_text[:12000],
                    urls=urls[:results_per_topic],
                    fetched_url=fetched_url,
                    fetched_text=fetched_text,
                    discovered_at=utc_now_iso(),
                )

                if cache.add(item):
                    new_items += 1
                    logger.info(
                        "[RESEARCH] NEW | %s | urls=%s | fetched=%s",
                        topic,
                        len(item.urls),
                        bool(item.fetched_text),
                    )
                else:
                    logger.info("[RESEARCH] Duplicate/no-change | %s", topic)

    logger.info("[RESEARCH] Dedicated MCP client closed")
    return new_items


async def run_research_cycle(
    cache: ResearchCache,
    research_server: str,
    topics: List[str],
    interval_seconds: int,
    results_per_topic: int,
    fetch_top: int,
) -> None:
    cache.last_scan_started_at = utc_now_iso()
    cache.last_scan_status = "running"
    cache.last_error = ""

    logger.info("[RESEARCH] Starting scan: %s topic(s)", len(topics))

    try:
        new_items = await run_research_via_mcp(
            research_server=research_server,
            topics=topics,
            results_per_topic=results_per_topic,
            fetch_top=fetch_top,
            cache=cache,
        )

        cache.last_scan_status = "ok"
        logger.info("[RESEARCH] Cycle complete: %s new item(s)", new_items)

    except asyncio.CancelledError:
        raise

    except Exception as exc:
        cache.last_scan_status = "error"
        cache.last_error = str(exc)
        logger.exception("[RESEARCH] Cycle failed: %s", exc)

    finally:
        cache.last_scan_finished_at = utc_now_iso()

        try:
            await asyncio.to_thread(
                cache.write_atomic,
                topics,
                interval_seconds,
                research_server,
            )
            logger.info("[RESEARCH] Cache written: %s", cache.output_path)

        except Exception as exc:
            logger.error("[RESEARCH] Could not write cache: %s", exc)


async def research_scanner_forever() -> None:
    topics = parse_topics()

    if not topics:
        logger.info(
            "[RESEARCH] Scanner disabled: ATLAS_RESEARCH_TOPICS is empty"
        )
        while True:
            await asyncio.sleep(3600)

    research_server = os.environ.get(
        "ATLAS_RESEARCH_SERVER",
        DEFAULT_RESEARCH_SERVER,
    ).strip() or DEFAULT_RESEARCH_SERVER

    interval_seconds = env_int(
        "ATLAS_RESEARCH_INTERVAL_SECONDS",
        DEFAULT_RESEARCH_INTERVAL_SECONDS,
        minimum=MIN_RESEARCH_INTERVAL_SECONDS,
    )

    results_per_topic = env_int(
        "ATLAS_RESEARCH_RESULTS_PER_TOPIC",
        DEFAULT_RESEARCH_RESULTS_PER_TOPIC,
        minimum=1,
        maximum=10,
    )

    fetch_top = env_int(
        "ATLAS_RESEARCH_FETCH_TOP",
        DEFAULT_RESEARCH_FETCH_TOP,
        minimum=0,
        maximum=3,
    )

    max_items = env_int(
        "ATLAS_RESEARCH_MAX_ITEMS",
        DEFAULT_RESEARCH_MAX_ITEMS,
        minimum=10,
        maximum=1000,
    )

    output_path = os.environ.get(
        "ATLAS_RESEARCH_OUTPUT",
        DEFAULT_RESEARCH_OUTPUT,
    ).strip() or DEFAULT_RESEARCH_OUTPUT

    cache = ResearchCache(output_path, max_items)

    logger.info(
        "[RESEARCH] Enabled | server=%s | topics=%s | interval=%ss | "
        "results/topic=%s | fetch_top=%s | output=%s",
        research_server,
        topics,
        interval_seconds,
        results_per_topic,
        fetch_top,
        output_path,
    )

    while True:
        try:
            await run_research_cycle(
                cache=cache,
                research_server=research_server,
                topics=topics,
                interval_seconds=interval_seconds,
                results_per_topic=results_per_topic,
                fetch_top=fetch_top,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            logger.exception("[RESEARCH] Unexpected scanner failure: %s", exc)

        logger.info(
            "[RESEARCH] Sleeping %ss until next scan",
            interval_seconds,
        )

        await asyncio.sleep(interval_seconds)


def signal_handler(sig: int, frame: Any) -> None:
    logger.info("Received signal %s; shutting down...", sig)
    raise KeyboardInterrupt


async def main() -> None:
    endpoint_url = os.environ.get("MCP_ENDPOINT", "").strip()

    if not endpoint_url:
        raise RuntimeError("MCP_ENDPOINT is missing")

    target_arg = sys.argv[1] if len(sys.argv) >= 2 else None

    scanner_task = asyncio.create_task(
        research_scanner_forever(),
        name="atlas-research-scanner",
    )

    mcp_tasks: List[asyncio.Task] = []

    try:
        if target_arg:
            if not os.path.exists(target_arg):
                raise RuntimeError(
                    "Argument must be a local Python script path. "
                    "Run without arguments to start configured MCP servers."
                )

            logger.info("Starting local MCP script: %s", target_arg)

            mcp_tasks.append(
                asyncio.create_task(
                    connect_with_retry(endpoint_url, target_arg),
                    name="mcp-local-script",
                )
            )

        else:
            servers = configured_servers()

            enabled = [
                name
                for name, entry in servers.items()
                if not (entry or {}).get("disabled")
            ]

            if not enabled:
                raise RuntimeError(
                    "No enabled MCP servers found in mcp_config.json"
                )

            logger.info("Starting servers: %s", ", ".join(enabled))

            for server_name in enabled:
                mcp_tasks.append(
                    asyncio.create_task(
                        connect_with_retry(endpoint_url, server_name),
                        name=f"mcp-{server_name}",
                    )
                )

        await asyncio.gather(*mcp_tasks, scanner_task)

    finally:
        for task in [*mcp_tasks, scanner_task]:
            if not task.done():
                task.cancel()

        await asyncio.gather(
            *mcp_tasks,
            scanner_task,
            return_exceptions=True,
        )


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, signal_handler)

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Program interrupted / stopped")

    except Exception as exc:
        logger.exception("Program execution error: %s", exc)
        sys.exit(1)
