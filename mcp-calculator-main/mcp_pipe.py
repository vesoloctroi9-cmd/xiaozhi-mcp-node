"""
ATLAS MCP PIPE + RESEARCH SCANNER

- Bridge Xiaozhi WebSocket <-> MCP stdio servers from mcp_config.json.
- Auto-reconnect WebSocket and restart MCP child processes.
- Run a real, conservative research scanner while this process is awake.
- Save research results to /tmp/atlas_research.json.

IMPORTANT:
This scanner does NOT prevent Render Free from sleeping. It only runs while
this process is alive.

Required env:
    MCP_ENDPOINT=wss://...

Optional scanner env:
    ATLAS_RESEARCH_TOPICS=AI Việt Nam,công nghệ mới,thời tiết Việt Nam
    ATLAS_RESEARCH_INTERVAL_SECONDS=900
    ATLAS_RESEARCH_RESULTS_PER_TOPIC=5
    ATLAS_RESEARCH_FETCH_TOP=1
    ATLAS_RESEARCH_OUTPUT=/tmp/atlas_research.json
"""

import asyncio
import html
import ipaddress
import json
import logging
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, quote_plus, unquote, urlparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

import websockets
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# LOGGING
# ============================================================

LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger("MCP_PIPE")


# ============================================================
# MCP / WEBSOCKET SETTINGS
# ============================================================

INITIAL_BACKOFF = 1
MAX_BACKOFF = 30
STABLE_CONNECTION_SECONDS = 60

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_CLOSE_TIMEOUT = 10
WS_OPEN_TIMEOUT = 30


# ============================================================
# RESEARCH SETTINGS
# ============================================================

DEFAULT_RESEARCH_INTERVAL_SECONDS = 900
MIN_RESEARCH_INTERVAL_SECONDS = 300

DEFAULT_RESULTS_PER_TOPIC = 5
DEFAULT_FETCH_TOP = 1
DEFAULT_MAX_ITEMS = 100

DEFAULT_OUTPUT = "/tmp/atlas_research.json"

REQUEST_TIMEOUT_SECONDS = 15
PAGE_TEXT_LIMIT = 5000

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 ATLAS-Research/1.0"
)


# ============================================================
# COMMON HELPERS
# ============================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def env_int(
    name: str,
    default: int,
    minimum: Optional[int] = None,
) -> int:

    raw = os.environ.get(name, "").strip()

    if not raw:
        value = default

    else:
        try:
            value = int(raw)

        except ValueError:
            logger.warning(
                "%s=%r invalid; using %s",
                name,
                raw,
                default,
            )

            value = default

    if minimum is not None and value < minimum:

        logger.warning(
            "%s=%s below minimum %s; clamping",
            name,
            value,
            minimum,
        )

        value = minimum

    return value


def parse_topics() -> List[str]:

    raw = os.environ.get(
        "ATLAS_RESEARCH_TOPICS",
        "",
    ).strip()

    if not raw:
        return []

    topics: List[str] = []

    for part in raw.split(","):

        topic = part.strip()

        if topic and topic not in topics:
            topics.append(topic)

    return topics


# ============================================================
# MCP CONFIG
# ============================================================

def load_config():

    path = os.environ.get("MCP_CONFIG")

    if not path:
        path = os.path.join(
            os.getcwd(),
            "mcp_config.json",
        )

    if not os.path.exists(path):

        logger.warning(
            "MCP config not found: %s",
            path,
        )

        return {}

    try:

        with open(
            path,
            "r",
            encoding="utf-8",
        ) as f:

            cfg = json.load(f)

        logger.info(
            "Loaded MCP config: %s",
            path,
        )

        return cfg

    except Exception as exc:

        logger.error(
            "Failed to load config %s: %s",
            path,
            exc,
        )

        return {}


# ============================================================
# BUILD MCP CHILD PROCESS COMMAND
# ============================================================

def build_server_command(target=None):

    if target is None:

        if len(sys.argv) < 2:
            raise RuntimeError(
                "Missing MCP server target"
            )

        target = sys.argv[1]

    cfg = load_config()

    servers = (
        cfg.get("mcpServers", {})
        if isinstance(cfg, dict)
        else {}
    )

    # --------------------------------------------------------
    # CONFIGURED SERVER
    # --------------------------------------------------------

    if target in servers:

        entry = servers[target] or {}

        if entry.get("disabled"):

            raise RuntimeError(
                f"Server '{target}' is disabled in config"
            )

        transport_type = (
            entry.get("type")
            or entry.get("transportType")
            or "stdio"
        ).lower()

        child_env = os.environ.copy()

        for key, value in (
            entry.get("env") or {}
        ).items():

            child_env[str(key)] = str(value)

        # ----------------------------------------------------
        # STDIO MCP
        # ----------------------------------------------------

        if transport_type == "stdio":

            command = entry.get("command")
            args = entry.get("args") or []

            if not command:

                raise RuntimeError(
                    f"Server '{target}' is missing 'command'"
                )

            return [
                command,
                *args,
            ], child_env

        # ----------------------------------------------------
        # HTTP / SSE MCP
        # ----------------------------------------------------

        if transport_type in (
            "sse",
            "http",
            "streamablehttp",
        ):

            url = entry.get("url")

            if not url:

                raise RuntimeError(
                    f"Server '{target}' is missing 'url'"
                )

            command = [
                sys.executable,
                "-m",
                "mcp_proxy",
            ]

            if transport_type in (
                "http",
                "streamablehttp",
            ):

                command += [
                    "--transport",
                    "streamablehttp",
                ]

            for (
                header_name,
                header_value,
            ) in (
                entry.get("headers") or {}
            ).items():

                command += [
                    "-H",
                    str(header_name),
                    str(header_value),
                ]

            command.append(url)

            return command, child_env

        raise RuntimeError(
            f"Unsupported MCP transport: "
            f"{transport_type}"
        )

    # --------------------------------------------------------
    # LEGACY LOCAL SCRIPT
    # --------------------------------------------------------

    if not os.path.exists(target):

        raise RuntimeError(
            f"'{target}' is neither a configured "
            f"MCP server nor an existing Python script"
        )

    return [
        sys.executable,
        target,
    ], os.environ.copy()


# ============================================================
# WEBSOCKET -> MCP PROCESS
# ============================================================

async def pipe_websocket_to_process(
    websocket,
    process,
    target,
):

    while True:

        message = await websocket.recv()

        if isinstance(message, bytes):

            message = message.decode(
                "utf-8",
                errors="replace",
            )

        logger.debug(
            "[%s] << %s",
            target,
            message[:500],
        )

        if process.poll() is not None:

            raise RuntimeError(
                f"MCP process exited with code "
                f"{process.returncode}"
            )

        if (
            process.stdin is None
            or process.stdin.closed
        ):

            raise RuntimeError(
                "MCP process stdin unavailable/closed"
            )

        process.stdin.write(
            message + "\n"
        )

        process.stdin.flush()


# ============================================================
# MCP PROCESS -> WEBSOCKET
# ============================================================

async def pipe_process_to_websocket(
    process,
    websocket,
    target,
):

    while True:

        if process.stdout is None:

            raise RuntimeError(
                "MCP process stdout unavailable"
            )

        data = await asyncio.to_thread(
            process.stdout.readline
        )

        if not data:

            raise RuntimeError(
                f"MCP process stdout ended "
                f"(exit_code={process.poll()})"
            )

        logger.debug(
            "[%s] >> %s",
            target,
            data[:500],
        )

        await websocket.send(data)


# ============================================================
# MCP STDERR -> RENDER LOG
# ============================================================

async def pipe_process_stderr_to_terminal(
    process,
    target,
):

    if process.stderr is None:
        return

    while True:

        data = await asyncio.to_thread(
            process.stderr.readline
        )

        if not data:
            return

        sys.stderr.write(data)
        sys.stderr.flush()


# ============================================================
# WATCH MCP PROCESS
# ============================================================

async def wait_for_process_exit(
    process,
    target,
):

    exit_code = await asyncio.to_thread(
        process.wait
    )

    raise RuntimeError(
        f"[{target}] MCP process exited "
        f"with code {exit_code}"
    )


# ============================================================
# CONNECT MCP TO XIAOZHI
# ============================================================

async def connect_to_server(
    uri,
    target,
):

    process = None
    bridge_tasks = []

    try:

        logger.info(
            "[%s] Connecting to WebSocket server...",
            target,
        )

        async with websockets.connect(
            uri,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
            close_timeout=WS_CLOSE_TIMEOUT,
            open_timeout=WS_OPEN_TIMEOUT,
        ) as websocket:

            logger.info(
                "[%s] Successfully connected "
                "to WebSocket server",
                target,
            )

            command, child_env = (
                build_server_command(target)
            )

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

            logger.info(
                "[%s] Started server process: %s",
                target,
                " ".join(command),
            )

            ws_to_proc = asyncio.create_task(
                pipe_websocket_to_process(
                    websocket,
                    process,
                    target,
                )
            )

            proc_to_ws = asyncio.create_task(
                pipe_process_to_websocket(
                    process,
                    websocket,
                    target,
                )
            )

            proc_stderr = asyncio.create_task(
                pipe_process_stderr_to_terminal(
                    process,
                    target,
                )
            )

            proc_exit = asyncio.create_task(
                wait_for_process_exit(
                    process,
                    target,
                )
            )

            bridge_tasks = [
                ws_to_proc,
                proc_to_ws,
                proc_stderr,
                proc_exit,
            ]

            critical = {
                ws_to_proc,
                proc_to_ws,
                proc_exit,
            }

            done, _ = await asyncio.wait(
                critical,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:

                if task.cancelled():
                    continue

                exc = task.exception()

                if exc:
                    raise exc

            raise RuntimeError(
                f"[{target}] MCP bridge "
                f"ended unexpectedly"
            )

    except asyncio.CancelledError:
        raise

    except websockets.exceptions.ConnectionClosed as exc:

        logger.warning(
            "[%s] WebSocket connection closed: %s",
            target,
            exc,
        )

        raise

    except Exception as exc:

        logger.error(
            "[%s] Connection error: %s",
            target,
            exc,
        )

        raise

    finally:

        # ----------------------------------------------------
        # CANCEL BRIDGE TASKS
        # ----------------------------------------------------

        for task in bridge_tasks:

            if not task.done():
                task.cancel()

        if bridge_tasks:

            await asyncio.gather(
                *bridge_tasks,
                return_exceptions=True,
            )

        # ----------------------------------------------------
        # TERMINATE CHILD
        # ----------------------------------------------------

        if (
            process is not None
            and process.poll() is None
        ):

            logger.info(
                "[%s] Terminating server process",
                target,
            )

            try:

                process.terminate()

                await asyncio.to_thread(
                    process.wait,
                    5,
                )

            except subprocess.TimeoutExpired:

                logger.warning(
                    "[%s] MCP process did not stop; "
                    "killing it",
                    target,
                )

                process.kill()

                try:

                    await asyncio.to_thread(
                        process.wait,
                        5,
                    )

                except Exception:
                    pass

            except Exception as exc:

                logger.warning(
                    "[%s] Error terminating "
                    "MCP process: %s",
                    target,
                    exc,
                )

        if process is not None:

            logger.info(
                "[%s] Server process terminated",
                target,
            )


# ============================================================
# AUTO RECONNECT
# ============================================================

async def connect_with_retry(
    uri,
    target,
):

    reconnect_attempt = 0
    backoff = INITIAL_BACKOFF

    while True:

        started_at = (
            asyncio.get_running_loop().time()
        )

        try:

            await connect_to_server(
                uri,
                target,
            )

            raise RuntimeError(
                "MCP connection ended unexpectedly"
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            lifetime = (
                asyncio.get_running_loop().time()
                - started_at
            )

            if (
                lifetime
                >= STABLE_CONNECTION_SECONDS
            ):

                reconnect_attempt = 0
                backoff = INITIAL_BACKOFF

                logger.info(
                    "[%s] Previous connection stable "
                    "for %.1fs; backoff reset",
                    target,
                    lifetime,
                )

            reconnect_attempt += 1

            logger.warning(
                "[%s] Connection closed "
                "(attempt %s, lifetime=%.1fs): %s",
                target,
                reconnect_attempt,
                lifetime,
                exc,
            )

            logger.info(
                "[%s] Reconnecting in %ss...",
                target,
                backoff,
            )

            await asyncio.sleep(
                backoff
            )

            backoff = min(
                backoff * 2,
                MAX_BACKOFF,
            )


# ============================================================
# RESEARCH RESULT OBJECT
# ============================================================

@dataclass
class ResearchItem:

    topic: str
    title: str
    url: str
    snippet: str

    page_title: str = ""
    page_excerpt: str = ""

    discovered_at: str = ""


# ============================================================
# DUCKDUCKGO HTML PARSER
# ============================================================

class DuckDuckGoResultParser(
    HTMLParser
):

    def __init__(self):

        super().__init__(
            convert_charrefs=True
        )

        self.results: List[
            Dict[str, str]
        ] = []

        self.in_link = False
        self.in_snippet = False

        self.href = ""

        self.title_parts: List[str] = []
        self.snippet_parts: List[str] = []

        self.pending_index: Optional[int] = None

    def handle_starttag(
        self,
        tag,
        attrs,
    ):

        attrs_dict = dict(attrs)

        class_value = (
            attrs_dict.get(
                "class",
                "",
            )
        )

        if (
            tag == "a"
            and "result__a"
            in class_value
        ):

            self.in_link = True

            self.href = (
                attrs_dict.get(
                    "href",
                    "",
                )
            )

            self.title_parts = []

        if (
            tag
            in ("a", "div", "span")
            and "result__snippet"
            in class_value
        ):

            self.in_snippet = True
            self.snippet_parts = []

    def handle_data(
        self,
        data,
    ):

        if self.in_link:

            self.title_parts.append(
                data
            )

        if self.in_snippet:

            self.snippet_parts.append(
                data
            )

    def handle_endtag(
        self,
        tag,
    ):

        if (
            tag == "a"
            and self.in_link
        ):

            title = " ".join(
                " ".join(
                    self.title_parts
                ).split()
            ).strip()

            url = (
                normalize_duckduckgo_url(
                    self.href
                )
            )

            if title and url:

                self.results.append(
                    {
                        "title": title,
                        "url": url,
                        "snippet": "",
                    }
                )

                self.pending_index = (
                    len(self.results) - 1
                )

            self.in_link = False
            self.href = ""
            self.title_parts = []

        if (
            tag
            in ("a", "div", "span")
            and self.in_snippet
        ):

            snippet = " ".join(
                " ".join(
                    self.snippet_parts
                ).split()
            ).strip()

            if (
                snippet
                and self.pending_index
                is not None
            ):

                self.results[
                    self.pending_index
                ][
                    "snippet"
                ] = snippet

            self.in_snippet = False
            self.snippet_parts = []


# ============================================================
# PAGE TEXT PARSER
# ============================================================

class VisibleTextParser(
    HTMLParser
):

    BLOCKED_TAGS = {
        "script",
        "style",
        "noscript",
        "svg",
        "canvas",
    }

    def __init__(self):

        super().__init__(
            convert_charrefs=True
        )

        self.block_depth = 0
        self.in_title = False

        self.title_parts: List[str] = []
        self.text_parts: List[str] = []

    def handle_starttag(
        self,
        tag,
        attrs,
    ):

        tag = tag.lower()

        if tag in self.BLOCKED_TAGS:

            self.block_depth += 1

        if tag == "title":

            self.in_title = True

    def handle_endtag(
        self,
        tag,
    ):

        tag = tag.lower()

        if tag == "title":

            self.in_title = False

        if (
            tag in self.BLOCKED_TAGS
            and self.block_depth > 0
        ):

            self.block_depth -= 1

    def handle_data(
        self,
        data,
    ):

        cleaned = " ".join(
            data.split()
        ).strip()

        if not cleaned:
            return

        if self.in_title:

            self.title_parts.append(
                cleaned
            )

        if self.block_depth == 0:

            self.text_parts.append(
                cleaned
            )

    @property
    def title(self):

        return " ".join(
            self.title_parts
        ).strip()

    @property
    def text(self):

        return " ".join(
            self.text_parts
        ).strip()


# ============================================================
# NORMALIZE DDG LINKS
# ============================================================

def normalize_duckduckgo_url(
    raw_url: str,
) -> str:

    if not raw_url:
        return ""

    raw_url = html.unescape(
        raw_url
    ).strip()

    if raw_url.startswith("//"):

        raw_url = (
            "https:"
            + raw_url
        )

    parsed = urlparse(
        raw_url
    )

    if (
        "duckduckgo.com"
        in (
            parsed.hostname
            or ""
        )
    ):

        uddg = parse_qs(
            parsed.query
        ).get(
            "uddg"
        )

        if uddg:

            return unquote(
                uddg[0]
            )

    return raw_url


# ============================================================
# BLOCK LOCAL / PRIVATE URLS
# ============================================================

def is_public_http_url(
    url: str,
) -> bool:

    try:

        parsed = urlparse(url)

        if (
            parsed.scheme
            not in ("http", "https")
            or not parsed.hostname
        ):

            return False

        host = parsed.hostname

        if host.lower() == "localhost":

            return False

        # Literal IP
        try:

            ip_obj = (
                ipaddress.ip_address(
                    host
                )
            )

            return not (
                ip_obj.is_private
                or ip_obj.is_loopback
                or ip_obj.is_link_local
                or ip_obj.is_reserved
                or ip_obj.is_multicast
            )

        except ValueError:
            pass

        # DNS
        addresses = socket.getaddrinfo(
            host,
            parsed.port or 443,
            type=socket.SOCK_STREAM,
        )

        for entry in addresses:

            ip_obj = (
                ipaddress.ip_address(
                    entry[4][0]
                )
            )

            if not (
                ip_obj.is_private
                or ip_obj.is_loopback
                or ip_obj.is_link_local
                or ip_obj.is_reserved
                or ip_obj.is_multicast
            ):

                return True

        return False

    except Exception:

        return False


# ============================================================
# SIMPLE HTTP REQUEST
# ============================================================

def http_request_text(
    url: str,
    method="GET",
    data=None,
    extra_headers=None,
) -> str:

    headers = {

        "User-Agent":
            os.environ.get(
                "ATLAS_RESEARCH_USER_AGENT",
                DEFAULT_USER_AGENT,
            ),

        "Accept":
            "text/html,"
            "application/xhtml+xml",

        "Accept-Language":
            "vi-VN,vi;q=0.9,"
            "en;q=0.7",

        "Connection":
            "close",
    }

    if extra_headers:

        headers.update(
            extra_headers
        )

    req = Request(
        url=url,
        data=data,
        method=method,
        headers=headers,
    )

    with urlopen(
        req,
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as resp:

        content_type = (
            resp.headers.get(
                "Content-Type",
                "",
            )
        )

        raw = resp.read(
            1_000_000
        )

    charset = "utf-8"

    match = re.search(
        r"charset=([^\s;]+)",
        content_type,
        re.I,
    )

    if match:

        charset = (
            match.group(1)
            .strip("\"'")
        )

    try:

        return raw.decode(
            charset,
            errors="replace",
        )

    except LookupError:

        return raw.decode(
            "utf-8",
            errors="replace",
        )


# ============================================================
# DUCKDUCKGO SEARCH
# ============================================================

def search_duckduckgo_sync(
    query: str,
    limit: int,
) -> List[Dict[str, str]]:

    body = (
        f"q={quote_plus(query)}"
        .encode("utf-8")
    )

    text = http_request_text(
        "https://html.duckduckgo.com/html/",
        method="POST",
        data=body,
        extra_headers={
            "Content-Type":
                "application/"
                "x-www-form-urlencoded",

            "Origin":
                "https://html.duckduckgo.com",

            "Referer":
                "https://html.duckduckgo.com/",
        },
    )

    parser = (
        DuckDuckGoResultParser()
    )

    parser.feed(text)

    output = []
    seen = set()

    for result in parser.results:

        url = (
            result.get(
                "url",
                "",
            ).strip()
        )

        title = (
            result.get(
                "title",
                "",
            ).strip()
        )

        snippet = (
            result.get(
                "snippet",
                "",
            ).strip()
        )

        if (
            not url
            or not title
            or url in seen
        ):

            continue

        seen.add(url)

        output.append(
            {
                "title":
                    title,

                "url":
                    url,

                "snippet":
                    snippet,
            }
        )

        if len(output) >= limit:
            break

    return output


# ============================================================
# ROBOTS.TXT
# ============================================================

def robots_allows(
    url: str,
) -> bool:

    try:

        parsed = urlparse(url)

        robots_url = (
            f"{parsed.scheme}://"
            f"{parsed.netloc}"
            f"/robots.txt"
        )

        rp = RobotFileParser()

        rp.set_url(
            robots_url
        )

        rp.read()

        user_agent = (
            os.environ.get(
                "ATLAS_RESEARCH_USER_AGENT",
                DEFAULT_USER_AGENT,
            )
        )

        return rp.can_fetch(
            user_agent,
            url,
        )

    except Exception:

        # Conservative behavior:
        # don't fetch if robots check fails.
        return False


# ============================================================
# FETCH RESULT PAGE
# ============================================================

def fetch_page_sync(
    url: str,
) -> Dict[str, str]:

    if not is_public_http_url(
        url
    ):

        raise RuntimeError(
            "Blocked non-public/invalid URL"
        )

    if not robots_allows(
        url
    ):

        raise RuntimeError(
            "robots.txt does not allow "
            "this fetch or could not be checked"
        )

    text = http_request_text(
        url
    )

    parser = VisibleTextParser()

    parser.feed(text)

    return {

        "page_title":
            parser.title[:500],

        "page_excerpt":
            parser.text[
                :PAGE_TEXT_LIMIT
            ],
    }


# ============================================================
# RESEARCH CACHE
# ============================================================

class ResearchCache:

    def __init__(
        self,
        output_path: str,
        max_items: int,
    ):

        self.output_path = Path(
            output_path
        )

        self.max_items = max_items

        self.items: List[
            ResearchItem
        ] = []

        self.seen_urls = set()

        self.last_scan_started_at = ""
        self.last_scan_finished_at = ""

        self.last_scan_status = (
            "not_started"
        )

        self.last_error = ""

    def add(
        self,
        item: ResearchItem,
    ) -> bool:

        if item.url in self.seen_urls:
            return False

        self.seen_urls.add(
            item.url
        )

        self.items.insert(
            0,
            item,
        )

        if (
            len(self.items)
            > self.max_items
        ):

            removed = (
                self.items[
                    self.max_items:
                ]
            )

            self.items = (
                self.items[
                    :self.max_items
                ]
            )

            kept_urls = {
                x.url
                for x
                in self.items
            }

            for old in removed:

                if (
                    old.url
                    not in kept_urls
                ):

                    self.seen_urls.discard(
                        old.url
                    )

        return True

    def write_atomic(
        self,
        topics: List[str],
        interval_seconds: int,
    ):

        payload = {

            "service":
                "ATLAS Research Scanner",

            "generated_at":
                utc_now_iso(),

            "topics":
                topics,

            "interval_seconds":
                interval_seconds,

            "last_scan_started_at":
                self.last_scan_started_at,

            "last_scan_finished_at":
                self.last_scan_finished_at,

            "last_scan_status":
                self.last_scan_status,

            "last_error":
                self.last_error,

            "item_count":
                len(self.items),

            "items":
                [
                    asdict(item)
                    for item
                    in self.items
                ],
        }

        self.output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        fd, temp_path = (
            tempfile.mkstemp(
                prefix=(
                    self.output_path.name
                    + "."
                ),
                suffix=".tmp",
                dir=str(
                    self.output_path.parent
                ),
            )
        )

        try:

            with os.fdopen(
                fd,
                "w",
                encoding="utf-8",
            ) as f:

                json.dump(
                    payload,
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            os.replace(
                temp_path,
                self.output_path,
            )

        finally:

            if os.path.exists(
                temp_path
            ):

                try:

                    os.remove(
                        temp_path
                    )

                except OSError:
                    pass


# ============================================================
# ONE RESEARCH CYCLE
# ============================================================

async def run_research_cycle(
    cache,
    topics,
    results_per_topic,
    fetch_top,
    interval_seconds,
):

    cache.last_scan_started_at = (
        utc_now_iso()
    )

    cache.last_scan_status = (
        "running"
    )

    cache.last_error = ""

    new_items = 0

    logger.info(
        "[RESEARCH] Starting scan: "
        "%s topic(s)",
        len(topics),
    )

    try:

        for topic in topics:

            logger.info(
                "[RESEARCH] Searching: %s",
                topic,
            )

            try:

                results = (
                    await asyncio.to_thread(
                        search_duckduckgo_sync,
                        topic,
                        results_per_topic,
                    )
                )

            except Exception as exc:

                logger.warning(
                    "[RESEARCH] Search failed "
                    "for %r: %s",
                    topic,
                    exc,
                )

                continue

            logger.info(
                "[RESEARCH] %s result(s) "
                "for: %s",
                len(results),
                topic,
            )

            for (
                index,
                result,
            ) in enumerate(results):

                url = result["url"]

                if (
                    url
                    in cache.seen_urls
                ):
                    continue

                page_title = ""
                page_excerpt = ""

                if index < fetch_top:

                    try:

                        fetched = (
                            await asyncio.to_thread(
                                fetch_page_sync,
                                url,
                            )
                        )

                        page_title = (
                            fetched[
                                "page_title"
                            ]
                        )

                        page_excerpt = (
                            fetched[
                                "page_excerpt"
                            ]
                        )

                    except Exception as exc:

                        logger.info(
                            "[RESEARCH] Fetch "
                            "skipped/failed: %s (%s)",
                            url,
                            exc,
                        )

                item = ResearchItem(

                    topic=topic,

                    title=(
                        result["title"]
                    ),

                    url=url,

                    snippet=(
                        result.get(
                            "snippet",
                            "",
                        )
                    ),

                    page_title=(
                        page_title
                    ),

                    page_excerpt=(
                        page_excerpt
                    ),

                    discovered_at=(
                        utc_now_iso()
                    ),
                )

                if cache.add(item):

                    new_items += 1

                    logger.info(
                        "[RESEARCH] NEW | "
                        "%s | %s",
                        topic,
                        item.title[:160],
                    )

        cache.last_scan_status = "ok"

    except Exception as exc:

        cache.last_scan_status = (
            "error"
        )

        cache.last_error = str(exc)

        logger.exception(
            "[RESEARCH] Cycle failed: %s",
            exc,
        )

    finally:

        cache.last_scan_finished_at = (
            utc_now_iso()
        )

        try:

            await asyncio.to_thread(
                cache.write_atomic,
                topics,
                interval_seconds,
            )

            logger.info(
                "[RESEARCH] Cycle complete: "
                "%s new item(s), cache=%s",
                new_items,
                cache.output_path,
            )

        except Exception as exc:

            logger.error(
                "[RESEARCH] Could not "
                "write cache: %s",
                exc,
            )


# ============================================================
# CONTINUOUS RESEARCH SCANNER
# ============================================================

async def research_scanner_forever():

    topics = parse_topics()

    if not topics:

        logger.info(
            "[RESEARCH] Scanner disabled: "
            "ATLAS_RESEARCH_TOPICS is empty"
        )

        return

    interval_seconds = env_int(
        "ATLAS_RESEARCH_INTERVAL_SECONDS",
        DEFAULT_RESEARCH_INTERVAL_SECONDS,
        minimum=MIN_RESEARCH_INTERVAL_SECONDS,
    )

    results_per_topic = env_int(
        "ATLAS_RESEARCH_RESULTS_PER_TOPIC",
        DEFAULT_RESULTS_PER_TOPIC,
        minimum=1,
    )

    fetch_top = env_int(
        "ATLAS_RESEARCH_FETCH_TOP",
        DEFAULT_FETCH_TOP,
        minimum=0,
    )

    max_items = env_int(
        "ATLAS_RESEARCH_MAX_ITEMS",
        DEFAULT_MAX_ITEMS,
        minimum=10,
    )

    output_path = os.environ.get(
        "ATLAS_RESEARCH_OUTPUT",
        DEFAULT_OUTPUT,
    )

    fetch_top = min(
        fetch_top,
        results_per_topic,
    )

    cache = ResearchCache(
        output_path,
        max_items,
    )

    logger.info(
        "[RESEARCH] Enabled | "
        "topics=%s | interval=%ss | "
        "results/topic=%s | "
        "fetch_top=%s | output=%s",
        topics,
        interval_seconds,
        results_per_topic,
        fetch_top,
        output_path,
    )

    while True:

        try:

            await run_research_cycle(
                cache,
                topics,
                results_per_topic,
                fetch_top,
                interval_seconds,
            )

        except asyncio.CancelledError:
            raise

        except Exception as exc:

            logger.exception(
                "[RESEARCH] Unexpected "
                "scanner error: %s",
                exc,
            )

        logger.info(
            "[RESEARCH] Sleeping %ss "
            "until next scan",
            interval_seconds,
        )

        await asyncio.sleep(
            interval_seconds
        )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def signal_handler(
    sig,
    frame,
):

    logger.info(
        "Received signal %s; "
        "shutting down...",
        sig,
    )

    raise KeyboardInterrupt


# ============================================================
# MAIN
# ============================================================

async def main():

    endpoint_url = os.environ.get(
        "MCP_ENDPOINT"
    )

    if not endpoint_url:

        raise RuntimeError(
            "MCP_ENDPOINT is missing"
        )

    target_arg = (
        sys.argv[1]
        if len(sys.argv) >= 2
        else None
    )

    # --------------------------------------------------------
    # START RESEARCH SCANNER
    # --------------------------------------------------------

    scanner_task = asyncio.create_task(
        research_scanner_forever(),
        name="atlas-research-scanner",
    )

    # --------------------------------------------------------
    # CONFIG MODE
    # --------------------------------------------------------

    if not target_arg:

        cfg = load_config()

        servers_cfg = (
            cfg.get(
                "mcpServers",
                {},
            )
            if isinstance(
                cfg,
                dict,
            )
            else {}
        )

        enabled = [

            name

            for (
                name,
                entry,
            )

            in servers_cfg.items()

            if not (
                entry or {}
            ).get(
                "disabled"
            )
        ]

        if not enabled:

            raise RuntimeError(
                "No enabled MCP servers "
                "found in mcp_config.json"
            )

        logger.info(
            "Starting servers: %s",
            ", ".join(enabled),
        )

        mcp_tasks = [

            asyncio.create_task(

                connect_with_retry(
                    endpoint_url,
                    server_name,
                ),

                name=(
                    f"mcp-{server_name}"
                ),
            )

            for server_name
            in enabled
        ]

        try:

            await asyncio.gather(
                *mcp_tasks,
                scanner_task,
            )

        finally:

            for task in [
                *mcp_tasks,
                scanner_task,
            ]:

                if not task.done():
                    task.cancel()

            await asyncio.gather(
                *mcp_tasks,
                scanner_task,
                return_exceptions=True,
            )

        return

    # --------------------------------------------------------
    # LEGACY LOCAL SCRIPT MODE
    # --------------------------------------------------------

    if os.path.exists(
        target_arg
    ):

        logger.info(
            "Starting local MCP script: %s",
            target_arg,
        )

        mcp_task = asyncio.create_task(

            connect_with_retry(
                endpoint_url,
                target_arg,
            ),

            name="mcp-local-script",
        )

        try:

            await asyncio.gather(
                mcp_task,
                scanner_task,
            )

        finally:

            for task in [
                mcp_task,
                scanner_task,
            ]:

                if not task.done():
                    task.cancel()

            await asyncio.gather(
                mcp_task,
                scanner_task,
                return_exceptions=True,
            )

        return

    raise RuntimeError(
        "Argument must be a local Python "
        "script path. To run configured "
        "MCP servers, run without arguments."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    signal.signal(
        signal.SIGINT,
        signal_handler,
    )

    if hasattr(
        signal,
        "SIGTERM",
    ):

        signal.signal(
            signal.SIGTERM,
            signal_handler,
        )

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "Program interrupted / stopped"
        )

    except Exception as exc:

        logger.exception(
            "Program execution error: %s",
            exc,
        )

        sys.exit(1)
