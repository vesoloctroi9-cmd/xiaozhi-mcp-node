"""
ATLAS SHARED MCP BRIDGE
Version: 2.3.0 DIAGNOSTICS

Mục tiêu
--------
1) Chỉ làm cầu nối Xiaozhi WebSocket <-> MCP.
2) Tự reconnect WebSocket với exponential backoff có giới hạn.
3) Tự khởi động lại MCP child process khi bridge/process lỗi.
4) Mở HTTP endpoint cho Render:
   - GET /        : liveness của Python process.
   - GET /health  : bridge-readiness (WebSocket + MCP child process).
   - GET /status  : JSON chẩn đoán chi tiết.
5) Theo dõi thụ động MCP JSON-RPC để biết tools/call có được gửi và trả kết quả hay không.
6) Theo dõi stderr của MCP để thấy request HTTP ra Internet nếu MCP server có log.
7) KHÔNG chạy Research Scanner trên Render.
8) Có TEST MODE hữu hạn để kiểm tra cơ chế idle/sleep của Render:
   - ping public URL của chính service mỗi 5 phút;
   - tối đa 60 phút;
   - tự dừng và ghi log PASS/STOP;
   - mặc định TẮT;
   - không phải cơ chế keep-alive thường trực.

Start Command trên Render
-------------------------
cd mcp-calculator-main && exec python mcp_pipe.py

Biến môi trường bắt buộc
------------------------
MCP_ENDPOINT=<Xiaozhi WebSocket endpoint>

Biến môi trường tùy chọn
------------------------
MCP_CONFIG=/path/to/mcp_config.json
MCP_LOG_LEVEL=INFO
PORT=10000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets
from aiohttp import ClientSession, ClientTimeout, web
from dotenv import load_dotenv


# ============================================================================
# ENV / LOGGING
# ============================================================================

load_dotenv()

VERSION = "2.3.0"
SERVICE_NAME = "ATLAS MCP"
LOG_LEVEL = os.environ.get("MCP_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("MCP_PIPE")


# ============================================================================
# CONNECTION SETTINGS
# ============================================================================

INITIAL_BACKOFF = 1
MAX_BACKOFF = 30
STABLE_CONNECTION_SECONDS = 60

WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_CLOSE_TIMEOUT = 10
WS_OPEN_TIMEOUT = 30

MAX_DIAG_LINE = 1200

SLEEP_TEST_DEFAULT_INTERVAL_SECONDS = 300
SLEEP_TEST_DEFAULT_DURATION_SECONDS = 3600
SLEEP_TEST_MIN_INTERVAL_SECONDS = 60
SLEEP_TEST_MAX_DURATION_SECONDS = 3600
SLEEP_TEST_REQUEST_TIMEOUT_SECONDS = 30


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_diag_text(text: str) -> str:
    """
    Giữ log đủ để chẩn đoán nhưng tránh lưu query string có thể chứa token.
    """
    text = text.strip()
    text = re.sub(r"(https?://[^\s?]+)\?[^\s]+", r"\1?<redacted>", text)
    return text[:MAX_DIAG_LINE]



def env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def env_int(
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw = os.environ.get(name, "").strip()

    try:
        value = int(raw) if raw else default
    except ValueError:
        logger.warning(
            "%s=%r invalid; using default=%s",
            name,
            raw,
            default,
        )
        value = default

    if minimum is not None and value < minimum:
        logger.warning(
            "%s=%s below minimum=%s; clamping",
            name,
            value,
            minimum,
        )
        value = minimum

    if maximum is not None and value > maximum:
        logger.warning(
            "%s=%s above maximum=%s; clamping",
            name,
            value,
            maximum,
        )
        value = maximum

    return value


# ============================================================================
# RUNTIME STATE
# ============================================================================

@dataclass
class ServerState:
    target: str

    websocket_connected: bool = False
    process_running: bool = False
    process_pid: Optional[int] = None

    reconnect_attempt: int = 0

    last_connected_at: str = ""
    last_disconnected_at: str = ""
    last_process_started_at: str = ""
    last_process_stopped_at: str = ""
    last_error: str = ""

    ws_to_mcp_messages: int = 0
    mcp_to_ws_messages: int = 0

    last_request_method: str = ""
    last_request_at: str = ""

    last_tool_name: str = ""
    last_tool_call_at: str = ""
    last_tool_result_at: str = ""
    last_tool_result_ok: Optional[bool] = None
    last_tool_error: str = ""
    last_tool_content_chars: int = 0
    last_tool_content_empty: Optional[bool] = None
    last_tool_content_preview: str = ""

    last_mcp_stderr: str = ""
    last_external_http_line: str = ""
    last_external_http_at: str = ""


class RuntimeState:
    def __init__(self) -> None:
        self.started_at = utc_now_iso()
        self.shutdown_requested = False
        self.expected_servers: Set[str] = set()
        self.servers: Dict[str, ServerState] = {}
        self.pending_tool_calls: Dict[str, Tuple[str, str, str]] = {}

        # Bounded Render sleep test state.
        self.sleep_test_enabled = False
        self.sleep_test_running = False
        self.sleep_test_run_id = ""
        self.sleep_test_started_at = ""
        self.sleep_test_stopped_at = ""
        self.sleep_test_interval_seconds = 0
        self.sleep_test_duration_seconds = 0
        self.sleep_test_target_url = ""
        self.sleep_test_sent = 0
        self.sleep_test_passed = 0
        self.sleep_test_failed = 0
        self.sleep_test_inbound_received = 0
        self.sleep_test_last_status: Optional[int] = None
        self.sleep_test_last_error = ""
        self.sleep_test_stop_reason = ""

    def ensure(self, target: str) -> ServerState:
        if target not in self.servers:
            self.servers[target] = ServerState(target=target)
        return self.servers[target]

    def set_expected(self, targets: List[str]) -> None:
        self.expected_servers = set(targets)
        for target in targets:
            self.ensure(target)

    def bridge_ready(self) -> bool:
        if not self.expected_servers:
            return False
        return all(
            self.ensure(target).websocket_connected
            and self.ensure(target).process_running
            for target in self.expected_servers
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "service": SERVICE_NAME,
            "version": VERSION,
            "started_at": self.started_at,
            "now": utc_now_iso(),
            "shutdown_requested": self.shutdown_requested,
            "health_scope": "bridge_only",
            "bridge_ready": self.bridge_ready(),
            "expected_servers": sorted(self.expected_servers),
            "sleep_test": {
                "enabled": self.sleep_test_enabled,
                "running": self.sleep_test_running,
                "run_id": self.sleep_test_run_id,
                "started_at": self.sleep_test_started_at,
                "stopped_at": self.sleep_test_stopped_at,
                "interval_seconds": self.sleep_test_interval_seconds,
                "duration_seconds": self.sleep_test_duration_seconds,
                "target_url": self.sleep_test_target_url,
                "sent": self.sleep_test_sent,
                "passed": self.sleep_test_passed,
                "failed": self.sleep_test_failed,
                "inbound_received": self.sleep_test_inbound_received,
                "last_status": self.sleep_test_last_status,
                "last_error": self.sleep_test_last_error,
                "stop_reason": self.sleep_test_stop_reason,
            },
            "servers": {
                key: asdict(value)
                for key, value in sorted(self.servers.items())
            },
        }


RUNTIME = RuntimeState()


# ============================================================================
# PASSIVE MCP JSON-RPC DIAGNOSTICS
# ============================================================================

def _json_message(text: str) -> Optional[Dict[str, Any]]:
    try:
        value = json.loads(text)
    except Exception:
        return None
    return value if isinstance(value, dict) else None


def observe_ws_to_mcp(target: str, text: str) -> None:
    state = RUNTIME.ensure(target)
    state.ws_to_mcp_messages += 1

    payload = _json_message(text)
    if not payload:
        return

    method = payload.get("method")
    if isinstance(method, str):
        state.last_request_method = method
        state.last_request_at = utc_now_iso()

    if method != "tools/call":
        return

    params = payload.get("params") or {}
    tool_name = params.get("name") if isinstance(params, dict) else None
    tool_name = str(tool_name or "")

    state.last_tool_name = tool_name
    state.last_tool_call_at = utc_now_iso()
    state.last_tool_result_at = ""
    state.last_tool_result_ok = None
    state.last_tool_error = ""
    state.last_tool_content_chars = 0
    state.last_tool_content_empty = None
    state.last_tool_content_preview = ""

    request_id = payload.get("id")
    if request_id is not None:
        RUNTIME.pending_tool_calls[f"{target}:{request_id}"] = (
            target,
            tool_name,
            state.last_tool_call_at,
        )

    logger.info("[%s] MCP tools/call -> %s", target, tool_name or "<unknown>")


def extract_mcp_result_text(result: Any) -> str:
    """Extract user-visible MCP result text for diagnostics only.

    This function never modifies the response sent back to Xiaozhi.
    """
    if isinstance(result, str):
        return result.strip()
    if not isinstance(result, dict):
        return ""

    parts: List[str] = []
    content = result.get("content")
    if isinstance(content, list):
        for item in content:
            if not isinstance(item, dict):
                continue
            value = item.get("text")
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())

    if not parts and result.get("structuredContent") is not None:
        try:
            parts.append(
                json.dumps(
                    result.get("structuredContent"),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        except Exception:
            pass

    return "\n".join(parts).strip()


def compact_preview(text: str, limit: int = 320) -> str:
    compact = re.sub(r"\s+", " ", text or "").strip()
    return sanitize_diag_text(compact)[:limit]


def observe_mcp_to_ws(target: str, text: str) -> None:
    state = RUNTIME.ensure(target)
    state.mcp_to_ws_messages += 1

    payload = _json_message(text)
    if not payload:
        return

    response_id = payload.get("id")
    if response_id is None:
        return

    key = f"{target}:{response_id}"
    pending = RUNTIME.pending_tool_calls.pop(key, None)
    if not pending:
        return

    state.last_tool_result_at = utc_now_iso()

    if payload.get("error") is not None:
        state.last_tool_result_ok = False
        state.last_tool_error = sanitize_diag_text(
            json.dumps(payload.get("error"), ensure_ascii=False)
        )
        logger.warning(
            "[%s] MCP tool result ERROR | tool=%s | %s",
            target,
            pending[1],
            state.last_tool_error,
        )
        return

    result = payload.get("result")
    is_error = False

    if isinstance(result, dict):
        is_error = bool(result.get("isError", False))

    tool_name = pending[1]
    content_text = extract_mcp_result_text(result)
    content_chars = len(content_text)
    content_empty = content_chars == 0
    preview = compact_preview(content_text)

    state.last_tool_content_chars = content_chars
    state.last_tool_content_empty = content_empty
    state.last_tool_content_preview = preview

    if is_error:
        state.last_tool_result_ok = False
        state.last_tool_error = sanitize_diag_text(
            json.dumps(result, ensure_ascii=False)
        )
        logger.warning(
            "[%s] MCP tool result isError=true | tool=%s | chars=%s",
            target,
            tool_name,
            content_chars,
        )
        return

    # A JSON-RPC success with empty content is not a usable Internet result for
    # search/fetch_content. Mark it clearly so /status and Render logs do not
    # misleadingly report PASS. Other MCP tools may legitimately return no text.
    if tool_name in {"search", "fetch_content"} and content_empty:
        state.last_tool_result_ok = False
        state.last_tool_error = "MCP returned success envelope but empty usable content"
        logger.warning(
            "[%s] MCP tool result EMPTY | tool=%s | chars=0",
            target,
            tool_name,
        )
        return

    state.last_tool_result_ok = True
    state.last_tool_error = ""
    logger.info(
        "[%s] MCP tool result OK | tool=%s | chars=%s | preview=%s",
        target,
        tool_name,
        content_chars,
        preview or "<no-text>",
    )


def observe_mcp_stderr(target: str, text: str) -> None:
    state = RUNTIME.ensure(target)
    clean = sanitize_diag_text(text)

    if not clean:
        return

    state.last_mcp_stderr = clean

    lowered = clean.lower()
    if (
        "http://" in lowered
        or "https://" in lowered
        or "http request" in lowered
        or "http/1.1" in lowered
        or "http/2" in lowered
    ):
        state.last_external_http_line = clean
        state.last_external_http_at = utc_now_iso()



# ============================================================================
# BOUNDED RENDER SLEEP TEST
# ============================================================================

def sleep_test_base_url() -> str:
    """
    Resolve the public Render URL.

    Priority:
    1) ATLAS_SLEEP_TEST_URL (explicit override)
    2) Render-provided RENDER_EXTERNAL_URL
    """
    explicit = os.environ.get("ATLAS_SLEEP_TEST_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")

    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    if render_url:
        return render_url.rstrip("/")

    return ""


async def handle_sleep_test_probe(request: web.Request) -> web.Response:
    """
    Dedicated inbound endpoint used only by bounded TEST MODE.

    The run_id is not a credential. It only distinguishes our test request
    from Render's normal health/liveness probes.
    """
    run_id = request.query.get("run_id", "")

    if (
        RUNTIME.sleep_test_running
        and run_id
        and run_id == RUNTIME.sleep_test_run_id
    ):
        RUNTIME.sleep_test_inbound_received += 1

        logger.info(
            "[SLEEP_TEST] INBOUND PASS | run_id=%s | received=%s",
            run_id,
            RUNTIME.sleep_test_inbound_received,
        )

        return web.json_response(
            {
                "service": SERVICE_NAME,
                "version": VERSION,
                "sleep_test": "inbound_pass",
                "run_id": run_id,
                "received": RUNTIME.sleep_test_inbound_received,
                "time": utc_now_iso(),
            },
            status=200,
        )

    return web.json_response(
        {
            "service": SERVICE_NAME,
            "version": VERSION,
            "sleep_test": "inactive_or_invalid_run",
            "time": utc_now_iso(),
        },
        status=404,
    )


async def bounded_render_sleep_test() -> None:
    """
    Run a finite self-probe test through this service's public Render URL.

    Safety/operational guarantees:
    - disabled by default;
    - minimum interval = 60s;
    - hard maximum duration = 3600s;
    - stops automatically;
    - every probe must return through the dedicated inbound endpoint;
    - does not continue after the test window.
    """
    enabled = env_bool("ATLAS_SLEEP_TEST_MODE", False)
    RUNTIME.sleep_test_enabled = enabled

    if not enabled:
        logger.info("[SLEEP_TEST] DISABLED")
        return

    interval_seconds = env_int(
        "ATLAS_SLEEP_TEST_INTERVAL_SECONDS",
        SLEEP_TEST_DEFAULT_INTERVAL_SECONDS,
        minimum=SLEEP_TEST_MIN_INTERVAL_SECONDS,
        maximum=SLEEP_TEST_MAX_DURATION_SECONDS,
    )

    duration_seconds = env_int(
        "ATLAS_SLEEP_TEST_DURATION_SECONDS",
        SLEEP_TEST_DEFAULT_DURATION_SECONDS,
        minimum=interval_seconds,
        maximum=SLEEP_TEST_MAX_DURATION_SECONDS,
    )

    base_url = sleep_test_base_url()
    if not base_url:
        RUNTIME.sleep_test_last_error = (
            "Missing ATLAS_SLEEP_TEST_URL and RENDER_EXTERNAL_URL"
        )
        RUNTIME.sleep_test_stop_reason = "missing_public_url"

        logger.error(
            "[SLEEP_TEST] STOP FAIL | no public URL. "
            "Set ATLAS_SLEEP_TEST_URL or use Render RENDER_EXTERNAL_URL."
        )
        return

    run_id = uuid.uuid4().hex[:12]
    target_url = f"{base_url}/__atlas_sleep_test"

    RUNTIME.sleep_test_running = True
    RUNTIME.sleep_test_run_id = run_id
    RUNTIME.sleep_test_started_at = utc_now_iso()
    RUNTIME.sleep_test_interval_seconds = interval_seconds
    RUNTIME.sleep_test_duration_seconds = duration_seconds
    RUNTIME.sleep_test_target_url = target_url
    RUNTIME.sleep_test_stop_reason = ""
    RUNTIME.sleep_test_last_error = ""

    logger.info(
        "[SLEEP_TEST] START | run_id=%s | interval=%ss | "
        "duration=%ss | target=%s",
        run_id,
        interval_seconds,
        duration_seconds,
        target_url,
    )

    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_seconds
    probe_number = 0

    timeout = ClientTimeout(
        total=SLEEP_TEST_REQUEST_TIMEOUT_SECONDS,
    )

    try:
        async with ClientSession(timeout=timeout) as session:
            while True:
                now = loop.time()

                if now >= deadline:
                    RUNTIME.sleep_test_stop_reason = "duration_complete"
                    break

                probe_number += 1
                RUNTIME.sleep_test_sent += 1

                probe_url = (
                    f"{target_url}"
                    f"?run_id={run_id}"
                    f"&probe={probe_number}"
                    f"&ts={int(now)}"
                )

                before_inbound = RUNTIME.sleep_test_inbound_received

                try:
                    async with session.get(
                        probe_url,
                        headers={
                            "Cache-Control": "no-cache",
                            "Pragma": "no-cache",
                            "User-Agent": "ATLAS-SLEEP-TEST/2.2",
                        },
                        allow_redirects=True,
                    ) as response:
                        status = response.status
                        body = await response.text()

                    RUNTIME.sleep_test_last_status = status

                    inbound_delta = (
                        RUNTIME.sleep_test_inbound_received
                        - before_inbound
                    )

                    if status == 200 and inbound_delta >= 1:
                        RUNTIME.sleep_test_passed += 1
                        RUNTIME.sleep_test_last_error = ""

                        logger.info(
                            "[SLEEP_TEST] PING PASS | probe=%s | "
                            "http=%s | inbound_delta=%s | "
                            "passed=%s failed=%s",
                            probe_number,
                            status,
                            inbound_delta,
                            RUNTIME.sleep_test_passed,
                            RUNTIME.sleep_test_failed,
                        )
                    else:
                        RUNTIME.sleep_test_failed += 1
                        RUNTIME.sleep_test_last_error = (
                            f"Unexpected response status={status} "
                            f"inbound_delta={inbound_delta} "
                            f"body={sanitize_diag_text(body)}"
                        )

                        logger.warning(
                            "[SLEEP_TEST] PING FAIL | probe=%s | %s",
                            probe_number,
                            RUNTIME.sleep_test_last_error,
                        )

                except asyncio.CancelledError:
                    RUNTIME.sleep_test_stop_reason = "cancelled"
                    raise

                except Exception as exc:
                    RUNTIME.sleep_test_failed += 1
                    RUNTIME.sleep_test_last_error = str(exc)

                    logger.warning(
                        "[SLEEP_TEST] PING FAIL | probe=%s | error=%s",
                        probe_number,
                        exc,
                    )

                remaining = deadline - loop.time()
                if remaining <= 0:
                    RUNTIME.sleep_test_stop_reason = "duration_complete"
                    break

                await asyncio.sleep(
                    min(interval_seconds, remaining)
                )

    finally:
        RUNTIME.sleep_test_running = False
        RUNTIME.sleep_test_stopped_at = utc_now_iso()

        if not RUNTIME.sleep_test_stop_reason:
            RUNTIME.sleep_test_stop_reason = "stopped"

        final_word = (
            "PASS"
            if RUNTIME.sleep_test_passed > 0
            and RUNTIME.sleep_test_failed == 0
            else "CHECK"
        )

        logger.info(
            "[SLEEP_TEST] STOP %s | reason=%s | "
            "sent=%s passed=%s failed=%s inbound=%s",
            final_word,
            RUNTIME.sleep_test_stop_reason,
            RUNTIME.sleep_test_sent,
            RUNTIME.sleep_test_passed,
            RUNTIME.sleep_test_failed,
            RUNTIME.sleep_test_inbound_received,
        )


# ============================================================================
# HTTP ENDPOINTS
# ============================================================================

async def handle_root(request: web.Request) -> web.Response:
    return web.Response(
        text=f"OK - {SERVICE_NAME} {VERSION} process alive",
        status=200,
        content_type="text/plain",
    )


async def handle_health(request: web.Request) -> web.Response:
    """
    Chỉ kiểm tra bridge readiness:
    - WebSocket Xiaozhi đang connected.
    - MCP child process đang running.

    Không tuyên bố Internet end-to-end PASS.
    """
    snapshot = RUNTIME.snapshot()
    status = 200 if snapshot["bridge_ready"] else 503
    return web.json_response(snapshot, status=status)


async def handle_status(request: web.Request) -> web.Response:
    return web.json_response(RUNTIME.snapshot(), status=200)


async def start_http_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/status", handle_status)
    app.router.add_get("/__atlas_sleep_test", handle_sleep_test_probe)

    port = int(os.environ.get("PORT", "10000"))

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(
        runner,
        host="0.0.0.0",
        port=port,
    )

    logger.info("[HTTP] Starting server on 0.0.0.0:%s", port)
    await site.start()
    logger.info(
        "[HTTP] / = liveness | /health = bridge readiness | /status = diagnostics | /__atlas_sleep_test = bounded test probe"
    )

    return runner


# ============================================================================
# MCP CONFIG
# ============================================================================

def config_path() -> str:
    return os.environ.get("MCP_CONFIG") or os.path.join(
        os.getcwd(),
        "mcp_config.json",
    )


def load_config() -> Dict[str, Any]:
    path = config_path()

    if not os.path.exists(path):
        raise RuntimeError(f"MCP config not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    except Exception as exc:
        raise RuntimeError(f"Failed to load MCP config {path}: {exc}") from exc

    if not isinstance(cfg, dict):
        raise RuntimeError("MCP config root must be a JSON object")

    raw_servers = cfg.get("mcpServers")
    if not isinstance(raw_servers, dict):
        raise RuntimeError("mcp_config.json must contain object 'mcpServers'")

    logger.info("Loaded MCP config: %s", path)
    return cfg


def configured_servers() -> Dict[str, Dict[str, Any]]:
    cfg = load_config()
    raw = cfg["mcpServers"]
    return raw


def enabled_server_names() -> List[str]:
    servers = configured_servers()

    enabled = [
        str(name)
        for name, entry in servers.items()
        if not (entry or {}).get("disabled")
    ]

    if not enabled:
        raise RuntimeError("No enabled MCP servers found in mcp_config.json")

    return enabled


def build_server_command(target: str) -> Tuple[List[str], Dict[str, str]]:
    servers = configured_servers()

    if target in servers:
        entry = servers[target] or {}

        if entry.get("disabled"):
            raise RuntimeError(
                f"Server '{target}' is disabled in mcp_config.json"
            )

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
                raise RuntimeError(
                    f"Server '{target}' is missing 'command'"
                )

            command = str(command)

            if shutil.which(command) is None:
                raise RuntimeError(
                    f"Executable '{command}' for MCP server '{target}' "
                    "was not found in PATH"
                )

            return [
                command,
                *[str(arg) for arg in args],
            ], child_env

        if transport_type in ("sse", "http", "streamablehttp"):
            url = entry.get("url")
            if not url:
                raise RuntimeError(
                    f"Server '{target}' (type {transport_type}) is missing 'url'"
                )

            cmd = [sys.executable, "-m", "mcp_proxy"]

            if transport_type in ("http", "streamablehttp"):
                cmd += ["--transport", "streamablehttp"]

            for header_name, header_value in (
                entry.get("headers") or {}
            ).items():
                cmd += ["-H", str(header_name), str(header_value)]

            cmd.append(str(url))
            return cmd, child_env

        raise RuntimeError(
            f"Unsupported MCP transport for '{target}': {transport_type}"
        )

    if os.path.exists(target):
        return [sys.executable, target], os.environ.copy()

    raise RuntimeError(
        f"'{target}' is neither a configured MCP server nor an existing script"
    )


# ============================================================================
# ASYNC MCP BRIDGE
# ============================================================================

async def pipe_websocket_to_process(
    websocket: Any,
    process: asyncio.subprocess.Process,
    target: str,
) -> None:
    if process.stdin is None:
        raise RuntimeError("MCP process stdin unavailable")

    while True:
        message = await websocket.recv()

        if isinstance(message, bytes):
            message = message.decode("utf-8", errors="replace")

        observe_ws_to_mcp(target, message)
        logger.debug("[%s] WS -> MCP: %s", target, message[:500])

        if process.returncode is not None:
            raise RuntimeError(
                f"MCP process exited with code {process.returncode}"
            )

        process.stdin.write((message + "\n").encode("utf-8"))
        await process.stdin.drain()


async def pipe_process_to_websocket(
    process: asyncio.subprocess.Process,
    websocket: Any,
    target: str,
) -> None:
    if process.stdout is None:
        raise RuntimeError("MCP process stdout unavailable")

    while True:
        raw = await process.stdout.readline()

        if not raw:
            raise RuntimeError(
                f"MCP process stdout ended (exit_code={process.returncode})"
            )

        data = raw.decode("utf-8", errors="replace")

        observe_mcp_to_ws(target, data)
        logger.debug("[%s] MCP -> WS: %s", target, data[:500])

        await websocket.send(data)


async def pipe_process_stderr_to_terminal(
    process: asyncio.subprocess.Process,
    target: str,
) -> None:
    if process.stderr is None:
        return

    while True:
        raw = await process.stderr.readline()
        if not raw:
            return

        data = raw.decode("utf-8", errors="replace").rstrip("\r\n")
        observe_mcp_stderr(target, data)

        # Giữ nguyên log MCP server để Render Logs có bằng chứng runtime.
        logger.info("[%s][MCP] %s", target, data)


async def wait_for_process_exit(
    process: asyncio.subprocess.Process,
    target: str,
) -> None:
    exit_code = await process.wait()
    raise RuntimeError(
        f"[{target}] MCP process exited with code {exit_code}"
    )


async def terminate_process(
    process: Optional[asyncio.subprocess.Process],
    target: str,
) -> None:
    state = RUNTIME.ensure(target)

    if process is None:
        state.process_running = False
        state.process_pid = None
        return

    if process.returncode is not None:
        state.process_running = False
        state.process_pid = None
        state.last_process_stopped_at = utc_now_iso()
        return

    logger.info("[%s] Terminating MCP server process", target)

    try:
        process.terminate()
        await asyncio.wait_for(process.wait(), timeout=5)

    except asyncio.TimeoutError:
        logger.warning(
            "[%s] MCP process did not stop in 5s; killing it",
            target,
        )
        process.kill()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            pass

    except ProcessLookupError:
        pass

    except Exception as exc:
        logger.warning(
            "[%s] Error while terminating MCP process: %s",
            target,
            exc,
        )

    finally:
        state.process_running = False
        state.process_pid = None
        state.last_process_stopped_at = utc_now_iso()


async def connect_to_server(
    uri: str,
    target: str,
) -> None:
    state = RUNTIME.ensure(target)
    process: Optional[asyncio.subprocess.Process] = None
    bridge_tasks: List[asyncio.Task] = []

    state.websocket_connected = False
    state.process_running = False
    state.process_pid = None
    state.last_error = ""

    try:
        logger.info("[%s] Connecting to Xiaozhi WebSocket...", target)

        async with websockets.connect(
            uri,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
            close_timeout=WS_CLOSE_TIMEOUT,
            open_timeout=WS_OPEN_TIMEOUT,
        ) as websocket:
            state.websocket_connected = True
            state.last_connected_at = utc_now_iso()
            state.last_error = ""

            logger.info(
                "[%s] Successfully connected to Xiaozhi WebSocket",
                target,
            )

            command, child_env = build_server_command(target)

            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=child_env,
            )

            state.process_running = True
            state.process_pid = process.pid
            state.last_process_started_at = utc_now_iso()

            logger.info(
                "[%s] Started MCP server process pid=%s: %s",
                target,
                process.pid,
                " ".join(command),
            )

            ws_to_proc = asyncio.create_task(
                pipe_websocket_to_process(websocket, process, target),
                name=f"{target}-ws-to-mcp",
            )
            proc_to_ws = asyncio.create_task(
                pipe_process_to_websocket(process, websocket, target),
                name=f"{target}-mcp-to-ws",
            )
            proc_stderr = asyncio.create_task(
                pipe_process_stderr_to_terminal(process, target),
                name=f"{target}-stderr",
            )
            proc_exit = asyncio.create_task(
                wait_for_process_exit(process, target),
                name=f"{target}-process-exit",
            )

            bridge_tasks = [
                ws_to_proc,
                proc_to_ws,
                proc_stderr,
                proc_exit,
            ]

            critical_tasks = {
                ws_to_proc,
                proc_to_ws,
                proc_exit,
            }

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

            raise RuntimeError(
                f"[{target}] MCP bridge ended unexpectedly"
            )

    except asyncio.CancelledError:
        raise

    except websockets.exceptions.ConnectionClosed as exc:
        state.last_error = f"WebSocket closed: {exc}"
        logger.warning(
            "[%s] Xiaozhi WebSocket connection closed: %s",
            target,
            exc,
        )
        raise

    except Exception as exc:
        state.last_error = str(exc)
        logger.error("[%s] Connection/bridge error: %s", target, exc)
        raise

    finally:
        state.websocket_connected = False
        state.last_disconnected_at = utc_now_iso()

        for task in bridge_tasks:
            if not task.done():
                task.cancel()

        if bridge_tasks:
            await asyncio.gather(
                *bridge_tasks,
                return_exceptions=True,
            )

        await terminate_process(process, target)


async def connect_with_retry(
    uri: str,
    target: str,
) -> None:
    state = RUNTIME.ensure(target)

    reconnect_attempt = 0
    backoff = INITIAL_BACKOFF

    while True:
        started_at = asyncio.get_running_loop().time()

        try:
            state.reconnect_attempt = reconnect_attempt
            await connect_to_server(uri, target)
            raise RuntimeError("MCP connection ended unexpectedly")

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            lifetime = (
                asyncio.get_running_loop().time()
                - started_at
            )

            if lifetime >= STABLE_CONNECTION_SECONDS:
                reconnect_attempt = 0
                backoff = INITIAL_BACKOFF
                logger.info(
                    "[%s] Previous bridge stable for %.1fs; backoff reset",
                    target,
                    lifetime,
                )

            reconnect_attempt += 1
            state.reconnect_attempt = reconnect_attempt
            state.last_error = str(exc)

            logger.warning(
                "[%s] Bridge closed "
                "(attempt=%s, lifetime=%.1fs): %s",
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

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)


# ============================================================================
# MAIN / GRACEFUL SHUTDOWN
# ============================================================================

async def main() -> None:
    endpoint_url = os.environ.get("MCP_ENDPOINT", "").strip()

    if not endpoint_url:
        raise RuntimeError("MCP_ENDPOINT is missing")

    target_arg = sys.argv[1] if len(sys.argv) >= 2 else None

    if target_arg:
        if not os.path.exists(target_arg):
            raise RuntimeError(
                "Argument must be a local Python script path. "
                "Run without arguments to start configured MCP servers."
            )
        targets = [target_arg]
    else:
        targets = enabled_server_names()

    RUNTIME.set_expected(targets)

    logger.info(
        "%s v%s starting | targets=%s",
        SERVICE_NAME,
        VERSION,
        ", ".join(targets),
    )

    http_runner = await start_http_server()

    sleep_test_task = asyncio.create_task(
        bounded_render_sleep_test(),
        name="bounded-render-sleep-test",
    )

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown(sig_name: str) -> None:
        if RUNTIME.shutdown_requested:
            return

        RUNTIME.shutdown_requested = True
        logger.info(
            "Received %s; graceful shutdown requested",
            sig_name,
        )
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(
                sig,
                request_shutdown,
                sig.name,
            )
        except (NotImplementedError, RuntimeError):
            pass

    mcp_tasks = [
        asyncio.create_task(
            connect_with_retry(endpoint_url, target),
            name=f"mcp-{target}",
        )
        for target in targets
    ]

    try:
        await shutdown_event.wait()

    finally:
        logger.info(
            "Cleaning up MCP bridge tasks, bounded sleep test, and HTTP server..."
        )

        if not sleep_test_task.done():
            sleep_test_task.cancel()

        await asyncio.gather(
            sleep_test_task,
            return_exceptions=True,
        )

        for task in mcp_tasks:
            if not task.done():
                task.cancel()

        if mcp_tasks:
            await asyncio.gather(
                *mcp_tasks,
                return_exceptions=True,
            )

        await http_runner.cleanup()

        logger.info("%s stopped cleanly", SERVICE_NAME)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Program interrupted / stopped")

    except Exception as exc:
        logger.exception("Program execution error: %s", exc)
        sys.exit(1)
