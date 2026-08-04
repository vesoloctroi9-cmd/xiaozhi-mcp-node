"""
ATLAS NODE-2 MCP BRIDGE
Version: 2.1.0

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
8) KHÔNG tự ping/keep-alive để né cơ chế sleep của Render.

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
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import websockets
from aiohttp import web
from dotenv import load_dotenv


# ============================================================================
# ENV / LOGGING
# ============================================================================

load_dotenv()

VERSION = "2.1.0"
SERVICE_NAME = "ATLAS NODE-2 MCP"
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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_diag_text(text: str) -> str:
    """
    Giữ log đủ để chẩn đoán nhưng tránh lưu query string có thể chứa token.
    """
    text = text.strip()
    text = re.sub(r"(https?://[^\s?]+)\?[^\s]+", r"\1?<redacted>", text)
    return text[:MAX_DIAG_LINE]


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

    request_id = payload.get("id")
    if request_id is not None:
        RUNTIME.pending_tool_calls[f"{target}:{request_id}"] = (
            target,
            tool_name,
            state.last_tool_call_at,
        )

    logger.info("[%s] MCP tools/call -> %s", target, tool_name or "<unknown>")


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

    state.last_tool_result_ok = not is_error

    if is_error:
        state.last_tool_error = sanitize_diag_text(
            json.dumps(result, ensure_ascii=False)
        )
        logger.warning(
            "[%s] MCP tool result isError=true | tool=%s",
            target,
            pending[1],
        )
    else:
        logger.info(
            "[%s] MCP tool result OK | tool=%s",
            target,
            pending[1],
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
        "[HTTP] / = liveness | /health = bridge readiness | /status = diagnostics"
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
        logger.info("Cleaning up MCP bridge tasks and HTTP server...")

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
