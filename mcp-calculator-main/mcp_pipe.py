"""
ATLAS NODE-2 MCP PIPE
Version: 2.0.0

Scope
-----
- Xiaozhi WebSocket <-> MCP bridge only.
- Automatic WebSocket reconnect with bounded backoff.
- Automatic MCP child-process restart after disconnect/crash.
- Built-in HTTP liveness/readiness/status endpoints for Render.
- NO Research Scanner on Render. Research stays on GitHub Actions.
- This file owns Render's $PORT. Do NOT start a second `python -m http.server`.

Endpoints
---------
GET /         -> liveness, always 200 while this Python process is alive.
GET /health   -> readiness, 200 only when every enabled MCP server is connected
                 to Xiaozhi AND its MCP child process is running; otherwise 503.
GET /status   -> JSON diagnostic state.

Required environment
--------------------
MCP_ENDPOINT=<xiaozhi websocket endpoint>

Optional environment
--------------------
MCP_CONFIG=/path/to/mcp_config.json
MCP_LOG_LEVEL=INFO
PORT=10000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import subprocess
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


# ============================================================================
# RUNTIME STATE
# ============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ServerState:
    target: str
    websocket_connected: bool = False
    process_running: bool = False
    reconnect_attempt: int = 0
    last_connected_at: str = ""
    last_disconnected_at: str = ""
    last_process_started_at: str = ""
    last_process_stopped_at: str = ""
    last_error: str = ""


class RuntimeState:
    def __init__(self) -> None:
        self.started_at = utc_now_iso()
        self.expected_servers: Set[str] = set()
        self.servers: Dict[str, ServerState] = {}
        self.shutdown_requested = False

    def ensure(self, target: str) -> ServerState:
        if target not in self.servers:
            self.servers[target] = ServerState(target=target)
        return self.servers[target]

    def set_expected(self, targets: List[str]) -> None:
        self.expected_servers = set(targets)
        for target in targets:
            self.ensure(target)

    def ready(self) -> bool:
        if not self.expected_servers:
            return False

        for target in self.expected_servers:
            state = self.ensure(target)
            if not (state.websocket_connected and state.process_running):
                return False

        return True

    def snapshot(self) -> Dict[str, Any]:
        return {
            "service": "ATLAS NODE-2 MCP",
            "version": "2.0.0",
            "started_at": self.started_at,
            "now": utc_now_iso(),
            "shutdown_requested": self.shutdown_requested,
            "ready": self.ready(),
            "expected_servers": sorted(self.expected_servers),
            "servers": {
                key: asdict(value)
                for key, value in sorted(self.servers.items())
            },
        }


RUNTIME = RuntimeState()


# ============================================================================
# HTTP SERVER
# ============================================================================

async def handle_root(request: web.Request) -> web.Response:
    """
    Liveness only.

    Important:
    Returning 200 here means the Python process and HTTP event loop are alive.
    It does NOT claim MCP/Xiaozhi readiness.
    """
    return web.Response(
        text="OK - ATLAS NODE-2 process is alive",
        status=200,
        content_type="text/plain",
    )


async def handle_health(request: web.Request) -> web.Response:
    """
    Readiness endpoint.

    200 = all configured MCP bridges are connected to Xiaozhi and their child
          processes are running.
    503 = Python is alive, but MCP/Xiaozhi is not fully ready.
    """
    snapshot = RUNTIME.snapshot()

    if snapshot["ready"]:
        return web.json_response(snapshot, status=200)

    return web.json_response(snapshot, status=503)


async def handle_status(request: web.Request) -> web.Response:
    """Always return current diagnostic state."""
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
    logger.info("[HTTP] Liveness=/ | Readiness=/health | Diagnostics=/status")

    return runner


# ============================================================================
# MCP CONFIG
# ============================================================================

def load_config() -> Dict[str, Any]:
    path = os.environ.get("MCP_CONFIG") or os.path.join(
        os.getcwd(),
        "mcp_config.json",
    )

    if not os.path.exists(path):
        raise RuntimeError(f"MCP config not found: {path}")

    try:
        with open(path, "r", encoding="utf-8") as handle:
            config = json.load(handle)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load MCP config {path}: {exc}"
        ) from exc

    if not isinstance(config, dict):
        raise RuntimeError("MCP config root must be a JSON object")

    return config


def configured_servers() -> Dict[str, Dict[str, Any]]:
    config = load_config()
    raw = config.get("mcpServers", {})

    if not isinstance(raw, dict):
        raise RuntimeError("mcpServers must be a JSON object")

    return raw


def enabled_server_names() -> List[str]:
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

    return enabled


def build_server_command(
    target: str,
) -> Tuple[List[str], Dict[str, str]]:
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

            return [
                str(command),
                *[str(arg) for arg in args],
            ], child_env

        if transport_type in (
            "sse",
            "http",
            "streamablehttp",
        ):
            url = entry.get("url")

            if not url:
                raise RuntimeError(
                    f"Server '{target}' "
                    f"(type {transport_type}) is missing 'url'"
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

            for header_name, header_value in (
                entry.get("headers") or {}
            ).items():
                command += [
                    "-H",
                    str(header_name),
                    str(header_value),
                ]

            command.append(str(url))
            return command, child_env

        raise RuntimeError(
            f"Unsupported MCP transport: {transport_type}"
        )

    if os.path.exists(target):
        return [
            sys.executable,
            target,
        ], os.environ.copy()

    raise RuntimeError(
        f"'{target}' is neither a configured MCP server "
        "nor an existing script"
    )


# ============================================================================
# MCP BRIDGE
# ============================================================================

async def pipe_websocket_to_process(
    websocket: Any,
    process: subprocess.Popen,
    target: str,
) -> None:
    while True:
        message = await websocket.recv()

        if isinstance(message, bytes):
            message = message.decode(
                "utf-8",
                errors="replace",
            )

        logger.debug("[%s] WS -> MCP: %s", target, message[:500])

        if process.poll() is not None:
            raise RuntimeError(
                f"MCP process exited with code {process.returncode}"
            )

        if process.stdin is None or process.stdin.closed:
            raise RuntimeError(
                "MCP process stdin unavailable/closed"
            )

        process.stdin.write(message + "\n")
        process.stdin.flush()


async def pipe_process_to_websocket(
    process: subprocess.Popen,
    websocket: Any,
    target: str,
) -> None:
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
                "MCP process stdout ended "
                f"(exit_code={process.poll()})"
            )

        logger.debug("[%s] MCP -> WS: %s", target, data[:500])

        await websocket.send(data)


async def pipe_process_stderr_to_terminal(
    process: subprocess.Popen,
    target: str,
) -> None:
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


async def wait_for_process_exit(
    process: subprocess.Popen,
    target: str,
) -> None:
    exit_code = await asyncio.to_thread(process.wait)

    raise RuntimeError(
        f"[{target}] MCP process exited with code {exit_code}"
    )


async def terminate_process(
    process: Optional[subprocess.Popen],
    target: str,
) -> None:
    state = RUNTIME.ensure(target)

    if process is None:
        state.process_running = False
        return

    if process.poll() is not None:
        state.process_running = False
        state.last_process_stopped_at = utc_now_iso()
        logger.info(
            "[%s] Server process already stopped (%s)",
            target,
            process.returncode,
        )
        return

    logger.info("[%s] Terminating server process", target)

    try:
        process.terminate()
        await asyncio.to_thread(
            process.wait,
            5,
        )

    except subprocess.TimeoutExpired:
        logger.warning(
            "[%s] MCP process did not stop in 5s; killing it",
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
            "[%s] Error while terminating process: %s",
            target,
            exc,
        )

    finally:
        state.process_running = False
        state.last_process_stopped_at = utc_now_iso()

    logger.info("[%s] Server process terminated", target)


async def connect_to_server(
    uri: str,
    target: str,
) -> None:
    state = RUNTIME.ensure(target)
    process: Optional[subprocess.Popen] = None
    bridge_tasks: List[asyncio.Task] = []

    state.websocket_connected = False
    state.process_running = False
    state.last_error = ""

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
            state.websocket_connected = True
            state.last_connected_at = utc_now_iso()
            state.last_error = ""

            logger.info(
                "[%s] Successfully connected to WebSocket server",
                target,
            )

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

            state.process_running = True
            state.last_process_started_at = utc_now_iso()

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
                ),
                name=f"{target}-ws-to-proc",
            )

            proc_to_ws = asyncio.create_task(
                pipe_process_to_websocket(
                    process,
                    websocket,
                    target,
                ),
                name=f"{target}-proc-to-ws",
            )

            proc_stderr = asyncio.create_task(
                pipe_process_stderr_to_terminal(
                    process,
                    target,
                ),
                name=f"{target}-stderr",
            )

            proc_exit = asyncio.create_task(
                wait_for_process_exit(
                    process,
                    target,
                ),
                name=f"{target}-exit",
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
            "[%s] WebSocket connection closed: %s",
            target,
            exc,
        )

        raise

    except Exception as exc:
        state.last_error = str(exc)

        logger.error(
            "[%s] Connection error: %s",
            target,
            exc,
        )

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

        await terminate_process(
            process,
            target,
        )


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

            if lifetime >= STABLE_CONNECTION_SECONDS:
                reconnect_attempt = 0
                backoff = INITIAL_BACKOFF

                logger.info(
                    "[%s] Previous connection stable for %.1fs; "
                    "backoff reset",
                    target,
                    lifetime,
                )

            reconnect_attempt += 1
            state.reconnect_attempt = reconnect_attempt
            state.last_error = str(exc)

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

            await asyncio.sleep(backoff)

            backoff = min(
                backoff * 2,
                MAX_BACKOFF,
            )


# ============================================================================
# MAIN / SHUTDOWN
# ============================================================================

async def main() -> None:
    endpoint_url = os.environ.get(
        "MCP_ENDPOINT",
        "",
    ).strip()

    if not endpoint_url:
        raise RuntimeError(
            "MCP_ENDPOINT is missing"
        )

    target_arg = (
        sys.argv[1]
        if len(sys.argv) >= 2
        else None
    )

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

    for sig in (
        signal.SIGINT,
        signal.SIGTERM,
    ):
        try:
            loop.add_signal_handler(
                sig,
                request_shutdown,
                sig.name,
            )
        except (NotImplementedError, RuntimeError):
            # Fallback only matters on platforms without asyncio signal handlers.
            pass

    logger.info(
        "Starting MCP bridge(s): %s",
        ", ".join(targets),
    )

    mcp_tasks = [
        asyncio.create_task(
            connect_with_retry(
                endpoint_url,
                target,
            ),
            name=f"mcp-{target}",
        )
        for target in targets
    ]

    try:
        await shutdown_event.wait()

    finally:
        logger.info(
            "Cleaning up MCP tasks and HTTP server..."
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

        logger.info(
            "ATLAS NODE-2 stopped cleanly"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())

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
