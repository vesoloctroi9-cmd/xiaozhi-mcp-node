"""
ATLAS MCP PIPE
Simple MCP stdio <-> WebSocket bridge with automatic recovery.

Goals:
- Reconnect automatically when Xiaozhi WebSocket disconnects.
- Restart MCP child process automatically after failure.
- Detect dead WebSocket faster with ping/pong.
- Reset reconnect backoff after a stable connection.
- Run all enabled MCP servers from mcp_config.json.

Environment:
    MCP_ENDPOINT=<xiaozhi websocket endpoint>

Optional:
    MCP_CONFIG=/path/to/mcp_config.json
    MCP_LOG_LEVEL=INFO

Run:
    python mcp_pipe.py
"""

import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time

import websockets
from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

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
# RECOVERY / WEBSOCKET SETTINGS
# ============================================================

# Reconnect quickly after failure.
INITIAL_BACKOFF = 1

# Never wait more than 30 seconds between reconnect attempts.
MAX_BACKOFF = 30

# If a connection survives this long, the next disconnect
# is treated as a fresh incident and backoff resets to 1 second.
STABLE_CONNECTION_SECONDS = 60

# WebSocket keepalive.
WS_PING_INTERVAL = 20
WS_PING_TIMEOUT = 20
WS_CLOSE_TIMEOUT = 10


# ============================================================
# CONFIG
# ============================================================

def load_config():
    """
    Load JSON config from:
        1. $MCP_CONFIG
        2. ./mcp_config.json

    Returns dict or {}.
    """

    path = os.environ.get("MCP_CONFIG")

    if not path:
        path = os.path.join(os.getcwd(), "mcp_config.json")

    if not os.path.exists(path):
        logger.warning(f"MCP config not found: {path}")
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        logger.info(f"Loaded MCP config: {path}")
        return cfg

    except Exception as e:
        logger.error(f"Failed to load config {path}: {e}")
        return {}


# ============================================================
# BUILD MCP CHILD PROCESS COMMAND
# ============================================================

def build_server_command(target=None):
    """
    Build command and environment for an MCP server.

    Priority:
    - If target exists in config.mcpServers:
        use configured command.
    - Otherwise:
        treat target as a local Python script.

    Returns:
        ([command, args...], environment)
    """

    if target is None:
        if len(sys.argv) < 2:
            raise RuntimeError("Missing MCP server target")

        target = sys.argv[1]

    cfg = load_config()

    servers = (
        cfg.get("mcpServers", {})
        if isinstance(cfg, dict)
        else {}
    )

    # --------------------------------------------------------
    # CONFIGURED MCP SERVER
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

        for key, value in (entry.get("env") or {}).items():
            child_env[str(key)] = str(value)

        # ----------------------------------------------------
        # STDIO MCP SERVER
        # ----------------------------------------------------

        if transport_type == "stdio":

            command = entry.get("command")
            args = entry.get("args") or []

            if not command:
                raise RuntimeError(
                    f"Server '{target}' is missing 'command'"
                )

            return [command, *args], child_env

        # ----------------------------------------------------
        # HTTP / SSE MCP SERVER
        # ----------------------------------------------------

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

            headers = entry.get("headers") or {}

            for header_name, header_value in headers.items():
                command += [
                    "-H",
                    str(header_name),
                    str(header_value),
                ]

            command.append(url)

            return command, child_env

        raise RuntimeError(
            f"Unsupported MCP transport: {transport_type}"
        )

    # --------------------------------------------------------
    # LEGACY LOCAL PYTHON SCRIPT
    # --------------------------------------------------------

    script_path = target

    if not os.path.exists(script_path):
        raise RuntimeError(
            f"'{target}' is neither a configured MCP server "
            f"nor an existing Python script"
        )

    return [
        sys.executable,
        script_path,
    ], os.environ.copy()


# ============================================================
# WEBSOCKET -> MCP PROCESS
# ============================================================

async def pipe_websocket_to_process(
    websocket,
    process,
    target,
):
    """
    Receive JSON-RPC messages from Xiaozhi WebSocket
    and forward them to MCP process stdin.
    """

    while True:

        message = await websocket.recv()

        if isinstance(message, bytes):
            message = message.decode(
                "utf-8",
                errors="replace",
            )

        logger.debug(
            f"[{target}] << {message[:500]}"
        )

        if process.poll() is not None:
            raise RuntimeError(
                f"MCP process exited with code "
                f"{process.returncode}"
            )

        if process.stdin is None:
            raise RuntimeError(
                "MCP process stdin unavailable"
            )

        if process.stdin.closed:
            raise RuntimeError(
                "MCP process stdin is closed"
            )

        process.stdin.write(message + "\n")
        process.stdin.flush()


# ============================================================
# MCP PROCESS -> WEBSOCKET
# ============================================================

async def pipe_process_to_websocket(
    process,
    websocket,
    target,
):
    """
    Read MCP JSON-RPC responses from process stdout
    and send them back to Xiaozhi.
    """

    while True:

        if process.stdout is None:
            raise RuntimeError(
                "MCP process stdout unavailable"
            )

        data = await asyncio.to_thread(
            process.stdout.readline
        )

        if not data:

            exit_code = process.poll()

            raise RuntimeError(
                f"MCP process stdout ended "
                f"(exit_code={exit_code})"
            )

        logger.debug(
            f"[{target}] >> {data[:500]}"
        )

        await websocket.send(data)


# ============================================================
# MCP STDERR -> LOG
# ============================================================

async def pipe_process_stderr_to_terminal(
    process,
    target,
):
    """
    Forward MCP child process stderr to Render logs.
    """

    if process.stderr is None:
        return

    while True:

        data = await asyncio.to_thread(
            process.stderr.readline
        )

        if not data:
            return

        # Keep original MCP server logging readable.
        sys.stderr.write(data)
        sys.stderr.flush()


# ============================================================
# WAIT FOR CHILD PROCESS EXIT
# ============================================================

async def wait_for_process_exit(
    process,
    target,
):
    """
    Detect unexpected child process termination.
    """

    exit_code = await asyncio.to_thread(
        process.wait
    )

    raise RuntimeError(
        f"[{target}] MCP process exited "
        f"with code {exit_code}"
    )


# ============================================================
# CONNECT ONE MCP SERVER TO XIAOZHI
# ============================================================

async def connect_to_server(
    uri,
    target,
):
    """
    Connect to Xiaozhi WebSocket, start MCP server process,
    and bridge traffic in both directions.

    Any unexpected termination raises an exception so that
    connect_with_retry() starts a fresh connection/process.
    """

    process = None

    bridge_tasks = []

    try:

        logger.info(
            f"[{target}] Connecting to WebSocket server..."
        )

        async with websockets.connect(
            uri,
            ping_interval=WS_PING_INTERVAL,
            ping_timeout=WS_PING_TIMEOUT,
            close_timeout=WS_CLOSE_TIMEOUT,
            open_timeout=30,
        ) as websocket:

            logger.info(
                f"[{target}] Successfully connected "
                f"to WebSocket server"
            )

            # ------------------------------------------------
            # START MCP CHILD PROCESS
            # ------------------------------------------------

            command, child_env = build_server_command(
                target
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
                f"[{target}] Started server process: "
                f"{' '.join(command)}"
            )

            # ------------------------------------------------
            # START BRIDGE TASKS
            # ------------------------------------------------

            websocket_to_process = asyncio.create_task(
                pipe_websocket_to_process(
                    websocket,
                    process,
                    target,
                )
            )

            process_to_websocket = asyncio.create_task(
                pipe_process_to_websocket(
                    process,
                    websocket,
                    target,
                )
            )

            process_stderr = asyncio.create_task(
                pipe_process_stderr_to_terminal(
                    process,
                    target,
                )
            )

            process_exit = asyncio.create_task(
                wait_for_process_exit(
                    process,
                    target,
                )
            )

            bridge_tasks = [
                websocket_to_process,
                process_to_websocket,
                process_stderr,
                process_exit,
            ]

            # ------------------------------------------------
            # WATCH CRITICAL TASKS
            # ------------------------------------------------

            critical_tasks = {
                websocket_to_process,
                process_to_websocket,
                process_exit,
            }

            done, pending = await asyncio.wait(
                critical_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # ------------------------------------------------
            # IF ANY CRITICAL TASK ENDS, RECONNECT EVERYTHING
            # ------------------------------------------------

            for task in done:

                if task.cancelled():
                    continue

                exception = task.exception()

                if exception:
                    raise exception

            raise RuntimeError(
                f"[{target}] MCP bridge ended unexpectedly"
            )

    except asyncio.CancelledError:
        raise

    except websockets.exceptions.ConnectionClosed as e:

        logger.warning(
            f"[{target}] WebSocket connection closed: {e}"
        )

        raise

    except Exception as e:

        logger.error(
            f"[{target}] Connection error: {e}"
        )

        raise

    finally:

        # ----------------------------------------------------
        # CANCEL ASYNC TASKS
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
        # TERMINATE MCP CHILD PROCESS
        # ----------------------------------------------------

        if process is not None:

            if process.poll() is None:

                logger.info(
                    f"[{target}] Terminating server process"
                )

                try:

                    process.terminate()

                    await asyncio.to_thread(
                        process.wait,
                        5,
                    )

                except subprocess.TimeoutExpired:

                    logger.warning(
                        f"[{target}] MCP process did not stop "
                        f"in time; killing it"
                    )

                    process.kill()

                    try:
                        await asyncio.to_thread(
                            process.wait,
                            5,
                        )
                    except Exception:
                        pass

                except Exception as e:

                    logger.warning(
                        f"[{target}] Error while terminating "
                        f"MCP process: {e}"
                    )

            logger.info(
                f"[{target}] Server process terminated"
            )


# ============================================================
# AUTOMATIC RECONNECT LOOP
# ============================================================

async def connect_with_retry(
    uri,
    target,
):
    """
    Keep an MCP server connected forever.

    Retry sequence:
        1s -> 2s -> 4s -> 8s -> 16s -> 30s -> 30s...

    If the previous connection stayed alive for at least
    STABLE_CONNECTION_SECONDS, reset to 1 second.
    """

    reconnect_attempt = 0
    backoff = INITIAL_BACKOFF

    while True:

        started_at = asyncio.get_running_loop().time()

        try:

            await connect_to_server(
                uri,
                target,
            )

            # This function should normally never return.
            raise RuntimeError(
                "MCP connection ended unexpectedly"
            )

        except asyncio.CancelledError:
            raise

        except Exception as e:

            lifetime = (
                asyncio.get_running_loop().time()
                - started_at
            )

            # ------------------------------------------------
            # HEALTHY CONNECTION -> RESET BACKOFF
            # ------------------------------------------------

            if lifetime >= STABLE_CONNECTION_SECONDS:

                reconnect_attempt = 0
                backoff = INITIAL_BACKOFF

                logger.info(
                    f"[{target}] Previous connection was "
                    f"stable for {lifetime:.1f}s; "
                    f"reconnect backoff reset"
                )

            reconnect_attempt += 1

            logger.warning(
                f"[{target}] Connection closed "
                f"(attempt {reconnect_attempt}, "
                f"lifetime={lifetime:.1f}s): {e}"
            )

            logger.info(
                f"[{target}] Reconnecting in {backoff}s..."
            )

            await asyncio.sleep(backoff)

            backoff = min(
                backoff * 2,
                MAX_BACKOFF,
            )


# ============================================================
# SIGNAL HANDLING
# ============================================================

def signal_handler(sig, frame):
    """
    Graceful stop signal.
    """

    logger.info(
        f"Received signal {sig}; shutting down..."
    )

    raise KeyboardInterrupt


# ============================================================
# MAIN
# ============================================================

async def main():
    """
    Start all enabled MCP servers from mcp_config.json,
    or one local script if explicitly supplied.
    """

    endpoint_url = os.environ.get(
        "MCP_ENDPOINT"
    )

    if not endpoint_url:

        logger.error(
            "Please set the MCP_ENDPOINT "
            "environment variable"
        )

        raise RuntimeError(
            "MCP_ENDPOINT is missing"
        )

    target_arg = (
        sys.argv[1]
        if len(sys.argv) >= 2
        else None
    )

    # --------------------------------------------------------
    # RUN CONFIGURED MCP SERVERS
    # --------------------------------------------------------

    if not target_arg:

        cfg = load_config()

        servers_cfg = (
            cfg.get("mcpServers", {})
            if isinstance(cfg, dict)
            else {}
        )

        all_servers = list(
            servers_cfg.keys()
        )

        enabled = [
            name
            for name, entry
            in servers_cfg.items()
            if not (entry or {}).get(
                "disabled"
            )
        ]

        skipped = [
            name
            for name in all_servers
            if name not in enabled
        ]

        if skipped:

            logger.info(
                "Skipping disabled servers: "
                + ", ".join(skipped)
            )

        if not enabled:

            raise RuntimeError(
                "No enabled MCP servers found "
                "in mcp_config.json"
            )

        logger.info(
            "Starting servers: "
            + ", ".join(enabled)
        )

        tasks = [
            asyncio.create_task(
                connect_with_retry(
                    endpoint_url,
                    server_name,
                )
            )
            for server_name in enabled
        ]

        await asyncio.gather(*tasks)

        return

    # --------------------------------------------------------
    # LEGACY SINGLE LOCAL SCRIPT MODE
    # --------------------------------------------------------

    if os.path.exists(target_arg):

        logger.info(
            f"Starting local MCP script: {target_arg}"
        )

        await connect_with_retry(
            endpoint_url,
            target_arg,
        )

        return

    raise RuntimeError(
        "Argument must be a local Python script path. "
        "To run configured MCP servers, run without arguments."
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    signal.signal(
        signal.SIGINT,
        signal_handler,
    )

    if hasattr(signal, "SIGTERM"):
        signal.signal(
            signal.SIGTERM,
            signal_handler,
        )

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "Program interrupted / stopped"
        )

    except Exception as e:

        logger.exception(
            f"Program execution error: {e}"
        )

        sys.exit(1)
