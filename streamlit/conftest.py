"""Root conftest: manages Streamlit server lifecycle for E2E tests.

With pytest-xdist, this runs on the CONTROLLER process before workers spawn.
Workers inherit a running server — no per-worker startup needed.

A fresh server is started for every test run to avoid accumulated WebSocket
connection state from crashing Streamlit.  The server is launched via
double-fork so it is fully detached from the pytest process tree.

To stop the server manually:  lsof -ti :8503 | xargs kill
"""

import os
import socket
import subprocess
import sys
import time
import urllib.request
import urllib.error

APP_PORT = 8503
APP_URL = f"http://localhost:{APP_PORT}"
STARTUP_TIMEOUT = 60
_APP_DIR = os.path.dirname(os.path.abspath(__file__))


def _server_is_up():
    try:
        resp = urllib.request.urlopen(APP_URL, timeout=5)
        return resp.status == 200
    except (urllib.error.URLError, ConnectionError, OSError):
        return False


def _port_is_free():
    """Return True if APP_PORT is available for binding."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", APP_PORT))
            return True
        except OSError:
            return False


def _kill_port_holders():
    """Kill any process on APP_PORT and wait for the port to be free."""
    try:
        out = subprocess.check_output(
            ["lsof", "-ti", f":{APP_PORT}"],
            stderr=subprocess.DEVNULL, text=True,
        )
        for pid_str in out.strip().split():
            try:
                os.kill(int(pid_str), 9)  # SIGKILL
            except (ProcessLookupError, OSError, ValueError):
                pass
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    deadline = time.time() + 10
    while time.time() < deadline:
        if _port_is_free():
            return
        time.sleep(0.5)


# Inline Python script that double-forks to fully detach the server
# from the pytest process tree.  Without this, the Streamlit process
# receives SIGABRT when pytest exits (even with start_new_session=True).
_LAUNCHER = """\
import os, subprocess, sys
if os.fork() == 0:
    os.setsid()
    subprocess.Popen(
        sys.argv[1:],
        cwd=os.environ["APP_DIR"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    os._exit(0)
"""


def _start_server():
    """Launch Streamlit via double-fork and wait for it to be ready."""
    env = os.environ.copy()
    env["APP_DIR"] = _APP_DIR
    subprocess.run(
        [sys.executable, "-c", _LAUNCHER,
         "uv", "run", "streamlit", "run", "streamlit_app.py",
         "--server.port", str(APP_PORT),
         "--server.headless", "true",
         "--browser.gatherUsageStats", "false"],
        env=env,
        check=True,
    )

    deadline = time.time() + STARTUP_TIMEOUT
    while time.time() < deadline:
        if _server_is_up():
            return
        time.sleep(1)
    raise RuntimeError(f"Streamlit did not start within {STARTUP_TIMEOUT}s")


def pytest_configure(config):
    """Start a fresh Streamlit server before workers spawn (controller only)."""
    if hasattr(config, "workerinput"):
        return

    # Reuse a healthy server if one is already running (e.g. started externally
    # or left over from a previous run that exited cleanly).
    if _server_is_up():
        return

    # Kill zombie/stale processes holding the port, then start fresh.
    _kill_port_holders()
    _start_server()
