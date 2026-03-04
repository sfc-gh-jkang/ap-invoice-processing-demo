# Playwright E2E Testing

## Architecture

- 146 Playwright tests across 8 files in `streamlit/tests/`
- Single Streamlit server on port 8503
- `streamlit/conftest.py` manages server lifecycle via double-fork daemon
- `streamlit/tests/conftest.py` has fixtures and helpers (no server logic)
- pytest-xdist for parallel execution (`-n 2` in pyproject.toml addopts)

## Running Tests

```bash
cd streamlit

# Default (parallel, uses addopts -n 2):
uv run pytest

# Override addopts when passing -n manually:
uv run pytest -o "addopts=-ra -q" -n 2

# Sequential (useful for debugging):
uv run pytest -o "addopts=-ra -q" -n 0

# Single test:
uv run pytest -o "addopts=-ra -q" -n 0 tests/test_functional/test_landing.py::test_landing_title -v
```

## Server Lifecycle (conftest.py)

- `pytest_configure` runs on the xdist **controller** only (not workers)
- Workers detected via `hasattr(config, "workerinput")`
- If server already healthy (`_server_is_up()`), reuse it (enables back-to-back runs)
- If no server, kill zombies on port (`_kill_port_holders()`), then start fresh
- Server launched via **double-fork** to fully detach from pytest process tree
- Without double-fork, Streamlit receives SIGABRT (exit 134) when pytest exits

## Known Constraints

- **Max 2 concurrent browsers** (`-n 2`). `-n 4` overwhelms Streamlit and causes mass failures.
- **Accumulated WebSocket state** crashes Streamlit after ~3 consecutive test suites. Fresh server per run avoids this.
- **Port TIME_WAIT**: After SIGKILL, port needs ~5-10s before rebinding. `_kill_port_holders()` handles this.
- **Double-fork child errors are invisible**: Parent fork returns exit 0 even if child crashes. Always verify server actually starts after launching.
- **`os.path.dirname(__file__)`** returns empty string for root conftest.py. Must use `os.path.dirname(os.path.abspath(__file__))`.

## Current Performance (146 tests, -n 2)

- Cold start (no server): ~6:45 (includes ~30s server startup)
- Warm server: ~6:20
- Back-to-back runs: work (server reuse)

## Speedup Opportunities (target: 1-2 min)

1. **Reduce `wait_for_timeout(1000)`** in `tests/conftest.py:wait_for_streamlit()` — accounts for ~73s per worker (146 tests / 2 workers * 1s). Try 300-500ms.
2. **Group tests by page** — navigate once, run all assertions on same page load. Eliminates redundant `page.goto()` + `networkidle` waits (~3-5s each).
3. **Smarter `wait_for_streamlit`** — use `wait_for_selector` with specific expected elements instead of generic wait + sleep.
4. **Test-level page caching** — use session/module-scoped page fixtures that persist navigation state across tests for the same page.
5. **Cannot increase `-n` beyond 2** — Streamlit's single-process architecture can't handle more concurrent WebSocket connections.
