"""Live structural status for one parent turn's delegated children (Telegram).

``_ChildProgressRelay`` (tools/delegate_tool_progress.py) already threads
``task_index``/``task_count``/``subagent_id``/``tool_count``/``depth`` into
every ``subagent.*`` event it relays to the parent's progress callback. The
gateway only ever rendered ``subagent.complete`` failures from that stream;
every other child event fell through the ``tool.started``-only progress gate,
so a messaging surface saw nothing until a delegation died.

``DelegatedChildStatus`` turns that stream into one message per parent turn:
one ``send`` when the first child appears, then ``edit_message`` on that id
only. Rules that keep it safe for children that outlive their turn:

* Structural state only — ordinals, phases, tool counts. Never a goal, preview,
  tool argument, model name or error text: a detached child keeps reporting
  after the foreground turn has moved on, so nothing it says is trusted.
* Owned by exactly one ``TurnContext`` and bound to that turn's adapter, chat
  and thread at creation, so a late event can only ever edit its own bubble.
* Direct children only (``depth == 0``); a nested orchestrator's grandchildren
  are its business.
* One publisher task per board, paced by ``_MIN_EDIT_INTERVAL``: a burst of
  tool events collapses to the latest snapshot per interval instead of a queue
  of edits, and failed edits get bounded retries against the latest state
  rather than falling back to a second message.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DONE = frozenset({"ok", "completed", "success"})
_TIMEOUT = frozenset({"timeout"})
_STOPPED = frozenset({"interrupted", "cancelled", "canceled", "stalled"})
# Anything else on subagent.complete (error, failed, unknown, novel) is a failure: the child
# ended and did not report success.
_MARKS = {
    "spawned": "🚀",
    "thinking": "💭",
    "working": "⚙️",
    "done": "✅",
    "failed": "❌",
    "timeout": "⏱",
    "stopped": "⛔",
}
_TERMINAL = frozenset({"done", "failed", "timeout", "stopped"})
_EVENTS = frozenset({"subagent.start", "subagent.thinking", "subagent.tool", "subagent.complete"})
_MAX_ROWS = 8            # rendered rows; the tail collapses into one "…and N more" line
_MAX_TOOLS = 9999
_MIN_EDIT_INTERVAL = 3.0  # seconds between edits of one bubble (Telegram group budget is ~20/min)
_MAX_MISSES = 5          # consecutive delivery misses before waiting for a later state change
_MAX_RETRY_DELAY = 300.0  # do not let a hostile/buggy retry_after park the publisher indefinitely


def _phase_for(status: Any) -> str:
    status = str(status or "").strip().lower()
    if status in _DONE:
        return "done"
    if status in _TIMEOUT:
        return "timeout"
    if status in _STOPPED:
        return "stopped"
    return "failed"


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class DelegatedChildStatus:
    """Structural, privacy-safe status board owned by exactly one parent turn."""

    def __init__(
        self, adapter: Any, chat_id: str, metadata: Optional[dict], *,
        min_edit_interval: float = _MIN_EDIT_INTERVAL, clock=time.monotonic, sleep=asyncio.sleep,
    ) -> None:
        self._adapter, self._chat_id, self._metadata = adapter, str(chat_id), metadata
        self._min_edit_interval, self._clock, self._sleep = min_edit_interval, clock, sleep
        self._lock = threading.Lock()  # observe() runs on the agent's worker thread
        self._children: dict[str, dict[str, Any]] = {}
        self._expected_by_wave: dict[str, int] = {}
        self._fallback_wave = 0
        self._fallback_indices: set[int] = set()
        self._revision = 0
        self._published_revision = 0
        self._publisher_running = False
        self._message_id: Optional[str] = None
        self._last_edit_at: Optional[float] = None
        self._last_text: Optional[str] = None

    # ── agent worker thread ──────────────────────────────────────────────────────────────

    def observe(self, event_type: str, payload: dict[str, Any]) -> bool:
        """Apply one child event. True when a publisher must be started to flush the change."""
        if event_type not in _EVENTS or _as_int(payload.get("depth"), 0) != 0:
            return False
        task_index = max(0, _as_int(payload.get("task_index"), 0))
        key = str(payload.get("subagent_id") or f"task-{task_index}")
        with self._lock:
            child = self._children.get(key)
            if child is None:
                delegation_id = payload.get("delegation_id")
                if delegation_id:
                    wave_key = f"delegation:{delegation_id}"
                else:
                    # Older/synthetic producers have no delegation_id. A repeated task_index is
                    # then the best available boundary between sequential waves.
                    if task_index in self._fallback_indices:
                        self._fallback_wave += 1
                        self._fallback_indices.clear()
                    self._fallback_indices.add(task_index)
                    wave_key = f"fallback:{self._fallback_wave}"
                self._expected_by_wave[wave_key] = max(
                    self._expected_by_wave.get(wave_key, 0), _as_int(payload.get("task_count"), 0),
                )
                child = self._children[key] = {"ordinal": len(self._children) + 1, "phase": "spawned", "tools": 0}
                changed = True
            else:
                changed = False
            if child["phase"] not in _TERMINAL:
                if event_type == "subagent.thinking":
                    changed |= child["phase"] != "thinking"
                    child["phase"] = "thinking"
                elif event_type == "subagent.tool":
                    tools = min(_MAX_TOOLS, max(child["tools"], _as_int(payload.get("tool_count"), child["tools"] + 1)))
                    changed |= (child["phase"], child["tools"]) != ("working", tools)
                    child["phase"], child["tools"] = "working", tools
                elif event_type == "subagent.complete":
                    child["phase"] = _phase_for(payload.get("status"))
                    changed = True
            if not changed:
                return False
            self._revision += 1
            if self._publisher_running:
                return False
            self._publisher_running = True
            return True

    def publisher_not_started(self) -> None:
        """The caller could not schedule ``run()``; let the next event try again."""
        with self._lock:
            self._publisher_running = False

    # ── gateway loop ─────────────────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Publisher: edit the bubble to the latest snapshot until nothing is pending."""
        try:
            while True:
                with self._lock:
                    if self._revision == self._published_revision:
                        self._publisher_running = False
                        return
                    revision, text = self._revision, self._render()
                if self._last_edit_at is not None:
                    await self._sleep(max(0.0, self._last_edit_at + self._min_edit_interval - self._clock()))
                    with self._lock:  # a newer snapshot may have landed while pacing; publish that instead
                        revision, text = self._revision, self._render()
                misses = 0
                while True:
                    result = await self._deliver(text)
                    if result is True:
                        break
                    misses += 1
                    if misses >= _MAX_MISSES:
                        break
                    # Retry against the LATEST state, never a queued second message. Repeated misses
                    # are common during Telegram flood waits, so use bounded progressive backoff.
                    retry_delay = max(
                        self._min_edit_interval * misses,
                        getattr(result, "retry_after", None) or 0.0,
                    )
                    await self._sleep(min(_MAX_RETRY_DELAY, retry_delay))
                    with self._lock:
                        revision, text = self._revision, self._render()
                with self._lock:
                    if result is True:
                        self._published_revision = max(self._published_revision, revision)
                    else:
                        # Keep the failed revision pending. A later state change can start a new
                        # bounded publisher instead of silently treating stale text as delivered.
                        self._publisher_running = False
                        return
        except Exception:
            logger.debug("delegated child status publisher failed", exc_info=True)
            with self._lock:
                self._publisher_running = False

    async def _deliver(self, text: str):
        """True on success; otherwise the failed SendResult (or None) for its ``retry_after``."""
        if text == self._last_text:
            return True  # an invisible change (e.g. inside the collapsed tail) owes no edit
        try:
            if self._message_id is None:
                result = await self._adapter.send(self._chat_id, text, metadata=self._metadata)
                if getattr(result, "success", False) and getattr(result, "message_id", None):
                    self._message_id = str(result.message_id)
            else:
                result = await self._adapter.edit_message(self._chat_id, self._message_id, text)
        except Exception:
            logger.debug("delegated child status delivery failed", exc_info=True)
            return None
        self._last_edit_at = self._clock()
        if getattr(result, "success", False):
            self._last_text = text
            return True
        return result

    def _render(self) -> str:
        children = sorted(self._children.values(), key=lambda item: item["ordinal"])
        total = max(len(children), sum(self._expected_by_wave.values()))
        finished = sum(item["phase"] in _TERMINAL for item in children)
        lines = [f"🔀 Subagents · {finished}/{total} done"]
        overflow = len(children) - _MAX_ROWS
        shown = children[:_MAX_ROWS]
        for index, item in enumerate(shown):
            connector = "└" if overflow <= 0 and index == len(shown) - 1 else "├"
            line = f"{connector} #{item['ordinal']} {_MARKS[item['phase']]} {item['phase']}"
            if item["tools"]:
                line += f" · {item['tools']} {'tool' if item['tools'] == 1 else 'tools'}"
            lines.append(line)
        if overflow > 0:
            lines.append(f"└ …and {overflow} more")
        return "\n".join(lines)
