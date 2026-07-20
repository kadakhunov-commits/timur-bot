"""Request-scoped, secret-safe runtime tracing for Telegram reply pipelines."""

from __future__ import annotations

import contextvars
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class TraceState:
    trace_id: str
    kind: str
    chat_id: int
    message_id: int
    started_at: float


_TRACE: contextvars.ContextVar[TraceState | None] = contextvars.ContextVar("timur_runtime_trace", default=None)
_LLM_OUTCOME: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "timur_runtime_llm_outcome",
    default={},
)


def _safe_value(key: str, value: Any) -> str:
    key_low = key.lower()
    if (
        key_low in {"token", "authorization", "api_key", "secret"}
        or key_low.endswith(("_api_key", "_secret", "_authorization", "_base64"))
    ):
        return '"[redacted]"'
    if value is None:
        return "null"
    if isinstance(value, (bool, int, float)):
        return str(value).lower() if isinstance(value, bool) else str(value)
    text = " ".join(str(value).split())
    if len(text) > 300:
        text = text[:297] + "..."
    return json.dumps(text, ensure_ascii=False)


def trace_event(
    logger: logging.Logger,
    stage: str,
    event: str,
    *,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    state = _TRACE.get()
    if state is None:
        return
    elapsed_ms = int((time.perf_counter() - state.started_at) * 1000)
    fixed = {
        "trace_id": state.trace_id,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "elapsed_ms": elapsed_ms,
        "kind": state.kind,
        "chat_id": state.chat_id,
        "message_id": state.message_id,
        "stage": stage,
        "event": event,
    }
    payload = " ".join(f"{key}={_safe_value(key, value)}" for key, value in {**fixed, **fields}.items())
    logger.log(level, "TRACE %s", payload)


def start_trace(logger: logging.Logger, *, kind: str, chat_id: int, message_id: int) -> tuple[Any, Any]:
    trace_id = f"{kind}-{int(chat_id)}-{int(message_id)}-{uuid.uuid4().hex[:8]}"
    trace_token = _TRACE.set(
        TraceState(
            trace_id=trace_id,
            kind=str(kind),
            chat_id=int(chat_id),
            message_id=int(message_id),
            started_at=time.perf_counter(),
        )
    )
    llm_token = _LLM_OUTCOME.set({})
    trace_event(logger, "handler", "started")
    return trace_token, llm_token


def finish_trace(logger: logging.Logger, tokens: tuple[Any, Any], *, outcome: str) -> None:
    trace_event(logger, "handler", "finished", outcome=outcome)
    trace_token, llm_token = tokens
    _LLM_OUTCOME.reset(llm_token)
    _TRACE.reset(trace_token)


def set_llm_outcome(**fields: Any) -> None:
    # asyncio.create_task copies context variables, but mutable values retain
    # identity. Mutating the request-local dict makes the child call's outcome
    # visible to the parent handler after wait_for() completes.
    outcome = _LLM_OUTCOME.get()
    outcome.clear()
    outcome.update(fields)


def get_llm_outcome() -> dict[str, Any]:
    return dict(_LLM_OUTCOME.get())
