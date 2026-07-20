import asyncio
import logging
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("OPENAI_API_KEY", "test-api-key")

from timur_bot.services import bot_logic as runtime
from timur_bot.services.runtime_trace import finish_trace, start_trace, trace_event


def test_runtime_trace_correlates_events_and_redacts_secrets(caplog) -> None:
    with caplog.at_level(logging.INFO, logger="timur-bot"):
        tokens = start_trace(runtime.logger, kind="text", chat_id=-1001, message_id=42)
        trace_event(
            runtime.logger,
            "prompt",
            "built",
            prompt_tokens=123,
            api_key="must-not-leak",
            detail="line one\nline two",
        )
        finish_trace(runtime.logger, tokens, outcome="reply_sent")

    lines = [line for line in caplog.messages if line.startswith("TRACE ")]
    trace_ids = {part for line in lines for part in line.split() if part.startswith("trace_id=")}
    assert len(lines) == 3
    assert len(trace_ids) == 1
    assert "ts_utc=" in lines[0]
    assert "elapsed_ms=" in lines[0]
    assert "prompt_tokens=123" in lines[1]
    assert "must-not-leak" not in caplog.text
    assert 'api_key="[redacted]"' in lines[1]
    assert "line one line two" in lines[1]


def test_text_handler_traces_provider_failure_fallback_and_delivery(caplog) -> None:
    message = SimpleNamespace(
        chat_id=-1002,
        message_id=77,
        text="тимур ответь",
        caption=None,
        date=None,
        from_user=SimpleNamespace(id=7, first_name="а", username="a", is_bot=False),
        sender_chat=None,
        reply_to_message=None,
    )
    update = SimpleNamespace(effective_message=message)
    context = SimpleNamespace(bot=SimpleNamespace(id=999))
    memory = runtime.default_memory()

    async def provider_failure(_context, _chat_id, task, **_kwargs):
        task.close()
        runtime.set_llm_outcome(
            status="provider_error",
            status_code=429,
            error_type="RateLimitError",
            latency_ms=2500,
        )
        return ""

    with (
        caplog.at_level(logging.INFO, logger="timur-bot"),
        patch.object(runtime, "load_memory", return_value=memory),
        patch.object(runtime, "_handle_admin_pending_text", new=AsyncMock(return_value=False)),
        patch.object(runtime, "_handle_text_feedback", new=AsyncMock(return_value=False)),
        patch.object(runtime, "update_memory_with_message"),
        patch.object(runtime, "_observe_chat_humor"),
        patch.object(runtime, "_apply_message_mood_impact", return_value=False),
        patch.object(runtime, "_sync_mood_state"),
        patch.object(runtime, "_handle_mood_probe", new=AsyncMock(return_value=False)),
        patch.object(runtime, "should_reply_decision", return_value=runtime.ReplyDecision(True, "direct test")),
        patch.object(runtime, "build_humor_plan", return_value={"mode": "direct", "context": []}),
        patch.object(runtime, "build_chat_messages", return_value=[{"role": "user", "content": "test"}]),
        patch.object(runtime, "_run_with_typing", side_effect=provider_failure),
        patch.object(runtime, "send_reply_with_style", new=AsyncMock(return_value=True)) as send_reply,
    ):
        asyncio.run(runtime.text_handler(update, context))

    send_reply.assert_awaited_once()
    assert send_reply.await_args.args[3] == runtime.TECHNICAL_FALLBACK_REPLY
    trace_lines = [line for line in caplog.messages if line.startswith("TRACE ")]
    trace_ids = {part for line in trace_lines for part in line.split() if part.startswith("trace_id=")}
    assert len(trace_ids) == 1
    assert any('stage="fallback"' in line and 'reason="provider_error"' in line for line in trace_lines)
    assert any("llm_status_code=429" in line and 'llm_error_type="RateLimitError"' in line for line in trace_lines)
    assert any('outcome="technical_fallback_sent"' in line for line in trace_lines)
