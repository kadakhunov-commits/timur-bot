from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple
from zoneinfo import ZoneInfo


def _norm_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _parse_ts(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _backfill_start_utc(backfill_start_date_msk: str) -> datetime | None:
    raw = str(backfill_start_date_msk or "").strip()
    if not raw:
        return None
    try:
        start_local = datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Europe/Moscow"))
    except Exception:
        return None
    return start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def _clamp_score(score: float) -> float:
    return max(0.0, min(100.0, score))


def _contains_marker(text_low: str, markers: Sequence[str]) -> bool:
    return any(marker and marker in text_low for marker in markers)


def _build_pure_laugh_re(lexicon: Dict[str, Any]) -> re.Pattern[str]:
    source = str(
        lexicon.get(
            "pure_laugh_pattern",
            r"^(?:[!?.\s,;:()\-]*)(?:л+о+л+|а?ха(?:ха)+|пхаха+|ору+|кек+)(?:[!?.\s,;:()\-]*)$",
        )
    )
    try:
        return re.compile(source, re.IGNORECASE)
    except re.error:
        return re.compile(
            r"^(?:[!?.\s,;:()\-]*)(?:л+о+л+|а?ха(?:ха)+|пхаха+|ору+|кек+)(?:[!?.\s,;:()\-]*)$",
            re.IGNORECASE,
        )


def _is_laugh_response(text: str, laugh_markers: Sequence[str], pure_laugh_re: re.Pattern[str]) -> bool:
    normalized = _norm_text(text).lower()
    if not normalized:
        return False
    return bool(pure_laugh_re.match(normalized)) or _contains_marker(normalized, laugh_markers)


def extract_period_messages(
    messages: Sequence[Dict[str, Any]],
    *,
    period_hours: int,
    backfill_start_date_msk: str = "",
    now: datetime | None = None,
) -> List[Dict[str, Any]]:
    if not messages:
        return []
    end_ts = now or datetime.utcnow()
    cutoff = end_ts - timedelta(hours=max(1, int(period_hours)))
    backfill_cutoff = _backfill_start_utc(backfill_start_date_msk)
    if backfill_cutoff:
        cutoff = max(cutoff, backfill_cutoff)
    filtered = [msg for msg in messages if (_parse_ts(str(msg.get("ts", ""))) or datetime.min) >= cutoff]
    return filtered


def _count_laugh_marker_hits(text: str, markers: Sequence[str]) -> int:
    normalized = _norm_text(text).lower()
    if not normalized:
        return 0
    total = 0
    for marker in markers:
        token = str(marker or "").strip().lower()
        if not token:
            continue
        total += normalized.count(token)
    return total


def _reaction_stats(
    reaction_index: Dict[str, Any],
    *,
    chat_id: int,
    message_id: int,
) -> Dict[str, int]:
    key = f"{int(chat_id)}:{int(message_id)}"
    stats = reaction_index.get(key) if isinstance(reaction_index, dict) else {}
    if not isinstance(stats, dict):
        stats = {}
    return {
        "total": max(0, int(stats.get("total", 0))),
        "heart": max(0, int(stats.get("heart", 0))),
        "laugh": max(0, int(stats.get("laugh", 0))),
    }


def _time_gap_seconds(prev_msg: Dict[str, Any], next_msg: Dict[str, Any]) -> float | None:
    prev_ts = _parse_ts(str(prev_msg.get("ts", "")))
    next_ts = _parse_ts(str(next_msg.get("ts", "")))
    if not prev_ts or not next_ts:
        return None
    return max(0.0, (next_ts - prev_ts).total_seconds())


def _should_link_messages(
    left_msg: Dict[str, Any],
    right_msg: Dict[str, Any],
    *,
    source_chat_id: int,
    reaction_index: Dict[str, Any],
    laugh_markers: Sequence[str],
    pure_laugh_re: re.Pattern[str],
) -> bool:
    score = 0.0
    gap = _time_gap_seconds(left_msg, right_msg)
    if gap is not None:
        if gap <= 120:
            score += 1.0
        elif gap <= 240:
            score += 0.5
        elif gap > 720:
            return False

    if int(left_msg.get("user_id", 0)) != int(right_msg.get("user_id", 0)):
        score += 0.5

    right_text = _norm_text(str(right_msg.get("text", "")))
    if _is_laugh_response(right_text, laugh_markers, pure_laugh_re):
        score += 1.2

    stats = _reaction_stats(
        reaction_index,
        chat_id=source_chat_id,
        message_id=int(right_msg.get("message_id", 0)),
    )
    if stats["total"] > 0:
        score += 0.6
    return score >= 1.0


def _build_cluster(
    messages: Sequence[Dict[str, Any]],
    *,
    anchor_idx: int,
    source_chat_id: int,
    reaction_index: Dict[str, Any],
    laugh_markers: Sequence[str],
    pure_laugh_re: re.Pattern[str],
) -> Tuple[int, int]:
    start = anchor_idx
    end = anchor_idx
    max_left = 8
    max_right = 10
    max_cluster = 16

    while start > 0 and (anchor_idx - start) < max_left:
        if (end - (start - 1) + 1) > max_cluster:
            break
        if not _should_link_messages(
            messages[start - 1],
            messages[start],
            source_chat_id=source_chat_id,
            reaction_index=reaction_index,
            laugh_markers=laugh_markers,
            pure_laugh_re=pure_laugh_re,
        ):
            break
        start -= 1

    while end + 1 < len(messages) and (end - anchor_idx) < max_right:
        if ((end + 1) - start + 1) > max_cluster:
            break
        if not _should_link_messages(
            messages[end],
            messages[end + 1],
            source_chat_id=source_chat_id,
            reaction_index=reaction_index,
            laugh_markers=laugh_markers,
            pure_laugh_re=pure_laugh_re,
        ):
            break
        end += 1

    return start, end


def _score_anchor(
    messages: Sequence[Dict[str, Any]],
    *,
    idx: int,
    source_chat_id: int,
    reaction_index: Dict[str, Any],
    lexicon: Dict[str, Any],
    pure_laugh_re: re.Pattern[str],
) -> Tuple[float, List[str], List[str], List[Dict[str, Any]]]:
    msg = messages[idx]
    text = _norm_text(str(msg.get("text", "")))
    if not text:
        return 0.0, [], ["empty_text"], []

    low = text.lower()
    laugh_markers = sorted(
        set([str(x).lower() for x in (lexicon.get("laugh_markers") or [])] + [str(x).lower() for x in (lexicon.get("extra_laugh_markers") or [])])
    )
    habitual_markers = [str(x).lower() for x in (lexicon.get("habitual_laugh_markers") or [])]
    sarcasm_markers = [str(x).lower() for x in (lexicon.get("sarcasm_markers") or [])]
    toxicity_markers = [str(x).lower() for x in (lexicon.get("toxicity_markers") or [])]
    weights = lexicon.get("reaction_weights") if isinstance(lexicon.get("reaction_weights"), dict) else {}
    weight_total = float(weights.get("total", 0.35))
    weight_heart = float(weights.get("heart", 1.4))
    weight_laugh = float(weights.get("laugh", 1.2))

    positives: List[str] = []
    negatives: List[str] = []
    score = 18.0

    if bool(pure_laugh_re.match(low)):
        return 0.0, [], ["pure_laugh_response"], []

    if 3 <= len(text) <= 120:
        score += 8.0
        positives.append("punchy_shape")

    if _contains_marker(low, laugh_markers):
        score += 4.0
        positives.append("contains_laugh_marker")

    reactions = _reaction_stats(reaction_index, chat_id=source_chat_id, message_id=int(msg.get("message_id", 0)))
    if reactions["total"] > 0:
        score += min(20.0, reactions["total"] * weight_total)
        positives.append("reaction_density")
    if reactions["heart"] > 0:
        score += min(16.0, reactions["heart"] * weight_heart)
        positives.append("heart_reactions")
    if reactions["laugh"] > 0:
        score += min(14.0, reactions["laugh"] * weight_laugh)
        positives.append("laugh_reactions")

    after_window = messages[idx + 1 : idx + 7]
    laugh_after = [m for m in after_window if _is_laugh_response(str(m.get("text", "")), laugh_markers, pure_laugh_re)]
    if laugh_after:
        score += 16.0
        positives.append("laugh_after")
        distinct_authors = {int(m.get("user_id", 0)) for m in laugh_after if int(m.get("user_id", 0)) != int(msg.get("user_id", 0))}
        if distinct_authors:
            score += min(12.0, len(distinct_authors) * 4.0)
            positives.append("multi_author_laugh")
        if after_window and laugh_after[0] is after_window[0]:
            score += 4.0
            positives.append("immediate_laugh_after")
        if len(laugh_after) >= 2:
            score += 3.0
            positives.append("laugh_tail")
    else:
        if _contains_marker(low, habitual_markers) and len(text) <= 32:
            score -= 12.0
            negatives.append("habitual_laugh")

    if idx > 0 and int(messages[idx - 1].get("user_id", 0)) != int(msg.get("user_id", 0)):
        score += 2.0
        positives.append("turn_taking")

    if _contains_marker(low, toxicity_markers) and not laugh_after:
        score -= 9.0
        negatives.append("toxic_without_laugh")

    if _contains_marker(low, sarcasm_markers) and not laugh_after:
        score -= 7.0
        negatives.append("sarcasm_without_laugh")

    return _clamp_score(score), sorted(set(positives)), sorted(set(negatives)), laugh_after


def build_stage1_candidates(
    messages: Sequence[Dict[str, Any]],
    *,
    source_chat_id: int,
    source_chat_title: str,
    reaction_index: Dict[str, Any],
    settings: Dict[str, Any],
    lexicon: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not messages:
        return []

    stage1_min = max(0, min(100, int(settings.get("stage1_min_score", 42))))
    max_candidates = max(1, int(settings.get("max_candidates_per_scan", 30)))
    pure_laugh_re = _build_pure_laugh_re(lexicon)
    laugh_markers = sorted(
        set([str(x).lower() for x in (lexicon.get("laugh_markers") or [])] + [str(x).lower() for x in (lexicon.get("extra_laugh_markers") or [])])
    )
    rule_min_hearts = max(0, int(settings.get("rule_min_hearts", 3)))
    rule_min_laugh_markers = max(0, int(settings.get("rule_min_laugh_markers", 2)))
    has_reaction_data_for_chat = any(
        str(key).startswith(f"{int(source_chat_id)}:")
        and isinstance(value, dict)
        and int(value.get("total", 0)) > 0
        for key, value in (reaction_index.items() if isinstance(reaction_index, dict) else [])
    )

    drafts: List[Dict[str, Any]] = []
    used_anchors: set[int] = set()
    for idx, msg in enumerate(messages):
        message_id = int(msg.get("message_id", 0))
        if message_id <= 0 or message_id in used_anchors:
            continue
        score, positives, negatives, laugh_after = _score_anchor(
            messages,
            idx=idx,
            source_chat_id=source_chat_id,
            reaction_index=reaction_index,
            lexicon=lexicon,
            pure_laugh_re=pure_laugh_re,
        )
        if score < stage1_min:
            continue
        anchor_reactions = _reaction_stats(reaction_index, chat_id=source_chat_id, message_id=message_id)
        if has_reaction_data_for_chat and anchor_reactions["heart"] < rule_min_hearts:
            continue
        laugh_hits = _count_laugh_marker_hits(str(msg.get("text", "")), laugh_markers)
        for tail_msg in messages[idx + 1 : idx + 7]:
            laugh_hits += _count_laugh_marker_hits(str(tail_msg.get("text", "")), laugh_markers)
        if laugh_hits < rule_min_laugh_markers:
            continue

        start, end = _build_cluster(
            messages,
            anchor_idx=idx,
            source_chat_id=source_chat_id,
            reaction_index=reaction_index,
            laugh_markers=laugh_markers,
            pure_laugh_re=pure_laugh_re,
        )
        cluster = list(messages[start : end + 1])
        if not cluster:
            continue
        cluster_ids = [int(item.get("message_id", 0)) for item in cluster if int(item.get("message_id", 0)) > 0]
        if not cluster_ids:
            continue

        cluster_payload: List[Dict[str, Any]] = []
        for item in cluster:
            mid = int(item.get("message_id", 0))
            reactions = _reaction_stats(reaction_index, chat_id=source_chat_id, message_id=mid)
            cluster_payload.append(
                {
                    "message_id": mid,
                    "user_id": int(item.get("user_id", 0)),
                    "author": str(item.get("name") or item.get("username") or item.get("user_id") or "unknown"),
                    "text": _norm_text(str(item.get("text", ""))),
                    "ts": str(item.get("ts", "")),
                    "reaction_total": reactions["total"],
                    "reaction_heart": reactions["heart"],
                    "reaction_laugh": reactions["laugh"],
                }
            )

        positives_local = list(positives)
        if len(cluster_payload) > 2:
            positives_local.append("dynamic_cluster")
        if laugh_after:
            positives_local.append("laugh_tail")
        if has_reaction_data_for_chat:
            positives_local.append("rule_hearts")
        else:
            positives_local.append("rule_hearts_skipped_no_reactions")
        positives_local.append("rule_laugh_hits")

        drafts.append(
            {
                "source_chat_id": int(source_chat_id),
                "source_chat_title": source_chat_title,
                "message_ids": cluster_ids,
                "anchor_message_id": int(msg.get("message_id", 0)),
                "time_start": str(cluster_payload[0].get("ts") or msg.get("ts") or ""),
                "time_end": str(cluster_payload[-1].get("ts") or msg.get("ts") or ""),
                "pre_score": round(float(score), 2),
                "signals_pos": sorted(set(positives_local)),
                "signals_neg": sorted(set(negatives)),
                "cluster_messages": cluster_payload,
                "meta": {
                    "cluster_start_idx": start,
                    "cluster_end_idx": end,
                    "source_window_hours": int(settings.get("scan_period_hours", 24)),
                },
            }
        )
        used_anchors.add(message_id)

    drafts.sort(key=lambda x: (-float(x.get("pre_score", 0.0)), int(x.get("anchor_message_id", 0))))
    return drafts[:max_candidates]


def build_learning_profile(
    messages: Sequence[Dict[str, Any]],
    *,
    source_chat_id: int,
    source_chat_title: str,
    lexicon: Dict[str, Any],
    max_examples: int = 8,
) -> Dict[str, Any]:
    if not messages:
        return {
            "examples": [],
            "updated_at": datetime.utcnow().isoformat(),
            "source_stats": {
                "source_chat_id": int(source_chat_id),
                "source_chat_title": source_chat_title,
                "messages_total": 0,
                "examples_total": 0,
            },
        }

    laugh_markers = sorted(
        set([str(x).lower() for x in (lexicon.get("laugh_markers") or [])] + [str(x).lower() for x in (lexicon.get("extra_laugh_markers") or [])])
    )
    pure_laugh_re = _build_pure_laugh_re(lexicon)
    toxicity_markers = [str(x).lower() for x in (lexicon.get("toxicity_markers") or [])]
    examples: List[Dict[str, Any]] = []

    for idx, msg in enumerate(messages):
        text = _norm_text(str(msg.get("text", "")))
        if not text or len(text) < 3 or len(text) > 300:
            continue
        if pure_laugh_re.match(text.lower()):
            continue

        score = 0.0
        signals: List[str] = []
        if len(text) <= 120:
            score += 1.2
            signals.append("compact")
        hits = _count_laugh_marker_hits(text, laugh_markers)
        if hits > 0:
            score += 2.2 + min(2.0, hits * 0.4)
            signals.append("laugh_marker")
        if any(marker and marker in text.lower() for marker in toxicity_markers):
            score += 0.6
            signals.append("rough_style")
        after = messages[idx + 1 : idx + 4]
        if any(_is_laugh_response(str(item.get("text", "")), laugh_markers, pure_laugh_re) for item in after):
            score += 2.4
            signals.append("laugh_after")
        if score < 2.8:
            continue

        context_rows: List[str] = []
        for row in messages[max(0, idx - 2) : idx]:
            author = str(row.get("name") or row.get("username") or row.get("user_id") or "unknown")
            context_rows.append(f"{author}: {_norm_text(str(row.get('text', '')))}")
        examples.append(
            {
                "anchor_message_id": int(msg.get("message_id", 0)),
                "score": round(score, 2),
                "context": context_rows,
                "punchline": text,
                "signals": sorted(set(signals)),
            }
        )

    examples.sort(key=lambda x: (-float(x.get("score", 0.0)), int(x.get("anchor_message_id", 0))))
    trimmed = examples[: max(1, int(max_examples))]
    return {
        "examples": trimmed,
        "updated_at": datetime.utcnow().isoformat(),
        "source_stats": {
            "source_chat_id": int(source_chat_id),
            "source_chat_title": source_chat_title,
            "messages_total": len(messages),
            "examples_total": len(trimmed),
        },
    }
