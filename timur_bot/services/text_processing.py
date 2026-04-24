from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple


def normalize_token(token: str) -> str:
    return token.lower().replace("ё", "е").strip()


def extract_keywords(text: str, *, rus_stopwords: Set[str], en_stopwords: Set[str], limit: int = 6) -> List[str]:
    raw_tokens = re.findall(r"[a-zA-Zа-яА-ЯёЁ0-9]{3,}", text or "")
    if not raw_tokens:
        return []

    counter: Dict[str, int] = {}
    for token in raw_tokens:
        t = normalize_token(token)
        if t.isdigit() or t in rus_stopwords or t in en_stopwords:
            continue
        counter[t] = counter.get(t, 0) + 1

    sorted_tokens = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
    return [w for w, _ in sorted_tokens[:limit]]


def detect_archetype_scores(
    text: str,
    keywords: List[str],
    *,
    archetype_lexicon: Dict[str, Set[str]],
    rus_stopwords: Set[str],
    en_stopwords: Set[str],
) -> Dict[str, int]:
    scores = {k: 0 for k in archetype_lexicon}
    payload = set(
        extract_keywords(
            text,
            limit=25,
            rus_stopwords=rus_stopwords,
            en_stopwords=en_stopwords,
        )
        + keywords
    )

    for archetype, lex in archetype_lexicon.items():
        scores[archetype] = len(payload.intersection(lex))

    return scores


def sanitize_reply_text(raw: str) -> str:
    if not raw:
        return ""

    text = raw.strip()
    text = re.sub(r"^\s*тимур\s*[:\-]\s*", "", text, flags=re.IGNORECASE)
    # Remove accidental provider promo footer before any other normalization.
    text = re.sub(
        r"\s*promo:\s*upgrade to remove limits\s*$",
        "",
        text,
        flags=re.IGNORECASE,
    )
    text = text.lower()
    text = re.sub("[\U00010000-\U0010ffff]", "", text)
    if len(text) > 400:
        text = text[:400]
    return text.strip()


def split_into_chain(text: str) -> List[str]:
    parts = re.split(r"[.\n]+", text or "")
    parts = [p.strip() for p in parts if p.strip()]
    return parts[:3]


def top_items(counter: Dict[str, Any], n: int = 3) -> List[Tuple[str, float]]:
    pairs: List[Tuple[str, float]] = []
    for k, v in counter.items():
        try:
            pairs.append((k, float(v)))
        except Exception:
            continue
    pairs.sort(key=lambda x: (-x[1], x[0]))
    return pairs[:n]
