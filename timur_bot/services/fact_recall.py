from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

from timur_bot.services.fact_memory import ensure_fact_graph, infer_fact_subject, normalize_token, tagify


def _question_tokens(text: str) -> List[str]:
    return tagify(text, limit=10)


def _score_fact(
    fact: Dict[str, Any],
    *,
    target_entity_id: str,
    desired_attribute: str,
    tokens: List[str],
) -> float:
    score = float(fact.get("weight", 0.0)) * 0.55 + float(fact.get("confidence", 0.0))
    if target_entity_id and str(fact.get("entity_id")) == target_entity_id:
        score += 3.0
    if desired_attribute and str(fact.get("attribute")) == desired_attribute:
        score += 2.2
    overlap = set(tokens).intersection(set(fact.get("tags", [])))
    score += len(overlap) * 0.85
    value = normalize_token(str(fact.get("value", "")))
    if any(token in value for token in tokens):
        score += 0.6
    return round(score, 3)


def build_fact_recall_bundle(
    chat_mem: Dict[str, Any],
    question_text: str,
    *,
    max_facts: int = 4,
    max_neighbors: int = 6,
) -> Dict[str, Any]:
    graph = ensure_fact_graph(chat_mem)
    facts = list(graph.get("facts", []))
    tokens = _question_tokens(question_text)
    target_entity_id, _, _ = infer_fact_subject(chat_mem, question_text, "")
    desired_attribute = ""
    question_low = normalize_token(question_text)
    for hint, attr in (
        ("фамил", "surname"),
        ("родил", "birth_place"),
        ("родом", "origin"),
        ("жив", "residence"),
        ("зовут", "full_name"),
        ("имя", "full_name"),
        ("лет", "age"),
        ("школ", "school"),
        ("универ", "university"),
    ):
        if hint in question_low:
            desired_attribute = attr
            break

    ranked: List[Tuple[float, Dict[str, Any]]] = []
    for fact in facts:
        ranked.append(
            (
                _score_fact(
                    fact,
                    target_entity_id=target_entity_id,
                    desired_attribute=desired_attribute,
                    tokens=tokens,
                ),
                fact,
            )
        )
    ranked.sort(key=lambda item: (-item[0], str(item[1].get("id", ""))))
    top_facts = [fact for score, fact in ranked if score > 0][:max_facts]

    edge_map = graph.get("edges", {})
    neighbor_counter: Dict[str, float] = {}
    for key, weight in edge_map.items():
        if "|" not in key:
            continue
        left, right = key.split("|", 1)
        if left == target_entity_id and right.startswith("tag:"):
            neighbor_counter[right[4:]] = float(neighbor_counter.get(right[4:], 0.0)) + float(weight)
        elif right == target_entity_id and left.startswith("tag:"):
            neighbor_counter[left[4:]] = float(neighbor_counter.get(left[4:], 0.0)) + float(weight)

    neighbors = [
        {"tag": name, "weight": round(weight, 3)}
        for name, weight in sorted(neighbor_counter.items(), key=lambda item: (-item[1], item[0]))[:max_neighbors]
    ]
    lines = [f"{fact.get('entity_title')}: {fact.get('attribute')} = {fact.get('value')}" for fact in top_facts]
    return {
        "entity_id": target_entity_id,
        "tokens": tokens,
        "facts": top_facts,
        "neighbors": neighbors,
        "prompt": "\n".join(f"- {line}" for line in lines[:max_facts]),
    }


def build_miniapp_fact_map(chat_mem: Dict[str, Any], entity_id: str, *, max_nodes: int = 12) -> Dict[str, Any]:
    graph = ensure_fact_graph(chat_mem)
    entities = graph.get("entities", {})
    facts = [fact for fact in graph.get("facts", []) if str(fact.get("entity_id")) == entity_id]
    facts = sorted(
        facts,
        key=lambda item: (-float(item.get("weight", 0.0)), -float(item.get("confidence", 0.0)), str(item.get("id", ""))),
    )[:6]

    nodes: List[Dict[str, Any]] = []
    center = entities.get(entity_id, {"id": entity_id, "title": entity_id, "kind": "entity"})
    nodes.append({"id": entity_id, "label": str(center.get("title") or entity_id), "kind": "center", "weight": 3.0})
    edges: List[Dict[str, Any]] = []

    for fact in facts:
        fact_id = str(fact.get("id"))
        nodes.append(
            {
                "id": fact_id,
                "label": f"{fact.get('attribute')}: {fact.get('value')}",
                "kind": "fact",
                "weight": round(float(fact.get("weight", 1.0)) + float(fact.get("confidence", 0.0)), 3),
            }
        )
        edges.append({"from": entity_id, "to": fact_id, "label": "fact", "weight": 1.0})

    seen_tags = set()
    for fact in facts:
        for tag in fact.get("tags", [])[:4]:
            if tag in seen_tags:
                continue
            seen_tags.add(tag)
            tag_id = f"tag:{tag}"
            nodes.append({"id": tag_id, "label": tag, "kind": "tag", "weight": 1.0})
            edges.append({"from": entity_id, "to": tag_id, "label": "tag", "weight": 1.0})
            if len(nodes) >= max_nodes:
                break
        if len(nodes) >= max_nodes:
            break

    return {
        "nodes": nodes[:max_nodes],
        "edges": edges[: max_nodes * 2],
        "facts": [
            {
                "text": f"{fact.get('attribute')}: {fact.get('value')}",
                "subject": str(center.get("title") or entity_id),
                "tags": list(fact.get("tags", []))[:4],
                "confidence": float(fact.get("confidence", 0.0)),
                "source": str(fact.get("source", "fact-graph")),
            }
            for fact in facts
        ],
        "source": "fact-graph",
    }
