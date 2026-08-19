"""Replay integration for the anonymous quality corpus (prev vs v2).

The corpus feeds the offline benchmark tool so old and new pipelines are
compared on the same scenes. These tests stub every LLM call; a live run is a
separate, budget-controlled step.
"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from timur_bot.tools import humor_replay as replay


QUALITY_FIXTURE = Path(__file__).parent / "fixtures" / "humor_quality_scenes.json"


def _quality_scenes():
    return replay.load_replay_fixture(QUALITY_FIXTURE)


def test_quality_corpus_passes_replay_validation() -> None:
    scenes = _quality_scenes()
    assert len(scenes) >= 12
    assert all(scene["route"] == "ambient" for scene in scenes)


def test_quality_corpus_dry_run_with_prev_baseline_reports_cost() -> None:
    exit_code = replay.main(
        [
            "--runs", "1",
            "--compare", "prev,v2",
            "--fixture", str(QUALITY_FIXTURE),
            "--dry-run",
        ]
    )
    assert exit_code == 0
    # ambient scenes only: prev writer+critic (2) + v2 writer+critic (2) + blind judge (1)
    assert replay.estimate_max_api_calls(_quality_scenes(), 1, ["prev", "v2"]) == len(_quality_scenes()) * 5


def test_measure_scene_quality_flags_templates_and_repeats() -> None:
    scene = {
        "id": "sample",
        "route": "ambient",
        "expected_action": "JOKE",
        "anchor_phrases": ["получилось"],
        "messages": [{"name": "а", "text": "ну мы же планируем"}],
    }
    good = replay.measure_scene_quality(
        scene, {"action": "JOKE", "text": "планирование почти получилось"}
    )
    assert good["action_match"] is True
    assert good["anchored"] is True
    assert good["template_hit"] is False
    assert good["repeats_scene"] is False

    templated = replay.measure_scene_quality(
        scene, {"action": "JOKE", "text": "планирование — это когда планы"}
    )
    assert templated["template_hit"] is True

    repeated = replay.measure_scene_quality(
        {**scene, "messages": [{"name": "а", "text": "договорились встретиться завтра вечером"}]},
        {"action": "JOKE", "text": "договорились встретиться завтра вечером"},
    )
    assert repeated["repeats_scene"] is True

    silence = replay.measure_scene_quality(scene, {"action": "SILENCE", "text": ""})
    assert silence["action_match"] is False
    assert silence["gate"] is True
    assert silence["anchored"] is None


def test_prev_vs_v2_benchmark_reports_quality_per_side() -> None:
    scenes = _quality_scenes()
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0, model="test-model")
    with (
        patch.object(
            replay,
            "run_prev_ambient",
            return_value={"action": "JOKE", "text": "планирование — это когда планы"},
        ),
        patch.object(
            replay,
            "run_v2_ambient",
            return_value={"action": "SILENCE", "text": ""},
        ),
        patch.object(replay, "blind_judge", return_value="v2"),
    ):
        result = replay.run_benchmark(
            scenes,
            runs=1,
            model_names=["prev", "v2"],
            llm=llm,
            persona="p",
        )

    assert result["compare"] == "prev,v2"
    quality = result["ambient_quality"]
    assert quality["prev"]["template_hits"] > 0
    assert quality["v2"]["template_hits"] == 0
    assert quality["v2"]["ambient_outputs"] == len(scenes)
    assert quality["v2"]["gate_accuracy"] is not None
    assert all(record["quality"]["v2"]["gate"] is True for record in result["records"])
    # Blind judge picked v2 on every ambient scene.
    assert result["totals"]["v2"] == len(scenes)
    assert result["passed"] is True
