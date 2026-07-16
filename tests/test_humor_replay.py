from types import SimpleNamespace
from unittest.mock import patch

import pytest

from timur_bot.tools import humor_replay as replay
from timur_bot.tools.humor_replay import load_replay_fixture, main


def test_replay_fixture_has_required_coverage() -> None:
    scenes = load_replay_fixture()
    by_id = {scene["id"]: scene for scene in scenes}

    assert len(scenes) >= 30
    assert by_id["reasonable_ack"]["expected_action"] == "SILENCE"
    assert by_id["grout_technical"]["expected_action"] == "SILENCE"
    assert by_id["which_model"]["expected_action"] == "ANSWER"
    assert by_id["we_are_planning"]["expected_action"] == "JOKE"
    assert by_id["we_are_planning"]["critical"] is True
    assert {"митя", "кадыр"}.issubset(set(by_id["mitya_absent"]["forbidden_names"]))
    daily_body = by_id["daily_story_signature"]["messages"][-1]["text"]
    assert not daily_body.endswith("у вас как с этим обычно")
    assert replay.ensure_daily_signature(daily_body).endswith("у вас как с этим обычно")


def test_replay_cli_dry_run_makes_no_api_calls(capsys) -> None:
    assert main(["--runs", "3", "--compare", "legacy,v2", "--dry-run"]) == 0
    assert '"scenes": 30' in capsys.readouterr().out
    assert replay.estimate_max_api_calls(load_replay_fixture(), 3) == 312


def test_blind_winner_mapping_survives_ab_swap() -> None:
    assert replay._map_blind_winner("A", swapped=False) == "legacy"
    assert replay._map_blind_winner("B", swapped=False) == "v2"
    assert replay._map_blind_winner("A", swapped=True) == "v2"
    assert replay._map_blind_winner("B", swapped=True) == "legacy"
    assert replay._map_blind_winner("TIE", swapped=True) == "tie"


def test_compare_validation_and_cost_ceiling_apply_even_to_dry_run() -> None:
    with pytest.raises(ValueError):
        replay.validate_compare(["legacy", "legacy"])
    with pytest.raises(SystemExit):
        main(["--compare", "legacy,legacy", "--dry-run"])
    with pytest.raises(SystemExit):
        main(["--runs", "3", "--max-api-calls", "10", "--dry-run"])

    typo = {
        "id": "typo",
        "route": "ambinet",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "SILENCE",
    }
    with pytest.raises(ValueError, match="invalid route"):
        replay.validate_replay_scenes([typo])
    with pytest.raises(ValueError, match="invalid route"):
        replay.estimate_max_api_calls([typo], 3)
    missing_route = {
        "id": "missing",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "SILENCE",
    }
    with pytest.raises(ValueError, match="route is required"):
        replay.validate_replay_scenes([missing_route])


def test_direct_contract_checks_meaning_not_only_length() -> None:
    scene = {
        "id": "model",
        "route": "direct",
        "messages": [{"name": "а", "text": "какая модель?"}],
        "expected_action": "ANSWER",
        "required_all_phrases": ["deepseek", "v4"],
    }
    assert replay.validate_contract(scene, {"action": "ANSWER", "text": "совершенно неверный ответ"}) == [
        "required_phrase:deepseek",
        "required_phrase:v4",
    ]

    fixture = {item["id"]: item for item in load_replay_fixture()}
    assert "semantic:weather_uncertainty" in replay.validate_contract(
        fixture["direct_weather"],
        {"action": "ANSWER", "text": "да, дождь точно идет, проверь"},
    )
    assert "semantic:model_identity" in replay.validate_contract(
        fixture["which_model"],
        {"action": "ANSWER", "text": "deepseek не использую, я на google v4"},
    )
    assert "semantic:bot_identity" in replay.validate_contract(
        fixture["direct_bot_meta"],
        {"action": "ANSWER", "text": "ты бот, а я живой человек"},
    )
    assert "semantic:model_identity" in replay.validate_contract(
        fixture["which_model"],
        {"action": "ANSWER", "text": "я не на deepseek-v4 flash"},
    )
    assert "semantic:weather_uncertainty" in replay.validate_contract(
        fixture["direct_weather"],
        {"action": "ANSWER", "text": "не знаю зачем спрашиваешь, дождь точно идет"},
    )
    assert "semantic:differential_explanation" in replay.validate_contract(
        fixture["direct_explain_term"],
        {"action": "ANSWER", "text": "колеса не могут вращаться с разной скоростью в повороте"},
    )
    assert "semantic:bot_identity" in replay.validate_contract(
        fixture["direct_bot_meta"],
        {"action": "ANSWER", "text": "я бот? нет, я человек"},
    )
    assert "semantic:choice_criteria" in replay.validate_contract(
        fixture["direct_help"],
        {"action": "ANSWER", "text": "выбирай по цвету, время тут ни при чем"},
    )
    contradictory = {
        "which_model": "deepseek-v4 flash не моя модель",
        "direct_weather": "не знаю по погоде, дождь уже начался",
        "direct_explain_term": "колеса обязаны вращаться одинаково в повороте, не с разной скоростью",
        "reply_to_timur": "потому что завтра тоже нельзя",
        "direct_help": "если важна цена, бери самый дорогой",
        "direct_bot_meta": "да, я бот, но вообще-то нет",
    }
    for scene_id, answer in contradictory.items():
        assert replay.validate_contract(
            fixture[scene_id],
            {"action": "ANSWER", "text": answer},
        ), scene_id


def test_model_identity_contract_uses_the_configured_replay_model() -> None:
    scene = next(item for item in load_replay_fixture() if item["id"] == "which_model")

    assert replay.validate_contract(
        scene,
        {"action": "ANSWER", "text": "работаю на gpt-5 mini, дурь уже моя"},
        expected_model="openai/gpt-5-mini",
    ) == []
    assert "semantic:model_identity" in replay.validate_contract(
        scene,
        {"action": "ANSWER", "text": "работаю на deepseek-v4 flash"},
        expected_model="openai/gpt-5-mini",
    )
    assert replay.validate_contract(
        scene,
        {"action": "ANSWER", "text": "я на gemini 2.5 flash"},
        expected_model="google/gemini-2.5-flash",
    ) == []
    assert replay.validate_contract(
        scene,
        {"action": "ANSWER", "text": "я на gpt 4.1 mini"},
        expected_model="openai/gpt-4.1-mini",
    ) == []


def test_direct_semantic_judge_checks_the_whole_answer() -> None:
    class FakeJudgeLLM:
        model = "deepseek/deepseek-v4-flash"

        def __init__(self) -> None:
            self.messages = None

        def complete(self, messages, **kwargs) -> str:
            del kwargs
            self.messages = messages
            return '{"passes":false,"reason_code":"contradictory_tail"}'

    llm = FakeJudgeLLM()
    scene = next(item for item in load_replay_fixture() if item["id"] == "direct_bot_meta")
    verdict = replay.judge_direct_contract(
        llm,
        scene,
        {"action": "ANSWER", "text": "да, я бот, шучу конечно"},
        expected_model=llm.model,
    )

    assert verdict == {"valid": True, "passes": False, "reason_code": "contradictory_tail"}
    prompt = llm.messages[-1]["content"]
    assert "весь смысл ответа" in prompt
    assert "не отменяет" in prompt

    llm.complete = lambda *args, **kwargs: '{"passes":true,"reason_code":"illogical_reason"}'
    assert replay.judge_direct_contract(
        llm,
        scene,
        {"action": "ANSWER", "text": "да, я бот, шучу конечно"},
        expected_model=llm.model,
    ) == {"valid": False, "passes": False, "reason_code": "inconsistent_verdict"}


def test_benchmark_rejects_direct_answer_failed_by_semantic_judge() -> None:
    scene = next(item for item in load_replay_fixture() if item["id"] == "reply_to_timur")
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0, model="deepseek/deepseek-v4-flash")
    with (
        patch.object(
            replay,
            "run_v2_direct",
            return_value={"action": "ANSWER", "text": "потому что завтра будет апокалипсис"},
        ),
        patch.object(
            replay,
            "judge_direct_contract",
            return_value={"valid": True, "passes": False, "reason_code": "illogical_reason"},
        ),
    ):
        result = replay.run_benchmark(
            [scene],
            runs=1,
            model_names=["legacy", "v2"],
            llm=llm,
            persona="p",
        )

    assert result["passed"] is False
    assert result["contract_failures"][0]["errors"][-1] == "semantic_judge:illogical_reason"


def test_llm_uses_token_fallback_when_provider_omits_usage() -> None:
    response = SimpleNamespace(
        usage=None,
        choices=[SimpleNamespace(message=SimpleNamespace(content="коротко"))],
    )
    completions = SimpleNamespace(create=lambda **kwargs: response)
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    llm = replay.ReplayLLM(client, "model")

    assert llm.complete([{"role": "user", "content": "кириллица и ❤️"}], max_tokens=10, temperature=0) == "коротко"
    assert llm.tokens > 0


def test_malformed_provider_response_becomes_a_replay_error() -> None:
    response = SimpleNamespace(usage=None, choices=[])
    completions = SimpleNamespace(create=lambda **kwargs: response)
    client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    llm = replay.ReplayLLM(client, "model")

    with pytest.raises(replay.ReplayCallError):
        llm.complete([{"role": "user", "content": "x"}], max_tokens=10, temperature=0)
    assert llm.api_errors == 1


def test_benchmark_requires_enough_decided_comparisons() -> None:
    scene = {
        "id": "one",
        "route": "ambient",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "JOKE",
    }
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0)
    winners = iter(["v2", "tie", "tie"])
    with (
        patch.object(replay, "run_legacy", return_value={"action": "SILENCE", "text": ""}),
        patch.object(replay, "run_v2_ambient", return_value={"action": "JOKE", "text": "добивка"}),
        patch.object(replay, "blind_judge", side_effect=lambda *args, **kwargs: next(winners)),
    ):
        result = replay.run_benchmark(
            [scene],
            runs=3,
            model_names=["legacy", "v2"],
            llm=llm,
            persona="persona",
        )

    assert result["v2_win_rate_without_ties"] == 1.0
    assert result["decided_rate"] == pytest.approx(1 / 3, abs=0.0001)
    assert result["passed"] is False
    assert len(result["records"]) == 3
    assert result["records"][0]["legacy"]["action"] == "SILENCE"
    assert result["records"][0]["v2"]["text"] == "добивка"
    assert result["records"][0]["judge"] == "v2"


def test_invalid_generation_json_is_error_not_silence() -> None:
    class FakeLLM:
        model = "test-model"

        @staticmethod
        def complete(*args, **kwargs) -> str:
            return "not-json"

    scene = {
        "id": "bad",
        "route": "ambient",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "SILENCE",
    }
    assert replay.run_legacy(FakeLLM(), "persona", scene)["action"] == "ERROR"
    assert replay.run_v2_ambient(FakeLLM(), "persona", scene)["action"] == "ERROR"


def test_invalid_writer_or_critic_schema_is_not_quality_silence() -> None:
    class InvalidWriter:
        model = "test-model"

        @staticmethod
        def complete(*args, **kwargs) -> str:
            return '{"should_attempt":false,"candidates":[]}'

    scene = {
        "id": "bad_schema",
        "route": "ambient",
        "messages": [{"name": "а", "text": "резонно"}],
        "expected_action": "SILENCE",
    }
    assert replay.run_v2_ambient(InvalidWriter(), "persona", scene)["error"] == "writer_invalid_schema"

    valid_writer = (
        '{"should_attempt":true,"setup":"x","target":"x","scene_type":"x","relation":"x",'
        '"forbidden_moves":[],"candidates":['
        '{"text":"вариант один точный","mechanism":"logic","callback_key":""},'
        '{"text":"вариант два точный","mechanism":"status","callback_key":""},'
        '{"text":"вариант три точный","mechanism":"image","callback_key":""},'
        '{"text":"вариант четыре точный","mechanism":"understatement","callback_key":""}]}'
    )

    class InvalidCritic:
        model = "test-model"

        def __init__(self) -> None:
            self.outputs = iter([valid_writer, '{"winner_index":"none","score":"bad","reason_codes":[]}'])

        def complete(self, *args, **kwargs) -> str:
            return next(self.outputs)

    assert replay.run_v2_ambient(InvalidCritic(), "persona", scene)["error"] == "critic_invalid_schema"


def test_v2_audit_keeps_writer_filter_and_critic_trace() -> None:
    writer = (
        '{"should_attempt":true,"setup":"разрыв","target":"срок","scene_type":"contradiction",'
        '"relation":"pile_on","forbidden_moves":[],"candidates":['
        '{"text":"успели опоздать заранее","mechanism":"logic","callback_key":""},'
        '{"text":"срок чисто декоративный","mechanism":"status","callback_key":""},'
        '{"text":"календарь вышел из чата","mechanism":"image","callback_key":""},'
        '{"text":"ну почти вовремя","mechanism":"understatement","callback_key":""}]}'
    )
    critic = '{"winner_index":1,"score":92,"reason_codes":["local","short"]}'

    class FakeLLM:
        model = "test-model"

        def __init__(self) -> None:
            self.outputs = iter([writer, critic])

        def complete(self, *args, **kwargs) -> str:
            return next(self.outputs)

    scene = {
        "id": "trace",
        "route": "ambient",
        "messages": [
            {"name": "а", "text": "встречаемся в девять"},
            {"name": "б", "text": "я к десяти"},
        ],
        "expected_action": "JOKE",
    }
    output = replay.run_v2_ambient(FakeLLM(), "persona", scene)

    assert len(output["trace"]["writer_candidates"]) == 4
    assert output["trace"]["filtered_candidates"]
    assert output["trace"]["critic"] == {
        "winner_index": 1,
        "score": 92,
        "reason_codes": ["local", "short"],
    }


def test_critical_scene_loss_blocks_aggregate_pass() -> None:
    scenes = [
        {
            "id": "critical",
            "route": "ambient",
            "critical": True,
            "messages": [{"name": "а", "text": "setup"}],
            "expected_action": "JOKE",
        },
        {
            "id": "ordinary_1",
            "route": "ambient",
            "messages": [{"name": "а", "text": "setup"}],
            "expected_action": "JOKE",
        },
        {
            "id": "ordinary_2",
            "route": "ambient",
            "messages": [{"name": "а", "text": "setup"}],
            "expected_action": "JOKE",
        },
    ]
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0)
    winners = iter(["legacy", "v2", "v2"])
    with (
        patch.object(replay, "run_legacy", return_value={"action": "JOKE", "text": "legacy"}),
        patch.object(replay, "run_v2_ambient", return_value={"action": "JOKE", "text": "v2"}),
        patch.object(replay, "blind_judge", side_effect=lambda *args, **kwargs: next(winners)),
    ):
        result = replay.run_benchmark(
            scenes,
            runs=1,
            model_names=["legacy", "v2"],
            llm=llm,
            persona="p",
        )

    assert result["v2_win_rate_without_ties"] == pytest.approx(2 / 3, abs=0.0001)
    assert result["passed"] is False
    assert {item["scene"] for item in result["critical_failures"]} == {"critical"}


def test_critical_joke_requires_at_least_one_v2_win() -> None:
    scenes = [
        {
            "id": "critical",
            "route": "ambient",
            "critical": True,
            "messages": [{"name": "а", "text": "setup"}],
            "expected_action": "JOKE",
        },
        *[
            {
                "id": f"ordinary_{index}",
                "route": "ambient",
                "messages": [{"name": "а", "text": "setup"}],
                "expected_action": "JOKE",
            }
            for index in range(3)
        ],
    ]
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0)
    winners = iter(["tie", "v2", "v2", "v2"])
    with (
        patch.object(replay, "run_legacy", return_value={"action": "JOKE", "text": "legacy"}),
        patch.object(replay, "run_v2_ambient", return_value={"action": "JOKE", "text": "v2"}),
        patch.object(replay, "blind_judge", side_effect=lambda *args, **kwargs: next(winners)),
    ):
        result = replay.run_benchmark(
            scenes,
            runs=1,
            model_names=["legacy", "v2"],
            llm=llm,
            persona="p",
        )

    assert result["v2_win_rate_without_ties"] == 1.0
    assert result["decided_rate"] == 0.75
    assert result["passed"] is False
    assert any(item["errors"] == ["no_v2_win"] for item in result["critical_failures"])


def test_invalid_judge_json_is_counted_and_auditable() -> None:
    scene = {
        "id": "one",
        "route": "ambient",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "JOKE",
    }
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0)
    with (
        patch.object(replay, "run_legacy", return_value={"action": "SILENCE", "text": ""}),
        patch.object(replay, "run_v2_ambient", return_value={"action": "JOKE", "text": "добивка"}),
        patch.object(replay, "blind_judge", return_value="error"),
    ):
        result = replay.run_benchmark([scene], runs=1, model_names=["legacy", "v2"], llm=llm, persona="p")

    assert result["parse_errors"] == 1
    assert result["per_scene"]["one"]["error"] == 1
    assert result["records"][0]["error"] == "judge_invalid_json"


def test_interruption_returns_partial_audit_record() -> None:
    scene = {
        "id": "one",
        "route": "ambient",
        "messages": [{"name": "а", "text": "setup"}],
        "expected_action": "JOKE",
    }
    llm = SimpleNamespace(api_errors=0, calls=0, tokens=0)
    with (
        patch.object(replay, "run_legacy", return_value={"action": "SILENCE", "text": ""}),
        patch.object(replay, "run_v2_ambient", side_effect=KeyboardInterrupt),
    ):
        result = replay.run_benchmark([scene], runs=3, model_names=["legacy", "v2"], llm=llm, persona="p")

    assert result["interrupted"] is True
    assert result["passed"] is False
    assert result["records"] == [
        {
            "run": 1,
            "scene": "one",
            "route": "ambient",
            "legacy": {"action": "SILENCE", "text": ""},
            "error": "interrupted",
        }
    ]
