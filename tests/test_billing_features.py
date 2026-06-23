import pytest

from billing_system import BillingEngine, BillingError


def _engine(tmp_path) -> BillingEngine:
    return BillingEngine(tmp_path / "billing.json")


def test_default_chat_is_free(tmp_path) -> None:
    billing = _engine(tmp_path)
    feats = billing.effective_features(100)
    assert feats["tier"] == "free_promo"
    assert feats["memory_depth"] == "short"
    assert feats["max_daily_replies"] == 30
    assert feats["voice"] is False


def test_mock_activation_unlocks_plus(tmp_path) -> None:
    billing = _engine(tmp_path)
    result = billing.activate_mock(100, 999, tier="group_plus")
    assert result["entitlement"]["status"] == "active"
    feats = billing.effective_features(100)
    assert feats["tier"] == "group_plus"
    assert feats["memory_depth"] == "full"
    assert feats["voice"] is True
    assert feats["episodic_memory"] is True
    assert feats["max_daily_replies"] == 3000


def test_standard_tier_is_middle(tmp_path) -> None:
    billing = _engine(tmp_path)
    billing.activate_mock(100, 999, tier="group_standard")
    feats = billing.effective_features(100)
    assert feats["memory_depth"] == "standard"
    assert feats["friend_dossiers"] is True
    assert feats["episodic_memory"] is False


def test_reply_counter_increments_per_chat_per_day(tmp_path) -> None:
    billing = _engine(tmp_path)
    assert billing.bot_replies_today(100) == 0
    assert billing.register_bot_reply(100) == 1
    assert billing.register_bot_reply(100) == 2
    assert billing.bot_replies_today(100) == 2
    # other chat is independent
    assert billing.bot_replies_today(200) == 0


def test_trial_can_be_used_once(tmp_path) -> None:
    billing = _engine(tmp_path)
    billing.start_trial(100, 999)
    assert billing.effective_features(100)["tier"] == "group_plus"
    with pytest.raises(BillingError):
        billing.start_trial(100, 999)
