from timur_bot.services import feature_gate as fg

_PLUS = {
    "tier": "group_plus",
    "max_daily_replies": 3000,
    "persona_modes": ["default", "savage", "chill", "poet", "npc", "quotes"],
    "voice": True,
    "friend_dossiers": True,
    "episodic_memory": True,
    "memory_depth": "full",
}


def test_free_defaults_are_shallow() -> None:
    f = fg.FREE_FEATURES
    assert fg.memory_depth(f) == fg.MEMORY_SHORT
    assert fg.voice_allowed(f) is False
    assert fg.friend_dossiers_allowed(f) is False
    assert fg.episodic_memory_allowed(f) is False
    assert fg.daily_reply_cap(f) == 30


def test_depth_ordering() -> None:
    assert fg.depth_at_least(_PLUS, fg.MEMORY_STANDARD) is True
    assert fg.depth_at_least(fg.FREE_FEATURES, fg.MEMORY_STANDARD) is False
    assert fg.depth_at_least({"memory_depth": "standard"}, fg.MEMORY_FULL) is False


def test_mode_gating() -> None:
    # free tier cannot use savage -> falls back to default
    assert fg.gate_mode(fg.FREE_FEATURES, "savage") == "default"
    assert fg.is_mode_allowed(fg.FREE_FEATURES, "chill") is True
    # plus unlocks everything
    assert fg.gate_mode(_PLUS, "savage") == "savage"
    assert fg.is_mode_allowed(_PLUS, "poet") is True


def test_voice_and_social_flags_for_plus() -> None:
    assert fg.voice_allowed(_PLUS) is True
    assert fg.friend_dossiers_allowed(_PLUS) is True
    assert fg.episodic_memory_allowed(_PLUS) is True
    assert fg.memory_depth(_PLUS) == fg.MEMORY_FULL


def test_daily_cap_helper() -> None:
    assert fg.within_daily_reply_cap(fg.FREE_FEATURES, 29) is True
    assert fg.within_daily_reply_cap(fg.FREE_FEATURES, 30) is False
    assert fg.within_daily_reply_cap(_PLUS, 2999) is True


def test_unknown_depth_is_treated_as_short() -> None:
    assert fg.memory_depth({"memory_depth": "galaxy"}) == fg.MEMORY_SHORT
    assert fg.memory_depth(None) == fg.MEMORY_SHORT
