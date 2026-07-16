import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from timur_bot.services import voice_tts


def test_gemini_tts_uses_provider_level_http_timeout(monkeypatch) -> None:
    captured = {}

    def model_type(**kwargs):
        return SimpleNamespace(**kwargs)

    def http_options(**kwargs):
        captured["http_options"] = kwargs
        return SimpleNamespace(**kwargs)

    types = SimpleNamespace(
        HttpOptions=http_options,
        GenerateContentConfig=model_type,
        SpeechConfig=model_type,
        VoiceConfig=model_type,
        PrebuiltVoiceConfig=model_type,
    )

    class Models:
        @staticmethod
        def generate_content(**kwargs):
            captured["generate_content"] = kwargs
            inline_data = SimpleNamespace(data=b"\x00\x00")
            return SimpleNamespace(
                candidates=[SimpleNamespace(content=SimpleNamespace(parts=[SimpleNamespace(inline_data=inline_data)]))]
            )

    class Client:
        def __init__(self, **kwargs) -> None:
            captured["client"] = kwargs
            self.models = Models()

    google_module = ModuleType("google")
    genai_module = ModuleType("google.genai")
    genai_module.Client = Client
    genai_module.types = types
    google_module.genai = genai_module
    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.genai", genai_module)

    with patch.object(voice_tts, "_wav_to_ogg_opus_bytes", return_value=b"ogg"):
        result = voice_tts.synthesize_ogg_opus_from_text(
            api_key="key",
            model="voice-model",
            voice_name="voice",
            text="текст",
            timeout_seconds=1.8,
        )

    assert result == b"ogg"
    assert captured["http_options"] == {"timeout": 1800}
    assert captured["client"]["http_options"].timeout == 1800
