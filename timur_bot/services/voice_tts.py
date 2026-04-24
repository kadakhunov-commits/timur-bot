from __future__ import annotations

import io
import subprocess
import wave


def _pcm_to_wav_bytes(
    pcm_data: bytes,
    channels: int = 1,
    sample_rate: int = 24000,
    sample_width: int = 2,
) -> bytes:
    out = io.BytesIO()
    with wave.open(out, "wb") as wf:
        wf.setnchannels(channels)
        wf.setsampwidth(sample_width)
        wf.setframerate(sample_rate)
        wf.writeframes(pcm_data)
    return out.getvalue()


def _wav_to_ogg_opus_bytes(wav_data: bytes) -> bytes:
    proc = subprocess.run(
        [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-c:a",
            "libopus",
            "-b:a",
            "24k",
            "-vbr",
            "on",
            "-application",
            "voip",
            "-ac",
            "1",
            "-ar",
            "48000",
            "-f",
            "ogg",
            "pipe:1",
        ],
        input=wav_data,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or b"").decode("utf-8", errors="ignore")
        raise RuntimeError(f"ffmpeg convert failed: {err.strip()}")
    return proc.stdout


def synthesize_ogg_opus_from_text(
    *,
    api_key: str,
    model: str,
    voice_name: str,
    text: str,
) -> bytes:
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is empty")

    try:
        from google import genai
        from google.genai import types
    except Exception as e:
        raise RuntimeError(f"google-genai import failed: {e}") from e

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model=model,
        contents=text,
        config=types.GenerateContentConfig(
            response_modalities=["AUDIO"],
            speech_config=types.SpeechConfig(
                voice_config=types.VoiceConfig(
                    prebuilt_voice_config=types.PrebuiltVoiceConfig(
                        voice_name=voice_name,
                    )
                )
            ),
        ),
    )

    pcm_data = response.candidates[0].content.parts[0].inline_data.data
    wav_data = _pcm_to_wav_bytes(pcm_data)
    return _wav_to_ogg_opus_bytes(wav_data)
