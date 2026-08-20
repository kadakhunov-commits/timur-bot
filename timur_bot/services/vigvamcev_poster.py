"""Deterministic poster composition for the VIGVAMCEV series."""

from __future__ import annotations

import io
import random
from pathlib import Path
from typing import Any


POSTER_SIZE = (1280, 960)


class PosterError(RuntimeError):
    """Raised when a generated scene cannot be turned into a poster."""


def _font_candidates(configured: str = "") -> list[Path]:
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured))
    candidates.extend(
        [
            Path("/System/Library/Fonts/Supplemental/Arial Narrow Bold.ttf"),
            Path("/System/Library/Fonts/Avenir Next Condensed.ttc"),
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed-Bold.ttf"),
            Path("/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf"),
        ]
    )
    return candidates


def _load_font(size: int, *, configured: str = "", index: int = 0):
    from PIL import ImageFont

    for candidate in _font_candidates(configured):
        if not candidate.exists():
            continue
        try:
            return ImageFont.truetype(str(candidate), size=size, index=index)
        except (OSError, TypeError):
            continue
    return ImageFont.load_default()


def _fit_font(text: str, *, max_width: int, start_size: int, configured: str = ""):
    from PIL import Image, ImageDraw

    probe = Image.new("RGB", (1, 1))
    draw = ImageDraw.Draw(probe)
    for size in range(start_size, 13, -2):
        font = _load_font(size, configured=configured)
        box = draw.textbbox((0, 0), text, font=font, stroke_width=0)
        if box[2] - box[0] <= max_width:
            return font
    return _load_font(14, configured=configured)


def _gradient_background(size: tuple[int, int]):
    from PIL import Image, ImageDraw

    width, height = size
    image = Image.new("RGB", size, (246, 188, 47))
    draw = ImageDraw.Draw(image)
    top = (250, 211, 84)
    bottom = (220, 130, 27)
    for y in range(height):
        ratio = y / max(height - 1, 1)
        color = tuple(int(top[index] * (1 - ratio) + bottom[index] * ratio) for index in range(3))
        draw.line((0, y, width, y), fill=color)
    return image


def _draw_dots(image: Any, *, seed: str) -> None:
    from PIL import ImageDraw

    rng = random.Random(seed)
    draw = ImageDraw.Draw(image)
    for _ in range(18):
        x = rng.randint(20, POSTER_SIZE[0] - 20)
        y = rng.randint(90, POSTER_SIZE[1] - 80)
        radius = rng.randint(4, 16)
        draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=(16, 16, 16))


def _paste_scene(canvas: Any, scene_bytes: bytes) -> None:
    from PIL import Image, ImageEnhance, ImageOps

    try:
        scene = Image.open(io.BytesIO(scene_bytes)).convert("RGB")
    except Exception as exc:  # pragma: no cover - Pillow error text varies
        raise PosterError("Polza вернул изображение, которое не удалось открыть") from exc

    scene = ImageOps.fit(scene, (1120, 690), method=Image.Resampling.LANCZOS, centering=(0.5, 0.48))
    scene = ImageEnhance.Contrast(scene).enhance(1.08)
    scene = ImageEnhance.Color(scene).enhance(1.12)
    # Keep a visible yellow frame, matching the source posters instead of
    # allowing a full-bleed generated background to replace the composition.
    canvas.paste(scene, (80, 155))


def _draw_header(image: Any, *, configured_font: str = "") -> None:
    from PIL import ImageDraw

    draw = ImageDraw.Draw(image)
    header = "#ВИГВАМЦЕВ: ИСТОРИИ2"
    font = _fit_font(header, max_width=1130, start_size=70, configured=configured_font)
    box = draw.textbbox((0, 0), header, font=font, stroke_width=2)
    width = box[2] - box[0]
    x = (POSTER_SIZE[0] - width) // 2
    y = 35
    draw.text((x + 5, y + 5), header, font=font, fill=(221, 155, 26), stroke_width=4, stroke_fill=(0, 0, 0))
    draw.text((x, y), header, font=font, fill=(250, 250, 250), stroke_width=2, stroke_fill=(0, 0, 0))


def _paste_identity_layer(canvas: Any, layer_bytes: bytes) -> None:
    from PIL import Image

    try:
        layer = Image.open(io.BytesIO(layer_bytes))
        if "A" not in layer.getbands():
            raise PosterError("approved identity layer должен быть PNG с прозрачностью")
        layer = layer.convert("RGBA")
        layer.thumbnail((360, 520), Image.Resampling.LANCZOS)
        x = 165
        y = 290
        canvas.paste(layer, (x, y), layer)
    except PosterError:
        raise
    except Exception as exc:  # pragma: no cover - Pillow error text varies
        raise PosterError("approved identity layer не удалось открыть") from exc


def _draw_name_plate(
    image: Any,
    clone_name: str,
    *,
    post_no: int | None,
    experiment_no: int | None,
    configured_font: str = "",
) -> None:
    from PIL import ImageDraw

    draw = ImageDraw.Draw(image)
    plate = [(735, 760), (1280, 665), (1280, 960), (650, 960)]
    draw.polygon(plate, fill=(7, 7, 7))
    name = str(clone_name or "ВИГВАМЦЕВ").upper()
    font = _fit_font(name, max_width=500, start_size=68, configured=configured_font)
    box = draw.textbbox((0, 0), name, font=font, stroke_width=1)
    text_width = box[2] - box[0]
    x = 1260 - text_width
    draw.text((x + 3, 758), name, font=font, fill=(120, 120, 120), stroke_width=2, stroke_fill=(0, 0, 0))
    draw.text((x, 750), name, font=font, fill=(250, 250, 250), stroke_width=1, stroke_fill=(0, 0, 0))

    meta_parts = []
    if post_no is not None:
        meta_parts.append(f"№{int(post_no)}")
    if experiment_no is not None:
        meta_parts.append(f"ЭКСПЕРИМЕНТ {int(experiment_no)}")
    if meta_parts:
        meta = " · ".join(meta_parts)
        meta_font = _load_font(20, configured=configured_font)
        draw.text((785, 900), meta, font=meta_font, fill=(196, 196, 196))


def compose_poster(
    scene_bytes: bytes,
    clone_name: str,
    *,
    post_no: int | None = None,
    experiment_no: int | None = None,
    configured_font: str = "",
    identity_layer_bytes: bytes | None = None,
) -> bytes:
    """Compose a stable 1280×960 branded poster from a generated scene."""

    from PIL import Image

    canvas = _gradient_background(POSTER_SIZE)
    _draw_dots(canvas, seed=f"{clone_name}:{post_no}:{experiment_no}")
    _paste_scene(canvas, scene_bytes)
    if identity_layer_bytes:
        _paste_identity_layer(canvas, identity_layer_bytes)
    _draw_header(canvas, configured_font=configured_font)
    _draw_name_plate(
        canvas,
        clone_name,
        post_no=post_no,
        experiment_no=experiment_no,
        configured_font=configured_font,
    )

    output = io.BytesIO()
    canvas.save(output, format="PNG", optimize=True)
    return output.getvalue()
