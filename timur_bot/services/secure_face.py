from __future__ import annotations

import hashlib
import io
import json
import os
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable, List, Sequence, Tuple

import numpy as np
from PIL import Image, ImageDraw

if TYPE_CHECKING:
    from telegram import Message


SECURE_COMMAND_PATTERN = r"(?i)(?:^|\s)/secure(?:@\w+)?(?:\s|$)"
_ROOT_DIR = Path(__file__).resolve().parents[2]
_SUPPORTED_IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
_FACE_SIZE = (160, 160)
_TARGET_LABEL = 1
_ENGINE_LOCK = threading.Lock()
_ENGINE: "_SecureFaceEngine | None" = None
_ENGINE_SETTINGS_KEY: tuple[Any, ...] | None = None


@dataclass(frozen=True)
class SecureFaceSettings:
    enabled: bool
    ref_dir: Path
    cache_dir: Path
    max_side: int
    match_threshold: float
    emoji_chance: float
    min_ref_samples: int
    cascade_path: Path | None
    emoji_choices: Tuple[str, ...]

    @property
    def settings_key(self) -> tuple[Any, ...]:
        return (
            self.enabled,
            str(self.ref_dir.resolve()),
            str(self.cache_dir.resolve()),
            self.max_side,
            round(self.match_threshold, 6),
            round(self.emoji_chance, 6),
            self.min_ref_samples,
            str(self.cascade_path.resolve()) if self.cascade_path else "",
            self.emoji_choices,
        )


@dataclass(frozen=True)
class SecurePhotoResult:
    image_bytes: bytes
    matched_faces: int
    used_emoji: bool


def resolve_secure_source_message(message: "Message | None") -> "Message | None":
    if not message:
        return None
    if message.photo:
        return message
    if message.reply_to_message and message.reply_to_message.photo:
        return message.reply_to_message
    return None


def process_secure_photo(image_bytes: bytes) -> SecurePhotoResult:
    settings = _load_settings()
    if not settings.enabled:
        raise RuntimeError("SECURE_FACE_REF_DIR не задан. Укажи путь к референсам в .env")
    engine = _get_engine(settings)
    return engine.process(image_bytes)


def warmup_secure_face_model() -> str:
    settings = _load_settings()
    if not settings.enabled:
        return "disabled: SECURE_FACE_REF_DIR not set"
    engine = _get_engine(settings)
    engine.warmup()
    return "ready"


def _load_settings() -> SecureFaceSettings:
    ref_dir_raw = os.getenv("SECURE_FACE_REF_DIR", "").strip()
    cache_dir_raw = os.getenv("SECURE_FACE_CACHE_DIR", "").strip()
    cascade_path_raw = os.getenv("SECURE_FACE_CASCADE_PATH", "").strip()
    emoji_choices_raw = os.getenv("SECURE_FACE_EMOJI_CHOICES", "").strip()

    enabled = bool(ref_dir_raw)
    ref_dir = Path(ref_dir_raw).expanduser() if ref_dir_raw else (_ROOT_DIR / "data" / "secure_face_refs")
    cache_dir = Path(cache_dir_raw).expanduser() if cache_dir_raw else (_ROOT_DIR / "data" / "secure_face_cache")
    cascade_path = Path(cascade_path_raw).expanduser() if cascade_path_raw else None

    emoji_choices = tuple(x.strip() for x in emoji_choices_raw.split(",") if x.strip()) or (
        "🫥",
        "🫠",
        "🤡",
        "👾",
    )

    return SecureFaceSettings(
        enabled=enabled,
        ref_dir=ref_dir,
        cache_dir=cache_dir,
        max_side=_clamp_int(os.getenv("SECURE_FACE_MAX_SIDE", "960"), 320, 2048, 960),
        match_threshold=_clamp_float(os.getenv("SECURE_FACE_MATCH_THRESHOLD", "66"), 20.0, 140.0, 66.0),
        emoji_chance=_clamp_float(os.getenv("SECURE_FACE_EMOJI_CHANCE", "0.12"), 0.0, 1.0, 0.12),
        min_ref_samples=_clamp_int(os.getenv("SECURE_FACE_MIN_REF_SAMPLES", "3"), 2, 5000, 3),
        cascade_path=cascade_path,
        emoji_choices=emoji_choices,
    )


def _clamp_int(raw: str, low: int, high: int, default: int) -> int:
    try:
        value = int(raw)
    except Exception:
        value = default
    return max(low, min(high, value))


def _clamp_float(raw: str, low: float, high: float, default: float) -> float:
    try:
        value = float(raw)
    except Exception:
        value = default
    return max(low, min(high, value))


def _get_engine(settings: SecureFaceSettings) -> "_SecureFaceEngine":
    global _ENGINE, _ENGINE_SETTINGS_KEY
    with _ENGINE_LOCK:
        if _ENGINE is None or _ENGINE_SETTINGS_KEY != settings.settings_key:
            _ENGINE = _SecureFaceEngine(settings)
            _ENGINE_SETTINGS_KEY = settings.settings_key
        return _ENGINE


class _SecureFaceEngine:
    def __init__(self, settings: SecureFaceSettings) -> None:
        self.settings = settings
        self._cv2 = _import_cv2()
        self._cascade = self._load_cascade()
        self._recognizer: Any | None = None
        self._model_lock = threading.Lock()

    def warmup(self) -> None:
        self._ensure_model()

    def process(self, image_bytes: bytes) -> SecurePhotoResult:
        recognizer = self._ensure_model()
        original = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        analysis, scale = _resize_for_analysis(original, self.settings.max_side)
        analysis_arr = np.array(analysis, dtype=np.uint8)
        gray = self._cv2.cvtColor(analysis_arr, self._cv2.COLOR_RGB2GRAY)
        faces = self._detect_faces(gray)

        matched_boxes: List[tuple[int, int, int, int]] = []
        for bbox in faces:
            face_img = _extract_face(gray, bbox)
            if face_img is None:
                continue
            label, confidence = recognizer.predict(face_img)
            if int(label) == _TARGET_LABEL and float(confidence) <= self.settings.match_threshold:
                matched_boxes.append(_scale_bbox_to_original(bbox, scale, original.width, original.height))

        if not matched_boxes:
            output = io.BytesIO()
            original.save(output, format="PNG")
            return SecurePhotoResult(image_bytes=output.getvalue(), matched_faces=0, used_emoji=False)

        should_use_emoji = random.random() < self.settings.emoji_chance
        edited = _draw_secure_overlay(
            original,
            matched_boxes,
            use_emoji=should_use_emoji,
            emoji=random.choice(self.settings.emoji_choices),
        )
        output = io.BytesIO()
        edited.save(output, format="PNG")
        return SecurePhotoResult(
            image_bytes=output.getvalue(),
            matched_faces=len(matched_boxes),
            used_emoji=should_use_emoji,
        )

    def _ensure_model(self):
        if self._recognizer is not None:
            return self._recognizer
        with self._model_lock:
            if self._recognizer is None:
                self._recognizer = self._load_or_train_model()
        return self._recognizer

    def _detect_faces(self, gray_img: np.ndarray) -> list[tuple[int, int, int, int]]:
        boxes = self._cascade.detectMultiScale(
            gray_img,
            scaleFactor=1.12,
            minNeighbors=5,
            minSize=(36, 36),
        )
        if boxes is None or len(boxes) == 0:
            return []
        return [tuple(int(v) for v in box) for box in boxes]

    def _load_cascade(self):
        if self.settings.cascade_path:
            cascade_path = self.settings.cascade_path
        else:
            cascade_path = Path(getattr(self._cv2.data, "haarcascades", "")) / "haarcascade_frontalface_default.xml"
        if not cascade_path.exists():
            raise RuntimeError(f"Не найден cascade xml: {cascade_path}")
        classifier = self._cv2.CascadeClassifier(str(cascade_path))
        if classifier.empty():
            raise RuntimeError(f"Не удалось загрузить cascade xml: {cascade_path}")
        return classifier

    def _load_or_train_model(self):
        images = _collect_reference_images(self.settings.ref_dir)
        if not images:
            raise RuntimeError(
                f"В папке референсов нет изображений: {self.settings.ref_dir} "
                f"(поддержка: {', '.join(sorted(_SUPPORTED_IMAGE_SUFFIXES))})"
            )
        fingerprint = _build_ref_fingerprint(images)
        self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
        model_path = self.settings.cache_dir / "lbph_model.xml"
        meta_path = self.settings.cache_dir / "meta.json"

        meta = _read_json(meta_path)
        if (
            model_path.exists()
            and meta.get("fingerprint") == fingerprint
            and int(meta.get("min_ref_samples", 0)) == self.settings.min_ref_samples
        ):
            recognizer = self._create_recognizer()
            recognizer.read(str(model_path))
            return recognizer

        samples: List[np.ndarray] = []
        for path in images:
            sample = self._extract_reference_sample(path)
            if sample is not None:
                samples.append(sample)

        if len(samples) < self.settings.min_ref_samples:
            raise RuntimeError(
                f"Недостаточно референсов после разметки: {len(samples)}. "
                f"Нужно хотя бы {self.settings.min_ref_samples}. "
                f"Добавь фото, где лицо крупнее и смотрит во фронт."
            )

        labels = np.full((len(samples),), _TARGET_LABEL, dtype=np.int32)
        recognizer = self._create_recognizer()
        recognizer.train(samples, labels)
        recognizer.write(str(model_path))
        _write_json(
            meta_path,
            {
                "fingerprint": fingerprint,
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "sample_count": len(samples),
                "min_ref_samples": self.settings.min_ref_samples,
            },
        )
        return recognizer

    def _extract_reference_sample(self, image_path: Path) -> np.ndarray | None:
        try:
            pil_img = Image.open(image_path).convert("RGB")
        except Exception:
            return None
        img_arr = np.array(pil_img, dtype=np.uint8)
        gray = self._cv2.cvtColor(img_arr, self._cv2.COLOR_RGB2GRAY)
        boxes = self._detect_faces(gray)
        if not boxes:
            return None
        # Для обучающих фото берём крупнейшее лицо как самое вероятное.
        box = max(boxes, key=lambda b: b[2] * b[3])
        return _extract_face(gray, box)

    def _create_recognizer(self):
        face_mod = getattr(self._cv2, "face", None)
        if face_mod is None or not hasattr(face_mod, "LBPHFaceRecognizer_create"):
            raise RuntimeError(
                "LBPH недоступен: нужен пакет opencv-contrib-python-headless. "
                "Проверь зависимости."
            )
        return face_mod.LBPHFaceRecognizer_create()


def _import_cv2():
    try:
        import cv2
    except ImportError as exc:
        raise RuntimeError(
            "Для /secure нужен OpenCV. Добавь opencv-contrib-python-headless в зависимости."
        ) from exc
    return cv2


def _collect_reference_images(ref_dir: Path) -> List[Path]:
    if not ref_dir.exists() or not ref_dir.is_dir():
        return []
    files = [p for p in sorted(ref_dir.iterdir()) if p.is_file() and p.suffix.lower() in _SUPPORTED_IMAGE_SUFFIXES]
    return files


def _build_ref_fingerprint(paths: Iterable[Path]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        stat = path.stat()
        digest.update(str(path.name).encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
    return digest.hexdigest()


def _extract_face(gray: np.ndarray, bbox: Sequence[int]) -> np.ndarray | None:
    x, y, w, h = (int(v) for v in bbox[:4])
    if w <= 0 or h <= 0:
        return None
    y0 = max(0, y)
    x0 = max(0, x)
    y1 = min(gray.shape[0], y + h)
    x1 = min(gray.shape[1], x + w)
    if y1 <= y0 or x1 <= x0:
        return None
    roi = gray[y0:y1, x0:x1]
    if roi.size == 0:
        return None
    import cv2

    normalized = cv2.equalizeHist(roi)
    face = cv2.resize(normalized, _FACE_SIZE, interpolation=cv2.INTER_AREA)
    return face


def _resize_for_analysis(image: Image.Image, max_side: int) -> tuple[Image.Image, float]:
    width, height = image.size
    longest = max(width, height)
    if longest <= max_side:
        return image, 1.0
    scale = max_side / float(longest)
    new_size = (max(1, int(round(width * scale))), max(1, int(round(height * scale))))
    resampling = getattr(Image, "Resampling", Image)
    return image.resize(new_size, resampling.LANCZOS), scale


def _scale_bbox_to_original(
    bbox: Sequence[int],
    scale: float,
    original_width: int,
    original_height: int,
) -> tuple[int, int, int, int]:
    x, y, w, h = (int(v) for v in bbox[:4])
    if scale <= 0:
        scale = 1.0
    inv_scale = 1.0 / scale
    ox = int(round(x * inv_scale))
    oy = int(round(y * inv_scale))
    ow = max(1, int(round(w * inv_scale)))
    oh = max(1, int(round(h * inv_scale)))
    ox = max(0, min(original_width - 1, ox))
    oy = max(0, min(original_height - 1, oy))
    ow = max(1, min(original_width - ox, ow))
    oh = max(1, min(original_height - oy, oh))
    return ox, oy, ow, oh


def _draw_secure_overlay(
    image: Image.Image,
    boxes: Sequence[tuple[int, int, int, int]],
    *,
    use_emoji: bool,
    emoji: str,
) -> Image.Image:
    base = image.convert("RGBA")
    draw = ImageDraw.Draw(base, "RGBA")
    for x, y, w, h in boxes:
        if use_emoji:
            _draw_weird_emoji(draw, x, y, w, h, emoji)
        else:
            _draw_red_marker(draw, x, y, w, h)
    return base.convert("RGB")


def _draw_red_marker(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int) -> None:
    stroke = max(6, int(max(w, h) * 0.08))
    left = x + int(w * 0.1)
    top = y + int(h * 0.12)
    right = x + int(w * 0.9)
    bottom = y + int(h * 0.88)

    draw.ellipse(
        (left, top, right, bottom),
        fill=(225, 22, 22, 75),
        outline=(245, 0, 0, 235),
        width=stroke,
    )

    line_count = 5
    for idx in range(line_count):
        t = idx / max(1, line_count - 1)
        y_line = int(top + (bottom - top) * t)
        wobble = int((h * 0.04) * (-1 if idx % 2 else 1))
        draw.line(
            (left - stroke, y_line + wobble, right + stroke, y_line - wobble),
            fill=(240, 0, 0, 145),
            width=max(2, stroke // 2),
        )


def _draw_weird_emoji(draw: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, emoji: str) -> None:
    pad = int(max(w, h) * 0.08)
    left = max(0, x - pad)
    top = max(0, y - pad)
    right = x + w + pad
    bottom = y + h + pad
    draw.ellipse((left, top, right, bottom), fill=(18, 18, 18, 220), outline=(255, 40, 40, 245), width=4)
    cx = (left + right) // 2
    cy = (top + bottom) // 2
    eye_r = max(2, int(min(w, h) * 0.08))
    eye_dx = int((right - left) * 0.2)
    eye_y = cy - int((bottom - top) * 0.12)
    draw.ellipse((cx - eye_dx - eye_r, eye_y - eye_r, cx - eye_dx + eye_r, eye_y + eye_r), fill=(255, 255, 255, 235))
    draw.ellipse((cx + eye_dx - eye_r, eye_y - eye_r, cx + eye_dx + eye_r, eye_y + eye_r), fill=(255, 255, 255, 235))
    draw.arc(
        (left + int((right - left) * 0.2), cy, right - int((right - left) * 0.2), bottom - int((bottom - top) * 0.2)),
        start=10,
        end=170,
        fill=(255, 255, 255, 235),
        width=max(2, int(min(w, h) * 0.05)),
    )
    # Короткая подпись-символ добавляет "странности", даже если emoji-глифы недоступны.
    text_x = cx - 6
    text_y = bottom - int((bottom - top) * 0.18)
    draw.text((text_x, text_y), emoji[:1], fill=(255, 255, 255, 215))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)
