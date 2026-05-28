from __future__ import annotations

import hashlib
import io
import json
import logging
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
logger = logging.getLogger("timur-bot.secureface")


@dataclass(frozen=True)
class SecureFaceSettings:
    enabled: bool
    ref_dir: Path
    cache_dir: Path
    max_side: int
    match_threshold: float
    emoji_chance: float
    min_ref_samples: int
    max_matches: int
    second_best_margin: float
    rescue_best_distance: float
    rescue_second_best_margin: float
    context_expand_side: float
    context_expand_top: float
    context_expand_bottom: float
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
            self.max_matches,
            round(self.second_best_margin, 6),
            round(self.rescue_best_distance, 6),
            round(self.rescue_second_best_margin, 6),
            round(self.context_expand_side, 6),
            round(self.context_expand_top, 6),
            round(self.context_expand_bottom, 6),
            str(self.cascade_path.resolve()) if self.cascade_path else "",
            self.emoji_choices,
        )


@dataclass(frozen=True)
class SecurePhotoResult:
    image_bytes: bytes
    matched_faces: int
    used_emoji: bool
    detected_faces: int
    best_distance: float | None


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
    meta = engine.warmup()
    return (
        "ready "
        f"(cache_hit={meta.get('cache_hit')} refs_total={meta.get('refs_total')} "
        f"refs_with_face={meta.get('samples')} threshold={meta.get('match_threshold')} "
        f"context={meta.get('context')})"
    )


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
        match_threshold=_clamp_float(os.getenv("SECURE_FACE_MATCH_THRESHOLD", "92"), 20.0, 180.0, 92.0),
        emoji_chance=_clamp_float(os.getenv("SECURE_FACE_EMOJI_CHANCE", "0.24"), 0.0, 1.0, 0.24),
        min_ref_samples=_clamp_int(os.getenv("SECURE_FACE_MIN_REF_SAMPLES", "3"), 2, 5000, 3),
        max_matches=_clamp_int(os.getenv("SECURE_FACE_MAX_MATCHES", "1"), 1, 5, 1),
        second_best_margin=_clamp_float(os.getenv("SECURE_FACE_SECOND_BEST_MARGIN", "5"), 0.0, 100.0, 5.0),
        rescue_best_distance=_clamp_float(os.getenv("SECURE_FACE_RESCUE_BEST_DISTANCE", "74"), 0.0, 250.0, 74.0),
        rescue_second_best_margin=_clamp_float(
            os.getenv("SECURE_FACE_RESCUE_SECOND_BEST_MARGIN", "2.5"), 0.0, 100.0, 2.5
        ),
        context_expand_side=_clamp_float(os.getenv("SECURE_FACE_CONTEXT_EXPAND_SIDE", "0.22"), 0.0, 1.5, 0.22),
        context_expand_top=_clamp_float(os.getenv("SECURE_FACE_CONTEXT_EXPAND_TOP", "0.35"), 0.0, 1.8, 0.35),
        context_expand_bottom=_clamp_float(os.getenv("SECURE_FACE_CONTEXT_EXPAND_BOTTOM", "0.12"), 0.0, 1.2, 0.12),
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
        self._model_meta: dict[str, Any] = {}

    def warmup(self) -> dict[str, Any]:
        self._ensure_model()
        return dict(self._model_meta)

    def process(self, image_bytes: bytes) -> SecurePhotoResult:
        recognizer = self._ensure_model()
        original = Image.open(io.BytesIO(image_bytes)).convert("RGB")
        analysis, scale = _resize_for_analysis(original, self.settings.max_side)
        analysis_arr = np.array(analysis, dtype=np.uint8)
        gray = self._cv2.cvtColor(analysis_arr, self._cv2.COLOR_RGB2GRAY)
        faces = self._detect_faces(gray)

        matched_candidates: List[tuple[float, tuple[int, int, int, int]]] = []
        distances: List[float] = []
        for bbox in faces:
            face_img = _extract_face_with_context(
                gray,
                bbox,
                all_boxes=faces,
                settings=self.settings,
            )
            if face_img is None:
                continue
            label, confidence = recognizer.predict(face_img)
            distance = float(confidence)
            distances.append(distance)
            if int(label) == _TARGET_LABEL and distance <= self.settings.match_threshold:
                matched_candidates.append(
                    (
                        distance,
                        _scale_bbox_to_original(bbox, scale, original.width, original.height),
                    )
                )
        best_distance = min(distances) if distances else None
        matched_boxes = self._select_final_matches(matched_candidates, len(faces))
        top_distances = ",".join(f"{d:.1f}" for d in sorted(distances)[:5]) if distances else "n/a"
        logger.info(
            "/secure inference: detected_faces=%s matched_faces=%s candidate_matches=%s best_distance=%s threshold=%.2f top_distances=%s max_side=%s",
            len(faces),
            len(matched_boxes),
            len(matched_candidates),
            f"{best_distance:.2f}" if best_distance is not None else "n/a",
            self.settings.match_threshold,
            top_distances,
            self.settings.max_side,
        )

        if not matched_boxes:
            output = io.BytesIO()
            original.save(output, format="PNG")
            return SecurePhotoResult(
                image_bytes=output.getvalue(),
                matched_faces=0,
                used_emoji=False,
                detected_faces=len(faces),
                best_distance=best_distance,
            )

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
            detected_faces=len(faces),
            best_distance=best_distance,
        )

    def _select_final_matches(
        self,
        matched_candidates: Sequence[tuple[float, tuple[int, int, int, int]]],
        detected_faces: int,
    ) -> List[tuple[int, int, int, int]]:
        if not matched_candidates:
            return []
        sorted_candidates = sorted(matched_candidates, key=lambda item: item[0])

        if self.settings.max_matches <= 1:
            best_distance, best_box = sorted_candidates[0]
            if detected_faces >= 2 and len(sorted_candidates) >= 2 and self.settings.second_best_margin > 0:
                second_distance = sorted_candidates[1][0]
                margin = second_distance - best_distance
                base_margin_ok = margin >= self.settings.second_best_margin
                rescue_ok = (
                    best_distance <= self.settings.rescue_best_distance
                    and margin >= self.settings.rescue_second_best_margin
                )
                if not base_margin_ok and not rescue_ok:
                    logger.info(
                        "/secure rejected as ambiguous: best=%.2f second=%.2f margin=%.2f required=%.2f rescue_best<=%.2f rescue_margin>=%.2f",
                        best_distance,
                        second_distance,
                        margin,
                        self.settings.second_best_margin,
                        self.settings.rescue_best_distance,
                        self.settings.rescue_second_best_margin,
                    )
                    return []
                if rescue_ok and not base_margin_ok:
                    logger.info(
                        "/secure accepted by rescue rule: best=%.2f second=%.2f margin=%.2f",
                        best_distance,
                        second_distance,
                        margin,
                    )
            return [best_box]

        return [box for _, box in sorted_candidates[: self.settings.max_matches]]

    def _ensure_model(self):
        if self._recognizer is not None:
            return self._recognizer
        with self._model_lock:
            if self._recognizer is None:
                self._recognizer = self._load_or_train_model()
        return self._recognizer

    def _detect_faces(self, gray_img: np.ndarray) -> list[tuple[int, int, int, int]]:
        passes = (
            {"scaleFactor": 1.12, "minNeighbors": 5, "minSize": (30, 30)},
            {"scaleFactor": 1.08, "minNeighbors": 4, "minSize": (24, 24)},
        )
        out: list[tuple[int, int, int, int]] = []
        for params in passes:
            boxes = self._cascade.detectMultiScale(gray_img, **params)
            if boxes is None or len(boxes) == 0:
                continue
            for box in boxes:
                candidate = tuple(int(v) for v in box)
                if not _looks_like_face_box(candidate, gray_img.shape):
                    continue
                if _is_new_box(candidate, out):
                    out.append(candidate)
        return out

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
        context_meta = self._context_meta()
        if (
            model_path.exists()
            and meta.get("fingerprint") == fingerprint
            and int(meta.get("min_ref_samples", 0)) == self.settings.min_ref_samples
            and dict(meta.get("context") or {}) == context_meta
        ):
            recognizer = self._create_recognizer()
            recognizer.read(str(model_path))
            self._model_meta = {
                "cache_hit": True,
                "refs_total": len(images),
                "samples": int(meta.get("sample_count", 0)),
                "match_threshold": self.settings.match_threshold,
                "context": context_meta,
            }
            logger.info(
                "/secure model cache hit: refs_total=%s refs_with_face=%s threshold=%.2f cache_dir=%s",
                len(images),
                int(meta.get("sample_count", 0)),
                self.settings.match_threshold,
                self.settings.cache_dir,
            )
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
                "context": context_meta,
            },
        )
        self._model_meta = {
            "cache_hit": False,
            "refs_total": len(images),
            "samples": len(samples),
            "match_threshold": self.settings.match_threshold,
            "context": context_meta,
        }
        logger.info(
            "/secure model trained: refs_total=%s refs_with_face=%s threshold=%.2f cache_dir=%s",
            len(images),
            len(samples),
            self.settings.match_threshold,
            self.settings.cache_dir,
        )
        return recognizer

    def _context_meta(self) -> dict[str, float]:
        return {
            "expand_side": round(self.settings.context_expand_side, 6),
            "expand_top": round(self.settings.context_expand_top, 6),
            "expand_bottom": round(self.settings.context_expand_bottom, 6),
        }

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
        return _extract_face_with_context(
            gray,
            box,
            all_boxes=boxes,
            settings=self.settings,
        )

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


def _extract_face_with_context(
    gray: np.ndarray,
    bbox: Sequence[int],
    *,
    all_boxes: Sequence[Sequence[int]],
    settings: SecureFaceSettings,
) -> np.ndarray | None:
    expanded = _expand_bbox_without_overlap(
        bbox,
        all_boxes=all_boxes,
        image_shape=gray.shape,
        expand_side=settings.context_expand_side,
        expand_top=settings.context_expand_top,
        expand_bottom=settings.context_expand_bottom,
    )
    return _extract_face(gray, expanded)


def _expand_bbox_without_overlap(
    bbox: Sequence[int],
    *,
    all_boxes: Sequence[Sequence[int]],
    image_shape: tuple[int, ...],
    expand_side: float,
    expand_top: float,
    expand_bottom: float,
) -> tuple[int, int, int, int]:
    x, y, w, h = (int(v) for v in bbox[:4])
    image_h, image_w = int(image_shape[0]), int(image_shape[1])
    if w <= 0 or h <= 0 or image_h <= 0 or image_w <= 0:
        return x, y, w, h

    left = x - int(round(w * expand_side))
    right = x + w + int(round(w * expand_side))
    top = y - int(round(h * expand_top))
    bottom = y + h + int(round(h * expand_bottom))

    left_limit, right_limit = 0, image_w
    top_limit, bottom_limit = 0, image_h
    for other in all_boxes:
        ox, oy, ow, oh = (int(v) for v in other[:4])
        if ow <= 0 or oh <= 0:
            continue
        if ox == x and oy == y and ow == w and oh == h:
            continue

        if _overlap_len(y, y + h, oy, oy + oh) > max(1.0, 0.25 * min(h, oh)):
            if ox + ow <= x:
                left_limit = max(left_limit, ox + ow + 1)
            elif ox >= x + w:
                right_limit = min(right_limit, ox - 1)

        if _overlap_len(x, x + w, ox, ox + ow) > max(1.0, 0.25 * min(w, ow)):
            if oy + oh <= y:
                top_limit = max(top_limit, oy + oh + 1)
            elif oy >= y + h:
                bottom_limit = min(bottom_limit, oy - 1)

    left = max(left_limit, left)
    right = min(right_limit, right)
    top = max(top_limit, top)
    bottom = min(bottom_limit, bottom)

    left = max(0, min(image_w - 1, left))
    top = max(0, min(image_h - 1, top))
    right = max(left + 1, min(image_w, right))
    bottom = max(top + 1, min(image_h, bottom))
    return left, top, right - left, bottom - top


def _overlap_len(a0: int, a1: int, b0: int, b1: int) -> float:
    return float(max(0, min(a1, b1) - max(a0, b0)))


def _looks_like_face_box(bbox: tuple[int, int, int, int], image_shape: tuple[int, int]) -> bool:
    x, y, w, h = bbox
    if w <= 0 or h <= 0:
        return False
    ratio = w / float(h)
    if ratio < 0.6 or ratio > 1.55:
        return False
    image_h, image_w = image_shape[:2]
    area_ratio = (w * h) / float(max(1, image_w * image_h))
    if area_ratio < 0.0009:
        return False
    if y + h <= int(image_h * 0.12):
        return False
    return True


def _is_new_box(candidate: tuple[int, int, int, int], existing: Sequence[tuple[int, int, int, int]]) -> bool:
    x, y, w, h = candidate
    cx = x + w / 2.0
    cy = y + h / 2.0
    for ex in existing:
        ex_x, ex_y, ex_w, ex_h = ex
        ex_cx = ex_x + ex_w / 2.0
        ex_cy = ex_y + ex_h / 2.0
        dist = ((cx - ex_cx) ** 2 + (cy - ex_cy) ** 2) ** 0.5
        if dist < max(w, h, ex_w, ex_h) * 0.25:
            return False
    return True


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
    rng = random.Random()
    left = x + int(w * 0.1)
    right = x + int(w * 0.9)
    top = y + int(h * 0.18)
    bottom = y + int(h * 0.82)
    if right <= left or bottom <= top:
        return

    sweep_count = max(4, min(7, int(h / 35) + 4))
    sweep_gap = (bottom - top) / float(max(1, sweep_count - 1))
    path: list[tuple[float, float]] = []

    current_left_to_right = True
    for i in range(sweep_count):
        y_line = top + i * sweep_gap + rng.uniform(-sweep_gap * 0.15, sweep_gap * 0.15)
        y_line = max(top, min(bottom, y_line))
        base_start = left + rng.uniform(0, (right - left) * 0.08)
        base_end = right - rng.uniform(0, (right - left) * 0.08)
        # Слегка гуляем длину концов каждого прохода.
        start_ext = rng.uniform(-(right - left) * 0.03, (right - left) * 0.09)
        end_ext = rng.uniform(-(right - left) * 0.03, (right - left) * 0.09)
        x_start = base_start - start_ext
        x_end = base_end + end_ext

        if current_left_to_right:
            path.append((x_start, y_line))
            path.append((x_end, y_line + rng.uniform(-2.0, 2.0)))
        else:
            path.append((x_end, y_line))
            path.append((x_start, y_line + rng.uniform(-2.0, 2.0)))
        current_left_to_right = not current_left_to_right

    smooth_path = _smooth_polyline(path, iterations=2)
    # Небольшой общий поворот, чтобы мазня выглядела живее.
    angle = rng.uniform(-0.22, 0.22)
    cx = (left + right) * 0.5
    cy = (top + bottom) * 0.5
    rotated_path = [_rotate_point(px, py, cx, cy, angle) for px, py in smooth_path]
    x_pad = max(2, int(w * 0.03))
    y_pad = max(2, int(h * 0.03))
    clamped_path = [
        (
            int(round(max(left - x_pad, min(right + x_pad, px)))),
            int(round(max(top - y_pad, min(bottom + y_pad, py)))),
        )
        for px, py in rotated_path
    ]
    if len(clamped_path) < 2:
        return

    color = (247, 62, 62, 225)
    width_main = max(10, int(min(w, h) * 0.28))
    width_soft = max(7, int(width_main * 0.75))
    draw.line(clamped_path, fill=color, width=width_main, joint="curve")
    draw.line(clamped_path, fill=color, width=width_soft, joint="curve")

    start_x, start_y = clamped_path[0]
    end_x, end_y = clamped_path[-1]
    radius = max(3, width_main // 2)
    draw.ellipse((start_x - radius, start_y - radius, start_x + radius, start_y + radius), fill=color)
    draw.ellipse((end_x - radius, end_y - radius, end_x + radius, end_y + radius), fill=color)


def _rotate_point(px: float, py: float, cx: float, cy: float, angle_rad: float) -> tuple[float, float]:
    cos_a = float(np.cos(angle_rad))
    sin_a = float(np.sin(angle_rad))
    dx = px - cx
    dy = py - cy
    return (
        cx + dx * cos_a - dy * sin_a,
        cy + dx * sin_a + dy * cos_a,
    )


def _smooth_polyline(points: Sequence[tuple[float, float]], iterations: int = 2) -> list[tuple[float, float]]:
    if len(points) < 2:
        return list(points)
    result = list(points)
    for _ in range(max(0, iterations)):
        if len(result) < 2:
            break
        smoothed: list[tuple[float, float]] = [result[0]]
        for idx in range(len(result) - 1):
            x0, y0 = result[idx]
            x1, y1 = result[idx + 1]
            q = (0.75 * x0 + 0.25 * x1, 0.75 * y0 + 0.25 * y1)
            r = (0.25 * x0 + 0.75 * x1, 0.25 * y0 + 0.75 * y1)
            smoothed.extend((q, r))
        smoothed.append(result[-1])
        result = smoothed
    return result


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
