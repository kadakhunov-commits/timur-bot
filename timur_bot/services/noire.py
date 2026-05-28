import io
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from telegram import Message


NOIRE_COMMAND_PATTERN = r"(?i)(?:^|\s)/noire(?:@\w+)?(?:\s|$)"


def _create_vignette_mask(size: tuple[int, int], *, radius: float = 0.5, intensity: float = 1.0):
    try:
        import numpy as np
    except ImportError as exc:
        raise RuntimeError("Для /noire нужен numpy. Установи зависимости из requirements.txt") from exc

    width, height = size
    x, y = np.meshgrid(np.arange(width), np.arange(height))
    cx, cy = width // 2, height // 2

    distance = np.sqrt((x - cx) ** 2 + (y - cy) ** 2) / np.sqrt(cx**2 + cy**2)
    mask = (1 - np.clip(distance, 0, 1) ** radius) ** intensity
    return (mask * 255).astype(np.uint8)


def _apply_vignette(final_image, *, blur_radius: int = 50, vignette_intensity: float = 0.3):
    try:
        from PIL import Image, ImageFilter
    except ImportError as exc:
        raise RuntimeError("Для /noire нужен Pillow. Установи зависимости из requirements.txt") from exc

    vignette_mask = _create_vignette_mask(final_image.size, radius=0.5, intensity=vignette_intensity)
    vignette_mask = Image.fromarray(vignette_mask).filter(ImageFilter.GaussianBlur(blur_radius))
    return Image.composite(final_image, Image.new("RGB", final_image.size, "black"), vignette_mask)


def convert_to_noire_png(image_bytes: bytes) -> bytes:
    try:
        import numpy as np
        from PIL import Image, ImageEnhance, ImageFilter, ImageOps
    except ImportError as exc:
        raise RuntimeError("Для /noire нужны numpy и Pillow. Установи зависимости из requirements.txt") from exc

    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    np_image = np.array(image)

    grey_image = ImageOps.grayscale(image)
    grey_image = ImageEnhance.Contrast(grey_image).enhance(1.5)
    np_grey = np.array(grey_image)

    red_mask = (np_image[:, :, 0] > np_image[:, :, 1] * 2.8) & (np_image[:, :, 0] > np_image[:, :, 2] * 2.8)
    np_final = np.where(red_mask[..., None], np_image, np_grey[..., None])
    final_image = Image.fromarray(np_final.astype(np.uint8))

    np_final = np.array(final_image)
    grains = np.random.normal(0, 16, np_final.shape)
    np_final = np.clip(np_final + grains, 0, 255)
    final_image = Image.fromarray(np_final.astype(np.uint8))

    final_image = final_image.filter(ImageFilter.UnsharpMask(radius=2, percent=150))
    final_image = _apply_vignette(final_image, blur_radius=50, vignette_intensity=0.3)

    output = io.BytesIO()
    final_image.save(output, format="PNG")
    return output.getvalue()


def resolve_noire_source_message(message: "Message | None") -> "Message | None":
    if not message:
        return None
    if message.photo:
        return message
    if message.reply_to_message and message.reply_to_message.photo:
        return message.reply_to_message
    return None
