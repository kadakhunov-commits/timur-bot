"""Small async client for Polza Media API image generation."""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import mimetypes
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class PolzaImageError(RuntimeError):
    """Raised for a failed or malformed Polza image operation."""


@dataclass(frozen=True)
class GeneratedImage:
    content: bytes
    generation_id: str = ""


class PolzaImageClient:
    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = "https://polza.ai/api/v1",
        model: str = "openai/gpt-5.4-image-2",
        aspect_ratio: str = "4:3",
        image_resolution: str = "2K",
        poll_interval_seconds: float = 5.0,
        timeout_seconds: float = 300.0,
        request_json: Callable[[str, str, Dict[str, Any] | None], Dict[str, Any]] | None = None,
        download_url: Callable[[str], bytes] | None = None,
        sleep: Callable[[float], Any] | None = None,
    ) -> None:
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.aspect_ratio = aspect_ratio
        self.image_resolution = image_resolution
        self.poll_interval_seconds = max(0.1, float(poll_interval_seconds))
        self.timeout_seconds = max(1.0, float(timeout_seconds))
        self._request_json_impl = request_json or self._request_json
        self._download_url_impl = download_url or self._download_url
        self._sleep = sleep or asyncio.sleep
        self._reference_cache: Dict[tuple[str, int, int], str] = {}

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def _request_json(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not self.api_key:
            raise PolzaImageError("POLZA_AI_API_KEY не настроен")
        url = path if path.startswith("http") else f"{self.base_url}/{path.lstrip('/')}"
        body = None
        headers = {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(url, data=body, headers=headers, method=method.upper())
        try:
            with urlopen(request, timeout=min(self.timeout_seconds, 60.0)) as response:
                raw = response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            raise PolzaImageError(f"Polza HTTP {method} {path} failed: {exc}") from exc
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PolzaImageError("Polza вернул некорректный JSON") from exc
        if not isinstance(parsed, dict):
            raise PolzaImageError("Polza вернул JSON не-объект")
        return parsed

    def _download_url(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": "timur-bot/vigvamcev"}, method="GET")
        try:
            with urlopen(request, timeout=min(self.timeout_seconds, 60.0)) as response:
                return bytes(response.read())
        except (HTTPError, URLError, TimeoutError) as exc:
            raise PolzaImageError(f"не удалось скачать результат Polza: {exc}") from exc

    async def _request(self, method: str, path: str, payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return await asyncio.to_thread(self._request_json_impl, method, path, payload)

    async def _download(self, url: str) -> bytes:
        return await asyncio.to_thread(self._download_url_impl, url)

    async def upload_reference(self, path: Path) -> str:
        path = Path(path)
        if not path.exists() or not path.is_file():
            raise PolzaImageError(f"не найден visual reference: {path}")
        stat = path.stat()
        cache_key = (str(path.resolve()), int(stat.st_mtime_ns), int(stat.st_size))
        cached = self._reference_cache.get(cache_key)
        if cached:
            return cached
        content = await asyncio.to_thread(path.read_bytes)
        mime_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        payload = {
            "base64": base64.b64encode(content).decode("ascii"),
            "mimeType": mime_type,
            "storagePolicy": "TEMP_UPLOAD",
        }
        response = await self._request("POST", "/storage/upload", payload)
        url = response.get("url")
        if not isinstance(url, str) or not url.startswith("http"):
            raise PolzaImageError("Polza Storage API не вернул URL референса")
        self._reference_cache[cache_key] = url
        return url

    @staticmethod
    def _extract_result_url(data: Any) -> str:
        if isinstance(data, dict):
            for key in ("url", "image_url", "download_url"):
                value = data.get(key)
                if isinstance(value, str) and value.startswith("http"):
                    return value
            for key in ("b64_json", "base64", "data"):
                value = data.get(key)
                if isinstance(value, str) and value.startswith("data:image/"):
                    return value
                if key in {"b64_json", "base64"} and isinstance(value, str) and len(value) > 100:
                    return "data:application/octet-stream;base64," + value
            for value in data.values():
                found = PolzaImageClient._extract_result_url(value)
                if found:
                    return found
        elif isinstance(data, list):
            for value in data:
                found = PolzaImageClient._extract_result_url(value)
                if found:
                    return found
        elif isinstance(data, str) and data.startswith("http"):
            return data
        return ""

    async def _resolve_result(self, response: Dict[str, Any]) -> bytes | None:
        result = self._extract_result_url(response.get("data", response))
        if not result:
            return None
        if result.startswith("data:"):
            _, encoded = result.split(",", 1)
            try:
                return base64.b64decode(encoded)
            except (ValueError, binascii.Error) as exc:
                raise PolzaImageError("Polza вернул некорректный base64 результат") from exc
        return await self._download(result)

    async def _poll(self, generation_id: str) -> bytes:
        started = time.monotonic()
        while time.monotonic() - started < self.timeout_seconds:
            response = await self._request("GET", f"/media/{generation_id}")
            status = str(response.get("status", "")).lower()
            if status == "completed":
                result = await self._resolve_result(response)
                if result:
                    return result
                raise PolzaImageError("Polza завершил генерацию без изображения")
            if status in {"failed", "cancelled"}:
                raise PolzaImageError(f"Polza image generation {status}: {response.get('error', '')}")
            await self._sleep(self.poll_interval_seconds)
        raise PolzaImageError("истёк таймаут ожидания Polza image generation")

    async def generate_scene(self, *, prompt: str, reference_paths: Iterable[Path] = ()) -> GeneratedImage:
        references = []
        for path in reference_paths:
            references.append(await self.upload_reference(Path(path)))
        payload = {
            "model": self.model,
            "input": {
                "prompt": prompt[:5000],
                "aspect_ratio": self.aspect_ratio,
                "image_resolution": self.image_resolution,
                "n": 1,
                "images": references,
            },
            "async": True,
        }
        response = await self._request("POST", "/media", payload)
        generation_id = str(response.get("id", "") or "")
        direct = await self._resolve_result(response)
        if direct:
            return GeneratedImage(content=direct, generation_id=generation_id)
        if not generation_id:
            raise PolzaImageError("Polza не вернул ни изображение, ни id генерации")
        return GeneratedImage(content=await self._poll(generation_id), generation_id=generation_id)
