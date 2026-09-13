from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from flask import Flask, Response, redirect, request, send_from_directory

from timur_bot.web.obshak_api import obshak_api
from timur_bot.web.runtime_meta import get_runtime_meta


ROOT_DIR = Path(__file__).resolve().parents[2]
MINIAPP_PUBLIC = ROOT_DIR / "miniapp" / "public"
OBSHAK_INDEX = MINIAPP_PUBLIC / "obshak.html"
LEGACY_ADMIN_INDEX = MINIAPP_PUBLIC / "admin.html"

app = Flask(__name__)
app.register_blueprint(obshak_api)


def _with_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _read_index(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _build_client_meta() -> dict[str, object]:
    meta = get_runtime_meta()
    build_number = (
        os.getenv("TIMUR_MINIAPP_BUILD", "").strip()
        or os.getenv("AMVERA_BUILD_ID", "").strip()
        or os.getenv("BUILD_NUMBER", "").strip()
    )
    build_label = meta.version
    if build_number:
        build_label = f"{build_label}+{build_number}"
    return {
        "servedAt": meta.deployed_at,
        "environment": os.getenv("AMVERA_ENV", "prod").strip() or "prod",
        "version": meta.version,
        "versionSource": meta.source,
        "build": build_number,
        "buildLabel": build_label,
        "source": "git" if meta.source in {"git", "env"} else "runtime",
    }


def _render_page(path: Path) -> Response:
    html = _read_index(path)
    banner = "<script>window.__TIMUR_MINIAPP_META__ = " + json.dumps(_build_client_meta(), ensure_ascii=False) + ";</script>"
    html = html.replace("</head>", f"{banner}\n</head>", 1)
    return Response(
        html,
        status=200,
        mimetype="text/html",
        headers={
            # Состояние миниаппа разное для разных чатов; HTML не кэшируем.
            "Cache-Control": "no-store, max-age=0",
        },
    )


def _no_store(response: Response) -> Response:
    # 302 тоже не кэшируем: старые клиенты иначе залипают на прежней странице.
    response.headers["Cache-Control"] = "no-store, max-age=0"
    return response


@app.get("/")
def root() -> Response:
    return _no_store(redirect("/miniapp", code=302))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    meta = get_runtime_meta()
    return {"status": "ok", "version": meta.version, "source": meta.source}


@app.get("/version")
def version() -> dict[str, str]:
    client_meta = _build_client_meta()
    return {
        "version": str(client_meta["version"]),
        "source": str(client_meta["versionSource"]),
        "build": str(client_meta["build"]),
        "build_label": str(client_meta["buildLabel"]),
        "deployed_at": str(client_meta["servedAt"]),
    }


@app.get("/miniapp")
def miniapp() -> Response:
    return _render_page(OBSHAK_INDEX)


@app.get("/miniapp/assets/<path:filename>")
def miniapp_asset(filename: str) -> Response:
    response = send_from_directory(MINIAPP_PUBLIC, filename)
    # Ассеты небольшие; ревалидация надёжнее, чем залипший старый JS после деплоя.
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.get("/admin-web")
def legacy_admin() -> Response:
    return _render_page(LEGACY_ADMIN_INDEX)


@app.get("/admin-web/launch")
def legacy_admin_launch() -> Response:
    state = request.args.get("state", "")
    if not state:
        return _no_store(redirect("/admin-web", code=302))
    return _no_store(redirect(_with_query_param("/admin-web", "state", state), code=302))


@app.get("/miniapp/launch")
def miniapp_launch() -> Response:
    """Старые ссылки «открыть миниапп» ведут в общак, а не в прежнюю админ-панель.

    Именно на этот путь смотрели кнопки из старых сообщений, поэтому раньше
    вместо кухни открывалась legacy-панель.
    """
    state = request.args.get("state", "")
    if not state:
        return _no_store(redirect("/miniapp", code=302))
    return _no_store(redirect(_with_query_param("/miniapp", "state", state), code=302))


def main() -> None:
    app.run(host="0.0.0.0", port=80)


if __name__ == "__main__":
    main()
