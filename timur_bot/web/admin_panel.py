from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from flask import Flask, Response, redirect, request

from timur_bot.web.runtime_meta import get_runtime_meta


ROOT_DIR = Path(__file__).resolve().parents[2]
MINIAPP_INDEX = ROOT_DIR / "miniapp" / "public" / "index.html"

app = Flask(__name__)


def _with_query_param(url: str, key: str, value: str) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query[key] = value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _read_index() -> str:
    return MINIAPP_INDEX.read_text(encoding="utf-8")


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


@app.get("/")
def root() -> Response:
    return redirect("/miniapp", code=302)


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
    html = _read_index()
    banner = "<script>window.__TIMUR_MINIAPP_META__ = " + json.dumps(_build_client_meta(), ensure_ascii=False) + ";</script>"
    html = html.replace("</head>", f"{banner}\n</head>", 1)
    return Response(
        html,
        status=200,
        mimetype="text/html",
        headers={
            # Mini App state may differ per chat; disable HTML caching.
            "Cache-Control": "no-store, max-age=0",
        },
    )


@app.get("/miniapp/launch")
def miniapp_launch() -> Response:
    state = request.args.get("state", "")
    if not state:
        return redirect("/miniapp", code=302)
    return redirect(_with_query_param("/miniapp", "state", state), code=302)


def main() -> None:
    port = int(os.getenv("PORT", "80"))
    app.run(host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
