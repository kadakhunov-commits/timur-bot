from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from flask import Flask, Response, redirect, request


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


@app.get("/")
def root() -> Response:
    return redirect("/miniapp", code=302)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/miniapp")
def miniapp() -> Response:
    html = _read_index()
    env_name = os.getenv("AMVERA_ENV", "prod")
    banner = (
        "<script>"
        "window.__TIMUR_MINIAPP_META__ = {"
        f"servedAt: '{os.getenv('AMVERA_DEPLOY_TIME', '')}',"
        f"environment: '{env_name}'"
        "};"
        "</script>"
    )
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

