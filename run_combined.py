#!/usr/bin/env python3
"""Run Telegram bot and Mini App admin web server in one process.

Amvera can execute only one entrypoint command. This bootstrap starts:
1) Flask Mini App server (port from PORT env, default 80)
2) Telegram bot polling loop
"""

from __future__ import annotations

import logging
import os
from threading import Thread

from timur_bot.app.runner import main as run_bot
from timur_bot.web.admin_panel import app as admin_app
from timur_bot.web.runtime_meta import get_runtime_meta


logger = logging.getLogger("timur-bot.combined")


def _resolve_port(default: int = 80) -> int:
    raw = os.getenv("PORT", "").strip()
    if not raw or raw.lower() == "null":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _run_admin_panel() -> None:
    # Disable reloader in worker thread to avoid double-start behavior.
    port = _resolve_port(80)
    admin_app.run(host="0.0.0.0", port=port, use_reloader=False)


def main() -> None:
    meta = get_runtime_meta()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger.info(
        "Starting combined runtime: admin panel + telegram bot version=%s source=%s",
        meta.version,
        meta.source,
    )

    web_thread = Thread(target=_run_admin_panel, name="miniapp-web", daemon=True)
    web_thread.start()

    # Keep bot in main thread; polling loop is blocking by design.
    run_bot()


if __name__ == "__main__":
    main()
