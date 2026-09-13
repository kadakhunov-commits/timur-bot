"""Связность фронтенда общака: каждая кнопка должна иметь обработчик.

Миниапп работает на делегировании: разметка помечает элементы `data-act`
(и `data-action` в сцене), а `app.js` разбирает их в `switch`. Забыть ветку
легко, а выглядит это как «кнопка не кликается» без единой ошибки в логах —
ровно так сломался выбор аватара при первом входе. Тест ловит рассинхрон.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OBSHAK_DIR = ROOT / "miniapp" / "public" / "obshak"
APP_JS = (OBSHAK_DIR / "app.js").read_text(encoding="utf-8")
SCENE_JS = (OBSHAK_DIR / "scene.js").read_text(encoding="utf-8")
OBSHAK_HTML = (ROOT / "miniapp" / "public" / "obshak.html").read_text(encoding="utf-8")

# Действия сцены, которые обрабатываются не как "action-open", а отдельной веткой.
SPECIAL_SCENE_ACTIONS = {"member", "light"}


def handled_actions() -> set:
    return set(re.findall(r'case "([a-zA-Z-]+)":', APP_JS))


def markup_actions() -> set:
    return set(re.findall(r'data-act="([a-zA-Z-]+)"', APP_JS))


def scene_hotspot_actions() -> set:
    from_hotspots = set(
        re.findall(r'hotspot\(\s*[\d.]+,\s*[\d.]+,\s*[\d.]+,\s*[\d.]+,\s*"([a-zA-Z-]+)"', SCENE_JS)
    )
    from_markup = set(re.findall(r'data-action="([a-zA-Z-]+)"', SCENE_JS))
    return from_hotspots | from_markup


def test_every_data_act_has_a_handler():
    missing = markup_actions() - handled_actions()
    assert not missing, f"кнопки без обработчика в app.js: {sorted(missing)}"


def test_every_scene_hotspot_has_a_handler():
    handled = handled_actions()
    missing = []
    for action in scene_hotspot_actions():
        if action in SPECIAL_SCENE_ACTIONS:
            # Эти ветки разбираются прямо в обработчике клика, а не через switch.
            if f'action === "{action}"' not in APP_JS:
                missing.append(action)
            continue
        if action + "-open" not in handled:
            missing.append(action + "-open")
    assert not missing, f"хотспоты сцены без обработчика: {sorted(missing)}"


def test_picker_buttons_answer_the_pick_action():
    # Отдельная проверка на конкретный регресс: выбор аватара при первом входе.
    assert 'data-act="pick"' in APP_JS
    assert 'case "pick":' in APP_JS


def test_obshak_page_references_existing_assets():
    assets = re.findall(r'/miniapp/assets/([^"?]+)', OBSHAK_HTML)
    assert assets, "страница общака не подключает ни одного ассета"
    for asset in assets:
        assert (ROOT / "miniapp" / "public" / asset).exists(), f"нет файла {asset}"


def test_obshak_page_has_required_containers():
    for container in ("stage", "shelf", "periodBar", "homeBars", "statusStrip", "catRoot", "fxCanvas"):
        assert f'id="{container}"' in OBSHAK_HTML, f"в разметке нет #{container}"
