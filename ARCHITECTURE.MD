# Architecture

## Обзор

Проект разделён на слои:

- `timur_bot/core` — инфраструктура и загрузка конфигов.
- `timur_bot/services` — бизнес-логика бота и чистые утилиты обработки текста.
- `timur_bot/handlers` — Telegram handlers (chat/admin/billing/owner).
- `timur_bot/app` — wiring роутера и запуск приложения.

Точка входа: `python -m timur_bot` (файл `timur_bot/__main__.py`).

## Поток запроса

1. Telegram update попадает в handler через `app/router.py`.
2. Handler вызывает runtime-функции из `services/bot_logic.py`.
3. `bot_logic.py`:
   - читает/обновляет память (`memory.json`);
   - применяет правила ответа (вероятности, лимиты, режимы);
   - собирает prompt-контекст;
   - вызывает OpenAI text/vision;
   - учитывает биллинг (`billing_system.py`).

## Конфиги

`core/config.py` загружает:

- `config/persona.yaml`
- `config/lexicon.yaml`
- `config/runtime.yaml`
- `.env` (секреты и endpoint)

Принцип приоритета:

- секреты (`TELEGRAM_BOT_TOKEN`, `OPENAI_API_KEY`, `OPENAI_BASE_URL`) — только `.env`;
- продуктовые настройки — YAML;
- при отсутствии опциональных ключей применяются дефолты.

## Тестовый контур

- `tests/test_baseline_snapshots.py` — snapshot базовой логики.
- `tests/test_refactor_parity.py` — parity старой/новой логики.
- `tests/test_router_smoke.py` — проверка регистрации handlers.
- `tests/test_config_loader.py` — валидация загрузчика YAML.

## Контекст для AI-агентов

Единая точка входа для контекста AI-ассистентов: `AGENTS.md`.
Там зафиксированы ключевые команды, соглашения и ограничения по проекту.
