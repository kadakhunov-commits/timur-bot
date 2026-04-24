# Timur Bot

Telegram-бот с персоной, памятью чата, vision-ответами и встроенным биллингом.

## Быстрый старт

1. Установи зависимости:

```bash
python3 -m pip install -r requirements.txt
```

2. Создай `.env` в корне проекта:

```env
TELEGRAM_BOT_TOKEN=...
OPENAI_API_KEY=...
OPENAI_BASE_URL=
```

`OPENAI_BASE_URL` опционален. Если пустой, используется стандартный endpoint SDK.

3. Запусти бота:

```bash
python3 -m timur_bot
```

Также поддерживается совместимый запуск:

```bash
python3 timur_bot.py
```

## Конфигурация

Основные конфиги лежат в `config/`:

- `config/persona.yaml` — system prompt, режимы личности, дефолты по стилю/био/токсичности.
- `config/lexicon.yaml` — стоп-слова, маркеры, тематические лексиконы, мемы/ссылки.
- `config/runtime.yaml` — модели, лимиты, вероятности, owner id.

Секреты хранятся только в `.env`.

## Тесты

```bash
pytest -q
```

Parity-тесты (ключевая логика до/после рефактора):

```bash
pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py
```

## Документация

- Архитектура: `ARCHITECTURE.MD`
- Инструкции для AI/LLM-агентов: `AGENTS.md`
