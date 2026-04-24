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

## Логи и изменения поведения

- Логи бота должны оставаться человекочитаемыми и объяснять причинно-следственную цепочку решений Тимура (почему ответил/пропустил, какой шанс применился, какой итоговый формат ответа выбран).
- Если меняется логика принятия решений или формат ответов, обязательно вместе с кодом обновляй и логику логгера в `timur_bot/services/bot_logic.py`, чтобы в логах оставалось понятное объяснение действий.

## Тесты

```bash
pytest -q
```

Parity-тесты (ключевая логика до/после рефактора):

```bash
pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py
```

## Импорт Telegram-выгрузки в память

Локальный импортёр HTML-выгрузки:

```bash
python3 -m timur_bot.tools.import_telegram_html \
  --src "/Users/unterlantas/Documents/тимур/tg" \
  --chat-id 93135242 \
  --mode merge \
  --dry-run \
  --apply-style-profile \
  --recent-days 14 \
  --max-recent-messages 24 \
  --max-recent-facts 120 \
  --max-long-facts 400
```

Боевой запуск (запишет `memory.json` и сделает `memory.backup.<timestamp>.json`):

```bash
python3 -m timur_bot.tools.import_telegram_html \
  --src "/Users/unterlantas/Documents/тимур/tg" \
  --chat-id 93135242 \
  --mode merge \
  --apply-style-profile \
  --no-raw-log
```

Только компактизация уже существующей памяти без нового импорта:

```bash
python3 -m timur_bot.tools.import_telegram_html \
  --chat-id 93135242 \
  --compact-only \
  --recent-days 14
```

Поддерживаемые флаги:

- `--src <path>` — папка с `messages*.html`
- `--chat-id <int>` — целевой chat id в `memory.json`
- `--mode merge|replace` — режим импорта (по умолчанию `merge`)
- `--dry-run` — только отчёт, без записи
- `--apply-style-profile` — обновляет `config.style_settings` автопрофилем из чата
- `--memory-path <path>` — альтернативный путь к `memory.json`
- `--recent-days <int>` — окно «недавней» памяти (по умолчанию `14`)
- `--max-recent-messages <int>` — лимит оперативных сообщений в `memory_layers`
- `--max-recent-facts <int>` — лимит недавних фактов в `memory_layers`
- `--max-long-facts <int>` — лимит долгой факт-памяти в `memory_layers`
- `--archive-path <path>` — опциональный jsonl-архив импортированных записей
- `--compact-only` — только сжатие слоев памяти без чтения `messages*.html`
- `--no-raw-log` — не хранит сырой текстовый лог Telegram в `memory.json` (поведение по умолчанию)
- `--keep-raw-log` — явно сохраняет сырой импортированный текст в `memory.log`

## Комедийная память

Feedback на ответы Тимура:

- reaction heart на сообщение Тимура — засчитать `funny`
- reaction poop/dislike на сообщение Тимура — засчитать `unfunny`
- reply `лол` на сообщение Тимура — засчитать `funny`
- reply `несмешно` на сообщение Тимура — засчитать `unfunny`

Owner-команды:

- `/bit <текст>` — добавить локальный прикол в joke bank
- `/bits` — показать топ локальных приколов
- `/funny` — вручную засчитать reply как удачный ответ
- `/unfunny` — вручную засчитать reply как неудачный ответ

## Выборка смешных примеров

Сгенерировать кандидаты `контекст -> удачный ответ` для ручной разметки или прогона через сильную модель:

```bash
python3 -m timur_bot.tools.export_funny_candidates \
  --src "/Users/unterlantas/Documents/тимур/tg" \
  --out "data/funny_candidates.jsonl" \
  --limit 500
```

Файл пишется в `data/`, эта папка игнорируется git.

После разметки (`"selected": true`) импортировать выбранные примеры обратно:

```bash
python3 -m timur_bot.tools.import_funny_examples \
  --src "data/funny_candidates.jsonl" \
  --chat-id 93135242
```

## Документация

- Архитектура: `ARCHITECTURE.MD`
- Инструкции для AI/LLM-агентов: `AGENTS.md`
