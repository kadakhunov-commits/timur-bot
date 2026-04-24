# AGENTS

Короткий контекст для AI/LLM-агентов, работающих с этим репозиторием.

## Что это за проект

- Telegram-бот с персоной + памятью чата + vision-ответами.
- Основной runtime: `timur_bot/services/bot_logic.py`.
- Встроенный биллинг-движок: `billing_system.py`.

## Где что лежит

- `timur_bot/core/config.py` — загрузка `.env` и YAML-конфигов.
- `timur_bot/app/router.py` — регистрация всех handlers.
- `timur_bot/handlers/*` — Telegram-обработчики команд/сообщений.
- `config/persona.yaml` — system prompt, режимы, дефолты.
- `config/lexicon.yaml` — стоп-слова, маркеры, тематические словари.
- `config/runtime.yaml` — модели, лимиты, вероятности.

## Как запускать

```bash
python3 -m pip install -r requirements.txt
python3 -m timur_bot
```

Совместимый запуск:

```bash
python3 timur_bot.py
```

## Переменные окружения

Обязательные:

- `TELEGRAM_BOT_TOKEN`
- `OPENAI_API_KEY`

Опциональная:

- `OPENAI_BASE_URL` (если пусто — дефолт OpenAI SDK)

## Что проверять после изменений

```bash
pytest -q
pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py
```

## Важные ограничения

- Не менять пользовательское поведение без явного запроса.
- Секреты хранить только в `.env`, не в YAML.
- Изменения промптов/лексиконов — через `config/*.yaml`.
