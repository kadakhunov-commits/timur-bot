# Контекст для LLM: как развивать юмор Тимура

## Задача и критерий успеха

Это Telegram-бот «Тимур»: он должен выглядеть не как ассистент или стендап-комик, а как живой участник конкретного дружеского чата. Сейчас приоритет развития — сделать его смешнее.

Под «смешнее» здесь не подразумеваются более высокий уровень токсичности, больше сообщений или набор повторяемых мемов. Целевой эффект — больше коротких, уместных и свежих добивок, которые цепляются за реальное противоречие в сцене. Хорошая шутка не требует объяснения и не вводит человека, которого не было в разговоре. Когда точного поворота нет, корректный результат — молчание или нормальный короткий ответ.

Успех нужно проверять по качеству подтверждённых ответов: сердцам, явному смеху в reply и отсутствию `/unfunny`, а не по числу генераций. Нельзя менять пользовательское поведение вне этой цели без отдельного запроса.

## Что уже реализовано

### Персона и прямые ответы

`config/persona.yaml` — основной источник голоса Тимура: маленькие буквы, короткий ответ, редкие локальные мемы, дружеский стёб без дешёвых универсальных оскорблений. Он запрещает «IQ комнатной температуры», нейроны, конструкции «x — это когда», «а то я думал» и объяснение шутки.

При прямом обращении `text_handler` строит контекст через `build_chat_messages` в `timur_bot/services/bot_logic.py`. В prompt уже попадают:

- последние сообщения и reply-цепочка;
- активный режим, тональность и mood;
- self-card Тимура;
- доступные по тарифу факты, досье, эпизоды и rolling memory;
- план юмора из `humor.py`.

Прямой ответ ограничен `adaptive_humor.direct_reply_max_chars` из `config/runtime.yaml`. Промпт требует сначала отвечать по смыслу, а шутить только по детали текущей сцены.

### Ambient adaptive humor

Если Тимура прямо не позвали, `_maybe_send_adaptive_snipe` может попробовать короткую реплику. Это не тот же путь, что обычный LLM-ответ.

```text
Новая реплика в чате
        ↓
единый snipe_gate: cooldown + разрыв сообщений, затем вероятность и токен-бюджет
        ↓
Writer: 2–3 варианта или молчание (плюс блокировки из фидбека)
        ↓
жёсткие фильтры: контекст, повторы, люди, шаблоны, длина, unfunny-механизмы
        ↓
Critic: один победитель, ❤️ или молчание
        ↓
отправка только при score ≥ candidate_threshold
        ↓
humor_scenes_v2 + feedback + метрики
```

Writer и Critic определены в `timur_bot/services/adaptive_humor.py`. Writer не может выбрать собственную шутку; Critic независим. Это ключевая защита от самоуверенной и натужной генерации. С версии схемы 8 подтверждённый `/unfunny` блокирует механизм и callback прямо в Writer-промпте и фильтре (`feedback_blocked_signals`).

Настройки находятся в `config/runtime.yaml` → `adaptive_humor`:

- `participation_rate` — максимальная частота попыток;
- `snipe_cooldown_minutes` и `min_human_messages` — единственный гейт навязчивости (`snipe_gate`);
- `ambient_reply_max_chars` — лимит короткой добивки;
- `candidate_threshold` — минимальная оценка Critic для отправки;
- `director_max_tokens`, `critic_max_tokens`, `background_daily_token_budget` — цена и нагрузка;
- `interjection_timeout_seconds` — дедлайн ambient-пути (writer + critic).

Сначала улучшать качество prompt-ов, фильтров или отбора; не повышать `participation_rate` и не понижать `candidate_threshold` без замера качества.

### Факт-чек по mention («@тимур это правда?»)

Отдельный пользовательский контур поверх цели «смешнее»: по строгому mention-entity бота Тимур проверяет утверждение и отвечает одним коротким вердиктом в стиле Grok. Триггер — только `mention` на @username бота (текстовый «тимур» не считается); утверждение берётся из replied-сообщения (reply-target), иначе из самого упоминающего текста. Ответ — reply на claim-сообщение.

```text
mention-entity + claim
        ↓
services/fact_check.py: маркеры, rate-limit (max_per_chat_per_hour), метки
        ↓
call_openai_fact_check (polza plugin web при web_search + polza.ai)
        ↓
нормализация: правда / скорее правда / полуправда / скорее нет / враньё / не проверяемо
        ↓
reply на claim, запись в humor_decisions_v2 + memory_layers.fact_check
```

Настройки — `config/runtime.yaml` → `fact_check` (`enabled`, `web_search`, `web_max_results`, `max_per_chat_per_hour`, `max_chars`, `timeout_seconds`). Если claim не нашли или rate-limit исчерпан — молчание, без ответа «не понимаю».

### Память о смешном

Активная обучающая единица — `humor_scenes_v2` в памяти чата. Она хранит сцену, выбранную реплику, механизм, callback-и и feedback. `select_positive_example` может передать Writer один похожий подтверждённый пример как принцип, но запрещает копировать текст.

Подтверждённые сигналы:

- сердечная реакция на сообщение Тимура;
- явный смех в reply на его сообщение;
- `/funny` в reply от owner;
- `/unfunny` и текстовая критика как отрицательный feedback.

Обычные реакции не считают юмор удачным. Смех на сообщение человека — не повод отвечать «лол»; Critic может только поставить этому сообщению ❤️, если это законченная шутка.

Важно: `/bit`, `import_funny_examples` и LLM-курация Telegram-истории сейчас сохраняют данные в обратимом карантине `legacy_humor_v1`. Они полезны для аудита и будущей миграции, но не служат активным банком готовых шуток. Не «чинить» это простым подключением legacy-данных к retrieval: там нет достаточной гарантии качества и контекста.

### Offline-обучение и оценка

Есть два самостоятельных инструмента:

- `export_funny_candidates` и `curate_funny_examples` — поиск исторических моментов вида «сцена → реплика → смех» и их ручная или LLM-разметка;
- `humor_replay` — слепое сравнение baseline/v2 на `tests/fixtures/humor_replay.json` (режим `legacy,v2`) или на анонимизированном quality-корпусе (режим `prev,v2`, `--fixture tests/fixtures/humor_quality_scenes.json`). Считает gate accuracy, anchoring, template/repeat violations; с проверкой контрактов, аудитом и лимитом API-вызовов.

### A/B системных промптов (persona_ab)

`python3 -m timur_bot.tools.persona_ab [--scopes all|ambient|direct] [--variants A,B,C,D] [--no-judge]` — слепой A/B вариантов системного промпта на тех же реальных сценах через прод-пайплайн `build_chat_messages` + реальная модель + слепой судья (funny/cringe/natural) и детерминированные метрики длины/шаблонов. Не меняет `memory.json`. Текущий победитель (D: короче + анти-кринж + живой пацан) уже зафиксирован в `config/persona.yaml`; полный отчёт в `docs/PERSONA_AB_REPORT.md`.

`funny_scan` — scheduler для owner: находит смешные кластеры по сердцам и маркерам смеха, запускает LLM-review и отдаёт кандидатов на проверку. По умолчанию выключен. Это исследовательский контур, а не источник автоматических ответов в чате.

## Техническая карта

| Зона | Основные файлы | Зачем смотреть |
| --- | --- | --- |
| Runtime | `timur_bot/services/bot_logic.py` | Роутинг update, prompt, отправка, фоновые циклы, owner-команды. |
| Adaptive humor | `services/adaptive_humor.py`, `services/humor.py`, `services/conversation_policy.py` | Writer/Critic, валидация, feedback, retrieval, cooldown. |
| Факт-чек | `services/fact_check.py`, `call_openai_fact_check` в `bot_logic.py` | Mention-триггер, rate-limit, нормализация вердикта, polza web-плагин. |
| Конфигурация | `config/persona.yaml`, `config/runtime.yaml`, `core/config.py` | Голос, лимиты, модели и нормализация параметров. |
| Память | `services/self_model.py`, `participant_memory.py`, `fact_memory.py`, `fact_recall.py`, `episodes.py`, `rolling_memory.py` | Что Тимур может вспомнить и на каких условиях. |
| Качество | `tests/test_adaptive_humor.py`, `test_humor.py`, `test_conversation_policy.py`, `test_humor_replay.py`, `test_humor_quality_scenes.py`, `test_humor_quality_replay.py`, `tests/fixtures/humor_quality_scenes.json` | Исполняемые требования к безопасному и смешному поведению и quality-gates по категориям сцен. |
| Операции | `README.md`, `ARCHITECTURE.MD`, `SUBSCRIPTION.md`, `docs/LLM_CONTEXT.md`, `docs/HUMOR_REDESIGN_REPORT.md`, `docs/PERSONA_AB_REPORT.md` | Запуск, импорт, деплой, тарифы, продуктовая цель, отчёт редизайна и отчёт A/B промптов. |

`memory.json` и `billing_state.json` — локальное изменяемое состояние; они могут содержать личные данные и не должны попадать в diff или prompt целиком. Конфиги YAML — версия продукта. Секреты — только в `.env`.

## Рекомендуемый порядок работы над юмором

1. Сформулировать узкую гипотезу, например: «Critic пропускает хорошие реакции на законченные шутки» или «фильтр отбрасывает нормальные контекстные добивки».
2. Найти существующие тесты и фикстуры для этого механизма; добавить минимальный воспроизводимый кейс до изменения, если его нет.
3. Изменить самый узкий слой: persona/config для формулировки, `adaptive_humor.py` для контракта или фильтра, `humor.py` для retrieval/feedback, `conversation_policy.py` для частоты.
4. Запустить узкие тесты, затем `pytest -q`. Для изменения генеративного контракта запустить `python3 -m timur_bot.tools.humor_replay --dry-run`; реальный LLM-benchmark требует контролировать расходы и результаты аудита.
5. Проверить, что изменения не делают Тимура разговорчивее или грубее просто ради метрики.

## Неподходящие решения

- Поднять `participation_rate`, опустить `candidate_threshold` или увеличить лимиты, не доказав, что проблема именно в недостатке попыток.
- Добавить длинный перечень мемов или одну «универсальную» формулу шутки.
- Передавать в prompt полную историю чата или весь `memory.json`.
- Убрать независимого Critic, жёсткие фильтры, cooldown или budget guard.
- Перемешать подтверждённые humor scenes с неразмеченной Telegram-историей.
- Исправлять ощущение «несмешно» повышением `toxicity_level`: едкость и меткость — разные свойства.

## Минимальная верификация

```bash
pytest -q tests/test_adaptive_humor.py tests/test_humor.py tests/test_conversation_policy.py
pytest -q tests/test_humor_replay.py tests/test_humor_quality_scenes.py tests/test_humor_quality_replay.py
pytest -q
```

Если менялись только YAML-конфиги, дополнительно достаточно `pytest -q tests/test_config_loader.py` вместе с тематическими тестами. Если менялась общая обработка сообщений или память, запускать также:

```bash
pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py
```

## Открытая продуктовая развилка

Проект уже умеет собирать и отбирать исторические смешные примеры, но не использует неаудированные legacy-импорты в live retrieval. Следующий осмысленный шаг — не просто включить эти примеры, а задать миграционный контракт: какие сигналы делают пример подтверждённым, как сохранить его сцену и механизм в `humor_scenes_v2`, как исключить копирование и как измерить результат в replay. Пока такого контракта нет, карантин — правильное поведение.
