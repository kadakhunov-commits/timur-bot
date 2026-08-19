# Итоговый отчёт: упрощение юмора и objective-тестирование

Выполнено по `docs/LLM_HUMOR_REDESIGN_PROMPT.md`, план — `docs/HUMOR_REDESIGN_PLAN.md`.

## 1. Диагноз с фактами из кода

- **Каскад гейтов вместо одного решения.** Ambient-путь требовал три независимых счётчика (`interjection_check_allowed`, `ordinary_reply_allowed`, `snipe_allowed` с attempts-счётчиком) до участия (`bot_logic.py`, старый `:4187-4211`). Каждое сообщение могло молча сгорать на любой ступени; attempts-счётчик сбрасывался даже у сгоревших попыток (`mark_snipe_attempt`), что давило частоту независимо от качества.
- **Обрезание Writer-JSON.** `director_max_tokens=180` на JSON с setup/target/relation/forbidden_moves + ровно 4 кандидатами — высокий риск truncate ⇒ `parse_director` пусто ⇒ бюджет списан, реплика не отправлена, видимая причина — «writer abstain».
- **Таймаут короче генерации.** `interjection_timeout_seconds=3` оборачивал два последовательных LLM-вызова (`_run_with_typing`, bot_logic.py `:8280`); реальные writer+critic не укладывались ⇒ тихий cancel.
- **Фидбек писался, но не влиял на генерацию.** `humor_stats_v2` считал mixed/unfunny-сигналы (humor.py `_rebuild_mechanism_stats`), но Writer и фильтры их не видели. «Обучение» было видимостью: подтверждённые плохие механизмы и callback-и генерировались заново.
- **Две несвязанные системы качества.** Прямой ответ и ambient-путь имели разные prompt-конструкции и общего владельца качества не имели.
- **Тесты доказывали контракты, не смешность.** Fixture `humor_replay.json` проверял `expected_action` (в основном SILENCE), blind judge сравнивал old-vs-new, но детерминированных метрик зацепления/шаблонов/повторов не было.
- В локальном `memory.json` на момент работ v2-решений не было (0 сцен/0 decisions) — live-baseline нулевой; воспроизводимый baseline создан в фикстурах.

## 2. Что удалено, упрощено, оставлено и почему

**Удалено (мёртвый/дублирующий live-код):**
- каскад pre-gates: `interjection_check_allowed`, `mark_interjection_checked`, `ordinary_reply_allowed`, `mark_reply_sent`, `mark_snipe_attempt` и счётчики `human_messages_since_*` для replies/checks/attempt (`conversation_policy.py`);
- поля Writer-JSON `target` и `forbidden_moves` (Critic их не читал, только расход токенов);
- требование «ровно 4 кандидата» — Writer экономит токены и получает меньше обрезаний.

**Упрощено:**
- единый гейт `snipe_gate` = cooldown (`snipe_cooldown_minutes`) + message-gap (`min_human_messages`) — не потребляет состояние, поэтому сгоревшая по бюджету попытка не навязывает дополнительный тихий интервал;
- Writer 2–4 кандидата (`MIN_CANDIDATES=2`), компактный JSON;
- `director_max_tokens` 180→350, clamp до 500 (schema v8 миграция переносит только старые дефолты, кастомные лимиты целы);
- `interjection_timeout_seconds` 3→15 (диапазон до 30) — writer+critic успевают;
- дублирующие ключи runtime.yaml убраны; schema v8 миграция с обратимым сохранением старых значений в `legacy_v1_settings`.

**Активировано (новый слой качества):**
- `feedback_blocked_signals` (humor.py) собирает механизмы и callback-и из сцен с rating `unfunny`;
- они передаются в Writer-промпт («заблокированные фидбеком чата») и в `filter_candidates` (`blocked_mechanisms`), т.е. подтверждённые плохие паттерны не доходят до генерации и до пользователя.

**Оставлено без изменений:** независимый Critic и порог 85 (менять без replay-замера нельзя), детерминированные фильтры (шаблоны, отсутствующие люди, повторы, длина), бюджет-гард, legacy-карантин `legacy_humor_v1`, прямой ответ, Telegram-роутинг, подписки, privacy, vision, voice, Mini App, секреты. Данные пользователя не удалены: сохранённое состояние читается безопасно, старые ключи архивируются.

## 3. Как подтверждённый юмор теперь влияет на будущие ответы

- Positive (`heart`, `direct_laugh`, `/funny`) → `humor_scenes_v2` → `select_positive_example` → один похожий пример в Writer-промпте (принцип, не копия) — как раньше.
- Negative (`/unfunny`, текстовая критика) → `feedback_blocked_signals` → блокировка механизма и callback в Writer-промпте и жёстком фильтре до отправки. Этого раньше не было.
- Все решения (JOKE/SILENCE/REACT, reason codes, токены, latency) пишутся в `humor_decisions_v2`; replay дополнительно считает `gate_accuracy`, `anchor_rate`, template/repeat violations по сторонам сравнения.

## 4. Тесты, replay и сравнения

Создано/обновлено:

| Артефакт | Назначение |
| --- | --- |
| `tests/fixtures/humor_quality_scenes.json` | 14 анонимизированных сцен: contextual twist (4), serious (2), technical, finished joke, no trigger (3), repeat risk, absent person, template trap. Без персональных данных |
| `tests/test_humor_quality_scenes.py` | Layer A: mocked-LLM полный snipe-путь по категориям — выбор silence/send/filter-drop, блокировка фидбеком |
| `tests/test_humor_quality_replay.py` | Layer B (dry-stubbed): `prev,v2` сравнение, стоимость, `measure_scene_quality` |
| `tests/test_conversation_policy.py` | переписан под единый `snipe_gate` |
| `tests/test_memory_layers_runtime.py` | миграции v1–v7 → v8, кастомные лимиты переживают миграцию |
| `tests/test_adaptive_humor.py`, `test_humor_replay.py` | обновлены под новый контракт Writer (2–4 кандидата, 350 токенов) |
| `timur_bot/tools/humor_replay.py` | `--compare prev,v2` (замороженный pre-redesign пайплайн), `anchor_phrases`, objective-метрики: `gate_accuracy`, `anchor_rate`, `template_hits`, `repeats_scene`; стоимость считается по паре сравнения |

Результаты прогонов:

- `pytest -q` → **300 passed, 1 skipped** (было 286; +14 новых тестов, ни одного неудачного контракта).
- `pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py` → зелёные (входят в полный прогон).
- `python3 -m timur_bot.tools.humor_replay --dry-run --runs 3 --compare legacy,v2` → `{"valid": true, "scenes": 30, "estimated_max_api_calls": 312}`.
- `python3 -m timur_bot.tools.humor_replay --dry-run --runs 1 --compare prev,v2 --fixture tests/fixtures/humor_quality_scenes.json` → `{"valid": true, "scenes": 14, "estimated_max_api_calls": 70}`.

## 5. Остаточный риск и следующий эксперимент

Автоматика доказывает структурные свойства: выбор gate, отсутствие шаблонов, повторов, missing-person, длину, зацепление по `anchor_phrases`. **Смешность** они не доказывают: blind-judge — LLM, а не человек; `anchor_phrases` в фикстуре определяют свойство «зацепился за деталь», но не качество слова.

Live `prev,v2` benchmark на 14 сценах стоит до 70 API-вызовов на прогон и требует `--dry-run` перед запуском — он не запускался без владельца бюджета.

Следующий эксперимент:
1. Запустить `python3 -m timur_bot.tools.humor_replay --runs 3 --compare prev,v2 --fixture tests/fixtures/humor_quality_scenes.json` с бюджетом и записать `ambient_quality` и `v2_win_rate_without_ties`.
2. Собрать 20–30 анонимизированных реальных сцен из чата (sed-анонимизация имён), добавить в quality-корпус с `anchor_phrases`.
3. Слепая ручная оценка владельцем: для каждой сцены old-vs-new без знания варианта; критерий — «какую реплику я бы лайкнул сердцом».
4. Только после этого двигать `candidate_threshold`/`participation_rate`.
