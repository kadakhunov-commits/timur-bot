# Подписочная механика тимура

Бот продаётся по подписке per-chat. Главный продукт — **осознанность/память**; тариф
управляет тем, насколько «живой» тимур в конкретном чате. Память = то, за что платят.

## Тарифы и что они открывают

| | free | standard | plus |
|---|---|---|---|
| глубина памяти (`memory_depth`) | short | standard | full |
| self-card (кто я, без противоречий) | ✅ | ✅ | ✅ |
| досье на друзей (`friend_dossiers`) | — | ✅ | ✅ |
| долгая память / ассоциации | — | ✅ | ✅ |
| эпизоды «помнишь как…» (`episodic_memory`) | — | — | ✅ |
| голос/кружки (`voice`) | — | — | ✅ |
| режимы персоны | default, chill | +savage, npc, quotes | все 6 |
| ответов в день (`max_daily_replies`) | 30 | 1200 | 3000 |
| watermark | ✅ | — | — |

Источник истины по фичам — `BillingEngine.tier_features(tier, mode)` в `billing_system.py`.

## Как тариф меняет поведение (реализовано)

Один шов: `BillingEngine.effective_features(chat_id)` резолвит entitlement → фичи
(free по умолчанию / при истечении). Интерпретация — чистый модуль
`timur_bot/services/feature_gate.py`. Гейты в рантайме:

- **Глубина памяти** — `build_chat_messages` подмешивает блоки по тиру: short = self-card +
  базовый чат; standard = + досье + долгая память + ассоциации; full = + эпизоды + редкие
  «глубокие» отсылки. Это монетизационный клин.
- **Режимы персоны** — `feature_gate.gate_mode`: запрошенный режим, не входящий в тариф,
  падает в `default` для этого чата.
- **Голос** — `can_send_voice` сначала проверяет `feature_gate.voice_allowed`.
- **Дневной лимит** — `send_reply_with_style` проверяет `billing.bot_replies_today` против
  `max_daily_replies`; за лимитом тимур молчит (нудж к апгрейду). Учёт — `register_bot_reply`.
- **Watermark** — уже было: `should_apply_free_watermark` для free.

## Оплата (сейчас — мок)

Платёжный путь end-to-end уже есть: `create_subscription_cycle` → `pay_invoice_mock` →
`_create_entitlement`. Для демо/тестов добавлены ярлыки:

- `/subscribe` — витрина тарифов + текущий статус.
- `/subscribe standard|plus` — мок-активация (`billing.activate_mock`).
- `/subscribe trial` — разовый триал plus на 7 дней (`billing.start_trial`).

`activate_mock` намеренно повторяет то, что сделает реальный `successful_payment`-колбэк:
создаёт активную подписку + entitlement. Реальные рельсы втыкаются ровно сюда.

## Что доделать для продакшена (спроектировано, не реализовано)

1. **Реальные платежи.**
   - **Telegram Stars (XTR)** — основной рельс: `send_invoice(currency="XTR")`,
     `PreCheckoutQueryHandler` (отвечать `ok=True` после валидации),
     `MessageHandler(filters.SUCCESSFUL_PAYMENT)` → вместо `activate_mock` дернуть
     `create_subscription_cycle`/активацию. Без provider-токена, работает в группах.
   - **YooKassa** (`provider_token`) — рублёвые карты, тот же колбэк.
   - **Возвраты** — Stars `refundStarPayment`; событие в ledger.
2. **Жизненный цикл.**
   - Истечение → авто-даунгрейд в free (`get_chat_entitlement` уже метит `expired`;
     гейт сам отдаёт free).
   - Напоминания о продлении (T-3д/T-1д) и **grace-период** через планировщик
     (переиспользовать инфраструктуру `funny_scan`/cron).
   - Триал → конверсия (хук `start_trial` готов).
3. **Витрина/онбординг.** Inline-кнопки покупки в `/subscribe` и paywall в miniapp;
   per-active-user квота через готовый `get_quote` (честно для больших чатов).
4. **Юнит-экономика.** Пер-чат токен-бюджеты на генерацию по тиру (как `daily_token_budget`
   у funny_scan), чтобы платный трафик не съедал маржу.
5. **Аналитика.** MRR / активные / конверсия из `ledger` → дашборд в miniapp.
6. **Комплаенс.** Privacy-политика (бот хранит личные факты), `/forget` + экспорт данных,
   чеки. Для бота-памяти это ещё и фича доверия.

## Тесты

- `tests/test_feature_gate.py` — чистая интерпретация фич.
- `tests/test_billing_features.py` — резолвер, мок-активация, триал, счётчик ответов.
