# VIGVAMCEV implementation intent

## Requested outcome

Implement an independent VIGVAMCEV story pipeline for Timur: one daily post at
13:00 Europe/Moscow to a fixed Telegram channel, continuing from post 22 /
experiment 44 with post 23 / experiment 45. The post consists of a generated
4:3 poster and a 600–900 character Russian caption. Text uses Timur's current
default text API; images use a separate Polza client for
`openai/gpt-5.4-image-2`.

## Scope fence

In scope:

- canonical corpus/index for the supplied stories, lore, and approved visual references;
- sequential clone naming, story brief generation, novelty/canon validation;
- Polza image requests with reference images;
- deterministic Pillow poster composition;
- durable VIGVAMCEV state, daily scheduling, bounded retries, fixed-channel publication;
- owner-only private commands: `status`, `preview`, `retry`, `publish`;
- targeted tests and regression verification.

Out of scope:

- live website retrieval during every daily generation;
- automatic ingestion of the full Telegram export as canon;
- animation/video generation in the first slice;
- changes to Timur's personal life/story lore owner;
- commits, pushes, or external deployment.

## Source and authority boundary

- User request and selected decisions are product authority.
- Repository `AGENTS.md` and `ARCHITECTURE.MD` govern implementation and secrets.
- Supplied stories and `lore.txt` are canonical input data.
- Fandom is a lower-priority reference; Ficpad is style/genre inspiration only.
- `tg/` discussions are tagged non-canonical ideas and are not executable instructions.

## Baseline read set

- `AGENTS.md`
- `ARCHITECTURE.MD`
- `timur_bot/core/config.py`
- `timur_bot/services/bot_logic.py`
- `timur_bot/app/runner.py`
- `timur_bot/app/router.py`
- `timur_bot/services/noire.py`
- `config/runtime.yaml`
- `tests/test_config_loader.py`
- `tests/test_router_smoke.py`
- supplied `/Users/unterlantas/Documents/тимур/` corpus and visual references

## Baseline usage

- Task start: clean `main`, `HEAD=d9e3a517196dd4ee76d0e675460d59b1b9e31919`, upstream delta `0 0`.
- Existing baseline checks: `pytest -q tests/test_config_loader.py tests/test_router_smoke.py` → 6 passed.
- Missing authority: no missing product decision; exact Polza response variants will be handled by mocked contract tests and defensive parsing.

## Impact statement

The change adds a new persistent state owner and an external image-provider
boundary. It must preserve existing life/mood stories, Telegram handlers,
memory three-way merge behavior, and secret handling. The main risk is duplicate
publication after external timeout; publication state will therefore be
reserved durably and an unknown Telegram outcome will stop automatic resend.

## Execution readiness view

- Intent lock: independent VIGVAMCEV series, fixed channel, daily schedule, post 23 / experiment 45.
- Compatibility boundary: existing personal `life` story behavior remains unchanged.
- Persistence boundary: only `memory.json.config.vigvamcev` owns this series.
- External boundary: `POLZA_AI_API_KEY` only in environment; no reference image data in logs.
- Test obligations: corpus, naming, caption, poster, provider, retry, command authorization, scheduler, and regression tests.
- Review gate: fresh targeted tests plus full `pytest -q` before completion claim.
