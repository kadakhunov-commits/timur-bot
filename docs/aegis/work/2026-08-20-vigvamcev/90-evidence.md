# VIGVAMCEV completion evidence

## Functional evidence

- Canon corpus loads all supplied story texts, the supplied `lore.txt`, recovered post 10 / experiment 32, the seed 22 / 44, the source hierarchy manifest, and selected visual references.
- Candidate validation checks sequence, allowlisted source word, `-цев` naming, uniqueness, canonical anchor overlap, caption length, causality, novelty motifs, and recent-story similarity.
- Drafts are marked `canon_status=draft`; only a successful Telegram send appends `canon_status=generated_canon` to history and advances the sequence.
- Polza references are uploaded through Storage API, image generation is polled through Media API, and the local poster compositor produces deterministic 1280×960 PNG output.
- Automatic scheduling uses `Europe/Moscow` and the configured publish time; fixed channel, bounded retries, owner-only private commands, and `publish_unknown` duplicate protection are wired.

## Verification evidence

- `python3 -m compileall -q timur_bot`
- `pytest -q tests/test_vigvamcev.py tests/test_config_loader.py tests/test_router_smoke.py` — 17 passed.
- `pytest -q tests/test_baseline_snapshots.py tests/test_refactor_parity.py` — 12 passed.
- `pytest -q` — 327 passed, 1 skipped.
- `git diff --check` — passed.
- Visual QA of a generated local poster — header, yellow/orange background, black dots, scene, diagonal name plate, and metadata are present.

## Deployment boundary

No real Polza or Telegram generation/publication request was made. The local deployment already resolves the existing Polza credential from `OPENAI_API_KEY` because `OPENAI_BASE_URL` points to Polza, and `config/vigvamcev.yaml` contains the discovered «Дискордники» channel ID. The bot still needs admin rights in that channel. The repository does not contain the full Telegram export or runtime artifacts.
