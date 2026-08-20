# VIGVAMCEV implementation reflection

## What worked

- Keeping the series in a separate service avoided coupling it to Timur's personal life/lore state.
- A two-stage image path (Polza scene plus local Pillow poster) makes typography and publication layout deterministic while preserving the Photoshop-collage style.
- Durable draft and publication states make retries stage-specific and prevent blind resend after an unknown Telegram timeout.

## Remaining risk

- Face fidelity is provider-dependent. The configured collage is sent as the primary identity reference, and an optional transparent local identity layer is supported, but the first live preview must still be reviewed by the owner.
- The configured channel and Polza key are intentionally deployment values rather than repository data.
