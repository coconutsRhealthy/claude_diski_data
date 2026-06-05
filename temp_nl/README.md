# temp_nl — every-other-day handoff

## Purpose

A temporary holding area for the per-day discount JSONs that get handed
off to the person who renders/posts the Instagram slides. Once per
publishing cycle (every other day) we drop:

1. A `netherlands_<date>.json` derived from a freshly-prepared
   `discounts.json` source file in that day's subfolder. Netherlands is
   *not* run by the main pipeline (no Apify scrape, no Claude
   extraction) — the source data comes from another project entirely
   and just needs to be reshaped into our standard schema.

2. The three real-pipeline market JSONs for the same date —
   `germany_<date>.json`, `france_<date>.json`, `uk_<date>.json` —
   pulled from R2 (the canonical handoff location, see Friday's
   conversation: R2 is what downstream consumers actually see, the
   droplet's local cache is just a backup).

End result per cycle: one folder named like `1jun/` or `3jun/`,
containing four `*_<YYYY-MM-DD>.json` files plus the original
`discounts.json`.

## Folder convention

`temp_nl/<Nmon>/` — e.g. `1jun/`, `3jun/`, `5jun/`. Just an informal
day marker; the actual date lives in the filenames.

Inside, the user drops `discounts.json` (the NL source). The procedure
below produces the four standardised JSONs.

## Input format: `discounts.json`

JSON array of comma-separated strings. Five fields per row:

```
"<company>, <code>, <percentage_or_money>, <source/influencer>, <MM-DD>"
```

Example:

```json
[
  "pinkgellac, SHOP20, 20, pinkgellac, 06-03",
  "lookfantastic, LFDISKI, 25, wgk, 06-03",
  "temu, apu12458, €100, wgk, 06-03"
]
```

Only fields 1, 2, 3 (company, code, discount) are used. Bare numbers
become `"<n>%"` via the `_percentage_str` helper; already-formatted
strings (`"€100"`, `"3F2+15"`, etc.) pass through untouched.

## Output schema (all four files)

```json
{
  "market": "netherlands",
  "date":   "2026-06-03",
  "discount_codes": [
    {"company": "pinkgellac", "code": "SHOP20", "discount": "20%"}
  ]
}
```

Sorted alphabetically by company (case-insensitive), then by code.
`discount` is `null` when no usable value can be derived.

## Procedure (two commands per cycle)

When the user prepares a new day folder with `discounts.json`, run
these two commands from the repo root. Substitute the folder name and
ISO date — nothing else changes.

### Step 1 — build the netherlands JSON

```
.venv/bin/python scripts/build_netherlands_json.py \
    --folder temp_nl/<dayfolder> \
    --date <YYYY-MM-DD>
```

Example:

```
.venv/bin/python scripts/build_netherlands_json.py \
    --folder temp_nl/5jun \
    --date 2026-06-05
```

The script reads `temp_nl/<dayfolder>/discounts.json`, normalises
each row via `social._percentage_str`, and writes
`netherlands_<YYYY-MM-DD>.json` next to it. Idempotent — safe to
re-run.

### Step 2 — pull germany / france / uk from R2

```
.venv/bin/python <<'PY'
from pathlib import Path
from dotenv import load_dotenv; load_dotenv('.env')
from discount_finder.r2 import _get_r2_client
from discount_finder import config

# === EDIT THESE TWO LINES PER RUN ===
out_dir   = Path('temp_nl/5jun')
today_iso = '2026-06-05'
# ====================================

r2 = _get_r2_client()
for m in ['germany', 'france', 'uk']:
    key  = f'{m}/{today_iso}/{m}_{today_iso}.json'
    dest = out_dir / f'{m}_{today_iso}.json'
    r2.download_file(config.CAROUSEL_BUCKET, key, str(dest))
    print(f"Downloaded {dest}")
PY
```

After both steps, verify with `ls temp_nl/<dayfolder>/` — should show
five files: the original `discounts.json` plus four
`*_<YYYY-MM-DD>.json`.

## Notes / gotchas

- **Date must be a cron day for the R2 downloads to work.** The droplet
  only runs the pipeline Mon/Wed/Fri 02:00 Amsterdam. On other days
  the R2 keys for that date won't exist and `download_file` will raise
  `NoSuchKey`. Either skip the downloads on off-days or wait for the
  next cron run.

- **R2 creds must be in the laptop's `.env`** (`R2_ACCOUNT_ID`,
  `R2_ACCESS_KEY`, `R2_SECRET_KEY`). See `.env.example` in repo root.
  These are gitignored.

- **`netherlands` is not in `config.MARKETS`** and the pipeline knows
  nothing about it. This folder is a deliberately-light side-channel —
  no Apify, no Claude, no R2 upload from our side. Just file reshaping.

- **One past quirk worth knowing**: some discount values come through
  as bare strings like `"3F2+15"` ("3-for-2 plus 15%"). The
  `_percentage_str` helper deliberately leaves these alone — only
  appends `%` to *pure* digit strings. Don't try to "fix" mixed-format
  values; the downstream renderer can interpret them.

- **Output filenames mirror the main pipeline's R2 keys** —
  `<market>_<YYYY-MM-DD>.json`. Don't deviate; the handoff person may
  consume them by pattern.
