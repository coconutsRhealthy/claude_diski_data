#!/usr/bin/env python3
"""Reshape a per-day Netherlands ``discounts.json`` into our standard schema.

The Netherlands side-channel (see ``temp_nl/README.md``) starts from a
free-form ``discounts.json`` produced by another project and needs a
single-pass conversion into the same schema the main pipeline writes to
R2 for germany / france / uk. This script is that conversion.

Input file shape (a JSON array of comma-separated strings):

    [
      "pinkgellac, SHOP20, 20, pinkgellac, 06-03",
      "temu,       apu12458, €100, wgk,   06-03"
    ]

Only fields 1, 2, 3 (company, code, raw discount) are used. The raw
discount is normalized via ``social._percentage_str``: bare numbers
become ``"<n>%"`` while strings with a currency symbol or other
formatting (``"€100"``, ``"3F2+15"``) pass through untouched.

Output:

    {
      "market": "netherlands",
      "date":   "<--date>",
      "discount_codes": [
        {"company": "...", "code": "...", "discount": "20%" | null}
      ]
    }

Sorted alphabetically by ``company`` then ``code``.

Usage:

    python temp_nl/build_netherlands_json.py --folder temp_nl/5jun --date 2026-06-05

This is *only* the NL migration. Pulling germany / france / uk JSONs
from R2 is a separate step — see ``temp_nl/README.md`` for that snippet.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from discount_finder.social import _percentage_str


MARKET = "netherlands"


def _parse_date(s: str) -> str:
    """Validate the ISO date and return it as a string."""
    return date.fromisoformat(s).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--folder", type=Path, required=True,
        help="Day folder containing discounts.json (e.g. temp_nl/5jun).",
    )
    parser.add_argument(
        "--date", type=_parse_date, required=True,
        help="Run date in ISO format (e.g. 2026-06-05). Used in the "
             "output's `date` field and the filename suffix.",
    )
    args = parser.parse_args()

    src = args.folder / "discounts.json"
    if not src.exists():
        print(f"!! Missing source: {src}", file=sys.stderr)
        return 1

    with src.open(encoding="utf-8") as f:
        rows = json.load(f)
    if not isinstance(rows, list):
        print(f"!! {src} is not a JSON array", file=sys.stderr)
        return 1

    codes: list[dict] = []
    bad = 0
    for r in rows:
        parts = [p.strip() for p in str(r).split(",")]
        if len(parts) < 3:
            bad += 1
            continue
        company, code, raw_discount = parts[0], parts[1], parts[2]
        if not company or not code:
            bad += 1
            continue
        codes.append(
            {
                "company": company,
                "code": code,
                "discount": _percentage_str(raw_discount) or None,
            }
        )
    codes.sort(key=lambda e: (e["company"].lower(), e["code"].upper()))

    out = args.folder / f"{MARKET}_{args.date}.json"
    payload = {"market": MARKET, "date": args.date, "discount_codes": codes}
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    summary = f"Wrote {out} ({len(codes)} codes)"
    if bad:
        summary += f"; skipped {bad} malformed row(s)"
    print(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
