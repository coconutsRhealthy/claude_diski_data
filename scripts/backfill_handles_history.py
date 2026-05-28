#!/usr/bin/env python3
"""One-shot backfill of handles.json from existing artifacts.

The pipeline only started writing to handles.json with the
"Select handles per run from growing pool, track yield" change.
Before that, every code we extracted was recorded in codes.json but
never attributed back to its handle, and every Apify scrape attempt
went unrecorded too. The new selector therefore sees ~all old
influencers as "brand-new" even though they've been scraped ~12 times
over the past month, wasting Apify budget on known duds.

This script reconstructs that history from ground truth:

  Pass 1: codes.json -> codes_found, last_code_seen_at
          (count each code's influencer.username; trust the count)
  Pass 2: influencers.txt -> runs_scraped, last_run_at
          (every handle in the old static list has been scraped
          ~--runs times since the cron started)

Run once per market, on the droplet against live data:

    docker run --rm \
      -v /srv/diski/data:/app/data \
      -v /srv/diski/inputs:/app/inputs \
      --entrypoint python diski-pipeline \
      scripts/backfill_handles_history.py --market germany

Or in --dry-run first to preview the deltas.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from discount_finder import config
from discount_finder.handles import HandlesRegistry


IG_HANDLE_RE = re.compile(r"instagram\.com/([^/?#\s]+)", re.I)
RESERVED_HANDLES = {"p", "reel", "reels", "explore", "stories", "tv", "accounts", "direct"}


def _parse_handle_from_url(url: str) -> str | None:
    m = IG_HANDLE_RE.search(url or "")
    if not m:
        return None
    h = m.group(1).strip().lower()
    if not h or h in RESERVED_HANDLES:
        return None
    return h


def _tier_counts(entries: dict) -> tuple[int, int, int]:
    proven = sum(1 for v in entries.values() if (v.get("codes_found") or 0) > 0)
    low = sum(
        1 for v in entries.values()
        if (v.get("codes_found") or 0) == 0 and (v.get("runs_scraped") or 0) > 0
    )
    untried = sum(
        1 for v in entries.values()
        if (v.get("codes_found") or 0) == 0 and (v.get("runs_scraped") or 0) == 0
    )
    return proven, low, untried


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market", required=True, choices=config.MARKETS)
    parser.add_argument(
        "--runs", type=int, default=12,
        help="Assumed runs_scraped for handles in the old influencers.txt "
             "(default 12 ≈ a month of 3x/week cron).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the deltas but don't write handles.json.",
    )
    args = parser.parse_args()

    market = args.market
    today = date.today()

    handles_path = config.handles_registry_path(market)
    codes_path = config.codes_registry_path(market)
    influencers_path = config.ROOT / "inputs" / market / "influencers.txt"

    for label, p in [
        ("handles.json", handles_path),
        ("codes.json", codes_path),
        ("influencers.txt", influencers_path),
    ]:
        if not p.exists():
            print(f"!! Missing: {label} at {p}", file=sys.stderr)
            return 1

    registry = HandlesRegistry(handles_path)
    proven_before, low_before, untried_before = _tier_counts(registry.entries())

    # ---------- Pass 1: codes.json -> codes_found, last_code_seen_at -----
    with open(codes_path) as f:
        codes_map = json.load(f)

    per_handle_count: dict[str, int] = {}
    per_handle_latest: dict[str, str] = {}
    skipped_no_username = 0
    for entry in codes_map.values():
        username = (entry.get("influencer") or {}).get("username")
        if not username:
            skipped_no_username += 1
            continue
        h = username.strip().lower()
        if not h:
            skipped_no_username += 1
            continue
        per_handle_count[h] = per_handle_count.get(h, 0) + 1
        latest = (
            entry.get("last_seen_at")
            or entry.get("last_published_at")
            or entry.get("first_seen_at")
            or ""
        )
        if latest > per_handle_latest.get(h, ""):
            per_handle_latest[h] = latest

    pass1_created = 0
    pass1_bumped_count = 0
    pass1_bumped_date = 0
    for h, count in per_handle_count.items():
        if h not in registry.entries():
            # The code's influencer was never registered as a handle.
            # Create it so the count has somewhere to live.
            registry.ensure(h, source="backfill-codes", today=today)
            pass1_created += 1
        entry = registry.entries()[h]
        # Trust codes.json as ground truth for the count.
        old_count = entry.get("codes_found") or 0
        if count != old_count:
            entry["codes_found"] = count
            pass1_bumped_count += 1
        latest_iso = per_handle_latest[h]
        if latest_iso and latest_iso > (entry.get("last_code_seen_at") or ""):
            entry["last_code_seen_at"] = latest_iso
            pass1_bumped_date += 1

    # ---------- Pass 2: influencers.txt -> runs_scraped, last_run_at ----
    in_static_list: set[str] = set()
    for line in influencers_path.read_text(encoding="utf-8").splitlines():
        h = _parse_handle_from_url(line.strip())
        if h:
            in_static_list.add(h)

    pass2_created = 0
    pass2_bumped_runs = 0
    pass2_set_run_date = 0
    today_iso = today.isoformat()
    for h in in_static_list:
        if h not in registry.entries():
            registry.ensure(h, source="backfill-influencers", today=today)
            pass2_created += 1
        entry = registry.entries()[h]
        if (entry.get("runs_scraped") or 0) < args.runs:
            entry["runs_scraped"] = args.runs
            pass2_bumped_runs += 1
        if not entry.get("last_run_at"):
            entry["last_run_at"] = today_iso
            pass2_set_run_date += 1

    proven_after, low_after, untried_after = _tier_counts(registry.entries())

    print(f"Market: {market}")
    print(f"  codes.json:      {len(codes_map)} codes, {len(per_handle_count)} distinct handles "
          f"({skipped_no_username} skipped: no username)")
    print(f"  influencers.txt: {len(in_static_list)} handles")
    print()
    print(f"  Pass 1 (codes.json -> codes_found / last_code_seen_at):")
    print(f"    created new handle entries: {pass1_created}")
    print(f"    bumped codes_found:         {pass1_bumped_count}")
    print(f"    bumped last_code_seen_at:   {pass1_bumped_date}")
    print(f"  Pass 2 (influencers.txt -> runs_scraped={args.runs} / last_run_at):")
    print(f"    created new handle entries: {pass2_created}")
    print(f"    bumped runs_scraped:        {pass2_bumped_runs}")
    print(f"    set last_run_at:            {pass2_set_run_date}")
    print()
    print(f"  Tier counts:")
    print(f"    before: proven={proven_before:5d}  low-yield={low_before:5d}  untried={untried_before:5d}")
    print(f"    after:  proven={proven_after:5d}  low-yield={low_after:5d}  untried={untried_after:5d}")
    print(f"    total:  {len(registry.entries())}")

    if args.dry_run:
        print("\n(dry-run: no changes written)")
        return 0

    registry.save()
    print(f"\nSaved {handles_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
