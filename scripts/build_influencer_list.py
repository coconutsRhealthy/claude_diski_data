#!/usr/bin/env python3
"""Build the operational influencers.txt for a market.

Maintains a per-market handle pool (``data/<market>/handles.json``) that
tracks Awin attractiveness *and* our own scrape/code history. Each call:

  1. Optionally imports Awin TSV rows into the pool (use --tsv).
  2. Scores every handle in the pool: composite of normalised Awin
     commission percentile and a smoothed, time-decayed hit rate.
  3. Selects the top --top handles. The first (1 - explore_pct) of slots
     go strictly by score; the rest are random-sampled from under-scraped
     handles so new winners can surface.
  4. Writes:
       inputs/<market>/influencers.txt          one IG URL per line, the
                                                operational list the
                                                pipeline reads.
       inputs/<market>/influencers_ranked.tsv   full pool with score
                                                components for auditing.

Awin commission/sales are confidential affiliate data. Both files live
under inputs/, which is gitignored — never copy them to output/.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# Make ``import discount_finder…`` work when the script is invoked directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from discount_finder import config
from discount_finder.handles import (
    DEFAULT_AWIN_WEIGHT,
    DEFAULT_EXPLORATION_PCT,
    DEFAULT_HALF_LIFE_DAYS,
    DEFAULT_HIT_WEIGHT,
    DEFAULT_SMOOTHING_ALPHA,
    DEFAULT_SMOOTHING_BETA,
    DEFAULT_TARGET,
    DEFAULT_UNDER_SCRAPED_THRESHOLD,
    HandlePool,
    handle_url,
    normalize_handle,
)


# Aggregator/network publishers that occasionally list an IG URL as their
# website — mostly already excluded by the IG-website gate, but kept as a
# safety net. Matched case-insensitively, exact-string against "Publisher".
AGGREGATOR_NAMES: set[str] = {
    "ltk",
    "stylink social media gmbh",
    "metapic germany",
    "vamp",
    "shoplooks.com",
    "kreatornow",
    "awin creator - influencer management services",
    "hangzhou shaomai cultural development co. ltd",
    "fanstoshop",
    "digchic",
    "netcraft digital ltd.",
    "mediafinity adtech private limited",
    "yeahpromos llc",
    "markads media inc.",
    "smart & successful s.r.l.",
    "clix inc",
    "collable partners limited",
    "vizeo & co",
    "moongency gmbh",
    "2bfree gmbh",
}


def _parse_number(s: str | None) -> float:
    """Awin uses comma as a thousands separator (e.g. '1,134,747.02')."""
    s = (s or "").strip().replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_tsv(tsv_path: Path) -> tuple[list[dict], dict]:
    """Return (rows, exclusion counts). Rows are deduped by handle, keeping
    the row with the highest commission across publisher IDs.
    """
    excluded_no_ig = 0
    excluded_aggregator = 0
    rows_by_handle: dict[str, dict] = {}
    seen = 0

    with tsv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            seen += 1
            publisher = (row.get("Publisher") or "").strip()
            handle = normalize_handle(row.get("Publisher Website") or "")
            if not handle:
                excluded_no_ig += 1
                continue
            if publisher.lower() in AGGREGATOR_NAMES:
                excluded_aggregator += 1
                continue
            sales = _parse_number(row.get("Sales"))
            clicks = _parse_number(row.get("Clicks"))
            commission = _parse_number(row.get("Commission"))
            cr = (sales / clicks) if clicks else 0.0
            entry = {
                "handle": handle,
                "publisher": publisher,
                "publisher_id": (row.get("Publisher ID") or "").strip(),
                "commission": commission,
                "clicks": clicks,
                "sales": sales,
                "cr": cr,
            }
            existing = rows_by_handle.get(handle)
            if existing is None or commission > existing["commission"]:
                rows_by_handle[handle] = entry

    return list(rows_by_handle.values()), {
        "seen": seen,
        "excluded_no_ig": excluded_no_ig,
        "excluded_aggregator": excluded_aggregator,
        "imported": len(rows_by_handle),
    }


def _write_outputs(
    project_root: Path,
    market: str,
    selection: dict,
    out_path: Path | None,
) -> tuple[Path, Path]:
    out_path = out_path or project_root / "inputs" / market / "influencers.txt"
    sidecar_path = out_path.parent / "influencers_ranked.tsv"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    selected_handles = {h["handle"] for h in selection["selected"]}

    with out_path.open("w", encoding="utf-8") as f:
        for h in selection["selected"]:
            f.write(f"{handle_url(h['handle'])}\n")

    with sidecar_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            "rank", "selected", "handle", "url",
            "awin_publisher", "awin_commission_eur", "awin_clicks", "awin_sales",
            "awin_cr", "runs_scraped", "codes_found", "last_code_seen_at",
            "awin_score", "hit_score", "composite_score",
        ])
        for i, h in enumerate(selection["all_scored"], start=1):
            awin = h.get("awin") or {}
            writer.writerow([
                i,
                "yes" if h["handle"] in selected_handles else "no",
                h["handle"],
                handle_url(h["handle"]),
                awin.get("publisher", ""),
                f"{awin.get('commission', 0):.2f}",
                f"{awin.get('clicks', 0):.0f}",
                f"{awin.get('sales', 0):.0f}",
                f"{awin.get('cr', 0):.4f}",
                h.get("runs_scraped", 0),
                h.get("codes_found", 0),
                h.get("last_code_seen_at") or "",
                f"{h['_awin_score']:.4f}",
                f"{h['_hit_score']:.4f}",
                f"{h['_score']:.4f}",
            ])

    return out_path, sidecar_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--market", required=True, choices=config.MARKETS,
        help="Market to operate on. Used for the data/<market>/handles.json "
             "ledger and the inputs/<market>/influencers.txt output.",
    )
    parser.add_argument(
        "--tsv", type=Path,
        help="Optional Awin publisher TSV. When provided, ingests rows into "
             "the handle pool (upserting Awin commission/clicks/sales). Skip "
             "this flag to re-select using only the existing ledger.",
    )
    parser.add_argument(
        "--top", type=int, default=DEFAULT_TARGET,
        help=f"Operational target (default: {DEFAULT_TARGET}). If the pool is "
             f"smaller than --top, the entire pool is written.",
    )
    parser.add_argument(
        "--awin-weight", type=float, default=DEFAULT_AWIN_WEIGHT,
        help=f"Score weight for normalised Awin commission percentile "
             f"(default: {DEFAULT_AWIN_WEIGHT}).",
    )
    parser.add_argument(
        "--hit-weight", type=float, default=DEFAULT_HIT_WEIGHT,
        help=f"Score weight for the smoothed/decayed hit rate "
             f"(default: {DEFAULT_HIT_WEIGHT}).",
    )
    parser.add_argument(
        "--half-life", type=int, default=DEFAULT_HALF_LIFE_DAYS,
        help=f"Hit-rate decay half-life in days (default: "
             f"{DEFAULT_HALF_LIFE_DAYS}). After this many days without a new "
             f"code, a handle's effective hit count is halved.",
    )
    parser.add_argument(
        "--alpha", type=float, default=DEFAULT_SMOOTHING_ALPHA,
        help=f"Bayesian smoothing α (default: {DEFAULT_SMOOTHING_ALPHA}).",
    )
    parser.add_argument(
        "--beta", type=float, default=DEFAULT_SMOOTHING_BETA,
        help=f"Bayesian smoothing β (default: {DEFAULT_SMOOTHING_BETA}). "
             f"Larger β means we trust raw hit-rate less for low-sample handles.",
    )
    parser.add_argument(
        "--exploration-pct", type=float, default=DEFAULT_EXPLORATION_PCT,
        help=f"Fraction of slots reserved for exploration (default: "
             f"{DEFAULT_EXPLORATION_PCT}). Sampled randomly from handles whose "
             f"runs_scraped < --under-scraped-threshold.",
    )
    parser.add_argument(
        "--under-scraped-threshold", type=int,
        default=DEFAULT_UNDER_SCRAPED_THRESHOLD,
        help=f"A handle is exploration-eligible until we've scraped it this "
             f"many times (default: {DEFAULT_UNDER_SCRAPED_THRESHOLD}).",
    )
    parser.add_argument(
        "--seed", type=int,
        help="Optional RNG seed for reproducible exploration sampling.",
    )
    parser.add_argument(
        "--out", type=Path,
        help="Override output .txt path. Defaults to "
             "inputs/<market>/influencers.txt.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    today = datetime.now(timezone.utc).date()

    pool = HandlePool(args.market)

    # First-time use: backfill from data/<market>/codes.json so we don't
    # discard existing code history.
    if not pool.path.exists():
        bs = pool.bootstrap_from_codes(today)
        if bs["total"]:
            print(
                f"Bootstrapped handle ledger from codes.json: "
                f"{bs['added']:,} entries.", file=sys.stderr,
            )

    # Optionally ingest a fresh Awin TSV.
    tsv_summary = None
    if args.tsv:
        if not args.tsv.exists():
            print(f"!! TSV not found: {args.tsv}", file=sys.stderr)
            return 1
        rows, tsv_summary = _parse_tsv(args.tsv)
        result = pool.import_awin(rows, today, source=f"awin:{args.market}")
        tsv_summary["awin_added"] = result["added"]
        tsv_summary["awin_updated"] = result["updated"]

    selection = pool.select_top(
        target=args.top,
        today=today,
        awin_weight=args.awin_weight,
        hit_weight=args.hit_weight,
        half_life=args.half_life,
        alpha=args.alpha,
        beta=args.beta,
        exploration_pct=args.exploration_pct,
        under_scraped_threshold=args.under_scraped_threshold,
        seed=args.seed,
    )

    pool.save()
    out_path, sidecar_path = _write_outputs(
        project_root, args.market, selection, args.out,
    )

    # Report.
    lines: list[str] = []
    if tsv_summary:
        lines.append(f"\nRead {tsv_summary['seen']:,} rows from {args.tsv}")
        lines.append(f"  excluded: not an IG URL           = {tsv_summary['excluded_no_ig']:,}")
        lines.append(f"  excluded: aggregator/network      = {tsv_summary['excluded_aggregator']:,}")
        lines.append(
            f"  TSV rows imported into pool       = {tsv_summary['imported']:,} "
            f"(+{tsv_summary['awin_added']:,} new handles, "
            f"{tsv_summary['awin_updated']:,} Awin records refreshed)"
        )
    s = pool.stats()
    lines.append(f"\nHandle pool ({args.market}): {s['pool_size']:,} total")
    lines.append(f"  with Awin record                  = {s['with_awin']:,}")
    lines.append(f"  ever scraped                      = {s['ever_scraped']:,}")
    lines.append(f"  ever yielded a code               = {s['with_codes']:,}")
    lines.append(f"  total codes recorded              = {s['total_codes_recorded']:,}")

    selected_n = len(selection["selected"])
    lines.append(f"\nSelection (target {args.top:,}):")
    lines.append(f"  exploit (top by score)            = {selection['exploit_count']:,}")
    lines.append(f"  explore (under-scraped sample)    = {selection['explore_count']:,}")
    if selection["fallback_count"]:
        lines.append(f"  back-fill (explore pool too small)= {selection['fallback_count']:,}")
    lines.append(f"  total selected                    = {selected_n:,}")
    lines.append(f"\nwritten to {out_path} ({selected_n:,} handles)")
    lines.append(f"audit sidecar at {sidecar_path}")
    print("\n".join(lines), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
