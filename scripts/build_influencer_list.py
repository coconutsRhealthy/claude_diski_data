#!/usr/bin/env python3
"""Rank an Awin publisher TSV into a candidate influencers.txt.

Filters to publishers whose website is an Instagram profile, drops a
denylist of known aggregator/network publishers, drops zero-sale and
low-conversion rows, then sorts by Awin commission and caps at --top.

Outputs:
  inputs/<market>/influencers_ranked.txt   — one IG URL per line, drop-in
                                              replacement for the live list
  inputs/<market>/influencers_ranked.tsv   — audit sidecar with commission /
                                              clicks / sales / CR / €-per-click
                                              for spot-checking the ranking

Awin commission/sales are confidential affiliate data. Both files are
written under inputs/, which is gitignored — never copy them to output/.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

# Known aggregator/network publishers that occasionally list an IG URL
# as their website. Matched case-insensitively, exact-string against the
# "Publisher" column. Add more as you spot them in the .tsv sidecar.
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

IG_URL_RE = re.compile(r"instagram\.com/([^/?#\s]+)", re.I)

# Path segments we accidentally pick up from /p/, /reel/, /explore/ links —
# none of these are real handles.
RESERVED_HANDLES: set[str] = {
    "p", "reel", "reels", "explore", "stories", "tv", "accounts", "direct",
}


def parse_handle(url: str) -> str | None:
    if not url:
        return None
    m = IG_URL_RE.search(url)
    if not m:
        return None
    handle = m.group(1).strip().lower()
    if not handle or handle in RESERVED_HANDLES:
        return None
    return handle


def parse_number(s: str) -> float:
    """Awin uses comma as a thousands separator (e.g. '1,134,747.02')."""
    s = (s or "").strip().replace(",", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tsv", type=Path, required=True,
        help="Path to the Awin publisher export .tsv.",
    )
    parser.add_argument(
        "--market", default="germany",
        help="Market name. Used to derive the default --out path.",
    )
    parser.add_argument(
        "--top", type=int, default=1500,
        help="Keep the top N publishers after filtering & sorting (default: 1500).",
    )
    parser.add_argument(
        "--cr-min", type=float, default=0.02,
        help="Minimum sales/clicks ratio (0.02 = 2%%). Drops linktree-style "
             "publishers who get clicks but rarely convert via codes.",
    )
    parser.add_argument(
        "--sales-min", type=int, default=1,
        help="Minimum number of attributed sales (default: 1).",
    )
    parser.add_argument(
        "--out", type=Path,
        help="Where to write the .txt list. Defaults to "
             "inputs/<market>/influencers_ranked.txt.",
    )
    args = parser.parse_args()

    if not args.tsv.exists():
        print(f"!! TSV not found: {args.tsv}", file=sys.stderr)
        return 1

    project_root = Path(__file__).resolve().parent.parent
    out_path = args.out or project_root / "inputs" / args.market / "influencers_ranked.txt"
    sidecar_path = out_path.with_suffix(".tsv")

    seen = 0
    excluded_no_ig = 0
    excluded_aggregator = 0
    excluded_low_sales = 0
    excluded_low_cr = 0

    rows_by_handle: dict[str, dict] = {}

    with args.tsv.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            seen += 1
            publisher = (row.get("Publisher") or "").strip()
            handle = parse_handle(row.get("Publisher Website") or "")
            if not handle:
                excluded_no_ig += 1
                continue
            if publisher.lower() in AGGREGATOR_NAMES:
                excluded_aggregator += 1
                continue
            sales = parse_number(row.get("Sales"))
            clicks = parse_number(row.get("Clicks"))
            commission = parse_number(row.get("Commission"))
            order_value = parse_number(row.get("Order Value"))
            if sales < args.sales_min:
                excluded_low_sales += 1
                continue
            cr = (sales / clicks) if clicks else 0.0
            if cr < args.cr_min:
                excluded_low_cr += 1
                continue
            entry = {
                "handle": handle,
                "publisher": publisher,
                "publisher_id": (row.get("Publisher ID") or "").strip(),
                "commission": commission,
                "clicks": clicks,
                "sales": sales,
                "order_value": order_value,
                "cr": cr,
                "comm_per_click": (commission / clicks) if clicks else 0.0,
            }
            # The same handle can appear under multiple Publisher IDs (e.g.
            # creator + their management agency). Keep the higher-commission row.
            existing = rows_by_handle.get(handle)
            if existing is None or entry["commission"] > existing["commission"]:
                rows_by_handle[handle] = entry

    candidates = sorted(
        rows_by_handle.values(),
        key=lambda e: (e["commission"], e["sales"]),
        reverse=True,
    )
    capped = candidates[: args.top]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for e in capped:
            f.write(f"https://www.instagram.com/{e['handle']}/\n")

    with sidecar_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow([
            "rank", "handle", "url", "publisher", "publisher_id",
            "commission_eur", "clicks", "sales", "order_value_eur",
            "cr", "eur_per_click",
        ])
        for i, e in enumerate(capped, start=1):
            writer.writerow([
                i,
                e["handle"],
                f"https://www.instagram.com/{e['handle']}/",
                e["publisher"],
                e["publisher_id"],
                f"{e['commission']:.2f}",
                f"{e['clicks']:.0f}",
                f"{e['sales']:.0f}",
                f"{e['order_value']:.2f}",
                f"{e['cr']:.4f}",
                f"{e['comm_per_click']:.4f}",
            ])

    print(
        f"\nRead {seen:,} rows from {args.tsv}\n"
        f"  excluded: not an IG URL           = {excluded_no_ig:,}\n"
        f"  excluded: aggregator/network      = {excluded_aggregator:,}\n"
        f"  excluded: < {args.sales_min} sale(s)              = {excluded_low_sales:,}\n"
        f"  excluded: CR < {args.cr_min*100:.1f}%               = {excluded_low_cr:,}\n"
        f"  unique IG handles after filters   = {len(rows_by_handle):,}\n"
        f"  written to {out_path} (top {len(capped):,} by commission)\n"
        f"  audit sidecar at {sidecar_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
