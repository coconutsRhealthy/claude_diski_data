"""Per-market handle pool with self-learning influencer selection.

Tracks every Instagram handle we've ever heard of (Awin imports plus
anyone we've scraped or extracted codes from) along with running stats.
Each pipeline run updates the ledger; each ``select_top()`` call blends
Awin attractiveness with our own historical hit rate to choose the
operational ``influencers.txt`` for the next run.

Ledger lives at ``data/<market>/handles.json``. Schema per entry::

    {
        "handle": "ystyleireland",
        "url": "https://www.instagram.com/ystyleireland/",
        "first_seen_at": "2026-05-04",
        "last_run_at": "2026-05-15",
        "last_code_seen_at": "2026-05-15",
        "runs_scraped": 5,
        "codes_found": 12,
        "source": "awin:uk",
        "awin": {                       # optional
            "publisher": "...",
            "publisher_id": "...",
            "commission": 591355.97,
            "clicks": 1896568,
            "sales": 80620,
            "cr": 0.0425,
            "imported_at": "2026-05-04"
        }
    }

Score = ``awin_weight * awin_percentile + hit_weight * smoothed_decayed_hit_rate``.
Selection takes ``(1 - exploration_pct)`` of slots strictly by score, then
fills the remainder by random sample from under-scraped handles so new
candidates get a chance to surface.
"""
from __future__ import annotations

import json
import math
import random
import re
from datetime import date
from pathlib import Path

from . import config


# --- selection knobs ---------------------------------------------------------

DEFAULT_TARGET = 1375
DEFAULT_AWIN_WEIGHT = 0.5
DEFAULT_HIT_WEIGHT = 0.5
# Codes-found signal halves every N days since the last code was seen. Keeps
# seasonal posters relevant for a month, then drops them down the ranking.
DEFAULT_HALF_LIFE_DAYS = 30
# Bayesian smoother: we treat every handle as having a baseline of α "hits"
# in β "trials" before any data, so a single lucky scrape can't blow past a
# proven Awin heavyweight.
DEFAULT_SMOOTHING_ALPHA = 1.0
DEFAULT_SMOOTHING_BETA = 10.0
DEFAULT_EXPLORATION_PCT = 0.15
# A handle counts as "under-scraped" (eligible for exploration sampling) until
# we've scraped it this many times.
DEFAULT_UNDER_SCRAPED_THRESHOLD = 3
# Handles without Awin data get this percentile — neutral, not penalised.
NO_AWIN_PERCENTILE = 0.5


# --- helpers -----------------------------------------------------------------

_IG_URL_RE = re.compile(r"instagram\.com/([^/?#\s]+)", re.I)
_RESERVED_HANDLES = {"p", "reel", "reels", "explore", "stories", "tv", "accounts", "direct"}


_HANDLE_CHARS_RE = re.compile(r"[a-z0-9._]+")


def normalize_handle(raw) -> str | None:
    """Reduce a URL, profile dict, or raw handle string to a lowercase IG handle.

    Accepts the influencer dict shape persisted in codes.json
    (``{"username": "...", ...}``) so callers don't have to dig the
    username out themselves. Non-IG URLs and free-text publisher names
    return None.
    """
    if not raw:
        return None
    if isinstance(raw, dict):
        raw = raw.get("username") or raw.get("ownerUsername") or ""
    if not isinstance(raw, str):
        return None
    s = raw.strip().lstrip("@")
    if not s:
        return None
    if "instagram.com/" in s.lower():
        m = _IG_URL_RE.search(s)
        if not m:
            return None
        s = m.group(1)
    elif "://" in s or "/" in s:
        # URL or path that isn't Instagram → not a handle.
        return None
    s = s.split("?")[0].split("#")[0].strip().lower()
    if not s or s in _RESERVED_HANDLES:
        return None
    # IG handles allow letters/digits/periods/underscores only — anything
    # else (whitespace, punctuation, accents) is a free-text artefact.
    if not _HANDLE_CHARS_RE.fullmatch(s):
        return None
    return s


def handle_url(handle: str) -> str:
    return f"https://www.instagram.com/{handle}/"


# --- pool --------------------------------------------------------------------


class HandlePool:
    def __init__(self, market: str):
        self.market = market
        self.path = config.handles_path(market)
        self._entries: dict[str, dict] = {}
        self._load()

    def __len__(self) -> int:
        return len(self._entries)

    def _load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as f:
            self._entries = json.load(f)

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self._entries, f, indent=2, sort_keys=True, ensure_ascii=False)

    def _ensure(self, handle: str, today: date, source: str) -> dict:
        entry = self._entries.get(handle)
        if entry is None:
            entry = {
                "handle": handle,
                "url": handle_url(handle),
                "first_seen_at": today.isoformat(),
                "last_run_at": None,
                "last_code_seen_at": None,
                "runs_scraped": 0,
                "codes_found": 0,
                "source": source,
            }
            self._entries[handle] = entry
        else:
            # Promote the source if the existing one was vaguer (e.g. a handle
            # we first learned from a code is later confirmed in Awin).
            if entry.get("source") in (None, "unknown") and source:
                entry["source"] = source
        return entry

    # --- ingestion ---

    def import_awin(self, rows: list[dict], today: date, source: str) -> dict:
        """Upsert Awin-derived rows. Each row needs:
        ``handle, publisher, publisher_id, commission, clicks, sales, cr``.
        """
        added = 0
        updated = 0
        for r in rows:
            handle = normalize_handle(r.get("handle"))
            if not handle:
                continue
            is_new = handle not in self._entries
            entry = self._ensure(handle, today, source=source)
            entry["awin"] = {
                "publisher": r.get("publisher", ""),
                "publisher_id": r.get("publisher_id", ""),
                "commission": float(r.get("commission") or 0),
                "clicks": float(r.get("clicks") or 0),
                "sales": float(r.get("sales") or 0),
                "cr": float(r.get("cr") or 0),
                "imported_at": today.isoformat(),
            }
            if is_new:
                added += 1
            else:
                updated += 1
        return {"added": added, "updated": updated, "total": len(self._entries)}

    def bootstrap_from_codes(self, today: date) -> dict:
        """Backfill stats from ``data/<market>/codes.json``.

        Counts each registry entry as one ``codes_found`` for its
        influencer, and pulls ``last_code_seen_at`` from the entry's
        ``last_published_at``. Sets ``runs_scraped = max(1, current)``
        for any handle with codes — we don't have per-run history pre-
        ledger, so this is a lower bound.
        """
        from .registry import CodesRegistry

        codes_path = config.codes_registry_path(self.market)
        if not codes_path.exists():
            return {"added": 0, "updated": 0, "total": len(self._entries)}

        registry = CodesRegistry(path=codes_path)
        added = 0
        updated = 0
        for code_entry in registry._entries.values():
            handle = normalize_handle(code_entry.get("influencer"))
            if not handle:
                continue
            is_new = handle not in self._entries
            entry = self._ensure(handle, today, source="codes_bootstrap")
            entry["codes_found"] = entry.get("codes_found", 0) + 1
            last_pub = (
                code_entry.get("last_published_at")
                or code_entry.get("first_seen_at")
                or today.isoformat()
            )
            if not entry["last_code_seen_at"] or last_pub > entry["last_code_seen_at"]:
                entry["last_code_seen_at"] = last_pub
            if entry["runs_scraped"] == 0:
                entry["runs_scraped"] = 1
            if is_new:
                added += 1
            else:
                updated += 1
        return {"added": added, "updated": updated, "total": len(self._entries)}

    def record_run(
        self,
        intended_handles: set[str],
        codes_by_handle: dict[str, int],
        run_date: date,
    ) -> dict:
        """Update counters after a pipeline run.

        ``intended_handles`` should be the full URL list we sent to the
        scraper (so handles whose accounts returned no posts still get
        their ``runs_scraped`` bumped). ``codes_by_handle`` maps the
        post.influencer string to the number of codes extracted.
        """
        run_iso = run_date.isoformat()
        intended_count = 0
        for raw in intended_handles:
            handle = normalize_handle(raw)
            if not handle:
                continue
            entry = self._ensure(handle, run_date, source="run")
            entry["runs_scraped"] = entry.get("runs_scraped", 0) + 1
            entry["last_run_at"] = run_iso
            intended_count += 1

        with_codes = 0
        for raw, n_codes in codes_by_handle.items():
            handle = normalize_handle(raw)
            if not handle or n_codes <= 0:
                continue
            entry = self._ensure(handle, run_date, source="run")
            entry["codes_found"] = entry.get("codes_found", 0) + n_codes
            entry["last_code_seen_at"] = run_iso
            # Handles that yield codes but weren't in intended_handles (e.g.
            # legacy data sources) still get credited for one scrape.
            if entry["last_run_at"] != run_iso:
                entry["runs_scraped"] = entry.get("runs_scraped", 0) + 1
                entry["last_run_at"] = run_iso
            with_codes += 1
        return {"intended": intended_count, "with_codes": with_codes}

    # --- scoring & selection ---

    def _awin_percentiles(self) -> dict[str, float]:
        """Map handle → percentile of Awin commission within Awin-known
        handles only. Highest commission → 1.0, lowest → 1/n.
        """
        with_awin = [
            (h, e.get("awin", {}).get("commission", 0))
            for h, e in self._entries.items()
            if e.get("awin")
        ]
        if not with_awin:
            return {}
        with_awin.sort(key=lambda x: x[1])
        n = len(with_awin)
        return {h: (i + 1) / n for i, (h, _) in enumerate(with_awin)}

    def _hit_score(
        self,
        entry: dict,
        today: date,
        half_life: int,
        alpha: float,
        beta: float,
    ) -> float:
        codes = entry.get("codes_found", 0)
        runs = entry.get("runs_scraped", 0)
        if codes == 0 or not entry.get("last_code_seen_at"):
            return alpha / (runs + beta)
        last = date.fromisoformat(entry["last_code_seen_at"])
        days = max(0, (today - last).days)
        decay = math.exp(-days * math.log(2) / half_life)
        effective_codes = codes * decay
        return (effective_codes + alpha) / (runs + beta)

    def score_all(
        self,
        today: date,
        awin_weight: float = DEFAULT_AWIN_WEIGHT,
        hit_weight: float = DEFAULT_HIT_WEIGHT,
        half_life: int = DEFAULT_HALF_LIFE_DAYS,
        alpha: float = DEFAULT_SMOOTHING_ALPHA,
        beta: float = DEFAULT_SMOOTHING_BETA,
    ) -> list[dict]:
        """Return every entry with score components attached, sorted desc."""
        awin_pct = self._awin_percentiles()
        scored: list[dict] = []
        for handle, entry in self._entries.items():
            awin_s = awin_pct.get(handle, NO_AWIN_PERCENTILE)
            hit_s = self._hit_score(entry, today, half_life, alpha, beta)
            scored.append({
                **entry,
                "_awin_score": awin_s,
                "_hit_score": hit_s,
                "_score": awin_weight * awin_s + hit_weight * hit_s,
            })
        scored.sort(key=lambda h: h["_score"], reverse=True)
        return scored

    def select_top(
        self,
        target: int,
        today: date,
        awin_weight: float = DEFAULT_AWIN_WEIGHT,
        hit_weight: float = DEFAULT_HIT_WEIGHT,
        half_life: int = DEFAULT_HALF_LIFE_DAYS,
        alpha: float = DEFAULT_SMOOTHING_ALPHA,
        beta: float = DEFAULT_SMOOTHING_BETA,
        exploration_pct: float = DEFAULT_EXPLORATION_PCT,
        under_scraped_threshold: int = DEFAULT_UNDER_SCRAPED_THRESHOLD,
        seed: int | None = None,
    ) -> dict:
        """Pick up to ``target`` handles. Returns selected entries plus
        a per-bucket count breakdown for the report.
        """
        scored = self.score_all(today, awin_weight, hit_weight, half_life, alpha, beta)

        if not scored:
            return {
                "selected": [],
                "all_scored": [],
                "exploit_count": 0,
                "explore_count": 0,
                "fallback_count": 0,
            }

        if len(scored) <= target:
            return {
                "selected": scored,
                "all_scored": scored,
                "exploit_count": len(scored),
                "explore_count": 0,
                "fallback_count": 0,
            }

        exploit_count = max(0, int(target * (1 - exploration_pct)))
        exploit = scored[:exploit_count]
        exploit_ids = {h["handle"] for h in exploit}

        explore_pool = [
            h for h in scored
            if h["handle"] not in exploit_ids
            and h.get("runs_scraped", 0) < under_scraped_threshold
        ]
        explore_target = target - exploit_count

        fallback_count = 0
        if len(explore_pool) <= explore_target:
            # Not enough under-scraped handles to fill the explore quota —
            # take all of them and back-fill from the next-best by score so
            # we still hit the target.
            explore = explore_pool
            used = exploit_ids | {h["handle"] for h in explore}
            backfill = [h for h in scored if h["handle"] not in used][
                : explore_target - len(explore)
            ]
            fallback_count = len(backfill)
            selected = exploit + explore + backfill
        else:
            rng = random.Random(seed)
            explore = rng.sample(explore_pool, explore_target)
            selected = exploit + explore

        return {
            "selected": selected,
            "all_scored": scored,
            "exploit_count": exploit_count,
            "explore_count": len(selected) - exploit_count - fallback_count,
            "fallback_count": fallback_count,
        }

    # --- summary ---

    def stats(self) -> dict:
        n = len(self._entries)
        with_awin = sum(1 for e in self._entries.values() if e.get("awin"))
        with_codes = sum(1 for e in self._entries.values() if e.get("codes_found", 0) > 0)
        scraped = sum(1 for e in self._entries.values() if e.get("runs_scraped", 0) > 0)
        total_codes = sum(e.get("codes_found", 0) for e in self._entries.values())
        return {
            "pool_size": n,
            "with_awin": with_awin,
            "with_codes": with_codes,
            "ever_scraped": scraped,
            "total_codes_recorded": total_codes,
        }


# --- CLI ---------------------------------------------------------------------


def _cli(argv: list[str]) -> int:
    import argparse
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(prog="python -m discount_finder.handles")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_stats = sub.add_parser("stats", help="Print pool summary for a market.")
    p_stats.add_argument("--market", required=True, choices=config.MARKETS)

    p_boot = sub.add_parser(
        "bootstrap",
        help="Backfill the ledger from data/<market>/codes.json. Idempotent — "
             "if you re-run, existing counters are preserved (codes_found is "
             "incremented again, so use --rebuild to start from scratch).",
    )
    p_boot.add_argument("--market", required=True, choices=config.MARKETS)
    p_boot.add_argument("--rebuild", action="store_true",
                        help="Discard the current ledger before bootstrapping.")

    args = parser.parse_args(argv)

    if args.cmd == "stats":
        pool = HandlePool(args.market)
        s = pool.stats()
        for k, v in s.items():
            print(f"{k:>22}: {v:,}")
        return 0

    if args.cmd == "bootstrap":
        pool = HandlePool(args.market)
        if args.rebuild:
            pool._entries = {}
        result = pool.bootstrap_from_codes(today)
        pool.save()
        print(
            f"Bootstrapped {args.market}: "
            f"+{result['added']:,} new, {result['updated']:,} updated, "
            f"total = {result['total']:,}."
        )
        return 0

    return 1


if __name__ == "__main__":
    import sys
    sys.exit(_cli(sys.argv[1:]))
