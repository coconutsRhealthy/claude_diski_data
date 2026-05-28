"""Per-run handle selection from the pool, balancing exploit vs explore.

Tiered policy with spillover:

  proven       (codes_found > 0)                   — top by codes_found,
                                                     tiebreak last_code_seen_at
  brand-new    (runs_scraped == 0, codes_found 0)  — random sample
  low-yield    (runs_scraped > 0, codes_found 0)   — oldest last_run_at first

Default split is 70/15/15. If a tier can't fill its quota, the deficit
spills first to brand-new (more exploration), then to low-yield retry.
Total is capped at ``target`` (default 1375); if the union of all tiers
is smaller we just scrape fewer.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date

from .handles import HandlesRegistry


@dataclass(frozen=True)
class _Split:
    proven: float = 0.70
    new: float = 0.15
    low: float = 0.15


def _classify(
    entries: dict[str, dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    proven, new, low = [], [], []
    for e in entries.values():
        codes = e.get("codes_found") or 0
        runs = e.get("runs_scraped") or 0
        if codes > 0:
            proven.append(e)
        elif runs == 0:
            new.append(e)
        else:
            low.append(e)
    return proven, new, low


def _pick_proven(pool: list[dict], n: int) -> list[dict]:
    if n <= 0 or not pool:
        return []
    ranked = sorted(
        pool,
        key=lambda e: (
            e.get("codes_found") or 0,
            e.get("last_code_seen_at") or "",
        ),
        reverse=True,
    )
    return ranked[:n]


def _pick_new(pool: list[dict], n: int, rng: random.Random) -> list[dict]:
    if n <= 0 or not pool:
        return []
    if n >= len(pool):
        return list(pool)
    return rng.sample(pool, n)


def _pick_low_yield(pool: list[dict], n: int) -> list[dict]:
    """Oldest last_run_at first — gives every low-yielder eventual rotation."""
    if n <= 0 or not pool:
        return []
    ranked = sorted(
        pool,
        key=lambda e: e.get("last_run_at") or "",  # None/empty sorts first
    )
    return ranked[:n]


def select_handles(
    registry: HandlesRegistry,
    *,
    target: int,
    today: date,
    rng: random.Random | None = None,
    split: _Split = _Split(),
) -> list[str]:
    """Return up to ``target`` bare handles to scrape this run."""
    rng = rng or random.Random()

    proven_pool, new_pool, low_pool = _classify(registry.entries())

    target_proven = round(target * split.proven)
    target_new = round(target * split.new)
    target_low = target - target_proven - target_new

    chosen_proven = _pick_proven(proven_pool, target_proven)
    spill = target_proven - len(chosen_proven)

    # Spillover priority 1: brand-new (user preference — favor exploration).
    chosen_new = _pick_new(new_pool, target_new + spill, rng)
    spill = (target_new + spill) - len(chosen_new)

    # Spillover priority 2: low-yield retry.
    chosen_low = _pick_low_yield(low_pool, target_low + spill)
    spill = (target_low + spill) - len(chosen_low)

    # Final fallback: any still-unfilled slots cascade back to brand-new
    # (e.g. when low-yield pool is empty, as on the very first runs).
    if spill > 0:
        picked = {e["handle"] for e in chosen_new}
        remaining_new = [e for e in new_pool if e["handle"] not in picked]
        chosen_new += _pick_new(remaining_new, spill, rng)

    chosen = chosen_proven + chosen_new + chosen_low
    handles = [e["handle"] for e in chosen]

    extra_new = len(chosen_new) - target_new
    extra_low = len(chosen_low) - target_low
    print(
        f"Selector: {len(handles)} handles = "
        f"{len(chosen_proven)} proven + {len(chosen_new)} new "
        f"+ {len(chosen_low)} low-yield "
        f"(target {target_proven}/{target_new}/{target_low}, "
        f"spillover new=+{max(extra_new, 0)} low=+{max(extra_low, 0)}). "
        f"Pool sizes: proven={len(proven_pool)} new={len(new_pool)} "
        f"low={len(low_pool)}.",
        flush=True,
    )

    return handles
