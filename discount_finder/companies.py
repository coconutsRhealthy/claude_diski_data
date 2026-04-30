"""Canonical company registry.

The LLM extracts a company string per discount code, but the same brand may
appear as ``Shein``, ``shein_us``, ``SHEIN Official``, etc. This module maps
those raw strings to a stable canonical id, persisted to ``data/companies.json``
so the registry grows automatically across runs and the frontend can rely on
a single id per brand.

Manual upkeep is rare; when needed, use the CLI:

    python -m discount_finder.companies list
    python -m discount_finder.companies merge <from_id> <into_id>
    python -m discount_finder.companies rename <id> "<Display Name>"
    python -m discount_finder.companies add-alias <id> "<alias>"
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from . import config

REGISTRY_PATH = config.ROOT / "data" / "companies.json"

# Trailing tokens dropped during normalization — regional locales and generic
# qualifiers that don't change brand identity.
_REGION_SUFFIXES = {
    "us", "usa", "eu", "uk", "gb", "fr", "de", "nl", "be", "it", "es",
    "au", "ca", "jp", "br", "mx", "world", "global", "international",
    "intl", "official", "store", "shop", "hq", "co", "inc",
    # TLD-ish suffixes: after "." → " " replacement, "shein.com" becomes
    # "shein com" so dropping "com" here collapses bare/.com forms.
    "com", "net", "org", "io", "app",
}

_NON_ALNUM = re.compile(r"[^a-z0-9\s]+")
_WS = re.compile(r"\s+")


def normalize_alias(raw: str) -> str:
    """Normalize a raw company string into a lookup-friendly alias key."""
    s = raw.strip().lower().lstrip("@")
    s = s.replace("_", " ").replace("-", " ").replace(".", " ").replace("&", " and ")
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    tokens = s.split()
    while tokens and tokens[-1] in _REGION_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def _slugify(alias: str) -> str:
    return re.sub(r"\s+", "", alias)


def _display_name(raw: str) -> str:
    s = raw.strip().lstrip("@").replace("_", " ").replace("-", " ").strip()
    if not s:
        return "Unknown"
    # Preserve mixed-case input (e.g. "HelloFresh"); title-case all-lower / all-upper.
    if s.isupper() or s.islower():
        return " ".join(w.capitalize() for w in s.split())
    return s


class CompanyRegistry:
    def __init__(self, path: Path = REGISTRY_PATH):
        self.path = path
        self._entries: dict[str, dict] = {}
        self._alias_to_canonical: dict[str, str] = {}
        self._slug_to_canonical: dict[str, str] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        with open(self.path) as f:
            self._entries = json.load(f)
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        self._alias_to_canonical = {}
        self._slug_to_canonical = {}
        for cid, entry in self._entries.items():
            for alias in entry.get("aliases", []):
                self._alias_to_canonical[alias] = cid
                self._slug_to_canonical[_slugify(alias)] = cid

    def save(self) -> None:
        if not self._dirty:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self._entries, f, indent=2, sort_keys=True, ensure_ascii=False)
        self._dirty = False

    def resolve(self, raw: str) -> tuple[str, str]:
        """Return ``(canonical_id, display_name)`` for a raw company string.

        Creates and persists a new canonical entry if the alias is unknown.
        """
        alias = normalize_alias(raw or "")
        if not alias:
            alias = "unknown"

        if alias in self._alias_to_canonical:
            cid = self._alias_to_canonical[alias]
            return cid, self._entries[cid]["display_name"]

        # Fall back to slug match so "hello fresh" collapses onto "hellofresh".
        slug = _slugify(alias) or "unknown"
        if slug in self._slug_to_canonical:
            cid = self._slug_to_canonical[slug]
            self._entries[cid]["aliases"].append(alias)
            self._alias_to_canonical[alias] = cid
            self._dirty = True
            return cid, self._entries[cid]["display_name"]

        cid = slug
        base = cid
        n = 2
        while cid in self._entries:
            cid = f"{base}{n}"
            n += 1

        self._entries[cid] = {
            "display_name": _display_name(raw),
            "aliases": [alias],
        }
        self._alias_to_canonical[alias] = cid
        self._slug_to_canonical[slug] = cid
        self._dirty = True
        return cid, self._entries[cid]["display_name"]

    # --- manual maintenance helpers ---

    def list_entries(self) -> list[tuple[str, dict]]:
        return sorted(self._entries.items())

    def merge(self, from_id: str, into_id: str) -> None:
        if from_id not in self._entries:
            raise KeyError(f"Unknown canonical_id: {from_id}")
        if into_id not in self._entries:
            raise KeyError(f"Unknown canonical_id: {into_id}")
        if from_id == into_id:
            return
        moved = self._entries.pop(from_id)
        target = self._entries[into_id]
        target["aliases"] = list(dict.fromkeys(target["aliases"] + moved["aliases"]))
        self._dirty = True
        self._rebuild_index()

    def rename(self, cid: str, display_name: str) -> None:
        if cid not in self._entries:
            raise KeyError(f"Unknown canonical_id: {cid}")
        self._entries[cid]["display_name"] = display_name
        self._dirty = True

    def add_alias(self, cid: str, alias: str) -> None:
        if cid not in self._entries:
            raise KeyError(f"Unknown canonical_id: {cid}")
        normalized = normalize_alias(alias)
        if not normalized or normalized in self._entries[cid]["aliases"]:
            return
        self._entries[cid]["aliases"].append(normalized)
        self._alias_to_canonical[normalized] = cid
        self._dirty = True


def _cli(argv: list[str]) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="python -m discount_finder.companies")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list", help="List all canonical companies and their aliases.")

    p_merge = sub.add_parser("merge", help="Merge one canonical into another.")
    p_merge.add_argument("from_id")
    p_merge.add_argument("into_id")

    p_rename = sub.add_parser("rename", help="Set the display name for a canonical.")
    p_rename.add_argument("canonical_id")
    p_rename.add_argument("display_name")

    p_alias = sub.add_parser("add-alias", help="Manually add an alias to a canonical.")
    p_alias.add_argument("canonical_id")
    p_alias.add_argument("alias")

    args = parser.parse_args(argv)
    reg = CompanyRegistry()

    if args.cmd == "list":
        for cid, entry in reg.list_entries():
            aliases = ", ".join(entry["aliases"])
            print(f"{cid}\t{entry['display_name']}\t[{aliases}]")
        return 0
    if args.cmd == "merge":
        reg.merge(args.from_id, args.into_id)
        reg.save()
        # Migrate any codes registered under the old canonical so the
        # codes registry and public feed stay in sync.
        from .registry import CodesRegistry, regenerate_public_feed

        new_display_name = reg._entries[args.into_id]["display_name"]
        codes = CodesRegistry()
        moved = codes.migrate_canonical(args.from_id, args.into_id, new_display_name)
        codes.save()
        feed_size = regenerate_public_feed()
        print(
            f"Merged company {args.from_id} → {args.into_id}; "
            f"migrated {moved} code(s); public feed now {feed_size} entries."
        )
        return 0
    if args.cmd == "rename":
        reg.rename(args.canonical_id, args.display_name)
        reg.save()
        print(f"Renamed {args.canonical_id} → {args.display_name}")
        return 0
    if args.cmd == "add-alias":
        reg.add_alias(args.canonical_id, args.alias)
        reg.save()
        print(f"Added alias to {args.canonical_id}")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(_cli(sys.argv[1:]))
