# scripts/seed_team_aliases.py
from __future__ import annotations
import argparse, csv, json, sys, os
from pathlib import Path
from typing import Iterable, Tuple, Dict, Any, List, DefaultDict
from collections import defaultdict

from core.db import get_cursor, _ph
from core.db import upsert_team  # reuse your existing helper

DEFAULT_CLUB_JSON = Path("data/team_aliases.json")
DEFAULT_NATIONAL_JSON = Path("data/national_team_aliases.json")


def _load_pairs(path: Path) -> Iterable[Tuple[str, str]]:
    """
    Yields (alias, team_name) pairs from:
      JSON (any of):
        1) [{"team": "Manchester United", "aliases": ["Man Utd","Man United"]}, ...]
        2) {"Man Utd": "Manchester United", "Man United": "Manchester United", ...}
        3) [["Man Utd", "Manchester United"], ["Man United","Manchester United"], ...]
      CSV:
        - with header: alias,team
        - or 2 columns per row: alias, team
    """
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            # case 1 or 3
            for item in data:
                if isinstance(item, dict) and "team" in item and "aliases" in item:
                    team = str(item["team"]).strip()
                    for a in item["aliases"]:
                        alias = str(a).strip()
                        if alias and team:
                            yield (alias, team)
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    alias, team = str(item[0]).strip(), str(item[1]).strip()
                    if alias and team:
                        yield (alias, team)
        elif isinstance(data, dict):
            # case 2
            for alias, team in data.items():
                alias = str(alias).strip()
                team = str(team).strip()
                if alias and team:
                    yield (alias, team)
        else:
            raise ValueError("Unsupported JSON structure")
    elif path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)
            if not rows:
                return
            # header?
            if len(rows[0]) >= 2 and {"alias", "team"}.issubset({c.strip().lower() for c in rows[0]}):
                header = [c.strip().lower() for c in rows[0]]
                ai, ti = header.index("alias"), header.index("team")
                for r in rows[1:]:
                    if len(r) <= max(ai, ti):
                        continue
                    alias = str(r[ai]).strip()
                    team = str(r[ti]).strip()
                    if alias and team:
                        yield (alias, team)
            else:
                # assume 2 columns
                for r in rows:
                    if len(r) < 2:
                        continue
                    alias = str(r[0]).strip()
                    team = str(r[1]).strip()
                    if alias and team:
                        yield (alias, team)
    else:
        raise ValueError("Unsupported file type (use .json or .csv)")


def seed_aliases_db(pairs: Iterable[Tuple[str, str]], *, dry_run: bool = False, casefold: bool = False) -> Dict[str, Any]:
    """
    Inserts alias -> team mapping into the DB (teams + team_aliases).
    - Skips identity (alias == team) after optional casefolding.
    - Idempotent via INSERT OR IGNORE (SQLite) / INSERT IGNORE (MySQL).
    """
    ph = _ph()
    is_sqlite = (ph == "?")
    sql = (
        "INSERT OR IGNORE INTO team_aliases(team_id, alias) VALUES({ph},{ph})".format(ph=ph)
        if is_sqlite else
        "INSERT IGNORE INTO team_aliases(team_id, alias) VALUES({ph},{ph})".format(ph=ph)
    )

    added = 0
    skipped_same = 0
    resolved = 0

    # Preload existing aliases to avoid touching DB for duplicates in the input file
    existing_aliases = set()
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT alias FROM team_aliases")
        for row in cur.fetchall():
            existing_aliases.add((row["alias"] if isinstance(row, dict) else row[0]).strip())

    to_insert: list[Tuple[int, str]] = []
    for alias, team in pairs:
        a = alias.casefold().strip() if casefold else alias.strip()
        t = team.casefold().strip() if casefold else team.strip()
        if not a or not t:
            continue
        if a == t:
            skipped_same += 1
            continue
        if alias in existing_aliases:
            continue

        team_id = upsert_team(team)  # creates team + self-alias if new
        resolved += 1
        to_insert.append((team_id, alias))

    if dry_run:
        return {"dry_run": True, "to_insert": len(to_insert), "resolved_teams": resolved, "skipped_identity": skipped_same}

    if to_insert:
        with get_cursor() as cur:
            cur.executemany(sql, to_insert)
            added = cur.rowcount if cur.rowcount is not None else 0

    return {"dry_run": False, "inserted": added, "resolved_teams": resolved, "skipped_identity": skipped_same}


def _load_json_map(path: Path) -> Dict[str, List[str]]:
    """
    Load canonical->aliases list mapping from JSON, creating a blank file if missing.
    """
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("Expected JSON object {canonical: [aliases...]}")
        # normalize shapes to list[str]
        out: Dict[str, List[str]] = {}
        for k, v in data.items():
            if isinstance(v, list):
                out[str(k)] = [str(x).strip() for x in v if str(x).strip()]
            elif isinstance(v, str):
                out[str(k)] = [v.strip()]
            else:
                out[str(k)] = []
        return out
    except Exception:
        return {}


def _merge_pairs_into_json_map(pairs: Iterable[Tuple[str, str]], existing: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """
    Merge (alias, team) into {team: [aliases...]}, dedupe & sort.
    """
    merged: DefaultDict[str, set] = defaultdict(set)
    for team, aliases in existing.items():
        for a in aliases:
            if a.strip() and a.strip() != team.strip():
                merged[team].add(a.strip())

    for alias, team in pairs:
        alias = alias.strip()
        team = team.strip()
        if not alias or not team or alias == team:
            continue
        merged[team].add(alias)

    # back to lists (sorted)
    return {team: sorted(list(aliases), key=str.lower) for team, aliases in merged.items()}


def merge_to_json(pairs: Iterable[Tuple[str, str]], json_path: Path) -> Dict[str, Any]:
    """
    Merge pairs into a JSON file (canonical->aliases list).
    """
    existing = _load_json_map(json_path)
    updated = _merge_pairs_into_json_map(pairs, existing)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(updated, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    # summarize changes
    added_cnt = sum(max(0, len(updated.get(t, [])) - len(existing.get(t, []))) for t in updated.keys())
    return {
        "json": str(json_path),
        "teams": len(updated),
        "aliases_added_estimate": added_cnt
    }


def main():
    ap = argparse.ArgumentParser(description="Seed team aliases into DB and/or JSON (SQLite/MySQL + JSON used by team_utils).")
    ap.add_argument("--file", "-f", required=True, help="Path to JSON or CSV aliases file")
    ap.add_argument("--dry-run", action="store_true", help="Parse and resolve, but do not write DB")
    ap.add_argument("--casefold", action="store_true", help="Case-insensitive identity check (alias==team) for DB path")

    # JSON outputs (optional)
    ap.add_argument("--to-json", help="Merge into a club aliases JSON (e.g., data/team_aliases.json)")
    ap.add_argument("--to-json-nationals", help="Merge into a national team aliases JSON (e.g., data/national_team_aliases.json)")
    ap.add_argument("--update-team-utils-json", choices=["clubs", "nationals", "both"],
                    help="Shortcut to write to default JSON files used by utils.team_utils")

    args = ap.parse_args()

    path = Path(args.file).expanduser()
    if not path.exists():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    try:
        pairs = list(_load_pairs(path))
    except Exception as e:
        print(f"Failed to load {path}: {e}", file=sys.stderr)
        sys.exit(2)

    # 1) DB seeding
    db_result = seed_aliases_db(pairs, dry_run=args.dry_run, casefold=args.casefold)

    # 2) JSON merges (optional)
    json_results: List[Dict[str, Any]] = []

    # explicit paths
    if args.to_json:
        json_results.append(merge_to_json(pairs, Path(args.to_json)))
    if args.to_json_nationals:
        json_results.append(merge_to_json(pairs, Path(args.to_json_nationals)))

    # defaults used by team_utils
    if args.update_team_utils_json:
        if args.update_team_utils_json in ("clubs", "both"):
            json_results.append(merge_to_json(pairs, DEFAULT_CLUB_JSON))
        if args.update_team_utils_json in ("nationals", "both"):
            json_results.append(merge_to_json(pairs, DEFAULT_NATIONAL_JSON))

    # Output summary
    output = {
        "db": db_result,
        "json_updates": json_results,
        "next_steps": (
            "Call utils.team_utils.reload_team_aliases() in your app (or restart) to pick up JSON changes."
            if json_results else
            "No JSON updated. Only DB aliases were seeded."
        )
    }
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
