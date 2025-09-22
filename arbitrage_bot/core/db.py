# core/db.py
from __future__ import annotations
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pymysql
from sqlalchemy.engine.url import make_url

from core.config import ENVCFG

# =========================================================
# DB CONNECTION
# =========================================================
DB_URL = ENVCFG.effective_db_url()
url = make_url(DB_URL)


def _is_sqlite() -> bool:
    return url.drivername.startswith("sqlite")


def _conn():
    if _is_sqlite():
        con = sqlite3.connect(url.database, detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        # speed tweaks for local sqlite
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA foreign_keys=ON;")
        return con

    con = pymysql.connect(
        host=url.host or "localhost",
        port=url.port or 3306,
        user=url.username,
        password=url.password,
        database=url.database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )
    # Force UTC for the session so DATETIME/TIMESTAMP comparisons match your UTC params
    with con.cursor() as cur:
        cur.execute("SET time_zone = '+00:00'")
    return con



@contextmanager
def get_cursor(commit: bool = True):
    con = _conn()
    try:
        cur = con.cursor()
        yield cur
        if commit:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _ph() -> str:
    return "?" if _is_sqlite() else "%s"


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _first_id(row):
    if not row:
        return None
    return row[0] if isinstance(row, tuple) else row.get("id")


# =========================================================
# INIT SCHEMA (+ gentle migrations)
# =========================================================
def init_db() -> None:
    pk = "INTEGER PRIMARY KEY AUTOINCREMENT" if _is_sqlite() else "INT AUTO_INCREMENT PRIMARY KEY"
    ts = "DATETIME" if _is_sqlite() else "TIMESTAMP"

    ddl = [
        f"""CREATE TABLE IF NOT EXISTS sports (
            id {pk},
            name VARCHAR(255) NOT NULL UNIQUE
        )""",
        f"""CREATE TABLE IF NOT EXISTS bookmakers (
            id {pk},
            name VARCHAR(255) NOT NULL UNIQUE,
            url VARCHAR(1024)
        )""",
        f"""CREATE TABLE IF NOT EXISTS teams (
            id {pk},
            name VARCHAR(255) NOT NULL UNIQUE
        )""",
        f"""CREATE TABLE IF NOT EXISTS team_aliases (
            id {pk},
            team_id INT NOT NULL,
            alias VARCHAR(255) NOT NULL UNIQUE,
            FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS arb_events (
            id {pk},
            sport_id INT NOT NULL,
            competition_name VARCHAR(255),
            category VARCHAR(255),
            start_time {ts},
            home_team_id INT NOT NULL,
            away_team_id INT NOT NULL,
            UNIQUE(sport_id, home_team_id, away_team_id, start_time),
            FOREIGN KEY(sport_id) REFERENCES sports(id) ON DELETE CASCADE,
            FOREIGN KEY(home_team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(away_team_id) REFERENCES teams(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS bookmaker_event_map (
            arb_event_id INT NOT NULL,
            bookmaker_id INT NOT NULL,
            bookmaker_event_id VARCHAR(255) NOT NULL,
            PRIMARY KEY(arb_event_id, bookmaker_id),
            FOREIGN KEY(arb_event_id) REFERENCES arb_events(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS markets (
            id {pk},
            arb_event_id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            line VARCHAR(64),
            UNIQUE(arb_event_id, name, line),
            FOREIGN KEY(arb_event_id) REFERENCES arb_events(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS odds (
            id {pk},
            market_id INT NOT NULL,
            bookmaker_id INT NOT NULL,
            outcome VARCHAR(255) NOT NULL,
            value DOUBLE NOT NULL,
            last_updated {ts} NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            UNIQUE(market_id, bookmaker_id, outcome),
            FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS odds_history (
            id {pk},
            market_id INT NOT NULL,
            bookmaker_id INT NOT NULL,
            outcome VARCHAR(255) NOT NULL,
            value DOUBLE NOT NULL,
            recorded_at {ts} NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        )""",
        # NOTE: new columns: line, legs_hash; new uniqueness (event_fingerprint, market_key, line, legs_hash)
        f"""CREATE TABLE IF NOT EXISTS opportunities (
            id {pk},
            arb_event_id INT NOT NULL,
            sport_id INT NOT NULL,
            event_fingerprint VARCHAR(255) NOT NULL,
            market_key VARCHAR(128) NOT NULL,
            line VARCHAR(64),
            profit_pct DOUBLE NOT NULL,
            legs_json TEXT NOT NULL,
            legs_hash VARCHAR(64),
            created_at {ts} NOT NULL DEFAULT (CURRENT_TIMESTAMP),
            FOREIGN KEY(arb_event_id) REFERENCES arb_events(id) ON DELETE CASCADE
        )""",
    ]

    with get_cursor() as cur:
        # core tables
        for stmt in ddl:
            cur.execute(stmt)

        # ---- Gentle migrations for opportunities ----
        def _col_exists(table: str, column: str) -> bool:
            if _is_sqlite():
                cur.execute(f"PRAGMA table_info({table})")
                cols = {row["name"] if isinstance(row, sqlite3.Row) else row[1] for row in cur.fetchall()}
                return column in cols
            else:
                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
                    """,
                    (url.database, table, column),
                )
                return bool(list(cur.fetchone().values())[0])

        # add line
        if not _col_exists("opportunities", "line"):
            try:
                cur.execute("ALTER TABLE opportunities ADD COLUMN line VARCHAR(64)")
            except Exception:
                pass
        # add legs_hash
        if not _col_exists("opportunities", "legs_hash"):
            try:
                cur.execute("ALTER TABLE opportunities ADD COLUMN legs_hash VARCHAR(64)")
            except Exception:
                pass

        # indices
        if _is_sqlite():
            cur.execute("CREATE INDEX IF NOT EXISTS idx_arb_events_sport_time ON arb_events(sport_id, start_time)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_markets_event ON markets(arb_event_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_odds_market ON odds(market_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_odds_bm ON odds(bookmaker_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_hist_market_time ON odds_history(market_id, recorded_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opps_created ON opportunities(created_at)")
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS uniq_opps_event_market_line_legs "
                "ON opportunities(event_fingerprint, market_key, line, legs_hash)"
            )
        else:
            def ensure_index(index_name: str, table_name: str, create_sql: str):
                cur.execute(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.statistics
                    WHERE table_schema=%s AND table_name=%s AND index_name=%s
                    """,
                    (url.database, table_name, index_name),
                )
                exists = cur.fetchone()
                exists = list(exists.values())[0] if isinstance(exists, dict) else exists[0]
                if not exists:
                    cur.execute(create_sql)

            ensure_index("idx_arb_events_sport_time", "arb_events",
                         "CREATE INDEX idx_arb_events_sport_time ON arb_events(sport_id, start_time)")
            ensure_index("idx_markets_event", "markets",
                         "CREATE INDEX idx_markets_event ON markets(arb_event_id)")
            ensure_index("idx_odds_market", "odds",
                         "CREATE INDEX idx_odds_market ON odds(market_id)")
            ensure_index("idx_odds_bm", "odds",
                         "CREATE INDEX idx_odds_bm ON odds(bookmaker_id)")
            ensure_index("idx_hist_market_time", "odds_history",
                         "CREATE INDEX idx_hist_market_time ON odds_history(market_id, recorded_at)")
            ensure_index("idx_opps_created", "opportunities",
                         "CREATE INDEX idx_opps_created ON opportunities(created_at)")

            # Best-effort: drop old unique on (event_fingerprint, market_key, created_at) if present
            try:
                cur.execute(
                    """
                    SELECT DISTINCT index_name
                    FROM information_schema.statistics
                    WHERE table_schema=%s AND table_name='opportunities'
                    GROUP BY index_name
                    """,
                    (url.database,),
                )
                idx_names = [list(r.values())[0] for r in cur.fetchall()]
                for name in idx_names:
                    if not name:
                        continue
                    cur.execute(
                        """
                        SELECT COLUMN_NAME, NON_UNIQUE
                        FROM information_schema.statistics
                        WHERE table_schema=%s AND table_name='opportunities' AND index_name=%s
                        ORDER BY SEQ_IN_INDEX
                        """,
                        (url.database, name),
                    )
                    cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
                    if cols == ["event_fingerprint", "market_key", "created_at"]:
                        cur.execute(f"ALTER TABLE opportunities DROP INDEX {name}")
                        break
            except Exception:
                pass

            ensure_index(
                "uniq_opps_event_market_line_legs", "opportunities",
                "CREATE UNIQUE INDEX uniq_opps_event_market_line_legs "
                "ON opportunities(event_fingerprint, market_key, line, legs_hash)"
            )

    print("✅ DB schema ready")


# =========================================================
# HISTORY SCHEMA DETECTOR
# =========================================================
_HISTORY_MODE = None  # "old" or "new"


def _detect_history_mode() -> str:
    """Check if odds_history table is in old or new shape."""
    global _HISTORY_MODE
    if _HISTORY_MODE:
        return _HISTORY_MODE
    try:
        with get_cursor(commit=False) as cur:
            if _is_sqlite():
                cur.execute("PRAGMA table_info(odds_history)")
                cols = {row["name"] if isinstance(row, sqlite3.Row) else row[1] for row in cur.fetchall()}
            else:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME='odds_history'
                    """,
                    (url.database,),
                )
                cols = {r["COLUMN_NAME"] for r in cur.fetchall()}
        if {"market_id", "bookmaker_id", "outcome", "value", "recorded_at"}.issubset(cols):
            _HISTORY_MODE = "new"
        elif {"odds_id", "value", "recorded_at"}.issubset(cols):
            _HISTORY_MODE = "old"
        else:
            _HISTORY_MODE = "new"
    except Exception:
        _HISTORY_MODE = "new"
    return _HISTORY_MODE


# =========================================================
# BASIC RESOLVERS
# =========================================================
def upsert_sport(name: str) -> int:
    ph = _ph()
    with get_cursor() as cur:
        if _is_sqlite():
            cur.execute(f"INSERT OR IGNORE INTO sports(name) VALUES({ph})", (name,))
        else:
            cur.execute(f"INSERT IGNORE INTO sports(name) VALUES({ph})", (name,))
        cur.execute(f"SELECT id FROM sports WHERE name={ph}", (name,))
        return _first_id(cur.fetchone())


def upsert_team(name: str) -> int:
    """Insert team if not exists, return id. Check aliases too."""
    ph = _ph()
    with get_cursor() as cur:
        cur.execute(f"SELECT team_id FROM team_aliases WHERE alias={ph}", (name,))
        row = cur.fetchone()
        if row:
            return row["team_id"] if not isinstance(row, tuple) else row[0]

        if _is_sqlite():
            cur.execute(f"INSERT OR IGNORE INTO teams(name) VALUES({ph})", (name,))
        else:
            cur.execute(f"INSERT IGNORE INTO teams(name) VALUES({ph})", (name,))
        cur.execute(f"SELECT id FROM teams WHERE name={ph}", (name,))
        team_id = _first_id(cur.fetchone())
        if team_id:
            if _is_sqlite():
                cur.execute(f"INSERT OR IGNORE INTO team_aliases(team_id,alias) VALUES({ph},{ph})", (team_id, name))
            else:
                cur.execute(f"INSERT IGNORE INTO team_aliases(team_id,alias) VALUES({ph},{ph})", (team_id, name))
        return team_id


def resolve_bookmaker_id(name: str, url_str: Optional[str] = None) -> int:
    ph = _ph()
    with get_cursor() as cur:
        if _is_sqlite():
            cur.execute(f"INSERT OR IGNORE INTO bookmakers(name,url) VALUES({ph},{ph})", (name, url_str))
        else:
            cur.execute(f"INSERT IGNORE INTO bookmakers(name,url) VALUES({ph},{ph})", (name, url_str))
        cur.execute(f"SELECT id FROM bookmakers WHERE name={ph}", (name,))
        return _first_id(cur.fetchone())


def resolve_sport_id(name: str) -> int:
    return upsert_sport(name)


# =========================================================
# EVENT HANDLER (bookmaker mapping is optional)
# =========================================================
def upsert_event(event_data: Dict[str, Any]) -> int:
    """
    event_data = {
      'sport_name': str, 'competition_name': str, 'category': str, 'start_time': datetime,
      'home_team': str, 'away_team': str,
      # optional mapping (recommended):
      'bookmaker_id': int, 'bookmaker_event_id': str
    }
    """
    sport_id = upsert_sport(event_data["sport_name"])
    home_id = upsert_team(event_data["home_team"])
    away_id = upsert_team(event_data["away_team"])

    ph = _ph()
    with get_cursor() as cur:
        cur.execute(
            f"SELECT id FROM arb_events WHERE sport_id={ph} AND home_team_id={ph} AND away_team_id={ph} AND start_time={ph}",
            (sport_id, home_id, away_id, event_data["start_time"]),
        )
        row = cur.fetchone()
        if row:
            arb_event_id = _first_id(row)
        else:
            cur.execute(
                f"INSERT INTO arb_events(sport_id,competition_name,category,start_time,home_team_id,away_team_id) "
                f"VALUES({ph},{ph},{ph},{ph},{ph},{ph})",
                (sport_id, event_data.get("competition_name"), event_data.get("category"),
                 event_data["start_time"], home_id, away_id),
            )
            arb_event_id = cur.lastrowid

        bm_id = event_data.get("bookmaker_id")
        bm_eid = event_data.get("bookmaker_event_id")
        if bm_id is not None and bm_eid is not None:
            cur.execute(
                f"REPLACE INTO bookmaker_event_map(arb_event_id,bookmaker_id,bookmaker_event_id) VALUES({ph},{ph},{ph})",
                (arb_event_id, bm_id, str(bm_eid)),
            )
    return int(arb_event_id)


def upsert_market(arb_event_id: int, name: str, line: Optional[str] = None) -> int:
    ph = _ph()
    with get_cursor() as cur:
        if _is_sqlite():
            cur.execute(
                f"INSERT INTO markets(arb_event_id,name,line) VALUES({ph},{ph},{ph}) "
                f"ON CONFLICT(arb_event_id,name,line) DO NOTHING",
                (arb_event_id, name, line),
            )
        else:
            cur.execute(
                f"INSERT INTO markets(arb_event_id,name,line) VALUES({ph},{ph},{ph}) "
                f"ON DUPLICATE KEY UPDATE id = id",
                (arb_event_id, name, line),
            )
        cur.execute(
            f"SELECT id FROM markets WHERE arb_event_id={ph} AND name={ph} "
            f"AND ((line IS NULL AND {ph} IS NULL) OR line={ph})",
            (arb_event_id, name, line, line),
        )
        return _first_id(cur.fetchone())


# =========================================================
# ODDS SNAPSHOT + HISTORY
# =========================================================
def upsert_odds_snapshot(market_id: int, bookmaker_id: int, outcome: str, value: float) -> int:
    ph = _ph()
    now = _utcnow()
    mode = _detect_history_mode()

    with get_cursor() as cur:
        cur.execute(
            f"SELECT id, value FROM odds WHERE market_id={ph} AND bookmaker_id={ph} AND outcome={ph}",
            (market_id, bookmaker_id, outcome),
        )
        row = cur.fetchone()
        if row:
            odds_id = row["id"] if not isinstance(row, tuple) else row[0]
            last_val = row["value"] if not isinstance(row, tuple) else row[1]
            if float(last_val) != float(value):
                cur.execute(f"UPDATE odds SET value={ph}, last_updated={ph} WHERE id={ph}", (value, now, odds_id))
                try:
                    if mode == "new":
                        cur.execute(
                            f"INSERT INTO odds_history(market_id,bookmaker_id,outcome,value,recorded_at) VALUES({ph},{ph},{ph},{ph},{ph})",
                            (market_id, bookmaker_id, outcome, value, now),
                        )
                    else:
                        cur.execute(
                            f"INSERT INTO odds_history(odds_id,value,recorded_at) VALUES({ph},{ph},{ph})",
                            (odds_id, value, now),
                        )
                except Exception as e:
                    print(f"[WARN] history_insert_failed: {e}")
            else:
                cur.execute(f"UPDATE odds SET last_updated={ph} WHERE id={ph}", (now, odds_id))
            return odds_id

        cur.execute(
            f"INSERT INTO odds(market_id,bookmaker_id,outcome,value,last_updated) VALUES({ph},{ph},{ph},{ph},{ph})",
            (market_id, bookmaker_id, outcome, value, now),
        )
        odds_id = cur.lastrowid
        try:
            if mode == "new":
                cur.execute(
                    f"INSERT INTO odds_history(market_id,bookmaker_id,outcome,value,recorded_at) VALUES({ph},{ph},{ph},{ph},{ph})",
                    (market_id, bookmaker_id, outcome, value, now),
                )
            else:
                cur.execute(
                    f"INSERT INTO odds_history(odds_id,value,recorded_at) VALUES({ph},{ph},{ph})",
                    (odds_id, value, now),
                )
        except Exception as e:
            print(f"[WARN] history_insert_failed: {e}")
        return odds_id


def upsert_odds(market_id: int, bookmaker_id: int, outcome: str, value: float) -> int:
    return upsert_odds_snapshot(market_id, bookmaker_id, outcome, value)


# =========================================================
# QUERIES FOR CALCULATOR
# =========================================================
def get_latest_odds_for_window(
    sport_id: int,
    start_from: datetime,
    start_to: datetime,
    market_names: Iterable[str],
    include_lines: bool = True,
) -> List[Dict[str, Any]]:
    """
    Returns odds rows for the calculator. Includes home/away team names so
    the calculator can canonicalize 1/X/2 even when outcomes are saved as
    team names by a bookmaker.
    """
    ph = _ph()
    names = list(market_names)
    if not names:
        return []
    with get_cursor(commit=False) as cur:
        q = (
            f"SELECT ae.id AS arb_event_id, ae.start_time, "
            f"       th.name AS home_team, ta.name AS away_team, "
            f"       m.id AS market_id, m.name AS market_name, m.line, "
            f"       o.bookmaker_id, o.outcome, o.value "
            f"FROM arb_events ae "
            f"JOIN teams th ON th.id = ae.home_team_id "
            f"JOIN teams ta ON ta.id = ae.away_team_id "
            f"JOIN markets m ON m.arb_event_id = ae.id "
            f"JOIN odds o ON o.market_id = m.id "
            f"WHERE ae.sport_id = {ph} AND ae.start_time >= {ph} AND ae.start_time < {ph} "
            f"  AND m.name IN ({','.join([ph]*len(names))}) "
            f"ORDER BY ae.start_time ASC"
        )
        cur.execute(q, (sport_id, start_from, start_to, *names))
        rows = cur.fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(dict(r) if isinstance(r, sqlite3.Row) else r)
        return out


# =========================================================
# MISC HELPERS
# =========================================================
def _json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False)


def bulk_execute(sql: str, params: List[Tuple[Any, ...]], commit: bool = True) -> None:
    with get_cursor(commit=commit) as cur:
        cur.executemany(sql, params)


if __name__ == "__main__":
    init_db()
