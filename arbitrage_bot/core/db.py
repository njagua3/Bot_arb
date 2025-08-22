# core/db.py

import sqlite3
import pymysql
from contextlib import contextmanager
from datetime import datetime, timedelta
from sqlalchemy.engine.url import make_url
from core.config import DB_URL
import json

# ================================================================
# DATABASE URL PARSING
# ================================================================
url = make_url(DB_URL)


def _conn():
    """
    Create a connection to MySQL or SQLite depending on DB_URL.
    """
    if url.drivername.startswith("sqlite"):
        con = sqlite3.connect(url.database)
        con.row_factory = sqlite3.Row
        return con
    else:
        return pymysql.connect(
            host=url.host or "localhost",
            port=url.port or 3306,
            user=url.username,
            password=url.password,
            database=url.database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )


def get_connection():
    return _conn()


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


# ================================================================
# SCHEMA INITIALIZATION
# ================================================================
def init_db():
    """
    Initialize schema including opportunities.
    SQLite: JSON → TEXT fallback
    MySQL: real JSON type
    """
    json_type = "JSON" if not url.drivername.startswith("sqlite") else "TEXT"

    ddl = [
        """
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS bookmakers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) UNIQUE,
            url VARCHAR(1024)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS markets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(255) UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_uid VARCHAR(64) UNIQUE,
            home_team_id INT,
            away_team_id INT,
            start_time DATETIME,
            market_id INT,
            FOREIGN KEY(home_team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(away_team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS odds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INT,
            bookmaker_id INT,
            option_key VARCHAR(32),
            decimal_odds DOUBLE,
            offer_url VARCHAR(1024),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_id, bookmaker_id, option_key),
            FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS odds_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INT,
            bookmaker_id INT,
            market_id INT,
            option_key VARCHAR(32),
            decimal_odds DOUBLE,
            offer_url VARCHAR(1024),
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        );
        """,
        f"""
        CREATE TABLE IF NOT EXISTS opportunities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_label VARCHAR(255),
            market VARCHAR(64),
            sport VARCHAR(64),
            start_time DATETIME,
            best_odds {json_type},
            best_books {json_type},
            best_urls {json_type},
            stakes {json_type},
            profit REAL,
            roi REAL,
            alert_sent BOOLEAN DEFAULT 0,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_label, market, start_time)
        );
        """,
        # Indexes for performance
        "CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time);",
        "CREATE INDEX IF NOT EXISTS idx_odds_match_bookmaker ON odds(match_id, bookmaker_id);",
    ]
    with get_cursor() as cur:
        for stmt in ddl:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"⚠️ Skipped statement due to: {e}")


# ================================================================
# PERSIST OPPORTUNITIES
# ================================================================
def save_opportunity(match_label, market, sport, start_time,
                     best_odds, best_books, best_urls, stakes, profit, roi):
    """
    Save detected arbitrage opportunity.
    If UNIQUE constraint exists, replaces existing row for same match/market/start_time.
    Works with both MySQL and SQLite.
    """
    placeholder = "?" if url.drivername.startswith("sqlite") else "%s"

    sql = f"""
        INSERT INTO opportunities 
        (match_label, market, sport, start_time,
         best_odds, best_books, best_urls, stakes, profit, roi)
        VALUES ({','.join([placeholder]*10)})
        ON CONFLICT(match_label, market, start_time) DO UPDATE SET
            best_odds=excluded.best_odds,
            best_books=excluded.best_books,
            best_urls=excluded.best_urls,
            stakes=excluded.stakes,
            profit=excluded.profit,
            roi=excluded.roi,
            detected_at=CURRENT_TIMESTAMP,
            alert_sent=0
    """ if url.drivername.startswith("sqlite") else f"""
        INSERT INTO opportunities 
        (match_label, market, sport, start_time,
         best_odds, best_books, best_urls, stakes, profit, roi)
        VALUES ({','.join([placeholder]*10)})
        ON DUPLICATE KEY UPDATE
            best_odds=VALUES(best_odds),
            best_books=VALUES(best_books),
            best_urls=VALUES(best_urls),
            stakes=VALUES(stakes),
            profit=VALUES(profit),
            roi=VALUES(roi),
            detected_at=CURRENT_TIMESTAMP,
            alert_sent=0
    """

    with get_cursor() as cur:
        cur.execute(sql, (
            match_label,
            market,
            sport,
            start_time,
            json.dumps(best_odds),
            json.dumps(best_books),
            json.dumps(best_urls),
            json.dumps(stakes),
            float(profit),
            float(roi),
        ))


# ================================================================
# FETCH RECENT OPPORTUNITIES
# ================================================================
def get_recent_opportunities(limit=20):
    placeholder = "?" if url.drivername.startswith("sqlite") else "%s"
    sql = f"""
        SELECT * FROM opportunities 
        ORDER BY detected_at DESC
        LIMIT {placeholder}
    """
    with get_cursor(commit=False) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


# ================================================================
# CLEANUP OLD DATA
# ================================================================
def cleanup_db(days=30):
    """
    Delete old matches, odds, and opportunities older than X days.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    placeholder = "?" if url.drivername.startswith("sqlite") else "%s"

    with get_cursor() as cur:
        cur.execute(f"DELETE FROM opportunities WHERE start_time < {placeholder}", (cutoff,))
        cur.execute(f"DELETE FROM odds WHERE last_updated < {placeholder}", (cutoff,))
        cur.execute(f"DELETE FROM odds_history WHERE recorded_at < {placeholder}", (cutoff,))
        cur.execute(f"DELETE FROM matches WHERE start_time < {placeholder}", (cutoff,))


# ================================================================
# UPSERT MATCH + ODDS
# ================================================================
def upsert_match(home_team, away_team, bookmaker, market_name,
                 odds_dict, start_time=None, start_time_iso=None, offer_url=None, match_uid=None):
    """
    Unified upsert for matches, teams, markets, bookmakers, odds, and odds history.
    Supports both floats and dict odds.
    """

    ph = "?" if url.drivername.startswith("sqlite") else "%s"

    def first_col(row):
        if row is None:
            return None
        return row[0] if isinstance(row, tuple) else list(row.values())[0]

    with get_cursor() as cur:
        # --- Teams ---
        for team in (home_team, away_team):
            if url.drivername.startswith("sqlite"):
                cur.execute(f"INSERT OR IGNORE INTO teams (name) VALUES ({ph})", (team,))
            else:
                cur.execute(f"INSERT IGNORE INTO teams (name) VALUES ({ph})", (team,))
        cur.execute(f"SELECT id FROM teams WHERE name={ph}", (home_team,))
        home_team_id = first_col(cur.fetchone())
        cur.execute(f"SELECT id FROM teams WHERE name={ph}", (away_team,))
        away_team_id = first_col(cur.fetchone())

        # --- Market ---
        if url.drivername.startswith("sqlite"):
            cur.execute(f"INSERT OR IGNORE INTO markets (name) VALUES ({ph})", (market_name,))
        else:
            cur.execute(f"INSERT IGNORE INTO markets (name) VALUES ({ph})", (market_name,))
        cur.execute(f"SELECT id FROM markets WHERE name={ph}", (market_name,))
        market_id = first_col(cur.fetchone())

        # --- Bookmaker ---
        any_offer_url = next((d.get("offer_url") for d in odds_dict.values()
                              if isinstance(d, dict) and d.get("offer_url")), offer_url)
        if url.drivername.startswith("sqlite"):
            cur.execute(f"INSERT OR IGNORE INTO bookmakers (name, url) VALUES ({ph}, {ph})",
                        (bookmaker, any_offer_url))
        else:
            cur.execute(f"INSERT IGNORE INTO bookmakers (name, url) VALUES ({ph}, {ph})",
                        (bookmaker, any_offer_url))
        cur.execute(f"SELECT id FROM bookmakers WHERE name={ph}", (bookmaker,))
        bookmaker_id = first_col(cur.fetchone())

        # --- Match ---
        if match_uid:
            if url.drivername.startswith("sqlite"):
                cur.execute(f"""
                    INSERT OR IGNORE INTO matches (match_uid, home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                """, (match_uid, home_team_id, away_team_id, start_time, market_id))
            else:
                cur.execute(f"""
                    INSERT IGNORE INTO matches (match_uid, home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                """, (match_uid, home_team_id, away_team_id, start_time, market_id))
            cur.execute(f"SELECT id FROM matches WHERE match_uid={ph}", (match_uid,))
        else:
            if url.drivername.startswith("sqlite"):
                cur.execute(f"""
                    INSERT OR IGNORE INTO matches (home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph})
                """, (home_team_id, away_team_id, start_time, market_id))
            else:
                cur.execute(f"""
                    INSERT IGNORE INTO matches (home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph})
                """, (home_team_id, away_team_id, start_time, market_id))
            cur.execute(f"""
                SELECT id FROM matches 
                WHERE home_team_id={ph} AND away_team_id={ph} AND start_time={ph} AND market_id={ph}
            """, (home_team_id, away_team_id, start_time, market_id))
        match_id = first_col(cur.fetchone())

        # --- Odds (handle float or dict) ---
        for option_key, odd_data in odds_dict.items():
            if isinstance(odd_data, dict):
                decimal_odds = odd_data.get("decimal_odds")
                offer_link = odd_data.get("offer_url", offer_url)
            else:
                decimal_odds = odd_data
                offer_link = offer_url

            # Odds History
            if url.drivername.startswith("sqlite"):
                cur.execute(f"""
                    INSERT INTO odds_history (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*6)})
                """, (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_link))
            else:
                cur.execute(f"""
                    INSERT INTO odds_history (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*6)})
                    ON DUPLICATE KEY UPDATE decimal_odds=VALUES(decimal_odds), offer_url=VALUES(offer_url)
                """, (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_link))

            # Current Odds (Upsert)
            if url.drivername.startswith("sqlite"):
                cur.execute(f"""
                    INSERT INTO odds (match_id, bookmaker_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*5)})
                    ON CONFLICT(match_id, bookmaker_id, option_key)
                    DO UPDATE SET decimal_odds=excluded.decimal_odds, offer_url=excluded.offer_url
                """, (match_id, bookmaker_id, option_key, decimal_odds, offer_link))
            else:
                cur.execute(f"""
                    INSERT INTO odds (match_id, bookmaker_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*5)})
                    ON DUPLICATE KEY UPDATE decimal_odds=VALUES(decimal_odds), offer_url=VALUES(offer_url)
                """, (match_id, bookmaker_id, option_key, decimal_odds, offer_link))

        return match_id



if __name__ == "__main__":
    print("Initializing database schema...")
    init_db()
    print("✅ Database schema ready (with opportunities + indexes + housekeeping).")
