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


def _is_sqlite() -> bool:
    return url.drivername.startswith("sqlite")


def _conn():
    """
    Create a connection to MySQL or SQLite depending on DB_URL.
    """
    if _is_sqlite():
        con = sqlite3.connect(url.database, detect_types=sqlite3.PARSE_DECLTYPES)
        con.row_factory = sqlite3.Row
        return con
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
    Initialize schema including opportunities and housekeeping.
    """
    is_sqlite = _is_sqlite()
    json_type = "TEXT" if is_sqlite else "JSON"
    pk = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "INT AUTO_INCREMENT PRIMARY KEY"

    ddl = [
        f"""CREATE TABLE IF NOT EXISTS teams (
            id {pk},
            name VARCHAR(255) UNIQUE
        )""",
        f"""CREATE TABLE IF NOT EXISTS bookmakers (
            id {pk},
            name VARCHAR(255) UNIQUE,
            url VARCHAR(1024)
        )""",
        f"""CREATE TABLE IF NOT EXISTS markets (
            id {pk},
            name VARCHAR(255),
            param VARCHAR(64),
            UNIQUE(name, param)
        )""",
        f"""CREATE TABLE IF NOT EXISTS matches (
            id {pk},
            match_uid VARCHAR(64) UNIQUE,
            home_team_id INT,
            away_team_id INT,
            start_time DATETIME,
            market_id INT,
            FOREIGN KEY(home_team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(away_team_id) REFERENCES teams(id) ON DELETE CASCADE,
            FOREIGN KEY(market_id) REFERENCES markets(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS odds (
            id {pk},
            match_id INT,
            bookmaker_id INT,
            option_key VARCHAR(32),
            decimal_odds DOUBLE,
            offer_url VARCHAR(1024),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_id, bookmaker_id, option_key),
            FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS odds_history (
            id {pk},
            match_id INT,
            bookmaker_id INT,
            market_id INT,
            option_key VARCHAR(32),
            decimal_odds DOUBLE,
            offer_url VARCHAR(1024),
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
            FOREIGN KEY(bookmaker_id) REFERENCES bookmakers(id) ON DELETE CASCADE
        )""",
        f"""CREATE TABLE IF NOT EXISTS opportunities (
            id {pk},
            match_label VARCHAR(255),
            market VARCHAR(64),
            sport VARCHAR(64),
            start_time DATETIME,
            best_odds {json_type},
            best_books {json_type},
            best_urls {json_type},
            stakes {json_type},
            profit DOUBLE,
            roi DOUBLE,
            alert_sent BOOLEAN DEFAULT 0,
            detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(match_label, market, start_time)
        )""",
    ]

    with get_cursor() as cur:
        for stmt in ddl:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"⚠️ Skipped statement due to: {e}")

        # --- Index Management ---
        if is_sqlite:
            cur.execute("CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_odds_match_bookmaker ON odds(match_id, bookmaker_id)")
        else:
            cur.execute("SHOW INDEX FROM matches WHERE Key_name='idx_matches_start_time'")
            if cur.fetchone():
                cur.execute("DROP INDEX idx_matches_start_time ON matches")

            cur.execute("SHOW INDEX FROM odds WHERE Key_name='idx_odds_match_bookmaker'")
            if cur.fetchone():
                cur.execute("DROP INDEX idx_odds_match_bookmaker ON odds")

            cur.execute("CREATE INDEX idx_matches_start_time ON matches(start_time)")
            cur.execute("CREATE INDEX idx_odds_match_bookmaker ON odds(match_id, bookmaker_id)")

            # --- Legacy Cleanup ---
            cur.execute("""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = 'odds_history'
                AND COLUMN_NAME = 'odds'
            """)
            if cur.fetchone():
                print("🧹 Removing legacy column 'odds' from odds_history...")
                cur.execute("ALTER TABLE odds_history DROP COLUMN odds")


# ================================================================
# HELPERS
# ================================================================
def _json(obj):
    """Safe JSON serialization with unicode support."""
    return json.dumps(obj, ensure_ascii=False)


def _utcnow():
    """Consistent UTC timestamps."""
    return datetime.utcnow()


# ================================================================
# PERSIST OPPORTUNITIES
# ================================================================
def save_opportunity(match_label, market, sport, start_time,
                     best_odds, best_books, best_urls, stakes, profit, roi):
    ph = "?" if _is_sqlite() else "%s"

    sql = (
        f"""INSERT INTO opportunities 
        (match_label, market, sport, start_time,
         best_odds, best_books, best_urls, stakes, profit, roi)
        VALUES ({','.join([ph]*10)})
        ON CONFLICT(match_label, market, start_time) DO UPDATE SET
            best_odds=excluded.best_odds,
            best_books=excluded.best_books,
            best_urls=excluded.best_urls,
            stakes=excluded.stakes,
            profit=excluded.profit,
            roi=excluded.roi,
            detected_at=CURRENT_TIMESTAMP,
            alert_sent=0"""
        if _is_sqlite() else
        f"""INSERT INTO opportunities 
        (match_label, market, sport, start_time,
         best_odds, best_books, best_urls, stakes, profit, roi)
        VALUES ({','.join([ph]*10)})
        ON DUPLICATE KEY UPDATE
            best_odds=VALUES(best_odds),
            best_books=VALUES(best_books),
            best_urls=VALUES(best_urls),
            stakes=VALUES(stakes),
            profit=VALUES(profit),
            roi=VALUES(roi),
            detected_at=CURRENT_TIMESTAMP,
            alert_sent=0"""
    )

    with get_cursor() as cur:
        cur.execute(sql, (
            match_label.strip() if match_label else None,
            market.strip() if market else None,
            sport.strip() if sport else None,
            start_time or _utcnow(),
            _json(best_odds),
            _json(best_books),
            _json(best_urls),
            _json(stakes),
            float(profit) if profit is not None else None,
            float(roi) if roi is not None else None,
        ))


# ================================================================
# FETCH RECENT OPPORTUNITIES
# ================================================================
def get_recent_opportunities(limit=20):
    ph = "?" if _is_sqlite() else "%s"
    sql = f"SELECT * FROM opportunities ORDER BY detected_at DESC LIMIT {ph}"

    with get_cursor(commit=False) as cur:
        cur.execute(sql, (limit,))
        return cur.fetchall()


# ================================================================
# CLEANUP OLD DATA
# ================================================================
def cleanup_db(days=30):
    cutoff = _utcnow() - timedelta(days=days)
    ph = "?" if _is_sqlite() else "%s"

    with get_cursor() as cur:
        cur.execute(f"DELETE FROM opportunities WHERE start_time < {ph}", (cutoff,))
        cur.execute(f"DELETE FROM odds WHERE last_updated < {ph}", (cutoff,))
        cur.execute(f"DELETE FROM odds_history WHERE recorded_at < {ph}", (cutoff,))
        cur.execute(f"DELETE FROM matches WHERE start_time < {ph}", (cutoff,))


# ================================================================
# UPSERT MATCH + ODDS
# ================================================================
def upsert_match(home_team, away_team, bookmaker, market_name, market_param,
                 odds_dict, start_time=None, start_time_iso=None,
                 offer_url=None, match_uid=None):

    ph = "?" if _is_sqlite() else "%s"

    def first_col(row):
        if row is None:
            return None
        return row[0] if isinstance(row, tuple) else list(row.values())[0]

    with get_cursor() as cur:
        # --- Teams ---
        for team in (home_team, away_team):
            if _is_sqlite():
                cur.execute(f"INSERT OR IGNORE INTO teams (name) VALUES ({ph})", (team.strip(),))
            else:
                cur.execute(f"INSERT IGNORE INTO teams (name) VALUES ({ph})", (team.strip(),))

        cur.execute(f"SELECT id FROM teams WHERE name={ph}", (home_team.strip(),))
        home_team_id = first_col(cur.fetchone())
        cur.execute(f"SELECT id FROM teams WHERE name={ph}", (away_team.strip(),))
        away_team_id = first_col(cur.fetchone())

        # --- Market ---
        if _is_sqlite():
            cur.execute(f"INSERT OR IGNORE INTO markets (name, param) VALUES ({ph}, {ph})",
                        (market_name.strip(), market_param))
        else:
            cur.execute(f"INSERT IGNORE INTO markets (name, param) VALUES ({ph}, {ph})",
                        (market_name.strip(), market_param))
        cur.execute(f"SELECT id FROM markets WHERE name={ph} AND param={ph}",
                    (market_name.strip(), market_param))
        market_id = first_col(cur.fetchone())

        # --- Bookmaker ---
        any_offer_url = next(
            (d.get("offer_url") for d in odds_dict.values()
             if isinstance(d, dict) and d.get("offer_url")),
            offer_url
        )
        if _is_sqlite():
            cur.execute(f"INSERT OR IGNORE INTO bookmakers (name, url) VALUES ({ph}, {ph})",
                        (bookmaker.strip(), any_offer_url))
        else:
            cur.execute(f"INSERT IGNORE INTO bookmakers (name, url) VALUES ({ph}, {ph})",
                        (bookmaker.strip(), any_offer_url))
        cur.execute(f"SELECT id FROM bookmakers WHERE name={ph}", (bookmaker.strip(),))
        bookmaker_id = first_col(cur.fetchone())

        # --- Match ---
        if match_uid:
            if _is_sqlite():
                cur.execute(f"""
                    INSERT OR IGNORE INTO matches
                    (match_uid, home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                """, (match_uid, home_team_id, away_team_id, start_time, market_id))
            else:
                cur.execute(f"""
                    INSERT IGNORE INTO matches
                    (match_uid, home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph}, {ph})
                """, (match_uid, home_team_id, away_team_id, start_time, market_id))
            cur.execute(f"SELECT id FROM matches WHERE match_uid={ph}", (match_uid,))
        else:
            if _is_sqlite():
                cur.execute(f"""
                    INSERT OR IGNORE INTO matches
                    (home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph})
                """, (home_team_id, away_team_id, start_time, market_id))
            else:
                cur.execute(f"""
                    INSERT IGNORE INTO matches
                    (home_team_id, away_team_id, start_time, market_id)
                    VALUES ({ph}, {ph}, {ph}, {ph})
                """, (home_team_id, away_team_id, start_time, market_id))
            cur.execute(f"""
                SELECT id FROM matches
                WHERE home_team_id={ph} AND away_team_id={ph} AND start_time={ph} AND market_id={ph}
            """, (home_team_id, away_team_id, start_time, market_id))
        match_id = first_col(cur.fetchone())

        # --- Odds ---
        for option_key, odd_data in odds_dict.items():
            if isinstance(odd_data, dict):
                decimal_odds = odd_data.get("decimal_odds")
                offer_link = odd_data.get("offer_url", offer_url)
            else:
                decimal_odds = odd_data
                offer_link = offer_url

            # Odds History
            if _is_sqlite():
                cur.execute(f"""
                    INSERT INTO odds_history
                    (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*6)})
                """, (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_link))
            else:
                cur.execute(f"""
                    INSERT INTO odds_history
                    (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*6)})
                    ON DUPLICATE KEY UPDATE decimal_odds=VALUES(decimal_odds), offer_url=VALUES(offer_url)
                """, (match_id, bookmaker_id, market_id, option_key, decimal_odds, offer_link))

            # Current Odds
            if _is_sqlite():
                cur.execute(f"""
                    INSERT INTO odds
                    (match_id, bookmaker_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*5)})
                    ON CONFLICT(match_id, bookmaker_id, option_key)
                    DO UPDATE SET decimal_odds=excluded.decimal_odds,
                                  offer_url=excluded.offer_url
                """, (match_id, bookmaker_id, option_key, decimal_odds, offer_link))
            else:
                cur.execute(f"""
                    INSERT INTO odds
                    (match_id, bookmaker_id, option_key, decimal_odds, offer_url)
                    VALUES ({",".join([ph]*5)})
                    ON DUPLICATE KEY UPDATE decimal_odds=VALUES(decimal_odds), offer_url=VALUES(offer_url)
                """, (match_id, bookmaker_id, option_key, decimal_odds, offer_link))

        return match_id


if __name__ == "__main__":
    print("Initializing database schema...")
    init_db()
    print("✅ Database schema ready (with opportunities + indexes + housekeeping).")
