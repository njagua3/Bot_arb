# core/telegram.py
from __future__ import annotations
import html
import time
import requests
import hashlib
from typing import Iterable, Optional, Dict, Any, List
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from telegram.ext import Updater, CommandHandler, CallbackContext
from telegram import Update, ParseMode

from core.config import ENVCFG
from core.settings import load_settings, save_settings, get_scan_interval
from core.logger import get_logger
from core.db import get_cursor  # for bookmaker & event lookups
from core.calculator import calculate_stakes  # stake split if not attached

logger = get_logger(__name__)

# --- Config from central source ---
TOKEN = ENVCFG.TELEGRAM_BOT_TOKEN
_chat_env = ENVCFG.TELEGRAM_CHAT_ID or ""
CHAT_IDS = {c.strip() for c in _chat_env.split(",") if c.strip()}

NAIROBI_TZ = ZoneInfo("Africa/Nairobi")
TG_MAX = 4096  # Telegram hard limit

# ===============================
# DB helpers (bookmaker cache)
# ===============================
_BM_CACHE: Dict[int, Dict[str, Optional[str]]] = {}

def _resolve_bookmaker(bm_id: int) -> Dict[str, Optional[str]]:
    if bm_id in _BM_CACHE:
        return _BM_CACHE[bm_id]
    q = "SELECT id, name, url FROM bookmakers WHERE id=?"
    params = (bm_id,)
    try:
        with get_cursor(commit=False) as cur:
            try:
                cur.execute(q, params)
            except Exception:
                cur.execute("SELECT id, name, url FROM bookmakers WHERE id=%s", (bm_id,))
            row = cur.fetchone()
    except Exception as e:
        logger.warning(f"bookmaker lookup failed for {bm_id}: {e}")
        row = None

    if not row:
        data = {"id": bm_id, "name": f"Book {bm_id}", "url": None}
    else:
        name = row["name"] if not isinstance(row, tuple) else row[1]
        url = row["url"] if not isinstance(row, tuple) else row[2]
        data = {"id": bm_id, "name": name, "url": url}

    _BM_CACHE[bm_id] = data
    return data

# ===============================
# DB helpers: event meta (teams, sport)
# ===============================
def _event_meta(arb_event_id: int) -> Dict[str, str]:
    sql_mysql = """
        SELECT t1.name AS home, t2.name AS away, s.name AS sport
        FROM arb_events ae
        JOIN teams t1 ON t1.id = ae.home_team_id
        JOIN teams t2 ON t2.id = ae.away_team_id
        JOIN sports s ON s.id = ae.sport_id
        WHERE ae.id = %s
    """
    sql_sqlite = sql_mysql.replace("%s", "?")
    try:
        with get_cursor(commit=False) as cur:
            try:
                cur.execute(sql_mysql, (arb_event_id,))
            except Exception:
                cur.execute(sql_sqlite, (arb_event_id,))
            row = cur.fetchone()
    except Exception:
        row = None

    if not row:
        return {"home": "Home", "away": "Away", "sport": "Football"}
    if isinstance(row, dict):
        return {"home": row.get("home") or "Home",
                "away": row.get("away") or "Away",
                "sport": row.get("sport") or "Football"}
    return {"home": row[0] or "Home", "away": row[1] or "Away", "sport": row[2] or "Football"}

# ===============================
# Low-level send
# ===============================
def _chunks(texts: List[str], max_len: int = 3500) -> List[str]:
    out, cur = [], ""
    for t in texts:
        if not cur:
            cur = t
            continue
        if len(cur) + 2 + len(t) <= max_len:
            cur += "\n\n" + t
        else:
            out.append(cur)
            cur = t
    if cur:
        out.append(cur)
    return out

def send_telegram_alert(message: str,
                        chat_ids: Optional[Iterable[str]] = None,
                        retries: int = 3,
                        backoff: float = 2.0) -> None:
    if not TOKEN or not (chat_ids or CHAT_IDS):
        logger.error("❌ Missing Telegram credentials or chat IDs.")
        return

    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    targets = list(chat_ids or CHAT_IDS)

    parts = [message]
    if len(message) > TG_MAX:
        parts = _chunks(message.split("\n\n"), max_len=3500)

    for chat_id in targets:
        for part in parts:
            data = {
                "chat_id": chat_id,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            for attempt in range(1, retries + 1):
                try:
                    r = requests.post(url, json=data, timeout=15)
                    if r.status_code == 429:
                        try:
                            ra = int(r.headers.get("Retry-After", "1"))
                        except Exception:
                            ra = 1
                        logger.warning(f"⏳ Rate limited (429). Sleeping {ra}s…")
                        time.sleep(max(1, ra))
                        continue
                    if r.ok and r.json().get("ok"):
                        logger.info(f"📩 Telegram alert sent to {chat_id}.")
                        break
                    logger.error(f"❌ Telegram API error for {chat_id}: {r.text}")
                except requests.exceptions.RequestException as e:
                    logger.warning(f"⚠️ Network issue (attempt {attempt}/{retries}) for {chat_id}: {e}")
                except Exception as e:
                    logger.exception(f"❌ Unexpected error for {chat_id}: {e}")

                if attempt < retries:
                    time.sleep(backoff * attempt)
            else:
                logger.error(f"❌ Failed to send Telegram alert to {chat_id} after retries.")

# ===============================
# Formatting helpers
# ===============================
def _esc(s: Any) -> str:
    return html.escape(str(s), quote=False)

def _fmt_pct(x: float) -> str:
    return f"{x:.2f}%"

def _fmt_money(x: float) -> str:
    return f"{x:,.2f}"

def _fmt_ko(dt_utc: datetime) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    local = dt_utc.astimezone(NAIROBI_TZ)
    return local.strftime("%Y-%m-%d %H:%M %Z")

def _normalize_outcome_key(k: str) -> str:
    k = (k or "").strip().lower()
    return {
        "home": "1", "1": "1", "1 (home)": "1",
        "draw": "x", "x": "x",
        "away": "2", "2": "2", "2 (away)": "2",
    }.get(k, k)

def _format_best_odds_1x2(opp) -> List[str]:
    order = ["1", "x", "2"]
    lines = []
    for key in order:
        leg_item = None
        for o, leg in opp.legs.items():
            if _normalize_outcome_key(o) == key:
                leg_item = (o, leg)
                break
        if not leg_item:
            continue
        o, leg = leg_item
        bm = _resolve_bookmaker(int(leg["bookmaker_id"]))
        val = float(leg["odds"])
        # FIX: avoid nested single quotes in f-string
        bm_name = bm.get("name") or f"Book {bm.get('id')}"
        lines.append(f"{key.upper()} ➤ {val:.2f} ({_esc(bm_name)})")
    return lines


# ===============================
# Pretty formatter (trader style)
# ===============================
def format_opp_pretty(opp) -> str:
    meta = _event_meta(int(opp.arb_event_id))
    market_title = str(opp.market_name) + (f" {opp.line}" if opp.line else "")

    n_outcomes = len(opp.legs or {})
    way_title = {2: "2-WAY", 3: "3-WAY"}.get(n_outcomes, f"{n_outcomes}-WAY")

    if (opp.market_name or "").strip().lower() in ("1x2", "1x2 full time", "match result"):
        odds_lines = _format_best_odds_1x2(opp)
    else:
        odds_lines = []
        for o, leg in opp.legs.items():
            bm = _resolve_bookmaker(int(leg["bookmaker_id"]))
            bm_name = bm.get("name") or f"Book {bm.get('id')}"  # FIX
            odds_lines.append(f"{_esc(o)} ➤ {float(leg['odds']):.2f} ({_esc(bm_name)})")

    stakes_map = getattr(opp, "stakes", None)
    if not stakes_map:
        try:
            s = load_settings()
            stakes_map = calculate_stakes({o: float(leg["odds"]) for o, leg in opp.legs.items()}, float(s.stake))
        except Exception:
            stakes_map = None

    stake_lines = []
    if stakes_map:
        if (opp.market_name or "").strip().lower() in ("1x2", "1x2 full time", "match result"):
            order = ["1", "x", "2"]
            for key in order:
                mkey = None
                for o in stakes_map.keys():
                    if _normalize_outcome_key(o) == key:
                        mkey = o
                        break
                if not mkey:
                    continue
                stake_lines.append(f"{key.upper()} = {float(stakes_map[mkey]):.2f}")
        else:
            for o, amt in stakes_map.items():
                stake_lines.append(f"{_esc(o)} = {float(amt):.2f}")

    profit_val = float(getattr(opp, "profit", 0.0))
    roi_val = float(getattr(opp, "roi", 0.0))

    header = [
        f"📣 {way_title} Arbitrage",
        f"🏟 { _esc(meta['home']) } vs { _esc(meta['away']) }",
        f"🎯 Market: { _esc(market_title) }",
        f"📅 Match Time: { _esc(_fmt_ko(opp.start_time)) }",
        f"⚽ Sport: { _esc(meta['sport'] or 'Football') }",
        "",
        "💰 Best Odds:",
        *odds_lines,
    ]

    if stake_lines:
        header += ["", "📊 Stake Split (KES):", *stake_lines]

    footer = [
        "",
        f"🟢 Profit: {profit_val:.2f} KES",
        f"📈 ROI: {roi_val:.2f}%",
    ]

    return "\n".join(header + footer)

# ===============================
# In-memory de-dup (DB-consistent hash)
# ===============================
_seen_hashes: Dict[str, float] = {}
_DEDUP_TTL_SEC = 30 * 60

def _opp_hash(opp) -> str:
    if hasattr(opp, "_legs_sig") and getattr(opp, "_legs_sig"):
        sig = str(opp._legs_sig)
    else:
        parts = "|".join(sorted(
            f"{str(o)}:{int(leg['bookmaker_id'])}:{round(float(leg['odds']), 3)}"
            for o, leg in opp.legs.items()
        ))
        sig = hashlib.sha1(parts.encode("utf-8")).hexdigest()[:16]
    return f"{opp.arb_event_id}|{str(opp.market_name)}|{str(opp.line or '')}|{sig}"

def send_opportunity(opp) -> bool:
    h = _opp_hash(opp)
    now = time.time()
    for k, ts in list(_seen_hashes.items()):
        if now - ts > _DEDUP_TTL_SEC:
            _seen_hashes.pop(k, None)
    if h in _seen_hashes:
        logger.info("⏭️ Skipping duplicate opportunity alert.")
        return False
    _seen_hashes[h] = now

    msg = format_opp_pretty(opp)
    send_telegram_alert(msg)
    return True

def send_opportunities(opps) -> int:
    sent = 0
    for o in opps:
        if send_opportunity(o):
            sent += 1
    return sent

# ===============================
# BOT COMMANDS
# ===============================
def _is_authorized(update: Update) -> bool:
    return str(update.effective_chat.id) in CHAT_IDS

def stake(update: Update, context: CallbackContext):
    if not _is_authorized(update):
        update.message.reply_text("❌ You're not authorized to change the stake.")
        return
    try:
        new_stake = float(context.args[0])
        if new_stake < 100:
            raise ValueError
        s = load_settings()
        s.stake = new_stake
        save_settings(s)
        update.message.reply_text(f"✅ Stake updated to {int(new_stake):,} KES.")
        logger.info(f"Stake updated to {new_stake} by user {update.effective_chat.id}")
    except Exception:
        update.message.reply_text("❗ Usage: /stake 15000")

def start(update: Update, context: CallbackContext):
    s = load_settings()
    stake_value = getattr(s, "stake", 0)
    affiliate_url = getattr(s, "affiliate_url", None)  # optional

    text = (
        "🤖 Hello! Welcome to ArbXtreme Bot 🎉\n\n"
        f"Your current stake is: <b>{stake_value:,.0f} KES</b>.\n"
        "(This is the total amount the bot will automatically split among all outcomes.)\n\n"
        "💰 Use /stake &lt;amount&gt; to change your stake.\n"
        "📖 Type /help to see all available commands.\n\n"
        "⚽ Before you start, make sure you’re signed up with these bookmakers:\n\n"
        "• ke.sportpesa.com\n"
        "• Betika.com\n"
        "• Hakibets.com\n"
        "• Cloudbet.com\n"
        "• Odibets.com\n"
        "• sportybet.com/ke\n"
        "• Kwikbet.co.ke\n"
        "• Mozzartbet.co.ke\n"
        "• Shabiki.com\n"
        "• Bangbet.com\n\n"
        "💡 <b>Pro Tip:</b> To avoid account closure by bookmakers, always round off your bets.\n\n"
        "🔗 <b>Affiliate Program:</b>\n"
        "Join our affiliate program and earn a 10% referral bonus!\n"
    )
    if affiliate_url:
        text += f"👉 <a href=\"{html.escape(affiliate_url, quote=True)}\">Click here to join</a>\n\n"
    else:
        text += "👉 Ask for our affiliate link to join.\n\n"

    text += "Good luck and profit smart! 🚀"

    update.message.reply_text(
        text,
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )

def help_command(update: Update, context: CallbackContext):
    update.message.reply_text(
        "📖 Commands:\n"
        "/start – Welcome + current stake & setup tips\n"
        "/stake <amount> – Update stake (authorized only)\n"
        "/status – Show bot status (authorized only)\n"
        "/help – Show this help message"
    )

def status_command(update: Update, context: CallbackContext):
    if not _is_authorized(update):
        update.message.reply_text("❌ You're not authorized to view status.")
        return
    s = load_settings()
    try:
        scan_interval = get_scan_interval()
    except Exception:
        scan_interval = s.scan_interval
    update.message.reply_text(
        "📊 Bot Status:\n"
        f"• Stake: {int(s.stake):,} KES\n"
        f"• Scan interval: {scan_interval} seconds\n"
        f"• Authorized users: {len(CHAT_IDS)}"
    )

# ===============================
# RUN BOT / TEST
# ===============================
def send_welcome_test() -> None:
    s = load_settings()
    msg = (
        "🤖 <b>Njagua Arb Bot</b> — Telegram link OK!\n"
        f"Stake: <b>{int(s.stake):,}</b> KES\n"
        "Commands: /start, /status, /stake &lt;amount&gt;\n"
        "This is a test broadcast to verify delivery."
    )
    send_telegram_alert(msg)

def run_bot():
    if not TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN missing.")
        return
    try:
        updater = Updater(TOKEN, use_context=True)  # v13 style
    except Exception as e:
        logger.error(f"❌ Failed to init Telegram Updater: {e}")
        return

    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("stake", stake))
    dp.add_handler(CommandHandler("status", status_command))
    dp.add_handler(CommandHandler("help", help_command))

    logger.info(f"🤖 Telegram Bot running with {len(CHAT_IDS)} authorized users.")
    updater.start_polling()

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="Send a test welcome message and exit")
    ap.add_argument("--run-bot", action="store_true", help="Run polling bot (default if no args)")
    args = ap.parse_args()

    if args.test:
        send_welcome_test()
    else:
        run_bot()
