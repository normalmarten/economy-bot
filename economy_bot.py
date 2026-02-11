# economy_bot.py
# Discord economy bot (Python): /daily (streaks), /balance,
# /beg (only if <10 coins, 1 hour cooldown, +50),
# /roulette (color/number, AMERICAN),
# /slots, /blackjack (Hit/Stand/Double/Surrender, 3:2 natural),
# /holdem (simple heads-up vs bot),
# /stats, /leaderboard (usernames),
# /shop /buy /inventory (collectibles),
# /loan (high-interest borrowing, repayment),
# achievements + /settings achievements on/off.
#
# Install:  pip install -U discord.py
# Token:    set DISCORD_TOKEN env var
# Run:      python economy_bot.py

import os
import time
import random
import sqlite3
import itertools
from dataclasses import dataclass
from typing import Optional, List, Union, Dict, Tuple

import discord
from discord import app_commands
from discord.ext import commands

# ----------------------------
# CONFIG
# ----------------------------
DB_PATH = "/app/data/economy.sqlite3"
DAILY_AMOUNT = 250
DAILY_COOLDOWN_SECONDS = 24 * 60 * 60  # 24h
DAILY_STREAK_GRACE_SECONDS = 48 * 60 * 60

STREAK_BONUS = {3: 100, 7: 250, 14: 500, 30: 1500}

MIN_BET = 10
MAX_BET = 100_000

# Roulette (keep casino-like, not OP)
STRAIGHT_UP_RETURN_MULT = 36  # total return (profit 35:1)
COLOR_RETURN_MULT = 3         # total return (profit 1:1)

# Optional: make losses sting more (0 disables)
ROULETTE_LOSS_FEE_PCT = 5     # extra % of bet deducted on losses only

# Blackjack rules
BJ_DEALER_STANDS_SOFT_17 = True
BJ_ALLOW_DOUBLE = True
BJ_ALLOW_SURRENDER = True
# Payouts:
# - regular win: profit 1:1 (total return 2x)
# - push: return bet
# - natural blackjack: profit 3:2 (total return bet + 1.5*bet)
BJ_WIN_RETURN_MULT_NUM = 2      # total return multiplier numerator (2x)
BJ_WIN_RETURN_MULT_DEN = 1
BJ_PUSH_RETURN_MULT_NUM = 1
BJ_PUSH_RETURN_MULT_DEN = 1
# Natural payout as fraction of bet profit: 3/2
BJ_NATURAL_PROFIT_NUM = 3
BJ_NATURAL_PROFIT_DEN = 2

# Slots config (house edge: tune payout table)
SLOTS_MIN_BET = 10
SLOTS_MAX_BET = 100_000

# Loans (very high interest)
LOAN_MAX_PRINCIPAL = 100_000
LOAN_DAILY_INTEREST_PCT = 25   # 25% per day 😈
LOAN_ORIGINATION_FEE_PCT = 10  # take 10% up front
LOAN_GRACE_SECONDS = 24 * 60 * 60  # interest accrues daily; we compound on access

# Achievements thresholds (new)
ACH_HIGH_ROLLER_BET = 50_000
ACH_MILLIONAIRE_WALLET = 1_000_000

# ----------------------------
# ROULETTE HELPERS
# ----------------------------
RED_NUMBERS = {
    1, 3, 5, 7, 9, 12, 14, 16, 18,
    19, 21, 23, 25, 27, 30, 32, 34, 36
}

Pocket = Union[int, str]  # int 0..36, or "00"
POCKETS: List[Pocket] = ["00", 0] + list(range(1, 37))

def pocket_color(p: Pocket) -> str:
    if p == "00" or p == 0:
        return "green"
    return "red" if int(p) in RED_NUMBERS else "black"

def fmt_pocket(p: Pocket) -> str:
    return "00" if p == "00" else str(p)

def loss_fee(bet: int) -> int:
    if ROULETTE_LOSS_FEE_PCT <= 0:
        return 0
    return (bet * ROULETTE_LOSS_FEE_PCT) // 100

# ----------------------------
# DATABASE
# ----------------------------
def _ensure_db_dir() -> None:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)

def db_connect() -> sqlite3.Connection:
    _ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _colnames(conn: sqlite3.Connection, table: str) -> set:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

def db_init() -> None:
    _ensure_db_dir()
    with db_connect() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS guild_users (
            guild_id   TEXT NOT NULL,
            user_id    TEXT NOT NULL,
            wallet     INTEGER NOT NULL DEFAULT 0,

            -- Daily
            last_daily INTEGER NOT NULL DEFAULT 0,
            daily_streak INTEGER NOT NULL DEFAULT 0,

            -- Beg
            last_beg INTEGER NOT NULL DEFAULT 0,
            beg_bonus_ready INTEGER NOT NULL DEFAULT 0,

            -- Roulette stats
            plays      INTEGER NOT NULL DEFAULT 0,
            wins       INTEGER NOT NULL DEFAULT 0,
            losses     INTEGER NOT NULL DEFAULT 0,
            wagered    INTEGER NOT NULL DEFAULT 0,
            profit     INTEGER NOT NULL DEFAULT 0,
            biggest_win INTEGER NOT NULL DEFAULT 0,

            -- Blackjack stats
            bj_plays   INTEGER NOT NULL DEFAULT 0,
            bj_wins    INTEGER NOT NULL DEFAULT 0,
            bj_losses  INTEGER NOT NULL DEFAULT 0,
            bj_pushes  INTEGER NOT NULL DEFAULT 0,
            bj_wagered INTEGER NOT NULL DEFAULT 0,
            bj_profit  INTEGER NOT NULL DEFAULT 0,
            bj_biggest_win INTEGER NOT NULL DEFAULT 0,

            -- Slots stats
            slots_plays INTEGER NOT NULL DEFAULT 0,
            slots_wins  INTEGER NOT NULL DEFAULT 0,
            slots_losses INTEGER NOT NULL DEFAULT 0,
            slots_wagered INTEGER NOT NULL DEFAULT 0,
            slots_profit INTEGER NOT NULL DEFAULT 0,
            slots_biggest_win INTEGER NOT NULL DEFAULT 0,

            -- Hold'em stats
            he_plays INTEGER NOT NULL DEFAULT 0,
            he_wins  INTEGER NOT NULL DEFAULT 0,
            he_losses INTEGER NOT NULL DEFAULT 0,
            he_wagered INTEGER NOT NULL DEFAULT 0,
            he_profit INTEGER NOT NULL DEFAULT 0,

            PRIMARY KEY (guild_id, user_id)
        )
        """)
        conn.commit()

        conn.execute("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id TEXT PRIMARY KEY,
            achievements_enabled INTEGER NOT NULL DEFAULT 1
        )
        """)
        conn.commit()

        # Collectible items (global)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            item_id   TEXT PRIMARY KEY,
            name      TEXT NOT NULL,
            price     INTEGER NOT NULL,
            kind      TEXT NOT NULL DEFAULT 'collectible',
            description TEXT NOT NULL DEFAULT ''
        )
        """)
        conn.commit()

        conn.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            guild_id TEXT NOT NULL,
            user_id  TEXT NOT NULL,
            item_id  TEXT NOT NULL,
            qty      INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, item_id)
        )
        """)
        conn.commit()

        conn.execute("""
        CREATE TABLE IF NOT EXISTS achievements (
            ach_id TEXT PRIMARY KEY,
            name   TEXT NOT NULL,
            description TEXT NOT NULL,
            reward INTEGER NOT NULL DEFAULT 0
        )
        """)
        conn.commit()

        conn.execute("""
        CREATE TABLE IF NOT EXISTS user_achievements (
            guild_id TEXT NOT NULL,
            user_id  TEXT NOT NULL,
            ach_id   TEXT NOT NULL,
            unlocked_at INTEGER NOT NULL,
            PRIMARY KEY (guild_id, user_id, ach_id)
        )
        """)
        conn.commit()

        # Loans (per-guild)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            guild_id TEXT NOT NULL,
            user_id  TEXT NOT NULL,
            principal INTEGER NOT NULL DEFAULT 0,   -- original borrowed (for reference)
            balance   INTEGER NOT NULL DEFAULT 0,   -- current owed (compounds)
            daily_interest_pct INTEGER NOT NULL DEFAULT 0,
            origination_fee_pct INTEGER NOT NULL DEFAULT 0,
            opened_at INTEGER NOT NULL DEFAULT 0,
            last_accrual INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        )
        """)
        conn.commit()

        seed_items(conn)          # DO NOT EDIT COLLECTIBLES
        seed_achievements(conn)   # safe: uses INSERT OR IGNORE

        # migrations (safe adds)
        existing_cols = _colnames(conn, "guild_users")
        add_cols = {
            "slots_plays": "INTEGER NOT NULL DEFAULT 0",
            "slots_wins": "INTEGER NOT NULL DEFAULT 0",
            "slots_losses": "INTEGER NOT NULL DEFAULT 0",
            "slots_wagered": "INTEGER NOT NULL DEFAULT 0",
            "slots_profit": "INTEGER NOT NULL DEFAULT 0",
            "slots_biggest_win": "INTEGER NOT NULL DEFAULT 0",
            "he_plays": "INTEGER NOT NULL DEFAULT 0",
            "he_wins": "INTEGER NOT NULL DEFAULT 0",
            "he_losses": "INTEGER NOT NULL DEFAULT 0",
            "he_wagered": "INTEGER NOT NULL DEFAULT 0",
            "he_profit": "INTEGER NOT NULL DEFAULT 0",
            "last_beg": "INTEGER NOT NULL DEFAULT 0",
            "beg_bonus_ready": "INTEGER NOT NULL DEFAULT 0",
        }
        for col, ddl in add_cols.items():
            if col not in existing_cols:
                conn.execute(f"ALTER TABLE guild_users ADD COLUMN {col} {ddl}")
        conn.commit()

def seed_items(conn: sqlite3.Connection) -> None:
    # DO NOT EDIT COLLECTIBLES (per your request)
    items = [
        ("collectible_01", "Monkey", 50, "collectible", "A hard worker."),
        ("collectible_02", "Le bean", 120, "collectible", "Works harder than the monkey."),
        ("collectible_03", "Femboy", 2500, "collectible", "Gyatt."),
        ("collectible_04", "Tomboy", 4000, "collectible", "will bully til nut."),
        ("collectible_05", "Goth mommy", 8000, "collectible", "Will step on you."),
        ("collectible_06", "Goth Furry Tomboy", 8000, "collectible", "PAWS."),
        ("collectible_07", "Anthropomorphic Alligator", 8000, "collectible", "Look at me Dom."),
        ("collectible_08", "Chun Li", 80000, "collectible", "The guy from Fortnite."),
        ("collectible_09", "E-Girl", 150_000, "collectible", "You are cooked gang."),
        ("collectible_10", "Gym Bro Tren Stimmer", 275_000, "collectible", "Isaac Macklemore"),
        ("collectible_11", "La Torta", 420_000, "collectible", "My Man."),
        ("collectible_12", "Catboy", 750_000, "collectible", "Warning: Scratch."),
        ("collectible_13", "CEO of Bad Dragon/Mythic Goth Mommy ", 1_200_000, "collectible", "SWEET MOTHER OF PEARL"),
        ("collectible_14", "Mythic Discord Kitten", 2_000_000, "collectible", "Pwincess."),
        ("collectible_15", "Kawaii Shy Anime Classmate", 3_500_000, "collectible", "Could be male or female."),
        ("collectible_16", "Furry Overlord", 5_000_000, "collectible", "Big Dih"),
        ("collectible_17", "Shares in Israel", 8_000_000, "collectible", "Might get some control."),
        ("collectible_18", "Ultra Powerful Slim-Thick Asian Robot", 12_000_000, "collectible", "Connor Burton."),
        ("collectible_19", "Twinky Little Ginger Slut", 20_000_000, "collectible", "Begley"),
        ("collectible_20", "Ultra Mega Super Golden Mythic Goon Figurine", 50_000_000, "collectible", "yep."),
        ("collectible_21", "Taki Fart Jar", 75_000_000, "collectible", "Spicy"),
        ("collectible_22", "Bro", 100_000_000, "collectible", "It is your homeboy."),
        ("collectible_23", "Waifu Body Pillow (Mythic)", 150_000_000, "collectible", ""),
        ("collectible_24", "Lopunny", 225_000_000, "collectible", "JOKER NOOOOO"),
        ("collectible_25", "Certified Freak Trophy", 300_000_000, "collectible", "Certified freak 7 days a week."),
        ("collectible_26", "Goonvana Portal", 450_000_000, "collectible", "Transport yourself to goonvana."),
        ("collectible_27", "E-Boy Vampire Overlord", 600_000_000, "collectible", "Yeah you are getting touched."),
        ("collectible_28", "Alt Girl", 800_000_000, "collectible", "you know it"),
        ("collectible_29", "First Date", 1_000_000_000, "collectible", "First date."),
        ("collectible_30", "An Actual Healthy Relationship With A Woman", 2_000_000_000_000, "collectible", "Who decided that."),
    ]
    for item_id, name, price, kind, desc in items:
        conn.execute("""
            INSERT OR IGNORE INTO items (item_id, name, price, kind, description)
            VALUES (?, ?, ?, ?, ?)
        """, (item_id, name, price, kind, desc))
    conn.commit()

def seed_achievements(conn: sqlite3.Connection) -> None:
    # Added a few more achievements (safe: INSERT OR IGNORE)
    ach = [
        ("first_daily", "First Daily", "Claim /daily for the first time.", 200),
        ("streak_7", "7-Day Streak", "Reach a 7-day daily streak.", 500),
        ("streak_30", "30-Day Streak", "Reach a 30-day daily streak.", 1200),

        ("roulette_big", "Big Spin", "Win 10,000+ coins net on roulette in one spin.", 750),

        ("bj_blackjack", "Natural Blackjack", "Get a natural blackjack (A + 10).", 750),
        ("bj_double", "Double Trouble", "Win a hand after doubling down.", 400),

        ("slots_jackpot", "Jackpot", "Hit the top slot payout.", 900),
        ("first_buy", "First Purchase", "Buy your first collectible item.", 300),

        ("loan_shark", "Loan Shark", "Take a loan.", 200),
        ("loan_paid", "Paid in Blood", "Fully repay a loan.", 500),

        ("holdem_win", "River King", "Win a hand of Texas Hold'em.", 600),

        ("high_roller", "High Roller", f"Place a single bet of {ACH_HIGH_ROLLER_BET:,}+ coins.", 800),
        ("millionaire", "Millionaire", f"Reach a wallet balance of {ACH_MILLIONAIRE_WALLET:,}+ coins.", 1500),
    ]
    for ach_id, name, desc, reward in ach:
        conn.execute("""
            INSERT OR IGNORE INTO achievements (ach_id, name, description, reward)
            VALUES (?, ?, ?, ?)
        """, (ach_id, name, desc, reward))
    conn.commit()

def ensure_user(conn: sqlite3.Connection, guild_id: int, user_id: int) -> None:
    conn.execute("""
        INSERT OR IGNORE INTO guild_users (guild_id, user_id)
        VALUES (?, ?)
    """, (str(guild_id), str(user_id)))

def ensure_settings(conn: sqlite3.Connection, guild_id: int) -> None:
    conn.execute("""
        INSERT OR IGNORE INTO guild_settings (guild_id, achievements_enabled)
        VALUES (?, 1)
    """, (str(guild_id),))

def achievements_enabled(conn: sqlite3.Connection, guild_id: int) -> bool:
    ensure_settings(conn, guild_id)
    row = conn.execute("SELECT achievements_enabled FROM guild_settings WHERE guild_id=?", (str(guild_id),)).fetchone()
    return bool(int(row["achievements_enabled"]))

def set_achievements_enabled(conn: sqlite3.Connection, guild_id: int, enabled: bool) -> None:
    ensure_settings(conn, guild_id)
    conn.execute("UPDATE guild_settings SET achievements_enabled=? WHERE guild_id=?", (1 if enabled else 0, str(guild_id),))

def get_user(conn: sqlite3.Connection, guild_id: int, user_id: int) -> sqlite3.Row:
    ensure_user(conn, guild_id, user_id)
    return conn.execute("SELECT * FROM guild_users WHERE guild_id=? AND user_id=?", (str(guild_id), str(user_id))).fetchone()

def update_wallet(conn: sqlite3.Connection, guild_id: int, user_id: int, delta: int) -> int:
    ensure_user(conn, guild_id, user_id)
    conn.execute("UPDATE guild_users SET wallet = wallet + ? WHERE guild_id=? AND user_id=?", (delta, str(guild_id), str(user_id)))
    row = conn.execute("SELECT wallet FROM guild_users WHERE guild_id=? AND user_id=?", (str(guild_id), str(user_id))).fetchone()
    return int(row["wallet"])

def set_last_daily(conn: sqlite3.Connection, guild_id: int, user_id: int, ts: int) -> None:
    ensure_user(conn, guild_id, user_id)
    conn.execute("UPDATE guild_users SET last_daily=? WHERE guild_id=? AND user_id=?", (ts, str(guild_id), str(user_id)))

def set_daily_streak(conn: sqlite3.Connection, guild_id: int, user_id: int, streak: int) -> None:
    ensure_user(conn, guild_id, user_id)
    conn.execute("UPDATE guild_users SET daily_streak=? WHERE guild_id=? AND user_id=?", (streak, str(guild_id), str(user_id)))

def set_last_beg(conn: sqlite3.Connection, guild_id: int, user_id: int, ts: int) -> None:
    ensure_user(conn, guild_id, user_id)
    conn.execute("UPDATE guild_users SET last_beg=? WHERE guild_id=? AND user_id=?", (ts, str(guild_id), str(user_id)))

def set_beg_bonus_ready(conn: sqlite3.Connection, guild_id: int, user_id: int, ready: bool) -> None:
    ensure_user(conn, guild_id, user_id)
    conn.execute(
        "UPDATE guild_users SET beg_bonus_ready=? WHERE guild_id=? AND user_id=?",
        (1 if ready else 0, str(guild_id), str(user_id))
    )

def apply_roulette_stats(conn: sqlite3.Connection, guild_id: int, user_id: int, bet: int, net: int) -> None:
    ensure_user(conn, guild_id, user_id)
    is_win = 1 if net > 0 else 0
    is_loss = 1 if net <= 0 else 0

    current_biggest = int(conn.execute(
        "SELECT biggest_win FROM guild_users WHERE guild_id=? AND user_id=?",
        (str(guild_id), str(user_id))
    ).fetchone()["biggest_win"])
    new_biggest = max(current_biggest, net) if net > 0 else current_biggest

    conn.execute("""
        UPDATE guild_users
        SET plays = plays + 1,
            wins = wins + ?,
            losses = losses + ?,
            wagered = wagered + ?,
            profit = profit + ?,
            biggest_win = ?
        WHERE guild_id=? AND user_id=?
    """, (is_win, is_loss, bet, net, new_biggest, str(guild_id), str(user_id)))

def apply_blackjack_stats(conn: sqlite3.Connection, guild_id: int, user_id: int, bet: int, net: int, outcome: str) -> None:
    ensure_user(conn, guild_id, user_id)
    w = 1 if outcome == "win" else 0
    l = 1 if outcome == "loss" else 0
    p = 1 if outcome == "push" else 0

    current_biggest = int(conn.execute(
        "SELECT bj_biggest_win FROM guild_users WHERE guild_id=? AND user_id=?",
        (str(guild_id), str(user_id))
    ).fetchone()["bj_biggest_win"])
    new_biggest = max(current_biggest, net) if net > 0 else current_biggest

    conn.execute("""
        UPDATE guild_users
        SET bj_plays = bj_plays + 1,
            bj_wins = bj_wins + ?,
            bj_losses = bj_losses + ?,
            bj_pushes = bj_pushes + ?,
            bj_wagered = bj_wagered + ?,
            bj_profit = bj_profit + ?,
            bj_biggest_win = ?
        WHERE guild_id=? AND user_id=?
    """, (w, l, p, bet, net, new_biggest, str(guild_id), str(user_id)))

def apply_slots_stats(conn: sqlite3.Connection, guild_id: int, user_id: int, bet: int, net: int) -> None:
    ensure_user(conn, guild_id, user_id)
    is_win = 1 if net > 0 else 0
    is_loss = 1 if net <= 0 else 0

    current_biggest = int(conn.execute(
        "SELECT slots_biggest_win FROM guild_users WHERE guild_id=? AND user_id=?",
        (str(guild_id), str(user_id))
    ).fetchone()["slots_biggest_win"])
    new_biggest = max(current_biggest, net) if net > 0 else current_biggest

    conn.execute("""
        UPDATE guild_users
        SET slots_plays = slots_plays + 1,
            slots_wins = slots_wins + ?,
            slots_losses = slots_losses + ?,
            slots_wagered = slots_wagered + ?,
            slots_profit = slots_profit + ?,
            slots_biggest_win = ?
        WHERE guild_id=? AND user_id=?
    """, (is_win, is_loss, bet, net, new_biggest, str(guild_id), str(user_id)))

def apply_holdem_stats(conn: sqlite3.Connection, guild_id: int, user_id: int, bet: int, net: int, won: bool) -> None:
    ensure_user(conn, guild_id, user_id)
    conn.execute("""
        UPDATE guild_users
        SET he_plays = he_plays + 1,
            he_wins = he_wins + ?,
            he_losses = he_losses + ?,
            he_wagered = he_wagered + ?,
            he_profit = he_profit + ?
        WHERE guild_id=? AND user_id=?
    """, (1 if won else 0, 0 if won else 1, bet, net, str(guild_id), str(user_id)))

def top_users(conn: sqlite3.Connection, guild_id: int, metric: str, limit: int = 10) -> List[sqlite3.Row]:
    metric_sql = {
        "wallet": "wallet",
        "profit": "profit",
        "wins": "wins",
        "bj_profit": "bj_profit",
        "bj_wins": "bj_wins",
        "slots_profit": "slots_profit",
        "he_profit": "he_profit",
    }.get(metric, "wallet")

    return conn.execute(f"""
        SELECT user_id, wallet, profit, wins, bj_profit, bj_wins, slots_profit, he_profit
        FROM guild_users
        WHERE guild_id = ?
        ORDER BY {metric_sql} DESC
        LIMIT ?
    """, (str(guild_id), limit)).fetchall()

def user_rank(conn: sqlite3.Connection, guild_id: int, user_id: int, metric: str) -> int:
    metric_sql = {
        "wallet": "wallet",
        "profit": "profit",
        "wins": "wins",
        "bj_profit": "bj_profit",
        "bj_wins": "bj_wins",
        "slots_profit": "slots_profit",
        "he_profit": "he_profit",
    }.get(metric, "wallet")

    ensure_user(conn, guild_id, user_id)

    value = conn.execute(
        f"SELECT {metric_sql} AS v FROM guild_users WHERE guild_id=? AND user_id=?",
        (str(guild_id), str(user_id))
    ).fetchone()["v"]

    above = conn.execute(
        f"SELECT COUNT(*) AS c FROM guild_users WHERE guild_id=? AND {metric_sql} > ?",
        (str(guild_id), value)
    ).fetchone()["c"]

    return int(above) + 1

# ----------------------------
# ACHIEVEMENTS
# ----------------------------
def has_achievement(conn: sqlite3.Connection, guild_id: int, user_id: int, ach_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM user_achievements WHERE guild_id=? AND user_id=? AND ach_id=?",
        (str(guild_id), str(user_id), ach_id)
    ).fetchone()
    return row is not None

def unlock_achievement(conn: sqlite3.Connection, guild_id: int, user_id: int, ach_id: str) -> Optional[int]:
    if has_achievement(conn, guild_id, user_id, ach_id):
        return None
    ach = conn.execute("SELECT reward FROM achievements WHERE ach_id=?", (ach_id,)).fetchone()
    if ach is None:
        return None
    reward = int(ach["reward"])
    conn.execute("""
        INSERT INTO user_achievements (guild_id, user_id, ach_id, unlocked_at)
        VALUES (?, ?, ?, ?)
    """, (str(guild_id), str(user_id), ach_id, int(time.time())))
    return reward

def list_user_achievements(conn: sqlite3.Connection, guild_id: int, user_id: int) -> List[sqlite3.Row]:
    return conn.execute("""
        SELECT a.ach_id, a.name, a.description, a.reward, ua.unlocked_at
        FROM user_achievements ua
        JOIN achievements a ON a.ach_id = ua.ach_id
        WHERE ua.guild_id=? AND ua.user_id=?
        ORDER BY ua.unlocked_at DESC
    """, (str(guild_id), str(user_id))).fetchall()

def maybe_unlock_common_achs(conn: sqlite3.Connection, guild_id: int, user_id: int, *,
                            bet_amount: Optional[int] = None,
                            wallet_after: Optional[int] = None) -> List[Tuple[str, int]]:
    """Unlock common achievements used by multiple commands. Returns [(name, reward), ...]."""
    newly: List[Tuple[str, int]] = []
    if not achievements_enabled(conn, guild_id):
        return newly

    if bet_amount is not None and bet_amount >= ACH_HIGH_ROLLER_BET:
        r = unlock_achievement(conn, guild_id, user_id, "high_roller")
        if r is not None:
            update_wallet(conn, guild_id, user_id, r)
            newly.append(("High Roller", r))

    if wallet_after is not None and wallet_after >= ACH_MILLIONAIRE_WALLET:
        r = unlock_achievement(conn, guild_id, user_id, "millionaire")
        if r is not None:
            update_wallet(conn, guild_id, user_id, r)
            newly.append(("Millionaire", r))

    return newly

# ----------------------------
# LOANS
# ----------------------------
def loan_row(conn: sqlite3.Connection, guild_id: int, user_id: int) -> sqlite3.Row:
    return conn.execute("SELECT * FROM loans WHERE guild_id=? AND user_id=?", (str(guild_id), str(user_id))).fetchone()

def accrue_loan(conn: sqlite3.Connection, guild_id: int, user_id: int, now: int) -> None:
    row = loan_row(conn, guild_id, user_id)
    if not row:
        return
    balance = int(row["balance"])
    if balance <= 0:
        return
    last = int(row["last_accrual"])
    if last <= 0:
        last = int(row["opened_at"])
    if last <= 0:
        last = now

    days = max(0, (now - last) // (24 * 60 * 60))
    if days <= 0:
        return

    rate_pct = int(row["daily_interest_pct"])
    for _ in range(days):
        balance += (balance * rate_pct) // 100

    conn.execute("""
        UPDATE loans SET balance=?, last_accrual=? WHERE guild_id=? AND user_id=?
    """, (balance, last + days * 24 * 60 * 60, str(guild_id), str(user_id)))

def set_loan(conn: sqlite3.Connection, guild_id: int, user_id: int, principal: int, balance: int, now: int) -> None:
    conn.execute("""
        INSERT INTO loans (guild_id, user_id, principal, balance, daily_interest_pct, origination_fee_pct, opened_at, last_accrual)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id) DO UPDATE SET
            principal=excluded.principal,
            balance=excluded.balance,
            daily_interest_pct=excluded.daily_interest_pct,
            origination_fee_pct=excluded.origination_fee_pct,
            opened_at=excluded.opened_at,
            last_accrual=excluded.last_accrual
    """, (str(guild_id), str(user_id), principal, balance, LOAN_DAILY_INTEREST_PCT, LOAN_ORIGINATION_FEE_PCT, now, now))

def clear_loan(conn: sqlite3.Connection, guild_id: int, user_id: int) -> None:
    conn.execute("DELETE FROM loans WHERE guild_id=? AND user_id=?", (str(guild_id), str(user_id)))

# ----------------------------
# BOT SETUP
# ----------------------------
intents = discord.Intents.default()
intents.members = True  # helps leaderboard display names reliably
bot = commands.Bot(command_prefix="!", intents=intents)

def require_guild(interaction: discord.Interaction) -> Optional[discord.Embed]:
    if interaction.guild is None:
        return discord.Embed(title="Not available in DMs", description="Use these commands inside a server.")
    return None

def is_admin_member(member: discord.Member) -> bool:
    return member.guild_permissions.administrator

@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"✅ Logged in as {bot.user} | Synced {len(synced)} commands")
    except Exception as e:
        print(f"❌ Command sync failed: {e}")

async def resolve_display_name(guild: discord.Guild, user_id: int) -> str:
    m = guild.get_member(user_id)
    if m is not None:
        return m.display_name
    try:
        m = await guild.fetch_member(user_id)
        return m.display_name
    except Exception:
        return f"User {user_id}"

def _validate_bet(bet: int) -> Optional[str]:
    if bet < MIN_BET or bet > MAX_BET:
        return f"Bet must be between **{MIN_BET:,}** and **{MAX_BET:,}** coins."
    return None

# ----------------------------
# BASIC COMMANDS
# ----------------------------
@bot.tree.command(name="balance", description="Check your (or someone else's) balance.")
@app_commands.describe(user="Optional: check someone else's balance")
async def balance(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    target = user or interaction.user
    with db_connect() as conn:
        row = get_user(conn, interaction.guild.id, target.id)
        wallet = int(row["wallet"])
    await interaction.response.send_message(embed=discord.Embed(
        title="Balance",
        description=f"**{target.mention}** has **{wallet:,}** coins."
    ))

# ----------------------------
# /DAILY (streaks + bonuses)
# ----------------------------
@bot.tree.command(name="daily", description="Claim your daily coins (streaks + bonuses).")
async def daily(interaction: discord.Interaction):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    now = int(time.time())

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)

        last = int(row["last_daily"])
        streak = int(row["daily_streak"])

        if last > 0 and (now - last) < DAILY_COOLDOWN_SECONDS:
            remaining = DAILY_COOLDOWN_SECONDS - (now - last)
            hrs = remaining // 3600
            mins = (remaining % 3600) // 60
            secs = remaining % 60
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Daily",
                    description=f"You're still on cooldown. Try again in **{hrs}h {mins}m {secs}s**."
                ),
                ephemeral=True
            )

        if last <= 0:
            new_streak = 1
        else:
            gap = now - last
            if gap <= DAILY_STREAK_GRACE_SECONDS:
                new_streak = streak + 1 if streak > 0 else 1
            else:
                new_streak = 1

        bonus = STREAK_BONUS.get(new_streak, 0)
        payout = DAILY_AMOUNT + bonus

        set_last_daily(conn, interaction.guild.id, interaction.user.id, now)
        set_daily_streak(conn, interaction.guild.id, interaction.user.id, new_streak)
        new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, payout)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id):
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "first_daily")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("First Daily", r))
            if new_streak >= 7:
                r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "streak_7")
                if r is not None:
                    update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                    newly.append(("7-Day Streak", r))
            if new_streak >= 30:
                r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "streak_30")
                if r is not None:
                    update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                    newly.append(("30-Day Streak", r))

        # common achievements (millionaire check)
        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, wallet_after=new_wallet)

        conn.commit()

    desc = (
        f"You claimed **{DAILY_AMOUNT:,}** coins.\n"
        f"Streak: **{new_streak}** day(s)\n"
    )
    if bonus:
        desc += f"Streak bonus: **+{bonus:,}**\n"
    desc += f"\nTotal: **+{payout:,}**\nBalance: **{new_wallet:,}**"

    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(embed=discord.Embed(title="Daily", description=desc))

# ----------------------------
# /BEG (only if <10 coins, 1 hour cooldown, +50)
# ----------------------------
BEG_COOLDOWN_SECONDS = 60 * 60  # 1 hour
BEG_MIN_WALLET_ALLOWED = 10     # must be strictly less than this
BEG_PAYOUT = 50

@bot.tree.command(name="beg", description="Beg for coins (only if you have <10 coins). 1 hour cooldown.")
async def beg(interaction: discord.Interaction):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    now = int(time.time())

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)

        wallet = int(row["wallet"])
        last = int(row["last_beg"])

        if wallet >= BEG_MIN_WALLET_ALLOWED:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Beg",
                    description="You have enough, play for more coins."
                ),
                ephemeral=True
            )

        remaining = BEG_COOLDOWN_SECONDS - (now - last)
        if remaining > 0:
            mins = remaining // 60
            secs = remaining % 60
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Beg",
                    description=(
                        f"**HAHA!** You're **super greedy**.\n"
                        f"\"I only have so many coins to give, be more grateful and do better in the games.\" \n\n"
                        f"Try again in **{mins}m {secs}s**."
                    )
                ),
                ephemeral=True
            )

        set_last_beg(conn, interaction.guild.id, interaction.user.id, now)
        new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, BEG_PAYOUT)
        set_beg_bonus_ready(conn, interaction.guild.id, interaction.user.id, False)

        newly = maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, wallet_after=new_wallet)

        conn.commit()

    desc = f"Yes, grovel for coins\n\nYou received **{BEG_PAYOUT:,}** coins.\nBalance: **{new_wallet:,}**"
    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(embed=discord.Embed(title="Beg", description=desc))

# ----------------------------
# ROULETTE
# ----------------------------
roulette = app_commands.Group(name="roulette", description="American roulette (0 and 00). Bet color or a number.")
bot.tree.add_command(roulette)

color_choices = [
    app_commands.Choice(name="red", value="red"),
    app_commands.Choice(name="black", value="black"),
]

@roulette.command(name="color", description="Bet on red or black.")
@app_commands.describe(bet="How many coins to bet", color="Pick red or black")
@app_commands.choices(color=color_choices)
async def roulette_color(interaction: discord.Interaction, bet: int, color: app_commands.Choice[str]):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    msg = _validate_bet(bet)
    if msg:
        return await interaction.response.send_message(embed=discord.Embed(title="Invalid bet", description=msg), ephemeral=True)

    choice = color.value
    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(row["wallet"])
        if bet > wallet:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(title="Not enough coins", description=f"You have **{wallet:,}** coins."),
                ephemeral=True
            )

        update_wallet(conn, interaction.guild.id, interaction.user.id, -bet)

        roll: Pocket = random.choice(POCKETS)
        rolled_color = pocket_color(roll)

        payout = bet * COLOR_RETURN_MULT if rolled_color == choice else 0
        fee = 0
        if payout == 0:
            fee = loss_fee(bet)
            if fee:
                update_wallet(conn, interaction.guild.id, interaction.user.id, -fee)

        new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, payout) if payout else int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
        net = payout - bet - fee
        apply_roulette_stats(conn, interaction.guild.id, interaction.user.id, bet, net)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id) and net >= 10_000:
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "roulette_big")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("Big Spin", r))

        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=bet, wallet_after=new_wallet)

        conn.commit()

    net_text = f"+{net:,}" if net >= 0 else f"{net:,}"
    desc = (
        f"You bet **{bet:,}** on **{choice}**.\n"
        f"Wheel: **{fmt_pocket(roll)} ({rolled_color})**\n"
    )
    if fee:
        desc += f"Loss fee: **-{fee:,}**\n"
    desc += f"\nNet: **{net_text}** coins\nNew balance: **{new_wallet:,}** coins"
    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(embed=discord.Embed(title="Roulette (Color)", description=desc))

@roulette.command(name="number", description="Bet a specific pocket (0, 00, or 1-36).")
@app_commands.describe(bet="How many coins to bet", pocket="Choose 0, 00, or 1-36 (type 00 for double zero)")
async def roulette_number(interaction: discord.Interaction, bet: int, pocket: str):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    msg = _validate_bet(bet)
    if msg:
        return await interaction.response.send_message(embed=discord.Embed(title="Invalid bet", description=msg), ephemeral=True)

    pocket = pocket.strip()
    chosen: Pocket
    if pocket == "00":
        chosen = "00"
    else:
        try:
            n = int(pocket)
        except ValueError:
            return await interaction.response.send_message(
                embed=discord.Embed(title="Invalid pocket", description="Pocket must be **00**, **0**, or **1-36**."),
                ephemeral=True
            )
        if n < 0 or n > 36:
            return await interaction.response.send_message(
                embed=discord.Embed(title="Invalid pocket", description="Pocket must be **00**, **0**, or **1-36**."),
                ephemeral=True
            )
        chosen = n

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(row["wallet"])
        if bet > wallet:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(title="Not enough coins", description=f"You have **{wallet:,}** coins."),
                ephemeral=True
            )

        update_wallet(conn, interaction.guild.id, interaction.user.id, -bet)

        roll: Pocket = random.choice(POCKETS)
        rolled_color = pocket_color(roll)

        payout = bet * STRAIGHT_UP_RETURN_MULT if roll == chosen else 0
        fee = 0
        if payout == 0:
            fee = loss_fee(bet)
            if fee:
                update_wallet(conn, interaction.guild.id, interaction.user.id, -fee)

        new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, payout) if payout else int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
        net = payout - bet - fee
        apply_roulette_stats(conn, interaction.guild.id, interaction.user.id, bet, net)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id) and net >= 10_000:
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "roulette_big")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("Big Spin", r))

        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=bet, wallet_after=new_wallet)

        conn.commit()

    net_text = f"+{net:,}" if net >= 0 else f"{net:,}"
    desc = (
        f"You bet **{bet:,}** on **{fmt_pocket(chosen)}**.\n"
        f"Wheel: **{fmt_pocket(roll)} ({rolled_color})**\n"
    )
    if fee:
        desc += f"Loss fee: **-{fee:,}**\n"
    desc += f"\nNet: **{net_text}** coins\nNew balance: **{new_wallet:,}** coins"
    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(embed=discord.Embed(title="Roulette (Number)", description=desc))

# ----------------------------
# SLOTS (3-reel, weighted, payout table)
# ----------------------------
SLOTS_SYMBOLS = ["🍒", "🍋", "🍇", "🔔", "💎", "👑"]
SLOTS_WEIGHTS = [40, 30, 18, 8, 3, 1]  # totals to 100

SLOTS_PAYOUT = {
    "🍒": 2,
    "🍋": 3,
    "🍇": 5,
    "🔔": 10,
    "💎": 25,
    "👑": 100,  # jackpot
}

def slots_spin() -> List[str]:
    return random.choices(SLOTS_SYMBOLS, weights=SLOTS_WEIGHTS, k=3)

@bot.tree.command(name="slots", description="Spin slots (3 reels). Match 3 to win.")
@app_commands.describe(bet="How many coins to bet")
async def slots(interaction: discord.Interaction, bet: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    if bet < SLOTS_MIN_BET or bet > SLOTS_MAX_BET:
        return await interaction.response.send_message(
            embed=discord.Embed(title="Invalid bet", description=f"Bet must be between **{SLOTS_MIN_BET:,}** and **{SLOTS_MAX_BET:,}**."),
            ephemeral=True
        )

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(row["wallet"])
        if bet > wallet:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(title="Not enough coins", description=f"You have **{wallet:,}** coins."),
                ephemeral=True
            )

        update_wallet(conn, interaction.guild.id, interaction.user.id, -bet)

        reels = slots_spin()
        payout = 0
        jackpot = False

        if reels[0] == reels[1] == reels[2]:
            sym = reels[0]
            mult = SLOTS_PAYOUT.get(sym, 0)
            payout = bet * mult
            jackpot = (sym == "👑")

        new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, payout) if payout else int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
        net = payout - bet

        apply_slots_stats(conn, interaction.guild.id, interaction.user.id, bet, net)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id) and jackpot:
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "slots_jackpot")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("Jackpot", r))

        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=bet, wallet_after=new_wallet)

        conn.commit()

    net_text = f"+{net:,}" if net >= 0 else f"{net:,}"
    line = " | ".join(reels)
    desc = f"🎰 **{line}**\n\nBet: **{bet:,}**\nNet: **{net_text}**\nBalance: **{new_wallet:,}**"
    if payout:
        desc += f"\nPayout: **{payout:,}**"
    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(embed=discord.Embed(title="Slots", description=desc))

# ----------------------------
# BLACKJACK (double + surrender, 3:2 naturals)
# ----------------------------
SUITS = ["♠", "♥", "♦", "♣"]
RANKS = ["A"] + [str(i) for i in range(2, 11)] + ["J", "Q", "K"]

def new_deck() -> List[str]:
    deck = [f"{r}{s}" for s in SUITS for r in RANKS]
    random.shuffle(deck)
    return deck

def draw_card(deck: List[str]) -> str:
    return deck.pop()

def card_value(rank: str) -> int:
    if rank in ("J", "Q", "K"):
        return 10
    if rank == "A":
        return 11
    return int(rank)

def hand_value(cards: List[str]) -> int:
    ranks = [c[:-1] for c in cards]
    total = sum(card_value(r) for r in ranks)
    aces = sum(1 for r in ranks if r == "A")
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total

def is_soft(cards: List[str]) -> bool:
    ranks = [c[:-1] for c in cards]
    total = sum(card_value(r) for r in ranks)
    aces = sum(1 for r in ranks if r == "A")
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    if "A" not in ranks:
        return False
    min_total = sum(1 if r == "A" else (10 if r in ("J", "Q", "K") else int(r)) for r in ranks)
    return (min_total + 10) <= 21

def is_natural_blackjack(cards: List[str]) -> bool:
    return len(cards) == 2 and hand_value(cards) == 21

def frac_mult(amount: int, num: int, den: int) -> int:
    return (amount * num) // den

@dataclass
class BJGame:
    bet: int
    deck: List[str]
    player: List[str]
    dealer: List[str]
    done: bool = False
    doubled: bool = False

BJ_GAMES: Dict[Tuple[int, int], BJGame] = {}  # (guild_id, user_id) -> game

class BlackjackView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int, timeout: float = 75.0):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your blackjack game.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        BJ_GAMES.pop((self.guild_id, self.user_id), None)

    def _render(self, game: BJGame, reveal_dealer: bool) -> discord.Embed:
        pv = hand_value(game.player)
        dv = hand_value(game.dealer)
        dealer_cards = game.dealer if reveal_dealer else [game.dealer[0], "??"]
        dealer_val = dv if reveal_dealer else "?"
        embed = discord.Embed(title="Blackjack")
        embed.add_field(name="Your Hand", value=f"{' '.join(game.player)}  (**{pv}**)", inline=False)
        embed.add_field(name="Dealer", value=f"{' '.join(dealer_cards)}  (**{dealer_val}**)", inline=False)
        extra = " | DOUBLED" if game.doubled else ""
        embed.set_footer(text=f"Bet: {game.bet:,} coins{extra}")
        return embed

    async def _edit_message(self, interaction: discord.Interaction, *, embed: discord.Embed, view: Optional[discord.ui.View]):
        # IMPORTANT FIX: slash-command interactions may already have a response sent.
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    async def _finish(self, interaction: discord.Interaction, outcome: str, payout: int, net: int, note: str = ""):
        key = (interaction.guild.id, interaction.user.id)
        game = BJ_GAMES.get(key)
        if not game:
            return

        with db_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if payout:
                new_wallet = update_wallet(conn, interaction.guild.id, interaction.user.id, payout)
            else:
                new_wallet = int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])

            apply_blackjack_stats(conn, interaction.guild.id, interaction.user.id, game.bet, net, outcome)

            newly: List[Tuple[str, int]] = []
            if achievements_enabled(conn, interaction.guild.id):
                if is_natural_blackjack(game.player):
                    r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "bj_blackjack")
                    if r is not None:
                        update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                        newly.append(("Natural Blackjack", r))
                if game.doubled and outcome == "win":
                    r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "bj_double")
                    if r is not None:
                        update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                        newly.append(("Double Trouble", r))

            newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=game.bet, wallet_after=new_wallet)

            conn.commit()

        game.done = True
        BJ_GAMES.pop(key, None)
        self.clear_items()
        embed = self._render(game, reveal_dealer=True)

        net_text = f"+{net:,}" if net >= 0 else f"{net:,}"
        result_line = {"win": "✅ You win!", "loss": "❌ You lose.", "push": "➖ Push."}[outcome]
        desc = f"{result_line} Net: **{net_text}**\nBalance: **{new_wallet:,}**"
        if note:
            desc = note + "\n" + desc
        if newly:
            desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

        embed.description = desc
        await self._edit_message(interaction, embed=embed, view=self)

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.primary)
    async def hit(self, interaction: discord.Interaction, _: discord.ui.Button):
        key = (interaction.guild.id, interaction.user.id)
        game = BJ_GAMES.get(key)
        if not game or game.done:
            return await interaction.response.send_message("No active blackjack game.", ephemeral=True)

        game.player.append(draw_card(game.deck))
        pv = hand_value(game.player)
        if pv > 21:
            await self._finish(interaction, "loss", payout=0, net=-game.bet, note="You busted.")
            return
        await interaction.response.edit_message(embed=self._render(game, reveal_dealer=False), view=self)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.success)
    async def stand(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._dealer_and_resolve(interaction)

    async def _dealer_and_resolve(self, interaction: discord.Interaction):
        key = (interaction.guild.id, interaction.user.id)
        game = BJ_GAMES.get(key)
        if not game or game.done:
            return

        while True:
            dv = hand_value(game.dealer)
            if dv < 17:
                game.dealer.append(draw_card(game.deck))
                continue
            if dv == 17 and (not BJ_DEALER_STANDS_SOFT_17) and is_soft(game.dealer):
                game.dealer.append(draw_card(game.deck))
                continue
            break

        pv = hand_value(game.player)
        dv = hand_value(game.dealer)

        if dv > 21 or pv > dv:
            payout = frac_mult(game.bet, BJ_WIN_RETURN_MULT_NUM, BJ_WIN_RETURN_MULT_DEN)
            net = payout - game.bet
            await self._finish(interaction, "win", payout=payout, net=net)
        elif pv == dv:
            payout = frac_mult(game.bet, BJ_PUSH_RETURN_MULT_NUM, BJ_PUSH_RETURN_MULT_DEN)
            net = payout - game.bet
            await self._finish(interaction, "push", payout=payout, net=net)
        else:
            await self._finish(interaction, "loss", payout=0, net=-game.bet)

    @discord.ui.button(label="Double", style=discord.ButtonStyle.secondary)
    async def double(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not BJ_ALLOW_DOUBLE:
            return await interaction.response.send_message("Doubling is disabled.", ephemeral=True)

        key = (interaction.guild.id, interaction.user.id)
        game = BJ_GAMES.get(key)
        if not game or game.done:
            return await interaction.response.send_message("No active blackjack game.", ephemeral=True)

        if len(game.player) != 2 or game.doubled:
            return await interaction.response.send_message("You can only double right after the deal.", ephemeral=True)

        with db_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = get_user(conn, interaction.guild.id, interaction.user.id)
            wallet = int(row["wallet"])
            if game.bet > wallet:
                conn.rollback()
                return await interaction.response.send_message("Not enough coins to double.", ephemeral=True)
            update_wallet(conn, interaction.guild.id, interaction.user.id, -game.bet)
            conn.commit()

        game.bet *= 2
        game.doubled = True
        game.player.append(draw_card(game.deck))

        pv = hand_value(game.player)
        if pv > 21:
            await self._finish(interaction, "loss", payout=0, net=-game.bet, note="You doubled and busted.")
            return

        await self._dealer_and_resolve(interaction)

    @discord.ui.button(label="Surrender", style=discord.ButtonStyle.danger)
    async def surrender(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not BJ_ALLOW_SURRENDER:
            return await interaction.response.send_message("Surrender is disabled.", ephemeral=True)

        key = (interaction.guild.id, interaction.user.id)
        game = BJ_GAMES.get(key)
        if not game or game.done:
            return await interaction.response.send_message("No active blackjack game.", ephemeral=True)

        if len(game.player) != 2 or game.doubled:
            return await interaction.response.send_message("You can only surrender right after the deal.", ephemeral=True)

        payout = game.bet // 2
        net = payout - game.bet
        await self._finish(interaction, "loss", payout=payout, net=net, note="You surrendered.")

@bot.tree.command(name="blackjack", description="Play blackjack vs dealer (double/surrender, 3:2 naturals).")
@app_commands.describe(bet="How many coins to bet")
async def blackjack(interaction: discord.Interaction, bet: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    msg = _validate_bet(bet)
    if msg:
        return await interaction.response.send_message(embed=discord.Embed(title="Invalid bet", description=msg), ephemeral=True)

    key = (interaction.guild.id, interaction.user.id)
    if key in BJ_GAMES:
        return await interaction.response.send_message("You already have an active blackjack game.", ephemeral=True)

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(row["wallet"])
        if bet > wallet:
            conn.rollback()
            return await interaction.response.send_message(embed=discord.Embed(title="Not enough coins", description=f"You have **{wallet:,}** coins."), ephemeral=True)
        update_wallet(conn, interaction.guild.id, interaction.user.id, -bet)

        # high roller check uses bet amount (deduct happens here)
        newly = maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=bet)
        conn.commit()

    deck = new_deck()
    game = BJGame(bet=bet, deck=deck, player=[draw_card(deck), draw_card(deck)], dealer=[draw_card(deck), draw_card(deck)])
    BJ_GAMES[key] = game

    view = BlackjackView(interaction.guild.id, interaction.user.id)
    embed = view._render(game, reveal_dealer=False)

    if is_natural_blackjack(game.player):
        dealer_bj = is_natural_blackjack(game.dealer)
        # Send initial message, then resolve via _finish (fixed to edit original response safely)
        await interaction.response.send_message(embed=embed, view=view)
        view.clear_items()
        if dealer_bj:
            payout = game.bet
            net = 0
            await view._finish(interaction, "push", payout=payout, net=net, note="Both have blackjack.")
        else:
            profit = (game.bet * BJ_NATURAL_PROFIT_NUM) // BJ_NATURAL_PROFIT_DEN
            payout = game.bet + profit
            net = payout - game.bet
            await view._finish(interaction, "win", payout=payout, net=net, note="Natural blackjack! (3:2)")
        return

    await interaction.response.send_message(embed=embed, view=view)

# ----------------------------
# TEXAS HOLD'EM (HEADS-UP vs BOT)
# ----------------------------
import asyncio
import secrets

RANK_ORDER = {r: i for i, r in enumerate(["2","3","4","5","6","7","8","9","10","J","Q","K","A"], start=2)}

def card_rank(c: str) -> str:
    return c[:-1]

def card_suit(c: str) -> str:
    return c[-1]

def poker_score_5(cards: List[str]) -> Tuple[int, List[int]]:
    ranks = [card_rank(c) for c in cards]
    suits = [card_suit(c) for c in cards]
    vals = sorted([RANK_ORDER[r] for r in ranks], reverse=True)

    counts: Dict[int, int] = {}
    for v in vals:
        counts[v] = counts.get(v, 0) + 1
    groups = sorted(counts.items(), key=lambda x: (x[1], x[0]), reverse=True)

    is_flush = len(set(suits)) == 1

    unique = sorted(set(vals), reverse=True)
    is_straight = False
    top_straight = None
    if len(unique) >= 5:
        for i in range(len(unique) - 4):
            window = unique[i:i+5]
            if window[0] - window[4] == 4:
                is_straight = True
                top_straight = window[0]
                break
        if not is_straight and set([14,5,4,3,2]).issubset(set(vals)):
            is_straight = True
            top_straight = 5

    if is_straight and is_flush:
        return (8, [top_straight])

    if groups[0][1] == 4:
        quad = groups[0][0]
        kicker = max(v for v in vals if v != quad)
        return (7, [quad, kicker])

    if groups[0][1] == 3 and len(groups) > 1 and groups[1][1] >= 2:
        trips = groups[0][0]
        pair = groups[1][0]
        return (6, [trips, pair])

    if is_flush:
        return (5, vals)

    if is_straight:
        return (4, [top_straight])

    if groups[0][1] == 3:
        trips = groups[0][0]
        kickers = [v for v in vals if v != trips][:2]
        return (3, [trips] + kickers)

    if groups[0][1] == 2 and len(groups) > 1 and groups[1][1] == 2:
        hi = max(groups[0][0], groups[1][0])
        lo = min(groups[0][0], groups[1][0])
        kicker = max(v for v in vals if v != hi and v != lo)
        return (2, [hi, lo, kicker])

    if groups[0][1] == 2:
        pair = groups[0][0]
        kickers = [v for v in vals if v != pair][:3]
        return (1, [pair] + kickers)

    return (0, vals)

def poker_best_7(cards7: List[str]) -> Tuple[int, List[int]]:
    best = None
    for combo in itertools.combinations(cards7, 5):
        score = poker_score_5(list(combo))
        if best is None or score > best:
            best = score
    return best if best is not None else (0, [])

def _stage_name(stage: int) -> str:
    return ["Preflop", "Flop", "Turn", "River", "Showdown"][stage]

def _community_for_stage(full_board: List[str], stage: int) -> List[str]:
    if stage <= 0:
        return []
    if stage == 1:
        return full_board[:3]
    if stage == 2:
        return full_board[:4]
    return full_board[:5]

HE_BOT_CALL_MAX_PCT_OF_POT = 60
HE_BOT_RAISE_CHANCE = 12
HE_BOT_RAISE_PCT_OF_POT = 40

@dataclass
class HoldemHU:
    game_id: str
    ante: int
    deck: List[str]
    player_hole: List[str]
    bot_hole: List[str]
    full_board: List[str]
    stage: int = 0
    pot: int = 0
    to_call_player: int = 0
    to_call_bot: int = 0
    invested_player: int = 0
    invested_bot: int = 0
    done: bool = False
    last_action: str = ""

HE_HU_GAMES_BY_ID: Dict[str, HoldemHU] = {}
HE_HU_ACTIVE_BY_USER: Dict[Tuple[int, int], str] = {}
HE_HU_LOCKS: Dict[Tuple[int, int], asyncio.Lock] = {}

def _he_lock_for(key: Tuple[int, int]) -> asyncio.Lock:
    lock = HE_HU_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        HE_HU_LOCKS[key] = lock
    return lock

class HoldemHUView(discord.ui.View):
    def __init__(self, guild_id: int, user_id: int, game_id: str, timeout: float = 90.0):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.user_id = user_id
        self.game_id = game_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("This isn't your Hold'em game.", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        key = (self.guild_id, self.user_id)
        active_id = HE_HU_ACTIVE_BY_USER.get(key)
        if active_id == self.game_id:
            HE_HU_ACTIVE_BY_USER.pop(key, None)
        HE_HU_GAMES_BY_ID.pop(self.game_id, None)

    async def _get_game_or_reply(self, interaction: discord.Interaction) -> Optional[HoldemHU]:
        game = HE_HU_GAMES_BY_ID.get(self.game_id)
        if not game or game.done:
            await interaction.response.send_message("No active Hold'em game.", ephemeral=True)
            return None
        return game

    def _render(self, game: HoldemHU, reveal_bot: bool = False) -> discord.Embed:
        comm = _community_for_stage(game.full_board, game.stage)
        board_text = " ".join(comm) if comm else "_(none yet)_"
        ph = " ".join(game.player_hole)
        bh = " ".join(game.bot_hole) if reveal_bot else "?? ??"

        embed = discord.Embed(title="Texas Hold'em — You vs Bot")
        embed.add_field(name="Stage", value=_stage_name(game.stage), inline=True)
        embed.add_field(name="Pot", value=f"**{game.pot:,}**", inline=True)
        embed.add_field(name="To Call (You)", value=f"**{game.to_call_player:,}**", inline=True)

        embed.add_field(name="Board", value=f"**{board_text}**", inline=False)
        embed.add_field(name="Your Hole", value=f"**{ph}**", inline=True)
        embed.add_field(name="Bot Hole", value=f"**{bh}**", inline=True)

        if game.last_action:
            embed.set_footer(text=game.last_action)

        return embed

    async def _safe_wallet_delta(self, guild_id: int, user_id: int, delta: int) -> bool:
        with db_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = get_user(conn, guild_id, user_id)
            wallet = int(row["wallet"])
            if delta < 0 and (-delta) > wallet:
                conn.rollback()
                return False
            update_wallet(conn, guild_id, user_id, delta)
            conn.commit()
        return True

    def _bot_decision(self, game: HoldemHU) -> str:
        if game.to_call_bot <= 0:
            if random.randint(1, 100) <= HE_BOT_RAISE_CHANCE:
                return "raise"
            return "check"

        pot_if_call = game.pot + game.to_call_bot
        pct = 100 if pot_if_call <= 0 else int((game.to_call_bot * 100) / max(1, pot_if_call))
        if pct <= HE_BOT_CALL_MAX_PCT_OF_POT:
            if random.randint(1, 100) <= 10:
                return "raise"
            return "call"
        return "call" if random.randint(1, 100) <= 25 else "fold"

    def _next_stage(self, game: HoldemHU) -> None:
        game.to_call_player = 0
        game.to_call_bot = 0
        game.stage += 1
        if game.stage > 3:
            game.stage = 4

    def _at_showdown(self, game: HoldemHU) -> bool:
        return game.stage >= 4

    async def _finish(self, interaction: discord.Interaction, game: HoldemHU, player_won: Optional[bool], note: str):
        key = (interaction.guild.id, interaction.user.id)

        payout = 0
        if player_won is True:
            payout = game.pot
            await self._safe_wallet_delta(interaction.guild.id, interaction.user.id, payout)
        elif player_won is None:
            payout = game.invested_player
            await self._safe_wallet_delta(interaction.guild.id, interaction.user.id, payout)

        net = payout - game.invested_player

        with db_connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            apply_holdem_stats(conn, interaction.guild.id, interaction.user.id, game.invested_player, net, won=(player_won is True))

            newly: List[Tuple[str, int]] = []
            if (player_won is True) and achievements_enabled(conn, interaction.guild.id):
                r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "holdem_win")
                if r is not None:
                    update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                    newly.append(("River King", r))

            # common achievements
            wallet_after = int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
            newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=game.ante, wallet_after=wallet_after)

            conn.commit()

        game.done = True

        active_id = HE_HU_ACTIVE_BY_USER.get(key)
        if active_id == game.game_id:
            HE_HU_ACTIVE_BY_USER.pop(key, None)
        HE_HU_GAMES_BY_ID.pop(game.game_id, None)

        self.clear_items()

        embed = self._render(game, reveal_bot=True)

        net_text = f"+{net:,}" if net >= 0 else f"{net:,}"
        result = "➖ Push." if player_won is None else ("✅ You win!" if player_won else "❌ You lose.")
        embed.description = f"{note}\n\n{result} Net: **{net_text}**"
        await interaction.response.edit_message(embed=embed, view=self)

    async def _resolve_showdown(self, interaction: discord.Interaction, game: HoldemHU):
        board = game.full_board[:5]
        p_score = poker_best_7(game.player_hole + board)
        b_score = poker_best_7(game.bot_hole + board)

        if p_score > b_score:
            await self._finish(interaction, game, True, "Showdown.")
        elif p_score < b_score:
            await self._finish(interaction, game, False, "Showdown.")
        else:
            await self._finish(interaction, game, None, "Showdown (tie).")

    async def _bot_act(self, interaction: discord.Interaction, game: HoldemHU):
        decision = self._bot_decision(game)

        if decision == "fold":
            game.last_action = "Bot folded."
            await self._finish(interaction, game, True, "Bot folded.")
            return

        if decision == "call":
            game.pot += game.to_call_bot
            game.invested_bot += game.to_call_bot
            game.last_action = f"Bot called **{game.to_call_bot:,}**."
            game.to_call_bot = 0
            game.to_call_player = 0

        elif decision == "check":
            game.last_action = "Bot checked."

        elif decision == "raise":
            base_pot = max(1, game.pot)
            raise_amt = max(game.ante, (base_pot * HE_BOT_RAISE_PCT_OF_POT) // 100)
            raise_amt = min(raise_amt, MAX_BET)
            total = game.to_call_bot + raise_amt

            game.pot += total
            game.invested_bot += total
            game.to_call_bot = 0

            game.to_call_player = raise_amt
            game.last_action = f"Bot raised. You must call **{raise_amt:,}** or fold."

        if game.to_call_player == 0 and game.to_call_bot == 0:
            if game.stage < 3:
                self._next_stage(game)
                game.last_action += " | Dealt next card(s)."
            else:
                game.stage = 4

        if self._at_showdown(game):
            await self._resolve_showdown(interaction, game)
            return

        await interaction.response.edit_message(embed=self._render(game, reveal_bot=False), view=self)

    async def _player_bet_or_raise(self, interaction: discord.Interaction, amount: int):
        key = (interaction.guild.id, interaction.user.id)
        async with _he_lock_for(key):
            game = HE_HU_GAMES_BY_ID.get(self.game_id)
            if not game or game.done:
                return await interaction.response.send_message("No active Hold'em game.", ephemeral=True)

            if amount <= 0:
                return await interaction.response.send_message("Bet must be positive.", ephemeral=True)

            total = game.to_call_player + amount

            ok = await self._safe_wallet_delta(interaction.guild.id, interaction.user.id, -total)
            if not ok:
                return await interaction.response.send_message("You don't have enough coins for that bet/raise.", ephemeral=True)

            game.pot += total
            game.invested_player += total

            game.to_call_bot = amount
            game.to_call_player = 0
            game.last_action = f"You bet/raised **{total:,}** (raise amount **{amount:,}**)."

            await self._bot_act(interaction, game)

    @discord.ui.button(label="Check / Call", style=discord.ButtonStyle.success)
    async def check_call(self, interaction: discord.Interaction, _: discord.ui.Button):
        key = (interaction.guild.id, interaction.user.id)
        async with _he_lock_for(key):
            game = await self._get_game_or_reply(interaction)
            if not game:
                return

            if game.to_call_player > 0:
                need = game.to_call_player
                ok = await self._safe_wallet_delta(interaction.guild.id, interaction.user.id, -need)
                if not ok:
                    return await interaction.response.send_message("You don't have enough coins to call.", ephemeral=True)

                game.pot += need
                game.invested_player += need
                game.last_action = f"You called **{need:,}**."
                game.to_call_player = 0
                game.to_call_bot = 0
            else:
                game.last_action = "You checked."

            await self._bot_act(interaction, game)

    @discord.ui.button(label="Bet / Raise", style=discord.ButtonStyle.primary)
    async def bet_raise(self, interaction: discord.Interaction, _: discord.ui.Button):
        async with _he_lock_for((interaction.guild.id, interaction.user.id)):
            game = await self._get_game_or_reply(interaction)
            if not game:
                return
            await interaction.response.send_modal(BetModal(self))

    @discord.ui.button(label="Fold", style=discord.ButtonStyle.danger)
    async def fold(self, interaction: discord.Interaction, _: discord.ui.Button):
        async with _he_lock_for((interaction.guild.id, interaction.user.id)):
            game = await self._get_game_or_reply(interaction)
            if not game:
                return
            game.last_action = "You folded."
            await self._finish(interaction, game, False, "You folded.")

class BetModal(discord.ui.Modal, title="Hold'em Bet / Raise"):
    amount = discord.ui.TextInput(
        label="Amount",
        placeholder="Enter how many coins to bet/raise",
        required=True,
        max_length=10
    )

    def __init__(self, view: HoldemHUView):
        super().__init__()
        self.view_ref = view

    async def on_submit(self, interaction: discord.Interaction):
        view = self.view_ref
        game = HE_HU_GAMES_BY_ID.get(view.game_id)
        if not game or game.done:
            return await interaction.response.send_message("No active Hold'em game.", ephemeral=True)

        try:
            amt = int(str(self.amount).strip())
        except ValueError:
            return await interaction.response.send_message("Enter a valid number.", ephemeral=True)

        await view._player_bet_or_raise(interaction, amt)

@bot.tree.command(name="holdem", description="Play Texas Hold'em heads-up vs a bot (buttons: check/call, bet/raise, fold).")
@app_commands.describe(ante="Ante to start (both you and bot put this in the pot).")
async def holdem(interaction: discord.Interaction, ante: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    if ante < MIN_BET or ante > MAX_BET:
        return await interaction.response.send_message(
            embed=discord.Embed(title="Invalid ante", description=f"Ante must be between **{MIN_BET:,}** and **{MAX_BET:,}**."),
            ephemeral=True
        )

    key = (interaction.guild.id, interaction.user.id)

    active_id = HE_HU_ACTIVE_BY_USER.get(key)
    if active_id:
        g = HE_HU_GAMES_BY_ID.get(active_id)
        if g and not g.done:
            return await interaction.response.send_message("You already have an active Hold'em game.", ephemeral=True)
        HE_HU_ACTIVE_BY_USER.pop(key, None)
        HE_HU_GAMES_BY_ID.pop(active_id, None)

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, interaction.user.id)
        if ante > int(row["wallet"]):
            conn.rollback()
            return await interaction.response.send_message("You don't have enough coins to ante.", ephemeral=True)
        update_wallet(conn, interaction.guild.id, interaction.user.id, -ante)

        # high roller check
        newly = maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, bet_amount=ante)
        conn.commit()

    deck = new_deck()
    player_hole = [draw_card(deck), draw_card(deck)]
    bot_hole = [draw_card(deck), draw_card(deck)]
    full_board = [draw_card(deck) for _ in range(5)]

    game_id = secrets.token_hex(8)

    game = HoldemHU(
        game_id=game_id,
        ante=ante,
        deck=deck,
        player_hole=player_hole,
        bot_hole=bot_hole,
        full_board=full_board,
        stage=0,
        pot=ante * 2,
        to_call_player=0,
        to_call_bot=0,
        invested_player=ante,
        invested_bot=ante,
        done=False,
        last_action=f"Both anted **{ante:,}**. Your move."
    )

    HE_HU_GAMES_BY_ID[game_id] = game
    HE_HU_ACTIVE_BY_USER[key] = game_id

    view = HoldemHUView(interaction.guild.id, interaction.user.id, game_id=game_id)
    embed = view._render(game, reveal_bot=False)
    if newly:
        embed.description = "🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])
    await interaction.response.send_message(embed=embed, view=view)

# ----------------------------
# SHOP / COLLECTIBLES
# ----------------------------
def add_to_inventory(conn: sqlite3.Connection, guild_id: int, user_id: int, item_id: str, qty: int = 1) -> None:
    conn.execute("""
        INSERT INTO inventory (guild_id, user_id, item_id, qty)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, user_id, item_id)
        DO UPDATE SET qty = qty + excluded.qty
    """, (str(guild_id), str(user_id), item_id, qty))

@bot.tree.command(name="shop", description="View collectible items you can buy.")
async def shop(interaction: discord.Interaction):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    with db_connect() as conn:
        rows = conn.execute(
            "SELECT item_id, name, price, description FROM items ORDER BY price ASC"
        ).fetchall()

    if not rows:
        return await interaction.response.send_message(
            embed=discord.Embed(title="Shop", description="No items available."),
            ephemeral=True
        )

    lines = []
    for r in rows:
        lines.append(
            f"**{r['item_id']}** — {r['name']} — **{int(r['price']):,}**\n_{r['description']}_"
        )

    embed = discord.Embed(
        title="Shop (Collectibles)",
        description="\n\n".join(lines)
    )

    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="buy", description="Buy a collectible item from the shop.")
@app_commands.describe(item_id="The item_id from /shop", qty="How many to buy (default 1)")
async def buy(interaction: discord.Interaction, item_id: str, qty: Optional[int] = 1):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    item_id = item_id.strip()
    qty = 1 if qty is None else qty

    if qty <= 0 or qty > 100:
        return await interaction.response.send_message(
            embed=discord.Embed(title="Buy", description="qty must be between 1 and 100."),
            ephemeral=True
        )

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")

        item = conn.execute(
            "SELECT item_id, name, price FROM items WHERE item_id=?",
            (item_id,)
        ).fetchone()

        if not item:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(title="Buy", description="Invalid item_id. Use /shop."),
                ephemeral=True
            )

        price = int(item["price"])
        total = price * qty

        u = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(u["wallet"])

        if total > wallet:
            conn.rollback()
            return await interaction.response.send_message(
                embed=discord.Embed(
                    title="Not enough coins",
                    description=f"Cost: **{total:,}**\nYou have: **{wallet:,}**"
                ),
                ephemeral=True
            )

        update_wallet(conn, interaction.guild.id, interaction.user.id, -total)
        add_to_inventory(conn, interaction.guild.id, interaction.user.id, item_id, qty)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id):
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "first_buy")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("First Purchase", r))

        new_wallet = int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, wallet_after=new_wallet)

        conn.commit()

    desc = (
        f"Bought **{qty}× {item['name']}** (`{item_id}`)\n"
        f"Cost: **{total:,}**\n"
        f"Balance: **{new_wallet:,}**"
    )

    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])

    await interaction.response.send_message(
        embed=discord.Embed(title="Purchase complete", description=desc),
        ephemeral=True
    )

@bot.tree.command(name="inventory", description="See your collectible inventory.")
@app_commands.describe(user="Optional: view someone else's inventory")
async def inventory(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    target = user or interaction.user

    with db_connect() as conn:
        rows = conn.execute("""
            SELECT i.item_id, it.name, i.qty
            FROM inventory i
            JOIN items it ON it.item_id = i.item_id
            WHERE i.guild_id=? AND i.user_id=? AND i.qty > 0
            ORDER BY it.price ASC
        """, (str(interaction.guild.id), str(target.id))).fetchall()

    if not rows:
        return await interaction.response.send_message(
            embed=discord.Embed(
                title="Inventory",
                description=f"{target.mention} has no items yet."
            ),
            ephemeral=True
        )

    lines = [
        f"• **{r['name']}** (`{r['item_id']}`) × **{int(r['qty'])}**"
        for r in rows
    ]

    await interaction.response.send_message(
        embed=discord.Embed(
            title="Inventory",
            description=f"For {target.mention}\n\n" + "\n".join(lines)
        ),
        ephemeral=True
    )

# ----------------------------
# LOAN COMMANDS
# ----------------------------
loan = app_commands.Group(name="loan", description="High-interest loans. Dangerous.")
bot.tree.add_command(loan)

@loan.command(name="take", description="Take a high-interest loan (very expensive).")
@app_commands.describe(amount="How much to borrow (principal)")
async def loan_take(interaction: discord.Interaction, amount: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    if amount < 100 or amount > LOAN_MAX_PRINCIPAL:
        return await interaction.response.send_message(embed=discord.Embed(title="Invalid amount", description=f"Amount must be 100–{LOAN_MAX_PRINCIPAL:,}."), ephemeral=True)

    now = int(time.time())
    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = loan_row(conn, interaction.guild.id, interaction.user.id)
        if existing and int(existing["balance"]) > 0:
            accrue_loan(conn, interaction.guild.id, interaction.user.id, now)
            updated = loan_row(conn, interaction.guild.id, interaction.user.id)
            conn.commit()
            return await interaction.response.send_message(
                embed=discord.Embed(title="Loan already active", description=f"You already owe **{int(updated['balance']):,}**. Repay it first."),
                ephemeral=True
            )

        fee = (amount * LOAN_ORIGINATION_FEE_PCT) // 100
        receive = amount - fee
        set_loan(conn, interaction.guild.id, interaction.user.id, principal=amount, balance=amount, now=now)
        update_wallet(conn, interaction.guild.id, interaction.user.id, receive)

        newly: List[Tuple[str, int]] = []
        if achievements_enabled(conn, interaction.guild.id):
            r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "loan_shark")
            if r is not None:
                update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                newly.append(("Loan Shark", r))

        wallet_after = int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
        newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, wallet_after=wallet_after)

        conn.commit()

    desc = (
        f"Principal: **{amount:,}**\n"
        f"Origination fee ({LOAN_ORIGINATION_FEE_PCT}%): **-{fee:,}**\n"
        f"You received: **{receive:,}**\n\n"
        f"Interest: **{LOAN_DAILY_INTEREST_PCT}% per day**, compounding."
    )
    if newly:
        desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])
    await interaction.response.send_message(embed=discord.Embed(title="Loan taken", description=desc))

@loan.command(name="status", description="Check your loan balance (accrues interest).")
async def loan_status(interaction: discord.Interaction):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    now = int(time.time())
    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        accrue_loan(conn, interaction.guild.id, interaction.user.id, now)
        row = loan_row(conn, interaction.guild.id, interaction.user.id)
        conn.commit()

    if not row or int(row["balance"]) <= 0:
        return await interaction.response.send_message(embed=discord.Embed(title="Loan status", description="You have **no active loan**."))

    desc = (
        f"Balance owed: **{int(row['balance']):,}**\n"
        f"Daily interest: **{int(row['daily_interest_pct'])}%**\n"
        f"Opened: <t:{int(row['opened_at'])}:R>\n"
        f"Last accrual: <t:{int(row['last_accrual'])}:R>"
    )
    await interaction.response.send_message(embed=discord.Embed(title="Loan status", description=desc))

@loan.command(name="repay", description="Repay part (or all) of your loan.")
@app_commands.describe(amount="How much to repay")
async def loan_repay(interaction: discord.Interaction, amount: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    if amount <= 0:
        return await interaction.response.send_message(embed=discord.Embed(title="Invalid amount", description="Amount must be positive."), ephemeral=True)

    now = int(time.time())
    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        accrue_loan(conn, interaction.guild.id, interaction.user.id, now)
        row = loan_row(conn, interaction.guild.id, interaction.user.id)
        if not row or int(row["balance"]) <= 0:
            conn.rollback()
            return await interaction.response.send_message(embed=discord.Embed(title="No loan", description="You have no active loan."), ephemeral=True)

        bal = int(row["balance"])
        u = get_user(conn, interaction.guild.id, interaction.user.id)
        wallet = int(u["wallet"])
        pay = min(amount, wallet, bal)

        if pay <= 0:
            conn.rollback()
            return await interaction.response.send_message("You don't have coins to repay.", ephemeral=True)

        update_wallet(conn, interaction.guild.id, interaction.user.id, -pay)
        new_bal = bal - pay

        if new_bal <= 0:
            clear_loan(conn, interaction.guild.id, interaction.user.id)
            newly: List[Tuple[str, int]] = []
            if achievements_enabled(conn, interaction.guild.id):
                r = unlock_achievement(conn, interaction.guild.id, interaction.user.id, "loan_paid")
                if r is not None:
                    update_wallet(conn, interaction.guild.id, interaction.user.id, r)
                    newly.append(("Paid in Blood", r))

            wallet_after = int(get_user(conn, interaction.guild.id, interaction.user.id)["wallet"])
            newly += maybe_unlock_common_achs(conn, interaction.guild.id, interaction.user.id, wallet_after=wallet_after)

            conn.commit()
            desc = f"Paid **{pay:,}**. Loan is **fully repaid**."
            if newly:
                desc += "\n\n🏆 **Achievement unlocked:** " + ", ".join([f"{n} (+{r:,})" for n, r in newly])
            return await interaction.response.send_message(embed=discord.Embed(title="Loan repaid", description=desc))

        conn.execute("UPDATE loans SET balance=? WHERE guild_id=? AND user_id=?", (new_bal, str(interaction.guild.id), str(interaction.user.id)))
        conn.commit()

    await interaction.response.send_message(embed=discord.Embed(
        title="Loan repaid",
        description=f"Paid **{pay:,}**.\nRemaining balance: **{new_bal:,}**"
    ))

# ----------------------------
# STATS / ACHIEVEMENTS / LEADERBOARD
# ----------------------------
@bot.tree.command(name="stats", description="View your casino stats (roulette + blackjack + slots + holdem).")
@app_commands.describe(user="Optional: view someone else's stats")
async def stats(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    target = user or interaction.user
    with db_connect() as conn:
        row = get_user(conn, interaction.guild.id, target.id)

    wallet = int(row["wallet"])
    streak = int(row["daily_streak"])

    plays = int(row["plays"]); wins = int(row["wins"]); losses = int(row["losses"])
    wagered = int(row["wagered"]); profit = int(row["profit"]); biggest_win = int(row["biggest_win"])
    r_win_rate = (wins / plays * 100.0) if plays else 0.0

    bj_plays = int(row["bj_plays"]); bj_wins = int(row["bj_wins"]); bj_losses = int(row["bj_losses"])
    bj_pushes = int(row["bj_pushes"]); bj_wagered = int(row["bj_wagered"]); bj_profit = int(row["bj_profit"])
    bj_big = int(row["bj_biggest_win"])
    b_win_rate = (bj_wins / bj_plays * 100.0) if bj_plays else 0.0

    s_plays = int(row["slots_plays"]); s_profit = int(row["slots_profit"]); s_big = int(row["slots_biggest_win"])
    h_plays = int(row["he_plays"]); h_wins = int(row["he_wins"]); h_profit = int(row["he_profit"])

    embed = discord.Embed(title="Casino Stats", description=f"Stats for **{target.mention}**")
    embed.add_field(name="Balance", value=f"**{wallet:,}** coins", inline=True)
    embed.add_field(name="Daily Streak", value=f"**{streak}**", inline=True)

    embed.add_field(
        name="Roulette",
        value=(
            f"Plays: **{plays:,}** | Win rate: **{r_win_rate:.1f}%**\n"
            f"W/L: **{wins:,}**/**{losses:,}** | Wagered: **{wagered:,}**\n"
            f"Profit: **{profit:+,}** | Biggest: **{biggest_win:+,}**"
        ),
        inline=False
    )

    embed.add_field(
        name="Blackjack",
        value=(
            f"Plays: **{bj_plays:,}** | Win rate: **{b_win_rate:.1f}%**\n"
            f"W/L/P: **{bj_wins:,}**/**{bj_losses:,}**/**{bj_pushes:,}** | Wagered: **{bj_wagered:,}**\n"
            f"Profit: **{bj_profit:+,}** | Biggest: **{bj_big:+,}**"
        ),
        inline=False
    )

    embed.add_field(
        name="Slots",
        value=f"Plays: **{s_plays:,}** | Profit: **{s_profit:+,}** | Biggest: **{s_big:+,}**",
        inline=False
    )

    embed.add_field(
        name="Hold'em",
        value=f"Hands: **{h_plays:,}** | Wins: **{h_wins:,}** | Profit: **{h_profit:+,}**",
        inline=False
    )

    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="achievements", description="Show your unlocked achievements.")
@app_commands.describe(user="Optional: view someone else's achievements")
async def achievements_cmd(interaction: discord.Interaction, user: Optional[discord.Member] = None):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    target = user or interaction.user
    with db_connect() as conn:
        rows = list_user_achievements(conn, interaction.guild.id, target.id)

    if not rows:
        return await interaction.response.send_message(embed=discord.Embed(title="Achievements", description=f"{target.mention} has no achievements yet."))

    lines = [f"• **{r['name']}** — {r['description']}" for r in rows[:25]]
    await interaction.response.send_message(embed=discord.Embed(title="Achievements", description=f"For {target.mention}\n\n" + "\n".join(lines)))

lb_choices = [
    app_commands.Choice(name="wealth (coins)", value="wallet"),
    app_commands.Choice(name="roulette profit", value="profit"),
    app_commands.Choice(name="roulette wins", value="wins"),
    app_commands.Choice(name="blackjack profit", value="bj_profit"),
    app_commands.Choice(name="slots profit", value="slots_profit"),
    app_commands.Choice(name="holdem profit", value="he_profit"),
]

@bot.tree.command(name="leaderboard", description="Top 10 users by category.")
@app_commands.describe(category="What to rank by")
@app_commands.choices(category=lb_choices)
async def leaderboard(interaction: discord.Interaction, category: Optional[app_commands.Choice[str]] = None):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)

    metric = category.value if category else "wallet"
    metric_label = {
        "wallet": "Wealth",
        "profit": "Roulette Profit",
        "wins": "Roulette Wins",
        "bj_profit": "Blackjack Profit",
        "slots_profit": "Slots Profit",
        "he_profit": "Hold'em Profit",
    }[metric]

    with db_connect() as conn:
        rows = top_users(conn, interaction.guild.id, metric=metric, limit=10)
        my_rank = user_rank(conn, interaction.guild.id, interaction.user.id, metric=metric)

    if not rows:
        return await interaction.response.send_message(embed=discord.Embed(title="Leaderboard", description="No data yet. Claim /daily to start!"))

    lines = []
    for i, r in enumerate(rows, start=1):
        uid = int(r["user_id"])
        name = await resolve_display_name(interaction.guild, uid)

        if metric == "wallet":
            value = int(r["wallet"])
            lines.append(f"**#{i}** — {name}: **{value:,}** coins")
        elif metric == "profit":
            value = int(r["profit"])
            lines.append(f"**#{i}** — {name}: **{value:+,}** profit")
        elif metric == "wins":
            value = int(r["wins"])
            lines.append(f"**#{i}** — {name}: **{value:,}** wins")
        elif metric == "bj_profit":
            value = int(r["bj_profit"])
            lines.append(f"**#{i}** — {name}: **{value:+,}** profit")
        elif metric == "slots_profit":
            value = int(r["slots_profit"])
            lines.append(f"**#{i}** — {name}: **{value:+,}** profit")
        else:
            value = int(r["he_profit"])
            lines.append(f"**#{i}** — {name}: **{value:+,}** profit")

    await interaction.response.send_message(embed=discord.Embed(
        title=f"Leaderboard — {metric_label}",
        description="\n".join(lines) + f"\n\nYour rank: **#{my_rank}**",
    ))

# ----------------------------
# ADMIN COMMANDS
# ----------------------------
admin = app_commands.Group(name="admin", description="Admin economy controls (manage server).")
bot.tree.add_command(admin)

@admin.command(name="give", description="Give coins to a user.")
@app_commands.describe(user="User to give coins to", amount="Amount to give")
async def admin_give(interaction: discord.Interaction, user: discord.Member, amount: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    if not is_admin_member(interaction.user):
        return await interaction.response.send_message("You need **Manage Server** or **Administrator**.", ephemeral=True)
    if amount <= 0:
        return await interaction.response.send_message("Amount must be positive.", ephemeral=True)

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        new_wallet = update_wallet(conn, interaction.guild.id, user.id, amount)
        conn.commit()

    await interaction.response.send_message(embed=discord.Embed(
        title="Admin Give",
        description=f"Gave **{amount:,}** coins to {user.mention}.\nNew balance: **{new_wallet:,}** coins."
    ))

@admin.command(name="take", description="Take coins from a user (floors at 0).")
@app_commands.describe(user="User to take coins from", amount="Amount to take")
async def admin_take(interaction: discord.Interaction, user: discord.Member, amount: int):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    if not is_admin_member(interaction.user):
        return await interaction.response.send_message("You need **Manage Server** or **Administrator**.", ephemeral=True)
    if amount <= 0:
        return await interaction.response.send_message("Amount must be positive.", ephemeral=True)

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = get_user(conn, interaction.guild.id, user.id)
        wallet = int(row["wallet"])
        take = min(amount, wallet)
        new_wallet = update_wallet(conn, interaction.guild.id, user.id, -take)
        conn.commit()

    await interaction.response.send_message(embed=discord.Embed(
        title="Admin Take",
        description=f"Took **{take:,}** coins from {user.mention}.\nNew balance: **{new_wallet:,}** coins."
    ))

# ----------------------------
# SETTINGS
# ----------------------------
settings = app_commands.Group(name="settings", description="Server settings (admin).")
bot.tree.add_command(settings)

@settings.command(name="achievements", description="Enable/disable achievements for this server.")
@app_commands.describe(enabled="true to enable, false to disable")
async def settings_achievements(interaction: discord.Interaction, enabled: bool):
    guild_err = require_guild(interaction)
    if guild_err:
        return await interaction.response.send_message(embed=guild_err, ephemeral=True)
    if not is_admin_member(interaction.user):
        return await interaction.response.send_message("You need **Manage Server** or **Administrator**.", ephemeral=True)

    with db_connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        set_achievements_enabled(conn, interaction.guild.id, enabled)
        conn.commit()

    await interaction.response.send_message(embed=discord.Embed(
        title="Settings updated",
        description=f"Achievements are now **{'enabled' if enabled else 'disabled'}** for this server."
    ))

# ----------------------------
# ENTRYPOINT
# ----------------------------
if __name__ == "__main__":
    db_init()
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("Set DISCORD_TOKEN environment variable.")
    bot.run(token)
