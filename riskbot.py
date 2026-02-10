import asyncio
import json
import random
import re
import secrets
import sqlite3
import traceback
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import aiohttp
import websockets

from commands import register_commands

PRIMARY_SERVER_ID = "01KH1MGTCYQ08XE7Z6XYN3437J"


# -----------------------------
# Config
# -----------------------------
@dataclass
class BotConfig:
    bot_token: str
    prefix: str = "r."
    api_base: str = "https://api.revolt.chat"
    ws_url: str = "wss://ws.revolt.chat"
    autumn_url: Optional[str] = None  # optional override


def load_config(path: str = "config.json") -> BotConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
        if not raw:
            raise RuntimeError("config.json is empty.")
        data = json.loads(raw)
    return BotConfig(
        bot_token=data["bot_token"],
        prefix=data.get("prefix", "r."),
        api_base=data.get("api_base", "https://api.revolt.chat"),
        ws_url=data.get("ws_url", "wss://ws.revolt.chat"),
        autumn_url=data.get("autumn_url"),
    )


async def discover_autumn_url(api_base: str) -> Optional[str]:
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(api_base.rstrip("/") + "/") as r:
                if r.status >= 400:
                    return None
                data = await r.json(content_type=None)

        if not isinstance(data, dict):
            return None
        features = data.get("features", {})
        if not isinstance(features, dict):
            return None
        autumn = features.get("autumn", {})
        if not isinstance(autumn, dict):
            return None
        url = autumn.get("url")
        if isinstance(url, str) and url.startswith("http"):
            return url.rstrip("/")
    except Exception:
        return None
    return None


# -----------------------------
# SQLite Store
# -----------------------------
class Store:
    def __init__(self, path: str = "riskbot.sqlite3"):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self._init()

    def _init(self):
        cur = self.db.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            channel_id TEXT PRIMARY KEY,
            op_user_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            map_message_id TEXT NOT NULL,
            map_attachment_id TEXT,
            map_filename TEXT,
            created_at INTEGER NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            channel_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            joined_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, user_id)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS rolls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            roll_value TEXT NOT NULL,
            order_text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            message_id TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            user_id TEXT,
            event_type TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            message_id TEXT NOT NULL
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS alliances (
            channel_id TEXT NOT NULL,
            user_a TEXT NOT NULL,
            user_b TEXT NOT NULL,
            status TEXT NOT NULL,
            proposed_by TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, user_a, user_b)
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS naps (
            channel_id TEXT NOT NULL,
            user_a TEXT NOT NULL,
            user_b TEXT NOT NULL,
            status TEXT NOT NULL,
            proposed_by TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, user_a, user_b)
        )
        """)

        # NEW: map snapshots per turn (MUP history)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS turnmaps (
            channel_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            map_message_id TEXT NOT NULL,
            map_attachment_id TEXT,
            map_filename TEXT,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, turn_number)
        )
        """)

        # NEW: player roster snapshots per turn
        cur.execute("""
        CREATE TABLE IF NOT EXISTS turnplayers (
            channel_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            user_id TEXT NOT NULL,
            player_name TEXT NOT NULL,
            joined_at INTEGER NOT NULL,
            PRIMARY KEY (channel_id, turn_number, user_id)
        )
        """)

        # NEW: navigation messages for reaction browsing
        cur.execute("""
        CREATE TABLE IF NOT EXISTS nav_messages (
            message_id TEXT PRIMARY KEY,
            channel_id TEXT NOT NULL,
            displayed_turn INTEGER NOT NULL,
            displayed_page INTEGER NOT NULL DEFAULT 0,
            view_type TEXT NOT NULL DEFAULT 'game',
            created_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL
        )
        """)

        # migrations (safe adds if older DB exists)
        cur.execute("PRAGMA table_info(games)")
        gcols = {row["name"] for row in cur.fetchall()}
        if "map_attachment_id" not in gcols:
            cur.execute("ALTER TABLE games ADD COLUMN map_attachment_id TEXT")
        if "map_filename" not in gcols:
            cur.execute("ALTER TABLE games ADD COLUMN map_filename TEXT")

        cur.execute("PRAGMA table_info(players)")
        pcols = {row["name"] for row in cur.fetchall()}
        if "player_name" not in pcols:
            cur.execute("ALTER TABLE players ADD COLUMN player_name TEXT")
            cur.execute("UPDATE players SET player_name='Player' WHERE player_name IS NULL OR player_name=''")

        cur.execute("PRAGMA table_info(nav_messages)")
        ncols = {row["name"] for row in cur.fetchall()}
        if "view_type" not in ncols:
            cur.execute("ALTER TABLE nav_messages ADD COLUMN view_type TEXT")
            cur.execute("UPDATE nav_messages SET view_type='game' WHERE view_type IS NULL OR view_type=''")
        if "displayed_page" not in ncols:
            cur.execute("ALTER TABLE nav_messages ADD COLUMN displayed_page INTEGER")
            cur.execute("UPDATE nav_messages SET displayed_page=0 WHERE displayed_page IS NULL")
        if "created_at" not in ncols:
            cur.execute("ALTER TABLE nav_messages ADD COLUMN created_at INTEGER")
            cur.execute(
                "UPDATE nav_messages SET created_at=updated_at WHERE created_at IS NULL OR created_at=0"
            )

        self.db.commit()

    # ---- game ----
    def game_get(self, channel_id: str) -> Optional[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM games WHERE channel_id = ?", (channel_id,))
        return cur.fetchone()

    def game_create(
        self,
        channel_id: str,
        op_user_id: str,
        map_message_id: str,
        map_attachment_id: Optional[str],
        map_filename: Optional[str],
    ):
        cur = self.db.cursor()
        cur.execute(
            "INSERT INTO games(channel_id, op_user_id, turn_number, map_message_id, map_attachment_id, map_filename, created_at) "
            "VALUES(?,?,?,?,?,?,?)",
            (channel_id, op_user_id, 0, map_message_id, map_attachment_id, map_filename, int(time.time())),
        )
        self.db.commit()

    def game_delete(self, channel_id: str):
        cur = self.db.cursor()
        cur.execute("DELETE FROM games WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM players WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM rolls WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM events WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM alliances WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM naps WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM turnmaps WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM turnplayers WHERE channel_id = ?", (channel_id,))
        cur.execute("DELETE FROM nav_messages WHERE channel_id = ?", (channel_id,))
        self.db.commit()

    def game_set_map(self, channel_id: str, map_message_id: str, map_attachment_id: Optional[str], map_filename: Optional[str]):
        cur = self.db.cursor()
        cur.execute(
            "UPDATE games SET map_message_id=?, map_attachment_id=?, map_filename=? WHERE channel_id=?",
            (map_message_id, map_attachment_id, map_filename, channel_id),
        )
        self.db.commit()

    def game_inc_turn(self, channel_id: str) -> int:
        cur = self.db.cursor()
        cur.execute("UPDATE games SET turn_number = turn_number + 1 WHERE channel_id = ?", (channel_id,))
        self.db.commit()
        cur.execute("SELECT turn_number FROM games WHERE channel_id = ?", (channel_id,))
        return int(cur.fetchone()["turn_number"])

    # ---- players ----
    def player_add_or_update(self, channel_id: str, user_id: str, player_name: str) -> Tuple[bool, int]:
        player_name = (player_name or "").strip() or "Player"
        cur = self.db.cursor()
        cur.execute("SELECT 1 FROM players WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        exists = cur.fetchone() is not None

        if exists:
            cur.execute("UPDATE players SET player_name=? WHERE channel_id=? AND user_id=?", (player_name, channel_id, user_id))
            self.db.commit()
            cur.execute("SELECT COUNT(*) AS c FROM players WHERE channel_id=?", (channel_id,))
            return False, int(cur.fetchone()["c"])

        cur.execute(
            "INSERT INTO players(channel_id, user_id, player_name, joined_at) VALUES(?,?,?,?)",
            (channel_id, user_id, player_name, int(time.time())),
        )
        self.db.commit()
        cur.execute("SELECT COUNT(*) AS c FROM players WHERE channel_id=?", (channel_id,))
        return True, int(cur.fetchone()["c"])

    def player_remove(self, channel_id: str, user_id: str) -> Tuple[bool, int]:
        cur = self.db.cursor()
        cur.execute("DELETE FROM players WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        changed = cur.rowcount > 0
        self.db.commit()
        cur.execute("SELECT COUNT(*) AS c FROM players WHERE channel_id=?", (channel_id,))
        return changed, int(cur.fetchone()["c"])

    def player_list(self, channel_id: str) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute("SELECT user_id, player_name FROM players WHERE channel_id=? ORDER BY joined_at ASC", (channel_id,))
        return cur.fetchall()

    def player_ids(self, channel_id: str) -> List[str]:
        cur = self.db.cursor()
        cur.execute("SELECT user_id FROM players WHERE channel_id=? ORDER BY joined_at ASC", (channel_id,))
        return [r["user_id"] for r in cur.fetchall()]

    def player_in_game(self, channel_id: str, user_id: str) -> bool:
        cur = self.db.cursor()
        cur.execute("SELECT 1 FROM players WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        return cur.fetchone() is not None

    def player_name_map(self, channel_id: str) -> Dict[str, str]:
        cur = self.db.cursor()
        cur.execute("SELECT user_id, player_name FROM players WHERE channel_id=?", (channel_id,))
        return {r["user_id"]: ((r["player_name"] or "Player").strip() or "Player") for r in cur.fetchall()}

    # ---- rolls ----
    def roll_add(self, channel_id: str, turn_number: int, user_id: str, roll_value: str, order_text: str, message_id: str) -> int:
        cur = self.db.cursor()
        cur.execute(
            """
            INSERT INTO rolls(channel_id, turn_number, user_id, roll_value, order_text, created_at, message_id)
            VALUES(?,?,?,?,?,?,?)
            """,
            (channel_id, turn_number, user_id, roll_value, order_text, int(time.time()), message_id),
        )
        self.db.commit()
        cur.execute(
            "SELECT COUNT(*) AS c FROM rolls WHERE channel_id=? AND turn_number=? AND user_id=?",
            (channel_id, turn_number, user_id),
        )
        return int(cur.fetchone()["c"])

    def rolls_for_turn(self, channel_id: str, turn_number: int) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT * FROM rolls
            WHERE channel_id=? AND turn_number=?
            ORDER BY created_at ASC, id ASC
            """,
            (channel_id, turn_number),
        )
        return cur.fetchall()

    # ---- events ----
    def event_add(
        self,
        channel_id: str,
        turn_number: int,
        user_id: Optional[str],
        event_type: str,
        summary_text: str,
        message_id: str,
    ):
        cur = self.db.cursor()
        cur.execute(
            """
            INSERT INTO events(channel_id, turn_number, user_id, event_type, summary_text, created_at, message_id)
            VALUES(?,?,?,?,?,?,?)
            """,
            (channel_id, turn_number, user_id, event_type, summary_text, int(time.time()), message_id),
        )
        self.db.commit()

    def events_for_turn(self, channel_id: str, turn_number: int) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT * FROM events
            WHERE channel_id=? AND turn_number=?
            ORDER BY created_at ASC, id ASC
            """,
            (channel_id, turn_number),
        )
        return cur.fetchall()

    # ---- alliances ----
    @staticmethod
    def _canon_pair(user_1: str, user_2: str) -> Tuple[str, str]:
        return (user_1, user_2) if user_1 <= user_2 else (user_2, user_1)

    def alliance_get(self, channel_id: str, user_1: str, user_2: str) -> Optional[sqlite3.Row]:
        ua, ub = self._canon_pair(user_1, user_2)
        cur = self.db.cursor()
        cur.execute(
            "SELECT * FROM alliances WHERE channel_id=? AND user_a=? AND user_b=?",
            (channel_id, ua, ub),
        )
        return cur.fetchone()

    def alliance_propose_or_accept(self, channel_id: str, proposer_id: str, target_id: str) -> str:
        ua, ub = self._canon_pair(proposer_id, target_id)
        existing = self.alliance_get(channel_id, proposer_id, target_id)
        now = int(time.time())
        cur = self.db.cursor()

        if not existing:
            cur.execute(
                """
                INSERT INTO alliances(channel_id, user_a, user_b, status, proposed_by, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (channel_id, ua, ub, "pending", proposer_id, now, now),
            )
            self.db.commit()
            return "proposed"

        status = (existing["status"] or "").strip().lower()
        proposed_by = existing["proposed_by"]
        if status == "accepted":
            return "already_accepted"
        if status != "pending":
            return "invalid_state"
        if proposed_by == proposer_id:
            return "already_pending"

        cur.execute(
            """
            UPDATE alliances
            SET status='accepted', updated_at=?
            WHERE channel_id=? AND user_a=? AND user_b=?
            """,
            (now, channel_id, ua, ub),
        )
        self.db.commit()
        return "accepted"

    def alliances_for_channel(self, channel_id: str) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT * FROM alliances
            WHERE channel_id=?
            ORDER BY
                CASE status WHEN 'pending' THEN 0 WHEN 'accepted' THEN 1 ELSE 2 END,
                updated_at DESC, created_at DESC, user_a ASC, user_b ASC
            """,
            (channel_id,),
        )
        return cur.fetchall()

    def alliance_break(self, channel_id: str, user_1: str, user_2: str) -> str:
        existing = self.alliance_get(channel_id, user_1, user_2)
        if not existing:
            return "missing"

        status = (existing["status"] or "").strip().lower()
        ua, ub = self._canon_pair(user_1, user_2)
        cur = self.db.cursor()
        cur.execute(
            "DELETE FROM alliances WHERE channel_id=? AND user_a=? AND user_b=?",
            (channel_id, ua, ub),
        )
        self.db.commit()
        if status == "accepted":
            return "broken"
        if status == "pending":
            return "cancelled_pending"
        return "removed"

    def nap_get(self, channel_id: str, user_1: str, user_2: str) -> Optional[sqlite3.Row]:
        ua, ub = self._canon_pair(user_1, user_2)
        cur = self.db.cursor()
        cur.execute(
            "SELECT * FROM naps WHERE channel_id=? AND user_a=? AND user_b=?",
            (channel_id, ua, ub),
        )
        return cur.fetchone()

    def nap_propose_or_accept(self, channel_id: str, proposer_id: str, target_id: str) -> str:
        ua, ub = self._canon_pair(proposer_id, target_id)
        existing = self.nap_get(channel_id, proposer_id, target_id)
        now = int(time.time())
        cur = self.db.cursor()

        if not existing:
            cur.execute(
                """
                INSERT INTO naps(channel_id, user_a, user_b, status, proposed_by, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?)
                """,
                (channel_id, ua, ub, "pending", proposer_id, now, now),
            )
            self.db.commit()
            return "proposed"

        status = (existing["status"] or "").strip().lower()
        proposed_by = existing["proposed_by"]
        if status == "accepted":
            return "already_accepted"
        if status != "pending":
            return "invalid_state"
        if proposed_by == proposer_id:
            return "already_pending"

        cur.execute(
            """
            UPDATE naps
            SET status='accepted', updated_at=?
            WHERE channel_id=? AND user_a=? AND user_b=?
            """,
            (now, channel_id, ua, ub),
        )
        self.db.commit()
        return "accepted"

    def naps_for_channel(self, channel_id: str) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT * FROM naps
            WHERE channel_id=?
            ORDER BY
                CASE status WHEN 'pending' THEN 0 WHEN 'accepted' THEN 1 ELSE 2 END,
                updated_at DESC, created_at DESC, user_a ASC, user_b ASC
            """,
            (channel_id,),
        )
        return cur.fetchall()

    def nap_break(self, channel_id: str, user_1: str, user_2: str) -> str:
        existing = self.nap_get(channel_id, user_1, user_2)
        if not existing:
            return "missing"

        status = (existing["status"] or "").strip().lower()
        ua, ub = self._canon_pair(user_1, user_2)
        cur = self.db.cursor()
        cur.execute(
            "DELETE FROM naps WHERE channel_id=? AND user_a=? AND user_b=?",
            (channel_id, ua, ub),
        )
        self.db.commit()
        if status == "accepted":
            return "broken"
        if status == "pending":
            return "cancelled_pending"
        return "removed"

    # ---- turnmaps (MUP history) ----
    def turnmap_upsert(
        self,
        channel_id: str,
        turn_number: int,
        map_message_id: str,
        map_attachment_id: Optional[str],
        map_filename: Optional[str],
    ):
        cur = self.db.cursor()
        cur.execute(
            """
            INSERT INTO turnmaps(channel_id, turn_number, map_message_id, map_attachment_id, map_filename, created_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(channel_id, turn_number) DO UPDATE SET
                map_message_id=excluded.map_message_id,
                map_attachment_id=excluded.map_attachment_id,
                map_filename=excluded.map_filename,
                created_at=excluded.created_at
            """,
            (channel_id, turn_number, map_message_id, map_attachment_id, map_filename, int(time.time())),
        )
        self.db.commit()

    def turnmap_get(self, channel_id: str, turn_number: int) -> Optional[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM turnmaps WHERE channel_id=? AND turn_number=?", (channel_id, turn_number))
        return cur.fetchone()

    def turnmap_latest(self, channel_id: str) -> Optional[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM turnmaps WHERE channel_id=? ORDER BY turn_number DESC LIMIT 1", (channel_id,))
        return cur.fetchone()

    def turnmap_latest_at_or_before(self, channel_id: str, turn_number: int) -> Optional[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT * FROM turnmaps
            WHERE channel_id=? AND turn_number<=?
            ORDER BY turn_number DESC
            LIMIT 1
            """,
            (channel_id, turn_number),
        )
        return cur.fetchone()

    # ---- turnplayers (player snapshots per turn) ----
    def turnplayers_replace_from_current(self, channel_id: str, turn_number: int):
        cur = self.db.cursor()
        cur.execute("DELETE FROM turnplayers WHERE channel_id=? AND turn_number=?", (channel_id, turn_number))
        cur.execute(
            """
            INSERT INTO turnplayers(channel_id, turn_number, user_id, player_name, joined_at)
            SELECT channel_id, ?, user_id, player_name, joined_at
            FROM players
            WHERE channel_id=?
            ORDER BY joined_at ASC
            """,
            (turn_number, channel_id),
        )
        self.db.commit()

    def turnplayers_for_turn(self, channel_id: str, turn_number: int) -> List[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute(
            """
            SELECT user_id, player_name, joined_at
            FROM turnplayers
            WHERE channel_id=? AND turn_number=?
            ORDER BY joined_at ASC
            """,
            (channel_id, turn_number),
        )
        return cur.fetchall()

    # ---- nav messages ----
    def nav_set(self, message_id: str, channel_id: str, displayed_turn: int, view_type: str = "game", displayed_page: int = 0):
        cur = self.db.cursor()
        now = int(time.time())
        cur.execute(
            """
            INSERT INTO nav_messages(message_id, channel_id, displayed_turn, displayed_page, view_type, created_at, updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(message_id) DO UPDATE SET
                displayed_turn=excluded.displayed_turn,
                displayed_page=excluded.displayed_page,
                view_type=excluded.view_type,
                updated_at=excluded.updated_at
            """,
            (message_id, channel_id, displayed_turn, displayed_page, view_type, now, now),
        )
        self.db.commit()

    def nav_get(self, message_id: str) -> Optional[sqlite3.Row]:
        cur = self.db.cursor()
        cur.execute("SELECT * FROM nav_messages WHERE message_id=?", (message_id,))
        return cur.fetchone()

    def nav_delete(self, message_id: str):
        cur = self.db.cursor()
        cur.execute("DELETE FROM nav_messages WHERE message_id=?", (message_id,))
        self.db.commit()

    def nav_update_state(self, message_id: str, displayed_turn: int, displayed_page: int):
        cur = self.db.cursor()
        cur.execute(
            "UPDATE nav_messages SET displayed_turn=?, displayed_page=?, updated_at=? WHERE message_id=?",
            (displayed_turn, displayed_page, int(time.time()), message_id),
        )
        self.db.commit()

    def nav_touch(self, message_id: str):
        cur = self.db.cursor()
        cur.execute("UPDATE nav_messages SET updated_at=? WHERE message_id=?", (int(time.time()), message_id))
        self.db.commit()


# -----------------------------
# REST helper
# -----------------------------
class StoatAPI:
    def __init__(self, base_url: str, bot_token: str):
        self.base_url = base_url.rstrip("/")
        self.bot_token = bot_token
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers={"X-Bot-Token": self.bot_token})
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.session:
            await self.session.close()

    async def request_json(self, method: str, path: str, payload: Optional[dict] = None) -> Tuple[int, dict, str]:
        assert self.session is not None
        url = f"{self.base_url}{path}"

        for attempt in range(5):
            async with self.session.request(method, url, json=payload) as resp:
                text = await resp.text()
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = {}

                if resp.status != 429:
                    return resp.status, data, text

                retry_after_ms = data.get("retry_after")
                if isinstance(retry_after_ms, (int, float)) and retry_after_ms > 0:
                    await asyncio.sleep(retry_after_ms / 1000.0)
                else:
                    await asyncio.sleep(1.0 + attempt)

        return 429, {}, "Rate limited too many times"

    async def send_message(self, channel_id: str, content: str, mentions: Optional[List[str]] = None) -> Optional[str]:
        payload: Dict[str, Any] = {"content": content}
        if mentions:
            payload["mentions"] = mentions
        status, data, text = await self.request_json("POST", f"/channels/{channel_id}/messages", payload)
        if status >= 400:
            print(f"[REST] Failed to send message ({status}): {text}")
            return None
        mid = data.get("_id") or data.get("id")
        return mid if isinstance(mid, str) else None

    async def get_channel(self, channel_id: str) -> Optional[dict]:
        status, data, text = await self.request_json("GET", f"/channels/{channel_id}", None)
        if status >= 400:
            print(f"[REST] Failed to get channel ({status}): {text}")
            return None
        return data if isinstance(data, dict) else None

    async def edit_message(self, channel_id: str, message_id: str, content: str) -> bool:
        payload: Dict[str, Any] = {"content": content}
        status, _data, text = await self.request_json(
            "PATCH",
            f"/channels/{channel_id}/messages/{message_id}",
            payload,
        )
        if status >= 400:
            print(f"[REST] Failed to edit message ({status}): {text}")
            return False
        return True

    async def add_reaction(self, channel_id: str, message_id: str, emoji: str):
        # Some unicode variants fail on some instances; try a small fallback set.
        variants = [emoji]
        if emoji in ("👈",):
            variants = ["👈", "⬅️", "⬅", "◀️", "◀"]
        elif emoji in ("👉",):
            variants = ["👉", "➡️", "➡", "▶️", "▶"]
        elif emoji in ("⬅", "⬅️"):
            variants = ["⬅️", "⬅", "◀️", "◀", "👈"]
        elif emoji in ("➡", "➡️"):
            variants = ["➡️", "➡", "▶️", "▶", "👉"]

        last_status = 0
        last_text = ""
        for em in variants:
            e = quote(em, safe="")
            status, _data, text = await self.request_json(
                "PUT",
                f"/channels/{channel_id}/messages/{message_id}/reactions/{e}",
                None,
            )
            if status < 400:
                return
            last_status = status
            last_text = text
            if status != 400:
                break

        print(f"[REST] Failed to add reaction ({last_status}): {last_text}")

    async def clear_reactions(self, channel_id: str, message_id: str) -> bool:
        status, _data, text = await self.request_json(
            "DELETE",
            f"/channels/{channel_id}/messages/{message_id}/reactions",
            None,
        )
        if status >= 400:
            print(f"[REST] Failed to clear reactions ({status}): {text}")
            return False
        return True

# -----------------------------
# Bot
# -----------------------------
class RiskBot:
    EMOJI_BACK = "👈"
    EMOJI_FWD = "👉"
    EMOJI_PAGE = "🤜"

    def __init__(self, config: BotConfig, autumn_url: str):
        self.config = config
        self.api = StoatAPI(config.api_base, config.bot_token)
        self.store = Store()
        self.user_id: Optional[str] = None
        self.autumn_url = autumn_url.rstrip("/")
        self.nav_cleanup_tasks: Dict[str, asyncio.Task] = {}
        self.channel_server_ids: Dict[str, str] = {}

        self.commands, self.help_text = register_commands(self)

    def log(self, scope: str, message: str, **ctx: Any):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        if ctx:
            parts = [f"{k}={ctx[k]!r}" for k in sorted(ctx.keys())]
            print(f"[{ts}] [{scope}] {message} | " + " ".join(parts))
            return
        print(f"[{ts}] [{scope}] {message}")

    def schedule_nav_reaction_expiry(self, channel_id: str, message_id: str, inactivity_seconds: int = 30):
        prev = self.nav_cleanup_tasks.get(message_id)
        if prev and not prev.done():
            prev.cancel()

        async def _worker():
            try:
                while True:
                    nav = self.store.nav_get(message_id)
                    if not nav:
                        return

                    now_ts = int(time.time())
                    last_active = int(nav["updated_at"]) if nav["updated_at"] is not None else now_ts
                    inactive_for = now_ts - last_active
                    remaining = inactivity_seconds - inactive_for
                    if remaining > 0:
                        await asyncio.sleep(remaining + 1)
                        continue

                    nav_channel = nav["channel_id"] if "channel_id" in nav.keys() and nav["channel_id"] else channel_id
                    self.log(
                        "NAV",
                        "Navigation expired; clearing reactions",
                        message_id=message_id,
                        channel_id=nav_channel,
                        inactive_s=inactive_for,
                    )
                    await self.api.clear_reactions(nav_channel, message_id)
                    self.store.nav_delete(message_id)
                    return
            except asyncio.CancelledError:
                return
            except Exception as e:
                self.log("NAV", "Cleanup task failed", message_id=message_id, error=str(e), exc_type=type(e).__name__)
                self.log("NAV", "Cleanup traceback", traceback=traceback.format_exc())
            finally:
                current = self.nav_cleanup_tasks.get(message_id)
                if current and current is asyncio.current_task():
                    self.nav_cleanup_tasks.pop(message_id, None)

        self.nav_cleanup_tasks[message_id] = asyncio.create_task(_worker())

    # Helpers used by commands.py
    async def reply(self, channel_id: str, text: str, mentions: Optional[List[str]] = None, return_message_id: bool = False) -> Optional[str]:
        mid = await self.api.send_message(channel_id, text, mentions=mentions)
        return mid if return_message_id else None

    def is_op(self, game: sqlite3.Row, user_id: str) -> bool:
        return user_id == game["op_user_id"]

    def gen_11_digit(self) -> str:
        lo = 10_000_000_000
        span = 90_000_000_000
        return str(lo + secrets.randbelow(span))

    def attachment_url(self, attachment_id: str, filename: str) -> str:
        return f"{self.autumn_url}/attachments/{attachment_id}/{filename}"

    async def ensure_channel_server_id(self, channel_id: str) -> Optional[str]:
        known = self.channel_server_ids.get(channel_id)
        if known:
            return known
        ch = await self.api.get_channel(channel_id)
        if not ch:
            return None
        server_id = ch.get("server")
        if isinstance(server_id, str) and server_id:
            self.channel_server_ids[channel_id] = server_id
            return server_id
        return None

    def message_url(self, channel_id: str, message_id: str) -> str:
        if PRIMARY_SERVER_ID:
            return f"https://app.revolt.chat/server/{PRIMARY_SERVER_ID}/channel/{channel_id}/{message_id}"
        server_id = self.channel_server_ids.get(channel_id)
        if server_id:
            return f"https://app.revolt.chat/server/{server_id}/channel/{channel_id}/{message_id}"
        return f"https://app.revolt.chat/channel/{channel_id}/{message_id}"

    def render_no_ping(self, text: str) -> str:
        return re.sub(r"<@([A-Za-z0-9]+)>", r"`\1`", text or "")

    def pick_image_attachment(self, event: dict) -> Tuple[Optional[str], Optional[str]]:
        atts = event.get("attachments") or []
        for a in atts:
            meta = a.get("metadata") or {}
            ct = a.get("content_type") or ""
            if meta.get("type") == "Image" or ct.startswith("image/"):
                att_id = a.get("_id")
                fn = a.get("filename")
                if att_id and fn:
                    return att_id, fn
        return None, None

    def parse_command(self, content: str) -> Optional[Tuple[str, str]]:
        pfx = self.config.prefix
        if not content.lower().startswith(pfx.lower()):
            return None
        tail = content[len(pfx):].strip()
        if not tail:
            return None
        parts = tail.split(maxsplit=1)
        return parts[0].lower(), (parts[1] if len(parts) > 1 else "")

    def render_turn_view(self, channel_id: str, turn: int, game: Optional[sqlite3.Row] = None) -> Optional[str]:
        game_row = game or self.store.game_get(channel_id)
        if not game_row:
            return None

        op_id = game_row["op_user_id"]
        current_turn = int(game_row["turn_number"])

        players = self.store.turnplayers_for_turn(channel_id, turn)
        if not players:
            players = self.store.player_list(channel_id)
        name_map = {p["user_id"]: ((p["player_name"] or "Player").strip() or "Player") for p in players}
        rolls = self.store.rolls_for_turn(channel_id, turn)
        first_roll_by_user: Dict[str, sqlite3.Row] = {}
        for r in rolls:
            uid = r["user_id"]
            if uid not in first_roll_by_user:
                first_roll_by_user[uid] = r

        tm = self.store.turnmap_latest_at_or_before(channel_id, turn)
        map_url = None
        if tm and tm["map_attachment_id"] and tm["map_filename"]:
            map_url = self.attachment_url(tm["map_attachment_id"], tm["map_filename"])

        player_lines: List[str] = []
        for p in players:
            uid = p["user_id"]
            pname = name_map.get(uid, "Player")
            player_lines.append(f"- **{pname}** (`{uid}`)")
        player_list_text = "\n".join(player_lines) if player_lines else "_(none)_"

        header = [
            "🗺️ **Risk Game**",
            f"OP: `{op_id}`",
            f"Viewing turn: **{turn}** (current: **{current_turn}**)",
            f"Players: **{len(players)}**",
            "",
            "**Player list:**",
            player_list_text,
            "",
            f"**Map (Turn {turn}):** {map_url if map_url else '_(no map stored)_'}",
            "",
            f"**Rolls (Turn {turn}) — first roll per player:**",
        ]

        body: List[str] = []
        for p in players:
            uid = p["user_id"]
            pname = name_map.get(uid, "Player")
            r = first_roll_by_user.get(uid)
            if not r:
                body.append(f"- **{pname}** (`{uid}`): _(no roll yet)_")
                continue

            val = r["roll_value"]
            order = (r["order_text"] or "").strip()
            roll_msg_id = (r["message_id"] or "").strip()
            roll_link = ""
            if roll_msg_id:
                roll_link = f" ([roll]({self.message_url(channel_id, roll_msg_id)}))"
            body.append(f"- **{pname}** (`{uid}`): **{val}**" + (f" — {order}" if order else "") + roll_link)

        text = "\n".join(header + body)
        if len(text) > 1900:
            text = "\n".join(header + body[:50] + ["", "_(truncated)_"])
        return text

    def _events_page_count(self, channel_id: str, turn: int, page_size: int = 15) -> int:
        if page_size < 1:
            page_size = 15
        total = len(self.store.events_for_turn(channel_id, turn))
        return max(1, (total + page_size - 1) // page_size)

    def render_events_view(
        self,
        channel_id: str,
        turn: int,
        page: int = 0,
        page_size: int = 15,
        game: Optional[sqlite3.Row] = None,
    ) -> Optional[str]:
        game_row = game or self.store.game_get(channel_id)
        if not game_row:
            return None

        current_turn = int(game_row["turn_number"])
        all_events = self.store.events_for_turn(channel_id, turn)
        if page_size < 1:
            page_size = 15
        page_count = max(1, (len(all_events) + page_size - 1) // page_size)
        page = max(0, min(page, page_count - 1))
        start = page * page_size
        end = start + page_size
        events = all_events[start:end]

        lines = [
            "📜 **Turn Events**",
            f"Viewing turn: **{turn}** (current: **{current_turn}**)",
            f"Page: **{page + 1}/{page_count}** (15 events per page, 🤜 to cycle pages)",
            "",
        ]
        if not events:
            lines.append("_(no events recorded for this turn)_")
        else:
            for ev in events:
                lines.append(f"- {self.render_no_ping(ev['summary_text'])}")

        text = "\n".join(lines)
        if len(text) > 1900:
            text = "\n".join(lines[:120] + ["", "_(truncated)_"])
        return text

    # ---- Reaction navigation ----
    def _emoji_is_back(self, emoji_id: str) -> bool:
        return emoji_id.startswith("👈") or emoji_id.startswith("⬅")

    def _emoji_is_fwd(self, emoji_id: str) -> bool:
        return emoji_id.startswith("👉") or emoji_id.startswith("➡")

    def _emoji_is_page(self, emoji_id: str) -> bool:
        return emoji_id.startswith("🤜")

    async def handle_nav_react(self, event: dict):
        """
        Stoat WS event (Server->Client):
          {
            "type": "MessageReact",
            "id": "{message_id}",
            "channel_id": "{channel_id}",
            "user_id": "{user_id}",
            "emoji_id": "{emoji_id}"
          }
        """
        msg_id = event.get("id")
        channel_id = event.get("channel_id")
        user_id = event.get("user_id")
        emoji_id = event.get("emoji_id")

        if not isinstance(msg_id, str) or not isinstance(channel_id, str) or not isinstance(user_id, str) or not isinstance(emoji_id, str):
            return

        # Ignore our own reactions
        if self.user_id and user_id == self.user_id:
            return

        nav = self.store.nav_get(msg_id)
        if not nav:
            return  # not a tracked nav message

        now_ts = int(time.time())
        last_active = int(nav["updated_at"]) if nav["updated_at"] is not None else now_ts
        if now_ts - last_active > 30:
            await self.api.clear_reactions(channel_id, msg_id)
            self.store.nav_delete(msg_id)
            return  # inactive nav window expired

        game = self.store.game_get(channel_id)
        if not game:
            return

        current_turn = int(game["turn_number"])
        shown_turn = int(nav["displayed_turn"])
        shown_page = int(nav["displayed_page"]) if "displayed_page" in nav.keys() and nav["displayed_page"] is not None else 0
        view_type = ((nav["view_type"] or "game").strip().lower() if "view_type" in nav.keys() else "game")
        new_turn = shown_turn
        new_page = shown_page

        if self._emoji_is_fwd(emoji_id):
            new_turn = 0 if shown_turn >= current_turn else shown_turn + 1
            if view_type == "events":
                new_page = 0
        elif self._emoji_is_back(emoji_id):
            new_turn = current_turn if shown_turn <= 0 else shown_turn - 1
            if view_type == "events":
                new_page = 0
        elif view_type == "events" and self._emoji_is_page(emoji_id):
            page_count = self._events_page_count(channel_id, shown_turn, page_size=15)
            new_page = 0 if shown_page + 1 >= page_count else shown_page + 1
        else:
            return

        # A valid nav react extends the inactivity window.
        self.store.nav_touch(msg_id)
        self.schedule_nav_reaction_expiry(channel_id, msg_id, inactivity_seconds=30)

        if new_turn == shown_turn and new_page == shown_page:
            return

        if view_type == "events":
            out = self.render_events_view(channel_id, new_turn, page=new_page, page_size=15, game=game)
        else:
            out = self.render_turn_view(channel_id, new_turn, game=game)
        if not out:
            return

        # Edit the same nav message in place instead of posting a new one.
        edited = await self.api.edit_message(channel_id, msg_id, out)
        if edited:
            self.store.nav_update_state(msg_id, new_turn, new_page)

    # ---------------- WS plumbing ----------------
    async def ws_ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({"type": "Ping", "data": 0}))
            except Exception as e:
                self.log("WS", "Ping loop stopped", error=str(e), exc_type=type(e).__name__)
                self.log("WS", "Ping traceback", traceback=traceback.format_exc())
                return

    async def handle_event(self, event: dict):
        etype = event.get("type")

        if etype == "Bulk":
            for inner in event.get("v", []):
                await self.handle_event(inner)
            return

        if etype == "Ready":
            if isinstance(event.get("user"), str):
                self.user_id = event["user"]
            else:
                users = event.get("users") or []
                if users and isinstance(users[0], dict) and "_id" in users[0]:
                    self.user_id = users[0]["_id"]
            channels = event.get("channels") or []
            if isinstance(channels, list):
                for ch in channels:
                    if not isinstance(ch, dict):
                        continue
                    cid = ch.get("_id")
                    sid = ch.get("server")
                    if isinstance(cid, str) and isinstance(sid, str) and sid:
                        self.channel_server_ids[cid] = sid
            print(f"[WS] Ready. user_id={self.user_id}")
            return

        # NEW: reaction navigation
        if etype == "MessageReact":
            await self.handle_nav_react(event)
            return

        if etype != "Message":
            return

        if self.user_id and event.get("author") == self.user_id:
            return

        content = (event.get("content") or "").strip()
        if not content:
            return

        parsed = self.parse_command(content)
        if not parsed:
            return

        cmd, argstr = parsed
        func = self.commands.get(cmd)
        if not func:
            return

        try:
            await func(self, event, argstr)
        except Exception as e:
            self.log(
                "BOT",
                "Command error",
                command=cmd,
                channel_id=event.get("channel"),
                author_id=event.get("author"),
                error=str(e),
                exc_type=type(e).__name__,
            )
            self.log("BOT", "Command traceback", traceback=traceback.format_exc())

    async def run_forever(self):
        backoff = 1.0
        connect_attempt = 0
        async with self.api:
            while True:
                connect_attempt += 1
                ping_task: Optional[asyncio.Task] = None
                try:
                    self.log(
                        "WS",
                        "Connecting",
                        ws_url=self.config.ws_url,
                        attempt=connect_attempt,
                        backoff_s=round(backoff, 3),
                    )
                    async with websockets.connect(
                        self.config.ws_url,
                        ping_interval=None,
                        close_timeout=10,
                    ) as ws:
                        await ws.send(json.dumps({"type": "Authenticate", "token": self.config.bot_token}))
                        self.log("WS", "Connected and Authenticate sent")
                        ping_task = asyncio.create_task(self.ws_ping_loop(ws))
                        backoff = 1.0

                        while True:
                            try:
                                raw = await ws.recv()
                            except Exception as e:
                                self.log("WS", "Receive failed", error=str(e), exc_type=type(e).__name__)
                                self.log("WS", "Receive traceback", traceback=traceback.format_exc())
                                raise

                            if not isinstance(raw, str):
                                self.log("WS", "Received non-text frame", frame_type=type(raw).__name__)
                                continue

                            try:
                                event = json.loads(raw)
                            except Exception as e:
                                snippet = raw[:300]
                                self.log(
                                    "WS",
                                    "JSON decode failed",
                                    error=str(e),
                                    exc_type=type(e).__name__,
                                    payload_snippet=snippet,
                                )
                                self.log("WS", "JSON decode traceback", traceback=traceback.format_exc())
                                continue

                            try:
                                await self.handle_event(event)
                            except Exception as e:
                                etype = event.get("type") if isinstance(event, dict) else None
                                self.log(
                                    "WS",
                                    "Event handling failed",
                                    event_type=etype,
                                    error=str(e),
                                    exc_type=type(e).__name__,
                                )
                                self.log("WS", "Event traceback", traceback=traceback.format_exc())
                                raise
                except Exception as e:
                    self.log("WS", "Disconnected", error=str(e), exc_type=type(e).__name__)
                    self.log("WS", "Disconnect traceback", traceback=traceback.format_exc())
                finally:
                    if ping_task and not ping_task.done():
                        ping_task.cancel()

                jitter = random.uniform(0, 0.5)
                sleep_for = backoff + jitter
                self.log("WS", "Reconnecting after delay", delay_s=round(sleep_for, 3))
                await asyncio.sleep(sleep_for)
                backoff = min(backoff * 2, 30.0)


async def main():
    cfg = load_config("config.json")
    autumn = cfg.autumn_url or await discover_autumn_url(cfg.api_base) or "https://autumn.revolt.chat"
    print("Using Autumn CDN:", autumn)
    bot = RiskBot(cfg, autumn)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
