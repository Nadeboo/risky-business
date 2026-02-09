import asyncio
import json
import random
import secrets
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import websockets

from commands import register_commands


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
    """Try to discover CDN base from API root. Falls back gracefully."""
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

        # migrations
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
        self.db.commit()

    def game_set_map(
        self,
        channel_id: str,
        map_message_id: str,
        map_attachment_id: Optional[str],
        map_filename: Optional[str],
    ):
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
        """
        Returns (is_new_join, player_count).
        If already joined, updates name.
        """
        player_name = (player_name or "").strip() or "Player"
        cur = self.db.cursor()

        cur.execute("SELECT 1 FROM players WHERE channel_id=? AND user_id=?", (channel_id, user_id))
        exists = cur.fetchone() is not None

        if exists:
            cur.execute(
                "UPDATE players SET player_name=? WHERE channel_id=? AND user_id=?",
                (player_name, channel_id, user_id),
            )
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
        cur.execute(
            "SELECT user_id, player_name FROM players WHERE channel_id=? ORDER BY joined_at ASC",
            (channel_id,),
        )
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
    def roll_add(
        self,
        channel_id: str,
        turn_number: int,
        user_id: str,
        roll_value: str,
        order_text: str,
        message_id: str,
    ) -> int:
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
            """
            SELECT COUNT(*) AS c FROM rolls
            WHERE channel_id=? AND turn_number=? AND user_id=?
            """,
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


# -----------------------------
# REST helper (rb.py style)
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

    async def post_json(self, path: str, payload: dict) -> Tuple[int, dict, str]:
        assert self.session is not None
        url = f"{self.base_url}{path}"

        for attempt in range(5):
            async with self.session.post(url, json=payload) as resp:
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

    async def send_message(self, channel_id: str, content: str, mentions: Optional[List[str]] = None):
        payload: Dict[str, Any] = {"content": content}
        if mentions:
            payload["mentions"] = mentions
        status, _data, text = await self.post_json(f"/channels/{channel_id}/messages", payload)
        if status >= 400:
            print(f"[REST] Failed to send message ({status}): {text}")


# -----------------------------
# Bot
# -----------------------------
class RiskBot:
    def __init__(self, config: BotConfig, autumn_url: str):
        self.config = config
        self.api = StoatAPI(config.api_base, config.bot_token)
        self.store = Store()
        self.user_id: Optional[str] = None
        self.autumn_url = autumn_url.rstrip("/")

        self.commands, self.help_text = register_commands(self)

    # Helpers used by commands.py
    async def reply(self, channel_id: str, text: str, mentions: Optional[List[str]] = None):
        await self.api.send_message(channel_id, text, mentions=mentions)

    def is_op(self, game: sqlite3.Row, user_id: str) -> bool:
        return user_id == game["op_user_id"]

    def gen_11_digit(self) -> str:
        lo = 10_000_000_000
        span = 90_000_000_000
        return str(lo + secrets.randbelow(span))

    def attachment_url(self, attachment_id: str, filename: str) -> str:
        return f"{self.autumn_url}/attachments/{attachment_id}/{filename}"

    def pick_image_attachment(self, event: dict) -> Tuple[Optional[str], Optional[str]]:
        """Return (attachment_id, filename) for the first image attachment, else (None, None)."""
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

    # ---------------- WS plumbing (rb.py-style) ----------------
    async def ws_ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({"type": "Ping", "data": 0}))
            except Exception:
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
            print(f"[WS] Ready. user_id={self.user_id}")
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
            await func(self, event, argstr)  # note: handler signature uses (bot, event, args)
        except Exception as e:
            print(f"[BOT] Command error: {e}")

    async def run_forever(self):
        backoff = 1.0
        async with self.api:
            while True:
                try:
                    async with websockets.connect(self.config.ws_url) as ws:
                        await ws.send(json.dumps({"type": "Authenticate", "token": self.config.bot_token}))
                        print("[WS] Connected + Authenticate sent.")
                        asyncio.create_task(self.ws_ping_loop(ws))
                        backoff = 1.0

                        while True:
                            raw = await ws.recv()
                            event = json.loads(raw)
                            await self.handle_event(event)

                except Exception as e:
                    print(f"[WS] Disconnected: {e}")

                jitter = random.uniform(0, 0.5)
                await asyncio.sleep(backoff + jitter)
                backoff = min(backoff * 2, 30.0)


async def main():
    cfg = load_config("config.json")
    autumn = cfg.autumn_url or await discover_autumn_url(cfg.api_base) or "https://autumn.revolt.chat"
    print("Using Autumn CDN:", autumn)
    bot = RiskBot(cfg, autumn)
    await bot.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
