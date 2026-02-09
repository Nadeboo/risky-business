import asyncio
import json
import random
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import aiohttp
import websockets


@dataclass
class BotConfig:
    bot_token: str
    prefix: str = "r."
    api_base: str = "https://api.revolt.chat"
    ws_url: str = "wss://ws.revolt.chat"


CommandFunc = Callable[["StoatBot", dict, List[str]], Awaitable[None]]


class StoatAPI:
    """Small REST helper with basic rate-limit handling."""

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
        assert self.session is not None, "API session not initialized"
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

    async def send_message(self, channel_id: str, content: str):
        status, _data, text = await self.post_json(
            f"/channels/{channel_id}/messages",
            {"content": content},
        )
        if status >= 400:
            print(f"[REST] Failed to send message ({status}): {text}")


class StoatBot:
    def __init__(self, config: BotConfig):
        self.config = config
        self.api = StoatAPI(config.api_base, config.bot_token)

        self.user_id: Optional[str] = None  # set after Ready

        # ✅ initialize help BEFORE registering commands
        self.command_help: Dict[str, str] = {}
        self.commands: Dict[str, CommandFunc] = {}

        # register built-in commands
        self.register_command("ping", self.cmd_ping, help_text="Reply with pong.")
        self.register_command("say", self.cmd_say, help_text="Echo text. Usage: r.say <text>")
        self.register_command("help", self.cmd_help, help_text="List commands.")

    def register_command(self, name: str, func: CommandFunc, help_text: str = ""):
        self.commands[name] = func
        if help_text:
            self.command_help[name] = help_text

    async def cmd_ping(self, event: dict, args: List[str]):
        channel_id = event.get("channel")
        if channel_id:
            await self.api.send_message(channel_id, "pong")

    async def cmd_say(self, event: dict, args: List[str]):
        channel_id = event.get("channel")
        if not channel_id:
            return
        if not args:
            await self.api.send_message(channel_id, f"Usage: {self.config.prefix}say <text>")
            return
        await self.api.send_message(channel_id, " ".join(args))

    async def cmd_help(self, event: dict, args: List[str]):
        channel_id = event.get("channel")
        if not channel_id:
            return

        if args:
            name = args[0].lower()
            if name in self.commands:
                desc = self.command_help.get(name, "(no description)")
                await self.api.send_message(channel_id, f"**{self.config.prefix}{name}** — {desc}")
            else:
                await self.api.send_message(channel_id, f"Unknown command: `{name}`")
            return

        lines = []
        for name in sorted(self.commands.keys()):
            desc = self.command_help.get(name, "")
            lines.append(f"- `{self.config.prefix}{name}` {('— ' + desc) if desc else ''}")
        await self.api.send_message(channel_id, "Commands:\n" + "\n".join(lines))

    async def ws_ping_loop(self, ws):
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send(json.dumps({"type": "Ping", "data": 0}))
            except Exception:
                return

    def parse_command(self, content: str) -> Optional[Tuple[str, List[str]]]:
        prefix = self.config.prefix
        if not content.lower().startswith(prefix.lower()):
            return None

        tail = content[len(prefix):].strip()
        if not tail:
            return None

        parts = tail.split()
        cmd = parts[0].lower()
        args = parts[1:]
        return cmd, args

    async def handle_event(self, event: dict):
        etype = event.get("type")

        if etype == "Bulk":
            for inner in event.get("v", []):
                await self.handle_event(inner)
            return

        if etype == "Ready":
            # best effort: try common fields for "my user id"
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

        content = (event.get("content") or "").strip()
        if not content:
            return

        # ignore ourselves
        if self.user_id and event.get("author") == self.user_id:
            return

        parsed = self.parse_command(content)
        if not parsed:
            return

        cmd, args = parsed
        func = self.commands.get(cmd)
        if not func:
            return

        try:
            await func(event, args)
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


def load_config(path: str = "config.json") -> BotConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return BotConfig(
        bot_token=data["bot_token"],
        prefix=data.get("prefix", "r."),
        api_base=data.get("api_base", "https://api.revolt.chat"),
        ws_url=data.get("ws_url", "wss://ws.revolt.chat"),
    )


if __name__ == "__main__":
    cfg = load_config("config.json")
    bot = StoatBot(cfg)
    asyncio.run(bot.run_forever())
