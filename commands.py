from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from riskbot import RiskBot  # for type hints only


CommandFunc = Callable[["RiskBot", dict, str], Awaitable[None]]


@dataclass(frozen=True)
class CommandSpec:
    name: str
    help: str
    handler: CommandFunc


def register_commands(bot: "RiskBot") -> Tuple[Dict[str, CommandFunc], Dict[str, str]]:
    """
    Returns:
      (commands, help_text)
    """
    commands: Dict[str, CommandFunc] = {}
    help_text: Dict[str, str] = {}

    def add(name: str, help_msg: str, fn: CommandFunc):
        commands[name] = fn
        help_text[name] = help_msg

    # --- r.help ---
    async def cmd_help(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return
        lines = ["**Commands:**"]
        for name in sorted(commands.keys()):
            lines.append(f"- `{bot.config.prefix}{name}` — {help_text.get(name, '')}")
        await bot.reply(channel_id, "\n".join(lines))

    # --- r.start ---
    async def cmd_start(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id")
        if not channel_id or not author_id or not msg_id:
            return

        if bot.store.game_get(channel_id):
            await bot.reply(channel_id, "A game is already active in this channel. OP must use `r.reset` first.")
            return

        att_id, fn = bot.pick_image_attachment(event)
        if not att_id or not fn:
            await bot.reply(channel_id, "You must attach a map image to start a game.")
            return

        bot.store.game_create(channel_id, author_id, msg_id, att_id, fn)
        map_url = bot.attachment_url(att_id, fn)

        await bot.reply(
            channel_id,
            "✅ Game created.\n"
            f"OP: <@{author_id}>\n"
            "Turn: **0**\n"
            "Players can join with `r.join <name>`.\n"
            "Roll with `r.roll <order>`.\n"
            "View with `r.game`.\n"
            f"Map: {map_url}",
        )

    # --- r.reset / r.end ---
    async def cmd_reset(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.is_op(game, author_id):
            await bot.reply(channel_id, "Only the OP can end/reset the game.")
            return

        bot.store.game_delete(channel_id)
        await bot.reply(channel_id, "🛑 Game ended. This channel is now clear.")

    # --- r.join <name> ---
    async def cmd_join(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game to join.")
            return

        name = (args or "").strip() or "Player"
        is_new, count = bot.store.player_add_or_update(channel_id, author_id, name)
        if is_new:
            await bot.reply(channel_id, f"✅ <@{author_id}> joined the game as **{name}**. Players: **{count}**")
        else:
            await bot.reply(channel_id, f"✏️ <@{author_id}> updated their name to **{name}**. Players: **{count}**")

    # --- r.quit ---
    async def cmd_quit(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game.")
            return

        removed, count = bot.store.player_remove(channel_id, author_id)
        if removed:
            await bot.reply(channel_id, f"👋 <@{author_id}> has left the game. Players: **{count}**")
        else:
            await bot.reply(channel_id, "You are not part of this game.")

    # --- r.roll [order...] ---
    async def cmd_roll(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game.")
            return
        if not bot.store.player_in_game(channel_id, author_id):
            await bot.reply(channel_id, "You must `r.join` before rolling.")
            return

        roll_value = bot.gen_11_digit()
        order_text = (args or "").strip()
        turn = int(game["turn_number"])
        count = bot.store.roll_add(channel_id, turn, author_id, roll_value, order_text, msg_id)

        name_map = bot.store.player_name_map(channel_id)
        pname = name_map.get(author_id, "Player")

        if order_text:
            await bot.reply(
                channel_id,
                f"🎲 Turn **{turn}** — **{pname}** (<@{author_id}>) rolled **{roll_value}** (roll #{count} this turn).\n"
                f"Order: {order_text}",
            )
        else:
            await bot.reply(
                channel_id,
                f"🎲 Turn **{turn}** — **{pname}** (<@{author_id}>) rolled **{roll_value}** (roll #{count} this turn).",
            )

    # --- r.game ---
    async def cmd_game(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return

        turn = int(game["turn_number"])
        op_id = game["op_user_id"]

        players = bot.store.player_list(channel_id)
        name_map = bot.store.player_name_map(channel_id)
        rolls = bot.store.rolls_for_turn(channel_id, turn)

        map_url = None
        if game["map_attachment_id"] and game["map_filename"]:
            map_url = bot.attachment_url(game["map_attachment_id"], game["map_filename"])

        player_lines: List[str] = []
        for p in players:
            uid = p["user_id"]
            pname = ((p["player_name"] or "Player").strip() or "Player")
            player_lines.append(f"- **{pname}** (<@{uid}>)")
        player_list_text = "\n".join(player_lines) if player_lines else "_(none)_"

        header = [
            "🗺️ **Risk Game**",
            f"OP: <@{op_id}>",
            f"Turn: **{turn}**",
            f"Players: **{len(players)}**",
            "",
            "**Player list:**",
            player_list_text,
            "",
            f"**Map:** {map_url if map_url else '_(no map stored)_'}",
            "",
            f"**Rolls (Turn {turn}) — all rolls, in order:**",
        ]

        body: List[str] = []
        if not rolls:
            body.append("_(no rolls yet)_")
        else:
            for r in rolls:
                uid = r["user_id"]
                pname = name_map.get(uid, "Player")
                val = r["roll_value"]
                order = (r["order_text"] or "").strip()
                body.append(f"- **{pname}** (<@{uid}>): **{val}**" + (f" — {order}" if order else ""))

        # chunk for message length limits
        chunks: List[str] = []
        cur = ""
        for line in (header + body):
            if len(cur) + len(line) + 1 > 1800:
                chunks.append(cur)
                cur = line
            else:
                cur = (cur + "\n" + line) if cur else line
        if cur:
            chunks.append(cur)

        for ch in chunks:
            await bot.reply(channel_id, ch)

    # --- r.mup [note...] (map optional) ---
    async def cmd_mup(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id")
        if not channel_id or not author_id or not msg_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game to advance.")
            return
        if not bot.is_op(game, author_id):
            await bot.reply(channel_id, "Only the OP can advance the turn.")
            return

        # Update map if image attached
        att_id, fn = bot.pick_image_attachment(event)
        if att_id and fn:
            bot.store.game_set_map(channel_id, msg_id, att_id, fn)

        new_turn = bot.store.game_inc_turn(channel_id)
        player_ids = bot.store.player_ids(channel_id)
        note = (args or "").strip()

        updated = bot.store.game_get(channel_id)
        map_url = None
        if updated and updated["map_attachment_id"] and updated["map_filename"]:
            map_url = bot.attachment_url(updated["map_attachment_id"], updated["map_filename"])

        msg = f"🔔 **Turn {new_turn}** is live. Please roll with `r.roll <order>`."
        if note:
            msg += f"\nNote: {note}"
        if map_url:
            msg += f"\nMap: {map_url}"

        await bot.reply(channel_id, msg, mentions=player_ids if player_ids else None)

    # Register all
    add("help", "Show this help message.", cmd_help)
    add("start", "Start a game (attach a map image).", cmd_start)
    add("reset", "End/reset the current game (OP only).", cmd_reset)
    add("end", "Alias for r.reset (OP only).", cmd_reset)
    add("join", "Join the current game. Usage: r.join <name>", cmd_join)
    add("quit", "Leave the current game.", cmd_quit)
    add("roll", "Generate an 11-digit roll and store it. Usage: r.roll [order...]", cmd_roll)
    add("game", "Show game state, map, and all rolls for the current turn.", cmd_game)
    add("mup", "Advance to next turn and ping players (OP only). Attach image to update map.", cmd_mup)

    return commands, help_text
