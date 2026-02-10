from __future__ import annotations

import re
import secrets
from typing import Awaitable, Callable, Dict, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from riskbot import RiskBot


CommandFunc = Callable[["RiskBot", dict, str], Awaitable[None]]

# Command synonyms are defined once here and wired automatically in register_commands.
# Keys are canonical command names; values are accepted alternate names (without prefix).
COMMAND_SYNONYMS: Dict[str, tuple[str, ...]] = {
    "game": ("g",),
}


def register_commands(bot: "RiskBot") -> Tuple[Dict[str, CommandFunc], Dict[str, str]]:
    commands: Dict[str, CommandFunc] = {}
    help_text: Dict[str, str] = {}

    def add(name: str, help_msg: str, fn: CommandFunc):
        commands[name] = fn
        help_text[name] = help_msg

    def add_synonyms(synonyms: Dict[str, tuple[str, ...]]):
        for canonical, aliases in synonyms.items():
            fn = commands.get(canonical)
            if not fn:
                continue
            for alias in aliases:
                alias_key = (alias or "").strip().lower()
                if not alias_key or alias_key in commands:
                    continue
                commands[alias_key] = fn
                help_text[alias_key] = f"Alias for `{bot.config.prefix}{canonical}`."

    async def publish_game_view(channel_id: str, turn: int, game: dict | None = None):
        text = bot.render_turn_view(channel_id, turn, game=game)
        if not text:
            await bot.reply(channel_id, "Could not render game state.")
            return

        sent_id = await bot.reply(channel_id, text, return_message_id=True)
        if sent_id:
            bot.store.nav_set(sent_id, channel_id, displayed_turn=turn, view_type="game", displayed_page=0)
            await bot.api.add_reaction(channel_id, sent_id, bot.EMOJI_BACK)
            await bot.api.add_reaction(channel_id, sent_id, bot.EMOJI_FWD)
            bot.schedule_nav_reaction_expiry(channel_id, sent_id, inactivity_seconds=30)

    async def publish_events_view(channel_id: str, turn: int, game: dict | None = None):
        text = bot.render_events_view(channel_id, turn, page=0, page_size=15, game=game)
        if not text:
            await bot.reply(channel_id, "Could not render turn events.")
            return

        sent_id = await bot.reply(channel_id, text, return_message_id=True)
        if sent_id:
            bot.store.nav_set(sent_id, channel_id, displayed_turn=turn, view_type="events", displayed_page=0)
            await bot.api.add_reaction(channel_id, sent_id, bot.EMOJI_BACK)
            await bot.api.add_reaction(channel_id, sent_id, bot.EMOJI_FWD)
            await bot.api.add_reaction(channel_id, sent_id, bot.EMOJI_PAGE)
            bot.schedule_nav_reaction_expiry(channel_id, sent_id, inactivity_seconds=30)

    def parse_dice_value(text: str) -> tuple[int, int] | None:
        m = re.fullmatch(r"(?i)(\d+)d(\d+)", text.strip())
        if not m:
            return None
        n = int(m.group(1))
        size = int(m.group(2))
        if n < 1 or size < 2:
            return None
        if n > 100 or size > 1000:
            return None
        return n, size

    def parse_ally_target(event: dict, args: str) -> str | None:
        mentions = event.get("mentions") or []
        mention_ids = [m for m in mentions if isinstance(m, str)]
        if len(mention_ids) == 1:
            return mention_ids[0]
        if len(mention_ids) > 1:
            return None

        raw = (args or "").strip()
        if not raw:
            return None

        m = re.search(r"<@([A-Za-z0-9]+)>", raw)
        if m:
            return m.group(1)

        token = raw.split(maxsplit=1)[0]
        if token.startswith("@"):
            token = token[1:]
        return token if token else None

    # --- r.help ---
    async def cmd_help(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return
        lines = ["**Commands:**"]
        for name in sorted(commands.keys()):
            lines.append(f"- `{bot.config.prefix}{name}` — {help_text.get(name, '')}")
        await bot.reply(channel_id, "\n".join(lines))

    # --- r.start (attach map image) ---
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

        # Create game at turn 0
        bot.store.game_create(channel_id, author_id, msg_id, att_id, fn)

        # Persist turn 0 map (first MUP)
        bot.store.turnmap_upsert(channel_id, 0, msg_id, att_id, fn)
        bot.store.turnplayers_replace_from_current(channel_id, 0)
        bot.store.event_add(channel_id, 0, author_id, "game_start", f"Game started by <@{author_id}>.", msg_id)

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
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game to join.")
            return

        name = (args or "").strip() or "Player"
        is_new, count = bot.store.player_add_or_update(channel_id, author_id, name)
        turn = int(game["turn_number"])
        bot.store.turnplayers_replace_from_current(channel_id, turn)
        if is_new:
            bot.store.event_add(channel_id, turn, author_id, "player_join", f"<@{author_id}> joined as **{name}**.", msg_id)
            await bot.reply(channel_id, f"✅ <@{author_id}> joined the game as **{name}**. Players: **{count}**")
        else:
            bot.store.event_add(channel_id, turn, author_id, "player_rename", f"<@{author_id}> updated their name to **{name}**.", msg_id)
            await bot.reply(channel_id, f"✏️ <@{author_id}> updated their name to **{name}**. Players: **{count}**")

    # --- r.quit ---
    async def cmd_quit(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game.")
            return

        removed, count = bot.store.player_remove(channel_id, author_id)
        if removed:
            turn = int(game["turn_number"])
            bot.store.turnplayers_replace_from_current(channel_id, turn)
            bot.store.event_add(channel_id, turn, author_id, "player_quit", f"<@{author_id}> left the game.", msg_id)
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
        summary = f"<@{author_id}> rolled **{roll_value}**."
        if order_text:
            summary += f" Order: {order_text}"
        bot.store.event_add(channel_id, turn, author_id, "roll", summary, msg_id)

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

    # --- r.dnd <XdY> [order...] ---
    async def cmd_dnd(bot: "RiskBot", event: dict, args: str):
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

        parts = (args or "").strip().split(maxsplit=1)
        if not parts:
            await bot.reply(channel_id, "Usage: `r.dnd <XdY> [order...]` (example: `r.dnd 6d8 attack north`).")
            return

        dice_expr = parts[0]
        parsed = parse_dice_value(dice_expr)
        if not parsed:
            await bot.reply(channel_id, "Invalid dice value. Use `XdY` like `1d10` or `6d8`.")
            return

        n, size = parsed
        order_text = (parts[1] if len(parts) > 1 else "").strip()

        rolls = [secrets.randbelow(size) + 1 for _ in range(n)]
        total = sum(rolls)

        turn = int(game["turn_number"])
        stored_order = f"[dnd {n}d{size}]"
        if order_text:
            stored_order += f" {order_text}"
        count = bot.store.roll_add(channel_id, turn, author_id, str(total), stored_order, msg_id)
        summary = f"<@{author_id}> rolled **{total}** from **{n}d{size}**."
        if order_text:
            summary += f" Order: {order_text}"
        bot.store.event_add(channel_id, turn, author_id, "dnd_roll", summary, msg_id)

        name_map = bot.store.player_name_map(channel_id)
        pname = name_map.get(author_id, "Player")
        roll_list = ", ".join(str(v) for v in rolls[:30])
        if len(rolls) > 30:
            roll_list += ", ..."

        msg = (
            f"🎲 Turn **{turn}** — **{pname}** (<@{author_id}>) rolled **{total}** "
            f"from **{n}d{size}** (roll #{count} this turn).\n"
            f"Dice: {roll_list}"
        )
        if order_text:
            msg += f"\nOrder: {order_text}"
        await bot.reply(channel_id, msg)

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
        await publish_game_view(channel_id, turn, game=game)

    # --- r.events [turn] ---
    async def cmd_events(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return

        current_turn = int(game["turn_number"])
        raw = (args or "").strip()
        if raw:
            try:
                turn = int(raw)
            except ValueError:
                await bot.reply(channel_id, "Usage: `r.events [turn]` where turn is a non-negative integer.")
                return
            if turn < 0:
                await bot.reply(channel_id, "Turn cannot be negative.")
                return
            if turn > current_turn:
                await bot.reply(channel_id, f"Turn **{turn}** does not exist yet. Current turn is **{current_turn}**.")
                return
        else:
            turn = current_turn

        await publish_events_view(channel_id, turn, game=game)

    # --- r.say <note...> ---
    async def cmd_say(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return

        note = (args or "").strip()
        if not note:
            await bot.reply(channel_id, "Usage: `r.say <note...>`")
            return

        if len(note) > 500:
            await bot.reply(channel_id, "Note is too long. Keep it under 500 characters.")
            return

        if not bot.store.player_in_game(channel_id, author_id) and not bot.is_op(game, author_id):
            await bot.reply(channel_id, "You must be in the game (or be OP) to add a note.")
            return

        turn = int(game["turn_number"])
        bot.store.event_add(channel_id, turn, author_id, "note", f"📝 <@{author_id}>: {note}", msg_id)
        await bot.reply(channel_id, f"📝 Note added to turn **{turn}** events.")

    # --- r.add @player [name...] ---
    async def cmd_add(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.is_op(game, author_id):
            await bot.reply(channel_id, "Only the OP can force-add players.")
            return

        target_id = parse_ally_target(event, args)
        if not target_id:
            await bot.reply(channel_id, "Usage: `r.add @player [name...]` (mention exactly one player).")
            return

        raw = (args or "").strip()
        name = ""
        if raw:
            name = re.sub(r"<@[A-Za-z0-9]+>", "", raw, count=1).strip()
            if not name and raw.startswith("@"):
                parts = raw.split(maxsplit=1)
                name = parts[1].strip() if len(parts) > 1 else ""
        name = name or "Player"

        is_new, count = bot.store.player_add_or_update(channel_id, target_id, name)
        turn = int(game["turn_number"])
        bot.store.turnplayers_replace_from_current(channel_id, turn)

        if is_new:
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "player_forced_add",
                f"➕ OP <@{author_id}> force-added <@{target_id}> as **{name}**.",
                msg_id,
            )
            await bot.reply(channel_id, f"✅ Added <@{target_id}> as **{name}**. Players: **{count}**")
        else:
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "player_forced_update",
                f"✏️ OP <@{author_id}> force-updated <@{target_id}> to **{name}**.",
                msg_id,
            )
            await bot.reply(channel_id, f"✏️ Updated <@{target_id}> to **{name}**. Players: **{count}**")

    # --- r.ally @player ---
    async def cmd_ally(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.store.player_in_game(channel_id, author_id):
            await bot.reply(channel_id, "You must `r.join` before making alliances.")
            return

        target_id = parse_ally_target(event, args)
        if not target_id:
            await bot.reply(channel_id, "Usage: `r.ally @player` (mention exactly one player).")
            return
        if target_id == author_id:
            await bot.reply(channel_id, "You cannot ally with yourself.")
            return
        if not bot.store.player_in_game(channel_id, target_id):
            await bot.reply(channel_id, "That user is not a player in this game.")
            return

        result = bot.store.alliance_propose_or_accept(channel_id, author_id, target_id)
        name_map = bot.store.player_name_map(channel_id)
        me = name_map.get(author_id, "Player")
        them = name_map.get(target_id, "Player")
        turn = int(game["turn_number"])

        if result == "proposed":
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "alliance_proposed",
                f"🤝 **{me}** (<@{author_id}>) proposed an alliance to **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"🤝 Alliance proposed: **{me}** and **{them}**. Waiting for <@{target_id}> to confirm with `r.ally`.")
            return

        if result == "accepted":
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "alliance_accepted",
                f"✅ Alliance accepted between **{me}** (<@{author_id}>) and **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"✅ Alliance accepted between **{me}** and **{them}**.")
            return

        if result == "already_pending":
            await bot.reply(channel_id, f"⏳ Alliance is already pending with **{them}**.")
            return

        if result == "already_accepted":
            await bot.reply(channel_id, f"✅ Alliance is already active between **{me}** and **{them}**.")
            return

        await bot.reply(channel_id, "Could not update alliance state.")

    # --- r.alliances ---
    async def cmd_alliances(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return

        rows = bot.store.alliances_for_channel(channel_id)
        name_map = bot.store.player_name_map(channel_id)

        pending_lines = []
        accepted_lines = []
        for a in rows:
            ua = a["user_a"]
            ub = a["user_b"]
            name_a = name_map.get(ua, "Player")
            name_b = name_map.get(ub, "Player")
            status = (a["status"] or "").strip().lower()
            proposed_by = a["proposed_by"]
            proposer_name = name_map.get(proposed_by, "Player")
            if status == "pending":
                pending_lines.append(
                    f"- **{name_a}** (`{ua}`) ↔ **{name_b}** (`{ub}`) — proposed by **{proposer_name}**"
                )
            elif status == "accepted":
                accepted_lines.append(f"- **{name_a}** (`{ua}`) 🤝 **{name_b}** (`{ub}`)")

        lines = [
            "🤝 **Alliances**",
            "",
            "**Accepted:**",
        ]
        lines.extend(accepted_lines if accepted_lines else ["_(none)_"])
        lines.extend([
            "",
            "**Pending (not accepted):**",
        ])
        lines.extend(pending_lines if pending_lines else ["_(none)_"])
        await bot.reply(channel_id, "\n".join(lines))

    # --- r.betray @player ---
    async def cmd_betray(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.store.player_in_game(channel_id, author_id):
            await bot.reply(channel_id, "You must `r.join` before breaking alliances.")
            return

        target_id = parse_ally_target(event, args)
        if not target_id:
            await bot.reply(channel_id, "Usage: `r.betray @player` (mention exactly one player).")
            return
        if target_id == author_id:
            await bot.reply(channel_id, "You cannot betray yourself.")
            return
        if not bot.store.player_in_game(channel_id, target_id):
            await bot.reply(channel_id, "That user is not a player in this game.")
            return

        result = bot.store.alliance_break(channel_id, author_id, target_id)
        name_map = bot.store.player_name_map(channel_id)
        me = name_map.get(author_id, "Player")
        them = name_map.get(target_id, "Player")

        if result == "broken":
            turn = int(game["turn_number"])
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "alliance_broken",
                f"💥 **{me}** (<@{author_id}>) betrayed **{them}** (<@{target_id}>). Alliance broken.",
                msg_id,
            )
            await bot.reply(channel_id, f"💥 Alliance broken between **{me}** and **{them}**.")
            return

        if result == "cancelled_pending":
            turn = int(game["turn_number"])
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "alliance_cancelled",
                f"✂️ **{me}** (<@{author_id}>) cancelled the pending alliance with **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"✂️ Pending alliance cancelled between **{me}** and **{them}**.")
            return

        if result == "removed":
            await bot.reply(channel_id, f"Alliance relation removed between **{me}** and **{them}**.")
            return

        await bot.reply(channel_id, f"There is no alliance with **{them}**.")

    # --- r.nap @player ---
    async def cmd_nap(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.store.player_in_game(channel_id, author_id):
            await bot.reply(channel_id, "You must `r.join` before making NAPs.")
            return

        target_id = parse_ally_target(event, args)
        if not target_id:
            await bot.reply(channel_id, "Usage: `r.nap @player` (mention exactly one player).")
            return
        if target_id == author_id:
            await bot.reply(channel_id, "You cannot create a NAP with yourself.")
            return
        if not bot.store.player_in_game(channel_id, target_id):
            await bot.reply(channel_id, "That user is not a player in this game.")
            return

        result = bot.store.nap_propose_or_accept(channel_id, author_id, target_id)
        name_map = bot.store.player_name_map(channel_id)
        me = name_map.get(author_id, "Player")
        them = name_map.get(target_id, "Player")
        turn = int(game["turn_number"])

        if result == "proposed":
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "nap_proposed",
                f"🕊️ **{me}** (<@{author_id}>) proposed a NAP to **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"🕊️ NAP proposed: **{me}** and **{them}**. Waiting for <@{target_id}> to confirm with `r.nap`.")
            return

        if result == "accepted":
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "nap_accepted",
                f"✅ NAP accepted between **{me}** (<@{author_id}>) and **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"✅ NAP accepted between **{me}** and **{them}**.")
            return

        if result == "already_pending":
            await bot.reply(channel_id, f"⏳ NAP is already pending with **{them}**.")
            return

        if result == "already_accepted":
            await bot.reply(channel_id, f"✅ NAP is already active between **{me}** and **{them}**.")
            return

        await bot.reply(channel_id, "Could not update NAP state.")

    # --- r.naps ---
    async def cmd_naps(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        if not channel_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return

        rows = bot.store.naps_for_channel(channel_id)
        name_map = bot.store.player_name_map(channel_id)

        pending_lines = []
        accepted_lines = []
        for n in rows:
            ua = n["user_a"]
            ub = n["user_b"]
            name_a = name_map.get(ua, "Player")
            name_b = name_map.get(ub, "Player")
            status = (n["status"] or "").strip().lower()
            proposed_by = n["proposed_by"]
            proposer_name = name_map.get(proposed_by, "Player")
            if status == "pending":
                pending_lines.append(
                    f"- **{name_a}** (`{ua}`) ↔ **{name_b}** (`{ub}`) — proposed by **{proposer_name}**"
                )
            elif status == "accepted":
                accepted_lines.append(f"- **{name_a}** (`{ua}`) 🕊️ **{name_b}** (`{ub}`)")

        lines = [
            "🕊️ **NAPs**",
            "",
            "**Accepted:**",
        ]
        lines.extend(accepted_lines if accepted_lines else ["_(none)_"])
        lines.extend([
            "",
            "**Pending (not accepted):**",
        ])
        lines.extend(pending_lines if pending_lines else ["_(none)_"])
        await bot.reply(channel_id, "\n".join(lines))

    # --- r.break @player ---
    async def cmd_break(bot: "RiskBot", event: dict, args: str):
        channel_id = event.get("channel")
        author_id = event.get("author")
        msg_id = event.get("_id") or event.get("id") or ""
        if not channel_id or not author_id:
            return

        game = bot.store.game_get(channel_id)
        if not game:
            await bot.reply(channel_id, "There is no active game in this channel.")
            return
        if not bot.store.player_in_game(channel_id, author_id):
            await bot.reply(channel_id, "You must `r.join` before breaking NAPs.")
            return

        target_id = parse_ally_target(event, args)
        if not target_id:
            await bot.reply(channel_id, "Usage: `r.break @player` (mention exactly one player).")
            return
        if target_id == author_id:
            await bot.reply(channel_id, "You cannot break with yourself.")
            return
        if not bot.store.player_in_game(channel_id, target_id):
            await bot.reply(channel_id, "That user is not a player in this game.")
            return

        result = bot.store.nap_break(channel_id, author_id, target_id)
        name_map = bot.store.player_name_map(channel_id)
        me = name_map.get(author_id, "Player")
        them = name_map.get(target_id, "Player")

        if result == "broken":
            turn = int(game["turn_number"])
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "nap_broken",
                f"💥 **{me}** (<@{author_id}>) broke the NAP with **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"💥 NAP broken between **{me}** and **{them}**.")
            return

        if result == "cancelled_pending":
            turn = int(game["turn_number"])
            bot.store.event_add(
                channel_id,
                turn,
                author_id,
                "nap_cancelled",
                f"✂️ **{me}** (<@{author_id}>) cancelled the pending NAP with **{them}** (<@{target_id}>).",
                msg_id,
            )
            await bot.reply(channel_id, f"✂️ Pending NAP cancelled between **{me}** and **{them}**.")
            return

        if result == "removed":
            await bot.reply(channel_id, f"NAP relation removed between **{me}** and **{them}**.")
            return

        await bot.reply(channel_id, f"There is no NAP with **{them}**.")

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

        # Advance turn
        new_turn = bot.store.game_inc_turn(channel_id)

        # Determine map for this turn:
        # - If attached image: use it
        # - else: carry forward last known turn map
        att_id, fn = bot.pick_image_attachment(event)
        if not att_id or not fn:
            prev = bot.store.turnmap_latest(channel_id)
            if prev:
                att_id = prev["map_attachment_id"]
                fn = prev["map_filename"]

        # Update game's "current map" fields (if we have them)
        if att_id and fn:
            bot.store.game_set_map(channel_id, msg_id, att_id, fn)
            bot.store.turnmap_upsert(channel_id, new_turn, msg_id, att_id, fn)
            map_url = bot.attachment_url(att_id, fn)
        else:
            # no map at all (should be rare)
            bot.store.turnmap_upsert(channel_id, new_turn, msg_id, None, None)
            map_url = None

        # Snapshot roster for the new turn.
        bot.store.turnplayers_replace_from_current(channel_id, new_turn)

        player_ids = bot.store.player_ids(channel_id)
        note = (args or "").strip()
        event_summary = f"Turn **{new_turn}** started by <@{author_id}>."
        if note:
            event_summary += f" Note: {note}"
        bot.store.event_add(channel_id, new_turn, author_id, "turn_advance", event_summary, msg_id)

        msg = f"🔔 **Turn {new_turn}** is live. Please roll with `r.roll <order>`."
        if note:
            msg += f"\nNote: {note}"
        if map_url:
            msg += f"\nMap: {map_url}"

        await bot.reply(channel_id, msg, mentions=player_ids if player_ids else None)
        updated_game = bot.store.game_get(channel_id)
        await publish_game_view(channel_id, new_turn, game=updated_game)

    # Register all
    add("help", "Show this help message.", cmd_help)
    add("start", "Start a game (attach a map image).", cmd_start)
    add("reset", "End/reset the current game (OP only).", cmd_reset)
    add("end", "Alias for r.reset (OP only).", cmd_reset)
    add("join", "Join the current game. Usage: r.join <name>", cmd_join)
    add("add", "Force-add a player (OP only). Usage: r.add @player [name...]", cmd_add)
    add("quit", "Leave the current game.", cmd_quit)
    add("roll", "Generate an 11-digit roll and store it. Usage: r.roll [order...]", cmd_roll)
    add("dnd", "Roll dice notation and store total. Usage: r.dnd <XdY> [order...]", cmd_dnd)
    add("game", "Show game state, map, and all rolls for the current turn. Adds ⬅️/➡️ to browse turns.", cmd_game)
    add("events", "Show all recorded events for a turn. Usage: r.events [turn]", cmd_events)
    add("say", "Add a note to the current turn's events. Usage: r.say <note...>", cmd_say)
    add("ally", "Propose/accept an alliance. Usage: r.ally @player", cmd_ally)
    add("alliances", "Show pending and accepted alliances.", cmd_alliances)
    add("betray", "Break an accepted alliance. Usage: r.betray @player", cmd_betray)
    add("nap", "Propose/accept a NAP. Usage: r.nap @player", cmd_nap)
    add("naps", "Show pending and accepted NAPs.", cmd_naps)
    add("break", "Break an accepted NAP. Usage: r.break @player", cmd_break)
    add("mup", "Advance to next turn and ping players (OP only). Attach image to update map.", cmd_mup)
    add_synonyms(COMMAND_SYNONYMS)

    return commands, help_text
