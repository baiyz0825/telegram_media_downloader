"""Rewrite pyrogram.get_chat_history"""

from datetime import datetime
from typing import AsyncGenerator, Optional, Union

import pyrogram

# pylint: disable = W0611
from pyrogram import raw, types, utils


async def get_chunk_v2(
    *,
    client: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    offset: int = 0,
    max_id: int = 0,
    from_message_id: int = 0,
    from_date: datetime = utils.zero_datetime(),
    reverse: bool = False
):
    """get chunk"""
    from_message_id = from_message_id or (1 if reverse else 0)

    messages = await utils.parse_messages(
        client,
        await client.invoke(
            raw.functions.messages.GetHistory(
                peer=await client.resolve_peer(chat_id),
                offset_id=from_message_id,
                offset_date=utils.datetime_to_timestamp(from_date),
                add_offset=offset * (-1 if reverse else 1) - (limit if reverse else 0),
                limit=limit,
                max_id=max_id,
                min_id=0,
                hash=0,
            ),
            sleep_threshold=60,
        ),
        replies=0,
    )

    if reverse:
        messages.reverse()

    return messages


# pylint: disable = C0301
async def get_chat_history_v2(
    self: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    max_id: int = 0,
    offset: int = 0,
    offset_id: int = 0,
    offset_date: datetime = utils.zero_datetime(),
    reverse: bool = False,
) -> Optional[AsyncGenerator["types.Message", None]]:
    """Get messages from a chat history."""
    current = 0
    total = limit or (1 << 31) - 1
    page_size = min(100, total)
    cursor = offset_id

    while True:
        messages = await get_chunk_v2(
            client=self,
            chat_id=chat_id,
            limit=min(page_size, total - current),
            offset=offset,
            max_id=max_id + 1 if max_id else 0,
            from_message_id=cursor,
            from_date=offset_date,
            reverse=reverse,
        )

        # IDs are boundaries, never counts: deleted IDs leave arbitrary gaps.
        # Sorting and an exclusive moving boundary also reject overlapping or
        # repeated pages without retaining the full channel's IDs in memory.
        candidates = {
            message.id: message
            for message in messages
            if message.id > 0
            and (not max_id or message.id <= max_id)
            and (
                not cursor or (message.id >= cursor if reverse else message.id < cursor)
            )
        }
        if not candidates:
            return
        for message_id in sorted(candidates, reverse=not reverse):
            message = candidates[message_id]
            yield message
            current += 1
            if current >= total:
                return
        cursor = max(candidates) + 1 if reverse else min(candidates)
        offset = 0
