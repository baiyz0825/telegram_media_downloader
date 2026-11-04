"""History paging respects ID gaps, direction and bounds."""

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from module.get_chat_history_v2 import get_chat_history_v2


def messages(*ids):
    return [SimpleNamespace(id=ident) for ident in ids]


class HistoryTests(unittest.IsolatedAsyncioTestCase):
    async def collect(self, pages, **kwargs):
        client = Mock()
        with patch(
            "module.get_chat_history_v2.get_chunk_v2", AsyncMock(side_effect=pages)
        ) as chunks:
            result = [
                item.id async for item in get_chat_history_v2(client, 123, **kwargs)
            ]
        client.get_chat_history.assert_not_called()
        return result, chunks

    async def test_reverse_gaps_overlapping_pages_and_max_bound(self):
        result, chunks = await self.collect(
            [messages(30, 10, 20, 20), messages(30, 50, 80), []],
            reverse=True,
            offset_id=10,
            max_id=50,
        )
        self.assertEqual(result, [10, 20, 30, 50])
        self.assertEqual(chunks.call_args_list[1].kwargs["from_message_id"], 31)
        self.assertTrue(
            all(call.kwargs["max_id"] == 51 for call in chunks.call_args_list)
        )

    async def test_descending_gaps_and_duplicate_pages_terminate(self):
        result, _ = await self.collect(
            [messages(40, 80, 60), messages(60, 40, 20), messages(20)],
            offset_id=100,
        )
        self.assertEqual(result, [80, 60, 40, 20])

    async def test_empty_page_never_scans_by_id_as_count(self):
        result, chunks = await self.collect([[]], reverse=True, offset_id=999999)
        self.assertEqual(result, [])
        self.assertEqual(chunks.await_count, 1)

    async def test_limit_and_offset_only_on_initial_page(self):
        result, chunks = await self.collect(
            [messages(5, 9), messages(12, 14)],
            reverse=True,
            limit=3,
            offset=2,
        )
        self.assertEqual(result, [5, 9, 12])
        self.assertEqual(chunks.call_args_list[0].kwargs["offset"], 2)
        self.assertEqual(chunks.call_args_list[1].kwargs["offset"], 0)
        self.assertEqual(chunks.call_args_list[1].kwargs["limit"], 1)


if __name__ == "__main__":
    unittest.main()
