"""Isolated Telegram HTTP lanes; no orders, secrets, database or network on import.

Offline candidate only. Pool isolation does not repair provider/network faults.
The caller must enter/exit the context on the same event loop as the bot.
"""
import asyncio
from contextlib import AsyncExitStack
import re

import aiohttp


class TelegramSessions:
    # Separate long polling, command responses, alerts and file uploads. None
    # shares the connector used for Binance orders, position reads or streams.
    LIMITS = {"alerts": 3, "commands": 2, "poll": 1, "files": 1}

    def __init__(self):
        self._stack = None
        self._sessions = {}

    async def __aenter__(self):
        if self._stack is not None:
            raise RuntimeError("TelegramSessions already entered")
        stack = AsyncExitStack()
        await stack.__aenter__()
        try:
            for name, size in self.LIMITS.items():
                connector = aiohttp.TCPConnector(limit=size, limit_per_host=size,
                                                 ttl_dns_cache=300)
                try:
                    session = aiohttp.ClientSession(connector=connector,
                                                    timeout=aiohttp.ClientTimeout(total=30))
                except BaseException:
                    await connector.close()
                    raise
                self._sessions[name] = await stack.enter_async_context(session)
        except BaseException:
            await stack.aclose()
            self._sessions.clear()
            raise
        self._stack = stack
        return self

    def select(self, lane):
        if self._stack is None or lane not in self._sessions:
            raise RuntimeError("Telegram HTTP lane unavailable")
        session = self._sessions[lane]
        if session.closed:
            raise RuntimeError("Telegram HTTP lane closed")
        return session

    async def __aexit__(self, exc_type, exc, tb):
        stack, self._stack = self._stack, None
        try:
            if stack is not None:
                return await stack.__aexit__(exc_type, exc, tb)
        finally:
            self._sessions.clear()


def redact_telegram_error(error, known_token=""):
    """Redact before truncating, including stale/other bot tokens in URLs."""
    text = str(error)
    if known_token:
        text = text.replace(known_token, "<redacted>")
    text = re.sub(r"(?i)(https?://api\.telegram\.org/(?:file/)?bot)[^/\s?'\"<>]+",
                  r"\1<redacted>", text)
    text = re.sub(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b", "<redacted>", text)
    return text[:500]


def valid_message_ack(data):
    """A queue admission or HTTP 200 is not a Telegram sendMessage receipt."""
    if not isinstance(data, dict) or data.get("ok") is not True:
        return False
    result = data.get("result")
    if not isinstance(result, dict):
        return False
    mid = result.get("message_id")
    return isinstance(mid, int) and not isinstance(mid, bool) and mid > 0
