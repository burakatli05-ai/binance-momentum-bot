from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BOT = (ROOT / "binance_momentum_bot" / "bot.py").read_text(encoding="utf-8")
EARLY = (ROOT / "binance_momentum_bot" / "early_v2_adapter.py").read_text(encoding="utf-8")


def test_telegram_commands_have_separate_priority_lane():
    assert 'telegram_command_context = contextvars.ContextVar' in BOT
    assert 'telegram_command_send_semaphore' in BOT
    assert 'telegram_background_send_semaphore' in BOT
    assert 'telegram_command_context.set(True)' in BOT
    assert 'telegram_send_lock = asyncio.Lock()' not in BOT


def test_telegram_retries_are_bounded_and_token_is_redacted():
    send_block = BOT.split('async def telegram_send(', 1)[1].split('async def telegram_public_alert', 1)[0]
    assert 'max_attempts = 2' in send_block
    assert 'total=5 if is_command else 8' in send_block
    assert '_telegram_safe_error(e)' in send_block
    assert 'repr=%r' not in send_block
    assert 'detail.replace(TELEGRAM_BOT_TOKEN, "<redacted>")' in BOT


def test_early_v2_notifications_do_not_block_worker():
    assert 'self._notify_tasks = set()' in EARLY
    assert 'asyncio.create_task(self._deliver_notify(session, item))' in EARLY
    drain = EARLY.split('async def _drain_notify', 1)[1].split('def _notify_open_if_new', 1)[0]
    assert "await self.b['telegram_send']" not in drain
    assert 'while len(self._notify_tasks) < 3' in drain
