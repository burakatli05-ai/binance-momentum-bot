"""Display-only conventions; internal ownership and accounting remain unchanged."""
from datetime import datetime, timedelta, timezone

IST = timezone(timedelta(hours=3))
BOT_DRY = '🤖 BOT / DRY'
MANUAL_LIVE = '👤 MANUEL / LIVE'
BOT_LIVE = '⚡ BOT / LIVE'


def ownership(source, mode=None):
    source = str(source or '').upper()
    if mode == 'DRY' or source in ('BOT DRY', 'BOT / DRY'):
        return BOT_DRY
    if source in ('BOT', 'BOT LIVE', 'BOT / LIVE'):
        return BOT_LIVE
    # UI category: account activity not matched to THIS bot. This is not proof
    # that a human placed an unmatched order. Keep UNKNOWN OWNERSHIP in storage.
    return MANUAL_LIVE


def number(value, suffix='', signed=False):
    if value is None: return '—'
    return (f'{value:+.2f}' if signed else f'{value:g}') + suffix


def timestamp(value):
    return datetime.fromtimestamp(value/1000, IST).strftime('%d.%m %H:%M:%S') if value else '—'


def pages(text, limit=3500):
    """Stay below Telegram's UTF-16 text limit, preferably at line boundaries."""
    result, page, units = [], '', 0
    for line in text.splitlines(keepends=True):
        count = len(line.encode('utf-16-le'))//2
        if page and units+count > limit:
            result.append(page.rstrip()); page, units = '', 0
        for char in line:
            size = 2 if ord(char) > 0xffff else 1
            if units+size > limit:
                result.append(page.rstrip()); page, units = '', 0
            page += char; units += size
    if page: result.append(page.rstrip())
    return result or ['—']
