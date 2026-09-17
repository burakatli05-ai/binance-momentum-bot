"""Local PNG cards and Telegram-only transport; text remains the fallback."""
import asyncio
from io import BytesIO
from pathlib import Path
import logging
import aiohttp
from telegram_cards import pages, BOT_DRY, BOT_LIVE, MANUAL_LIVE

log = logging.getLogger(__name__)
CARDS_PER_PAGE = 2


def text_page(view, cards, page, count):
    return '\n'.join([view['title'], view['date'], *view['summary'], f'Sayfa {page}/{count}', '',
        *('\n'.join(c['lines'])+'\nKaynak: '+c.get('provenance','—')+' | Fill: '+','.join(c.get('fill_ids',[]))+'\n' for c in cards), *view.get('notices', [])])


def batches(view):
    cards = view['cards']
    return [cards[i:i+CARDS_PER_PAGE] for i in range(0, len(cards), CARDS_PER_PAGE)] or [[]]


def _card_height(card):
    return max(310, 82 + len(card.get('lines', []))*40)


def render(view, cards, page, count):
    from PIL import Image, ImageDraw, ImageFont
    font_path = str(Path(__file__).with_name('fonts')/'OpenSans.ttf')
    fonts = {size: ImageFont.truetype(font_path, size) for size in (20, 23, 26, 30, 38)}
    width = 1000
    header = 170 + len(view['summary'])*34
    heights = [_card_height(card) for card in cards] or [310]
    gap = 20
    height = header + sum(heights) + gap*max(0, len(heights)-1) + 32
    im = Image.new('RGB', (width, height), '#091426')
    draw = ImageDraw.Draw(im)
    def line(text, x, y, size=26, color='#edf3fc', max_width=900):
        text = str(text)
        font = fonts[size]
        while draw.textlength(text, font=font) > max_width and text:
            text = text[:-2]+'…' if len(text) > 2 else ''
        draw.text((x,y), text, font=font, fill=color)
    title = view['title']
    if title.startswith('🟢 '):
        draw.ellipse((40,41,62,63), fill='#36e1a0')
        line(title[2:], 80, 25, 38)
    else: line(title, 40, 25, 38)
    line(view['date'], 40, 82, 23, '#94aac6')
    line(f'{page} / {count}', 860, 85, 23)
    for i, value in enumerate(view['summary']): line(value, 40, 125+i*34, 23)
    y = header
    for card, card_height in zip(cards, heights):
        color = '#36e1a0' if card.get('pnl') is not None and card['pnl'] > 0 else '#ff7288' if card.get('pnl') is not None and card['pnl'] < 0 else '#84b8ff'
        draw.rounded_rectangle((26,y,974,y+card_height-20), radius=18, fill='#13243b', outline=color, width=2)
        draw.rounded_rectangle((26,y,34,y+card_height-20), radius=4, fill=color)
        for j, value in enumerate(card['lines']):
            text = value
            if j == 1:
                source = card.get('source', MANUAL_LIVE)
                x, top = 50, y+61
                if source == BOT_DRY:
                    draw.rounded_rectangle((x,top+3,x+25,top+23),4,outline='#94aac6',width=2)
                    draw.ellipse((x+5,top+9,x+8,top+12),fill='white'); draw.ellipse((x+17,top+9,x+20,top+12),fill='white')
                    draw.line((x+12,top,x+12,top-4),fill='#94aac6',width=2)
                elif source == BOT_LIVE:
                    draw.polygon([(x+16,top),(x+3,top+15),(x+12,top+15),(x+8,top+29),(x+25,top+10),(x+16,top+10)],fill='#ffd66e')
                else:
                    draw.ellipse((x+7,top,x+20,top+13),fill='#94aac6')
                    draw.rounded_rectangle((x+2,top+16,x+25,top+28),8,fill='#94aac6')
                text = source.split(' ',1)[1]; line(text, 88, y+54, 23, '#c6d4e8')
                continue
            line(text, 50, y+14+j*40, 30 if j==0 else 23, color if j==0 else '#edf3fc', 872)
        y += card_height + gap
    if not cards: line('Gösterilecek kayıt yok.', 50, header+45, 26)
    output = BytesIO(); im.save(output, format='PNG')
    result = output.getvalue()
    if len(result) > 9_000_000 or width+height > 10000: raise ValueError('photo exceeds safe bounds')
    return result


async def send_photo(bot, session, png, caption, chat_id):
    if session is None or not bot.get('TELEGRAM_BOT_TOKEN') or not chat_id: return False
    form = aiohttp.FormData()
    form.add_field('chat_id', str(chat_id))
    if caption:
        form.add_field('caption', caption)
    form.add_field('photo', png, filename='positions.png', content_type='image/png')
    async with bot['telegram_send_lock']:
        async with session.post('https://api.telegram.org/bot'+bot['TELEGRAM_BOT_TOKEN']+'/sendPhoto',
            data=form, timeout=aiohttp.ClientTimeout(total=15, connect=6, sock_read=10)) as response:
            if response.status != 200: return False
            result = await response.json()
            return result.get('ok') is True


async def deliver(bot, session, view, chat_id, raw=False):
    chunks = batches(view)
    for i, cards in enumerate(chunks, 1):
        text = text_page(view, cards, i, len(chunks))
        sent = False
        if not raw:
            try:
                png = await asyncio.to_thread(render, view, cards, i, len(chunks))
                if view.get('caption', True):
                    labels = sorted({c.get('source', MANUAL_LIVE) for c in cards})
                    caption = '\n'.join([view['title'], f'{i}/{len(chunks)}', *labels, *view.get('notices',[])])
                    while len(caption.encode('utf-16-le'))//2 > 1000: caption = caption[:-1]
                else:
                    caption = ''
                sent = await send_photo(bot, session, png, caption, chat_id)
            except asyncio.CancelledError: raise
            except Exception as error:
                log.warning('Telegram card unavailable (%s); using text', type(error).__name__)
        if not sent:
            for part in pages(text):
                if not await bot['telegram_send'](session, part, chat_id=chat_id): return False
    return True
