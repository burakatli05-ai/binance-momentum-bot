# Binance Futures Momentum Scanner -> Telegram

Binance USDⓈ-M Futures'taki aktif USDT perpetual kontratlarını izler. 1 dakikalık fiyat ivmesi, göreceli hacim (RVOL), taker-buy oranı, 15 dakikalık breakout ve aday sinyallerde Open Interest değişimini birleştirerek Telegram alarmı üretir.

## 1) Telegram botunu oluştur

Telegram'da `@BotFather` ile yeni bot oluşturup token al. Bota bir mesaj gönder. Chat ID'ni öğrenip `.env` dosyasına yaz.

## 2) Kurulum

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/macOS:
# source .venv/bin/activate

pip install -r requirements.txt
copy .env.example .env   # Windows
# cp .env.example .env   # Linux/macOS
```

`.env` içindeki `TELEGRAM_BOT_TOKEN` ve `TELEGRAM_CHAT_ID` değerlerini doldur.

## 3) Çalıştır

```bash
python bot.py
```

Binance API key gerekmez. Bot yalnızca public market data kullanır ve emir açmaz.

## Varsayılan sinyal mantığı

- Tüm aktif USDT perpetual kontratlar taranır.
- Minimum 24 saat quote volume: 5M USDT.
- Aday için RVOL >= 2x, 1m fiyat >= +0.35%, taker-buy >= %58 ve skor >= 60 gerekir.
- RVOL, o anki 1 dakikalık mum hacminin dakika sonuna projeksiyonunun önceki 20 kapalı 1m mum ortalamasına oranıdır.
- 15m high breakout ek puan verir.
- Open Interest sadece aday coinlerde sorgulanır; yaklaşık 5 dakikalık baz ile karşılaştırılır.
- Aynı seviye alarmında 5 dakika cooldown vardır; sinyal seviyesi yükselirse cooldown içinde yeni alarm gelebilir.
- Sinyaller `signals.db` SQLite veritabanına yazılır.

## Uyarı

Bu yazılım yatırım tavsiyesi veya otomatik trade sistemi değildir. Momentum sinyalleri false-positive üretebilir; özellikle düşük likidite ve yüksek kaldıraç ciddi kayıp riski taşır. Önce paper trading / küçük boyutla istatistik toplamak mantıklıdır.
