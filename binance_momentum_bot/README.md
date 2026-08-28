# Binance Futures Momentum Scanner V4

V4 iki ayrı radar çalıştırır:

1. **Süreklilik teyitli momentum**: Tüm erken adaylar arka planda sessiz izlenir. Telegram yalnızca hareket 15 saniye arayla 3 kontrolde devam eder, hacim/agresif alış korunur ve giriş kalitesi eşiği geçilirse bildirim gönderir.
2. **Gainers radarı**: Aktif ve minimum hacmi geçen USDT perpetual kontratları 24 saatlik yüzde değişime göre sıralanır. TOP 50'ye sonradan giren coin bildirilir. Ayrıca yaklaşık 10 dakikada 25 veya daha fazla sıra yükselip ilk 100'e ulaşan coinler ayrı erken uyarı üretir.

## Spam koruması
- Bot açıldığında zaten TOP 50'de bulunan coinler topluca bildirilmez; ilk sıralama yalnızca başlangıç referansıdır.
- Aynı gainers olayı için varsayılan cooldown 30 dakikadır.
- Bir coin TOP 50'den çıkıp tekrar girerse yeni giriş bildirimi için en az 5 dakika dışarıda kalması gerekir.
- Bir coin aynı kontrolde hem TOP 50'ye girmiş hem hızlı yükselmişse iki mesaj yerine öncelikle giriş mesajı gönderilir.

## Varsayılan momentum ayarları
- Sessiz aday skoru: 58+
- Süreklilik: 15 saniye arayla 3 başarılı kontrol
- Giriş kalitesi: 76+
- Teyitli momentum cooldown: 20 dakika

## Varsayılan gainers ayarları
- `GAINERS_TOP_N=50`
- `GAINERS_POLL_SECONDS=30`
- `GAINERS_RAPID_WINDOW_SECONDS=600`
- `GAINERS_RAPID_MIN_POSITIONS=25`
- `GAINERS_RAPID_MAX_RANK=100`
- `GAINERS_ALERT_COOLDOWN_SECONDS=1800`
- `GAINERS_REENTRY_MIN_OUT_SECONDS=300`

## Telegram komutları
- `/status` — canlı bağlantı, momentum ve gainers ayarları
- `/top` — şu an ısınan ilk 10 coin (alarm değildir)
- `/gainers` — güncel ilk 20 Futures gainers
- `/test` — Telegram testi

Railway'deki mevcut `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` ve Root Directory ayarları değişmeden kullanılabilir.
