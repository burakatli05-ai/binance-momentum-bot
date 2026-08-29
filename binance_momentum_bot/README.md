# Binance Momentum Scanner V5.2

V5.2, V5.1'in momentum/gainers/işlem-bölgesi mantığını korur ve Telegram iletişimini sağlamlaştırır.

## Önemli değişiklikler
- Telegram mesajları geçici ağ hatalarında otomatik yeniden denenir (en fazla 4 deneme).
- Telegram 429 rate-limit yanıtındaki `retry_after` süresine uyulur.
- Aynı anda birden fazla Telegram mesajının çakışmasını azaltmak için gönderimler sıraya alınır.
- Railway loglarında artık gerçek hata türü (`ServerDisconnectedError`, timeout vb.), HTTP kodu ve Telegram yanıtı görünür.
- `/status`, `/top`, `/gainers`, `/funnel`, `/stats`, `/test` komutlarının `getUpdates` hataları ayrıntılı loglanır.
- V5.1 ayarları korunur: 3 kontrol × 15 sn, giriş kalitesi 72+, yükseliş skoru 66+ (env ile değiştirilmediyse).
- Tahmini alım bölgesi, kâr-al 1/2 ve geçersizlik seviyesi korunur.

## Railway
Mevcut `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` ve Root Directory ayarlarını değiştirmeyin.
