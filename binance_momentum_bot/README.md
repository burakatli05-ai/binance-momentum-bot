# Binance Momentum Scanner V5.5

V5.5, V5.4 Quality-First sisteminin sinyal üretimini değiştirmeden araştırma/çıkış gözlem katmanı ekler.

## Değişmeyenler

- Premium seçim filtreleri ve eşikleri
- Momentum / giriş kalitesi / yükseliş skoru hesapları
- 3×15 sn süreklilik yapısı
- Erken Momentum seçiciliği
- Tahmini alım bölgesi, TP1, TP2 ve geçersizlik formülleri
- Gainers ve Momentum Devamı yapısı

## Yeni özellikler

- 🧪 Shadow Kâr Koruma Adayı
- 🧪 Shadow Çıkış Adayı
- Premium sonrası ilk/aktif tepe takibi
- Tepeden -%0.5 / -%1 / -%1.5 / -%2 geri çekilme kayıtları
- Geri çekilme anında mikro yapı metrikleri
- Erken radar → Premium doğrudan ilişkilendirmesi
- Erken→Premium süre ve fiyat maliyeti kaydı
- `/shadowstats`

Shadow mesajları test içindir; gerçek satış emri veya otomatik işlem değildir.

## Railway

Mevcut V5.4 Railway değişkenleri yeterlidir. Yeni değişken eklemek zorunlu değildir. Varsayılan olarak Shadow takip ve Telegram Shadow bildirimi açıktır.

İsteğe bağlı değişkenler:

- `SHADOW_EXIT_ENABLED=1`
- `SHADOW_EXIT_NOTIFY=1`
- `SHADOW_MIN_PEAK_MFE_PCT=1.00`
- `SHADOW_PROTECT_MIN_PEAK_PCT=1.50`
- `SHADOW_PROTECT_DRAWDOWN_PCT=0.60`
- `SHADOW_EXIT_DRAWDOWN_PCT=1.00`
- `SHADOW_HARD_DRAWDOWN_PCT=2.00`
- `SHADOW_MIN_AGE_SECONDS=30`

## Telegram komutları

`/status` `/top` `/gainers` `/funnel` `/stats` `/radarstats` `/shadowstats` `/analiz COIN` `/test`
