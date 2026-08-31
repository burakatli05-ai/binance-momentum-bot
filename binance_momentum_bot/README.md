# Binance Momentum Scanner V5.6

V5.6, V5.5 Quality-First Premium üretimini ve TP1/TP2 hesaplarını değiştirmeden ölçüm ve araştırma altyapısını genişletir.

## Değişmeyen production kuralları

- Premium seçim fonksiyonları ve eşikleri
- Momentum / giriş kalitesi / yükseliş skoru hesapları
- 3×15 sn süreklilik yapısı
- Erken Momentum production seçiciliği
- Premium breakout / flow / aggressive buy / candidate-runup guard'ları
- Tahmini alım bölgesi, TP1, TP2 ve geçersizlik formülleri
- Shadow Exit eşikleri (hala test amaçlı)

## V5.6 yeni ölçüm katmanları

- Event-level aggTrade ile daha hassas Premium MFE/MAE ve trade-path takibi
- `episode_id` ile ayrı momentum hareketlerinin ilişkilendirilmesi
- Second-Wave / reacceleration shadow araştırma kayıtları
- Pre-Breakout shadow araştırma kayıtları
- 3/3 teyit sonrası reddedilen adayların outcome takibi
- Flow-to-price efficiency ve pre-episode hacim baseline alanları
- Squeeze-risk araştırma etiketi
- Gainers rank velocity + 1/5/15/30/60 dk outcome; Telegram push varsayılan kapalı
- First-wave peak ile 60 dk session peak ayrımı
- Çoklu wave / pullback olay kayıtları
- Shadow EXIT sonrası 30 sn / 1 dk / 5 dk / 15 dk outcome
- Coin bazında günün kaçıncı kullanıcı bildirimi olduğu
- SQLite WAL + busy timeout
- Persistent DB varsa restart sonrası açık tracker recovery
- AggTrade chunk bazında stream health
- `/researchstats`

## Telegram

Varsayılan kullanıcı bildirimleri:

- Erken Momentum: açık
- Premium: açık
- Shadow: açık ve açıkça TEST olarak işaretli
- Gainers otomatik push: kapalı; veri arka planda toplanır

Komutlar:

`/status` `/top` `/gainers` `/funnel` `/stats` `/radarstats` `/shadowstats` `/researchstats` `/analiz COIN` `/test`

## Railway

Mevcut Telegram değişkenleri yeterlidir. Yeni research değişkenleri zorunlu değildir; varsayılanlar güvenli ölçüm modundadır.

Önemli: `signals.db` Railway'in ephemeral dosya sisteminde ise deploy/restart sırasında kaybolabilir. Kalıcı Volume kullanılana kadar deploy öncesi DB yedeği alın.

## Araştırma prensibi

Second-Wave, Pre-Breakout, Gainers re-scan, squeeze/absorption ve Shadow outcome verileri production alım filtresi değildir. Yeterli forward örneklem oluşmadan Premium eşikleri değiştirilmemelidir.
