# Binance Momentum Scanner V5 — Data-tuned

V5, ilk canlı örneklemdeki 59 sinyal / 342 outcome kaydından çıkan bulgulara göre V4'ün filtrelerini sıkılaştırır.

## V5 değişiklikleri
- Teyit 3 kontrolden 4 kontrole çıktı (varsayılan 15 sn aralık).
- Momentum yoğunluğu, yükseliş potansiyeli ve giriş kalitesi ayrı skorlanır.
- Aşırı agresif alış (%88+) artık otomatik olarak güçlü kabul edilmez; FOMO/absorpsiyon cezası alabilir.
- Çok yüksek flow fakat zayıf fiyat ilerlemesi tükenme/absorpsiyon olarak cezalandırılır.
- 77–84 momentum bandı ve orta-yüksek fakat doygun olmayan alıcı baskısı daha dengeli değerlendirilir.
- Telegram teyitli alarmı için varsayılan giriş kalitesi 78+ ve yükseliş potansiyeli 70+ gerekir.
- `/stats` komutu tamamlanmış 60 dk sinyallerinin +%0.5 / +%1 / +%2 görme oranlarını, ortalama MFE ve MAE'yi gösterir.
- Gainers sistemi V4'teki gibi korunur.

## Telegram komutları
- `/status` bağlantı ve eşikler
- `/top` ısınan coinler (alarm değildir)
- `/gainers` Futures gainers
- `/stats` 60 dk gerçekleşen sinyal performansı
- `/test` bot testi

## Railway
Mevcut `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` ve Root Directory ayarları değişmez. V5 dosyalarını mevcut GitHub klasörüne yükleyip commit etmek yeterlidir.

## Önemli
Bu bir kural tabanlı araştırma/uyarı aracıdır. “Yükseliş potansiyeli” skoru kalibre edilmiş gerçek olasılık yüzdesi değildir ve alım emri/kâr garantisi anlamına gelmez. Örneklem büyüdükçe eşikler yeniden analiz edilmelidir.
