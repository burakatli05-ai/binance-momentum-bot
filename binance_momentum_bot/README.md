# Binance Momentum Scanner V5.1 — Balanced Confirmation + Trade Plan

V5.1, V5'in hiç sinyal üretmemesine yol açan aşırı sıkı filtreleri dengeler ve `/top` içindeki güçlü adayları otomatik olarak izleyip yalnızca süreklilik teyit edildiğinde Telegram'a alım fırsatı mesajı gönderir.

## V5.1 değişiklikleri
- Süreklilik teyidi 4 kontrolden 3 kontrole indirildi; varsayılan aralık 15 sn.
- Minimum giriş kalitesi 78'den 72'ye indirildi.
- Minimum yükseliş potansiyeli 70'ten 66'ya indirildi.
- Süreklilik için minimum momentum 64'ten 62'ye indirildi.
- FOMO/absorpsiyon, aşırı agresif alış ve aşırı uzama cezaları korunur.
- `/top` satırlarında `🎯` işareti, süreklilik gösterip alım fırsatına yaklaşan adayları belirtir.
- Bot `/top` adaylarını kullanıcı komutu olmadan zaten arka planda izler; kriterler tamamlanınca otomatik Telegram bildirimi gönderir.
- Teyitli bildirime tahmini işlem planı eklendi:
  - alım bölgesi
  - kâr al 1
  - kâr al 2
  - geçersizlik/stop seviyesi
  - yaklaşık risk/ödül oranı
- Bu seviyeler son kısa vadeli volatilite ve destek/dirençten türetilir; sabit yüzde hedef değildir.
- Yeni `/funnel` komutu adayların hangi filtrelerde elendiğini gösterir.
- `/stats` 60 dk tamamlanmış sinyal sonuçlarını göstermeye devam eder.
- Gainers sistemi değişmeden korunur.

## Telegram komutları
- `/status` bağlantı ve eşikler
- `/top` ısınan coinler; `🎯` alım fırsatına yaklaşan adayı gösterir
- `/gainers` Futures gainers
- `/funnel` bu deploy sırasında adayların hangi filtrelerde elendiği
- `/stats` 60 dk gerçekleşen sinyal performansı
- `/test` bot testi

## Railway
Mevcut `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` ve Root Directory ayarları değişmez. Dosyaları mevcut GitHub `binance_momentum_bot` klasörüne yükleyip commit etmek yeterlidir.

## Önemli
Bu bot otomatik emir açmaz. "Alım bölgesi", hedefler ve geçersizlik seviyesi kural tabanlı tahmini seviyelerdir; gerçek fiyat kayması, likidite, kaldıraç, komisyon ve piyasa koşullarını garanti etmez. "Yükseliş potansiyeli" skoru da kalibre edilmiş gerçek olasılık yüzdesi değildir. Yeni sinyaller biriktikçe eşikler yeniden analiz edilmelidir.
