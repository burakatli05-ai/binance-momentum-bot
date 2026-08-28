# Binance Momentum Scanner V2

Telegram'a **yükseliş başlarken** erken momentum alarmı gönderen Binance USDⓈ-M Futures tarayıcısı.

## V2'de ne değişti?

- 2026 Binance WebSocket ayrımına geçirildi:
  - `/market` = ticker, aggTrade, liquidation
  - `/public` = bookTicker
- 1 dakikalık mum beklemek yerine `aggTrade` akışını yaklaşık 100ms güncellemelerle izler.
- 10 sn / 30 sn / 60 sn fiyat ivmesi hesaplar.
- 10 sn / 30 sn / 60 sn **hacim hızı** ölçer. Coinin normal 1 dakikalık hacmiyle kıyaslar.
- Agresif alış oranını (`buyer is maker = false`) takip eder.
- En iyi bid/ask miktarından basit order-book baskısı ve spread ölçer.
- BTC'ye göre 30 saniyelik göreceli güç hesaplar.
- Short liquidation akışını takip eder.
- Gerçek adaylarda 5 dakikalık Open Interest istatistiğini kontrol eder.
- Çok uzamış hareketleri kovalamamak için ceza uygular.
- Sinyal sonrası 1/3/5/15/30/60 dakikalık sonuçları SQLite'a kaydeder.
- Telegram komutları:
  - `/status`
  - `/top`
  - `/test`
  - `/help`

## Railway güncelleme

Mevcut Railway projesinde Root Directory zaten `/binance_momentum_bot` ise bunu değiştirmeyin.

GitHub repository'nizdeki eski `binance_momentum_bot` klasörünün içeriğini bu klasördeki dosyalarla değiştirin. `TELEGRAM_BOT_TOKEN` ve `TELEGRAM_CHAT_ID` GitHub'a yazılmaz; Railway Variables altında kalır.

GitHub değişikliği sonrası Railway otomatik deploy yapmalıdır. Başlangıç mesajında `Momentum Scanner V2 başladı` görmelisiniz.

## İlk test

Telegram'da:

- `/test` -> bot cevap vermeli
- `/status` -> ticker/book/agg akışlarının yaşını göstermeli; normalde 90 sn altında olmalı
- `/top` -> henüz alarm üretmeyen ama o anda ısınan coinleri göstermeli

## Önemli

Bu yazılım piyasa tarama/uyarı aracıdır. Kâr garantisi vermez ve otomatik emir açmaz. Özellikle Futures/kaldıraç işlemlerinde sinyal istatistiklerini yeterli örneklemde ölçmeden otomatik trade'e çevirmeyin.
