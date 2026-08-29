# Binance Momentum Scanner V5.3 — Quality First

V5.3, V5.2'nin çalışan Binance Futures + Telegram altyapısını korur. Amaç daha fazla sinyal üretmek değil, daha seçici bildirim göndermek ve sonraki optimizasyon için daha iyi veri toplamaktır.

## Başlıca yenilikler

- **Premium ALIM FIRSATI filtresi:** 3/3 süreklilik yetmez; giriş kalitesi, yükseliş skoru, breakout, aggressive-buy tatlı bölgesi, kısa vadeli fiyat hızı, hacim akışı, order-book aşırılığı ve adaydan beri fiyatın ne kadar uzadığı birlikte kontrol edilir.
- **ERKEN MOMENTUM / İZLE:** Hareketin erken safhasında seçici radar uyarısı. Bu mesaj işlem sinyali değildir; 3/3 teyit beklenir.
- **/analiz COIN:** Örn. `/analiz TUT` veya sadece `TUT`. Coinin canlı momentum, flow, aggressive-buy, bid baskısı, BTC relatif güç, OI, breakout, Gainers sırası ve kural tabanlı işlem bölgesi gösterilir.
- **Momentum devamı:** Premium sinyal TP2 ölçeğini geçtikten sonra momentum hâlâ güçlü ise yalnızca bir kez devam uyarısı üretilebilir. Bu yeni giriş çağrısı değildir.
- **Aday geçmişi:** `candidate_events` tablosu ilk aday, erken uyarı, süreklilik geçişleri, reddedilme nedeni ve premium sinyali kaydeder.
- **Gainers olayları:** TOP giriş ve hızlı sıra yükselişi `gainers_events` tablosuna kaydedilir.
- **Signal meta:** giriş kalitesi, yükseliş skoru, aday run-up, Gainers sırası ve 24s hacim `signal_meta` tablosuna yazılır.

## Varsayılan premium yaklaşım

V5.3 daha az ama daha seçici ALIM FIRSATI hedefler. Varsayılan temel market-yapısı filtresi, elimizdeki 28 tamamlanmış V5.2 sinyal üzerinde geriye dönük olarak yaklaşık yarı kadar sinyal seçmiştir. Bu küçük ve aynı veri üzerinde yapılan bir örneklem testidir; gelecekte aynı başarıyı garanti etmez. V5.3 bu nedenle veriyi ayrıca kaydeder ve eşikler yeni örneklerle yeniden değerlendirilmelidir.

## Komutlar

- `/status`
- `/top`
- `/gainers`
- `/funnel`
- `/stats`
- `/analiz TUT`
- `TUT` (kısa kullanım)
- `/test`

## Railway

Mevcut değişkenler çalışmaya devam eder:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Yeni V5.3 eşiklerinin tamamı environment variable ile değiştirilebilir ancak ilk deploy'da varsayılanları kullanmak daha sağlıklıdır.

> Bu yazılım otomatik emir vermez. Üretilen seviyeler ve sinyaller kural tabanlı piyasa araştırmasıdır; kâr garantisi değildir. Gerçek para ile kullanımda pozisyon büyüklüğü, kaldıraç ve zarar sınırı ayrıca yönetilmelidir.
