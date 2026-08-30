# Binance Momentum Scanner V5.4 — Quality First + Path Tracking

V5.4, V5.3'ün çalışan Binance Futures + Telegram altyapısını ve Premium ALIM FIRSATI filtresini korur. Amaç sinyal sayısını artırmak değil; daha az gürültü, daha gerçekçi işlem-yolu ölçümü ve sonraki optimizasyon için daha kaliteli veri toplamaktır.

## V5.4'te ne değişti?

- **Premium filtre değiştirilmedi.** 17 canlı V5.3 Premium örneği hâlâ küçük bir örneklem olduğu için CYS/ZRO gibi birkaç kötü vakaya aşırı uyum yapılmadı.
- **Erken radar iki katmana ayrıldı.** İç radar hareketi 1/3 aşamasında DB'ye kaydeder; Telegram uyarısı ise varsayılan olarak ancak **2/3 süreklilik + daha sıkı erken kalite filtresi** sonrasında gider.
- **Erken radarların sonuçları artık ölçülüyor.** `radar_signals` ve `radar_outcomes` tabloları sayesinde Premium'a dönüşmeyen erken uyarıların da 1/3/5/15/30/60 dk MFE/MAE performansı görülebilir.
- **İşlem yolu ölçümü eklendi.** `signal_paths` tablosu alım bölgesi temasını, TP1/TP2/geçersizlik zamanlarını ve hangi olayın önce gerçekleştiğini kaydeder.
- **Alım bölgesi gelmeden hedefe kaçan hareket ayrı sayılır.** Böylece yalnızca “bir ara +%2 gördü” diye gerçekçi olmayan başarı yazılmaz.
- **TP1'e kadar ters hareket ölçülür.** Giriş kalitesini MFE'den bağımsız değerlendirmek mümkün olur.
- **`/stats` genişletildi.** Yeni V5.4 kayıtlarında alım bölgesi teması, TP1'in geçersizlikten önce gelme oranı, TP2 ve TP1'e kadar ters hareket gösterilir.
- **`/radarstats` eklendi.** Erken radarların gerçek 60 dk performansını gösterir.

## Erken uyarı mantığı

İç radarın varsayılan eşiği V5.3'e yakın tutulur; araştırma verisi kaybolmaz. Telegram'a giden erken uyarı ise 2/3 süreklilik ister ve ayrıca varsayılan olarak skor 70+, 30/60 sn pozitif ivme, yeterli flow, dengeli aggressive-buy, aşırı uzamamış 5 dk hareketi ve breakout/relatif güç koşullarını arar.

Bu uyarı **alım emri değildir**. Premium 3/3 teyit ve işlem kalitesi filtresi ayrı çalışmaya devam eder.

## Komutlar

- `/status`
- `/top`
- `/gainers`
- `/funnel`
- `/stats`
- `/radarstats`
- `/analiz TUT`
- `TUT`
- `/test`

## Railway

Mevcut değişkenler yeterlidir:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Yeni V5.4 eşikleri environment variable ile değiştirilebilir; ilk canlı örnekler toplanana kadar varsayılanları değiştirmemek daha sağlıklıdır.

> Bu yazılım otomatik emir vermez. Sinyaller ve seviyeler kural tabanlı piyasa araştırmasıdır; zarar etmeme veya kâr garantisi vermez. Pozisyon büyüklüğü, kaldıraç, stop ve toplam portföy riski ayrıca yönetilmelidir.
