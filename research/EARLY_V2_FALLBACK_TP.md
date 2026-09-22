# Early V2 fallback TP — kontrollü pilot

22 Eylül 2026. Uygulama tabanı: `4cbbda39ec1bb3d1bbbf6e79ccfc21d5d2df93b2`.

Teslim edilen ZIP patch'i bu tabana temiz uygulanır. Manifestteki içerik dosyalarının SHA-256 değerleri doğrulandı (manifestin kendi kendini kapsayan hash alanı hariç). Kayıtlı yerel proje eski `51ab369` sürümündeydi; geliştirme güncel GitHub tabanının ayrı kopyasında yapıldı.

## Koruma davranışı

- Early LIVE + Profit Lock OFF/SHADOW: native SL ile birlikte `SELL TAKE_PROFIT_MARKET`, `reduceOnly=true`, `positionSide=BOTH`, `workingType=CONTRACT_PRICE`. Miktar yalnız gerçekleşmiş Early miktarıdır. Fallback TP = gerçek Binance fill VWAP × (1 + yüzde/100), tick'e yukarı yuvarlanır.
- Varsayılan yüzde **1,0**; kabul edilen aralık **0,3–3,0**. Kaldıraçsız fiyat yüzdesidir, net kâr garantisi değildir. Yeni ayar yalnız yeni pozisyonlara uygulanır; açık işlemin kayıtlı hedefi değişmez.
- Fallback hedefi de minimum RR fiyat tavanına dahildir. Dar hedefi karşılamayan sinyal fiyatı kovalanmaz ve emir açılmaz. Miktar minimum lot/notional için büyütülmez.
- TP niyeti ve client ID POST öncesi kalıcıdır. Belirsiz yanıtta aynı ID sorgulanır, kör tekrar yoktur. Kısmi girişin yalnız dolan miktarı korunur; top-up yoktur.
- TP oluşturma/doğrulama başarısızlığında veya Profit Lock geçişinde belirsizlikte girişler kilitlenir. SL tutulur; borsa pozisyon sahipliği ve miktarı doğrulanırsa tek reduce-only acil kapanış denenir. Borsa erişilemiyorsa pozisyon kapalı varsayılmaz, manuel uzlaştırma gerekebilir.
- Profit Lock LIVE ayrı `EARLY_V2_PROFIT_LIVE_ALLOWED` bayrağına ve kullanıcı/sohbet bağlı ikinci onaya ihtiyaç duyar. Onay sonrasında geçiş bekler; stop koruması kontrol edilir, bot-owned sabit TP iptali GET ile teyit edilir, ancak sonra LIVE durumu ilan edilir. İptal belirsizse LIVE açılmaz.
- Restart, OFF, kill veya Profit Lock OFF/SHADOW'a dönüşte fallback yeniden sağlanır; yükseltilmiş stop aşağı indirilmez. Fallback hedefi artık yükseltilmiş stopun altındaysa eski hedef yukarı taşınmaz; sahipliği teyitli acil çıkış uygulanır.
- Pozisyon TP ya da SL ile kapanınca diğer bot-owned koruyucu emirler temizlenir, gerçek çıkış fill'leri/komisyon/funding ledger'a tahsis edilmeden sembol serbest bırakılmaz.
- Kill yeni girişleri durdurur; rutin olarak pozisyon kapatmaz. Koruma sağlanamaması ayrı acil çıkış koşuludur.

## Korunan kapsam

Early V1 ve Premium handler/risk/çıkış kodları korunur. Yalnız yeni bağlantılar çıkarıldığında tüm bot AST'si tabanla eşleşir. Aynı sembolde gerçek Early rezervasyonu/pozisyonu varken Premium ikinci giriş yerine `EARLY→PREMIUM_CONFIRMED` kaydeder. DRY Early gerçek Premium LIVE girişini engellemez.

Execution V2: fiyat tavanlı marketable LIMIT IOC; üç tavanın minimumu; no-chasing; partial fill koruması; varsayılan 0/en fazla 1 kontrollü retry; gerçek Binance fill VWAP tahsisi. Position Management V2 referans motoru, replay ve referans testleri korunmuştur.

Her başlangıçta Early **OFF**, Profit Lock **SHADOW**. Ortam bayrakları 1 olsa bile mod otomatik açılmaz. Production için yeni LIVE bayrakları açılmamalı; yoklukları varsayılan 0'dır. Premium başlangıç davranışı da değiştirilmedi: mevcut kod restart'ta kendi modunu OFF yapar.

## Telegram menüsü

`/earlyv2`: OFF/DRY/LIVE, tutar, kaldıraç, min V2 score, max pozisyon, risk, Execution V2, Profit Lock, pozisyonlar, günlük rapor, kill switch, fallback TP. Tutar/max pozisyon/fallback TP alt menülerinde doğrudan sayısal düğmeler vardır; komutla da ayarlanabilir. Yetkili chat ve kullanıcı ID birlikte doğrulanır. LIVE sırasında ayar değişimi reddedilir; önce OFF gerekir.

| Ayar | Varsayılan | Komut |
|---|---:|---|
| Tutar | 5 USDT | `/earlyv2 set margin 5` |
| Kaldıraç | 1x | `/earlyv2 set leverage 1` |
| Max açık Early | 1 | `/earlyv2 set max_positions 1` |
| Min V2 score | 90 | `/earlyv2 set min_score 90` |
| Günlük giriş denemesi | 3 | `/earlyv2 set daily_trades 3` |
| Günlük zarar/risk rezervi | 1 USDT | `/earlyv2 set daily_loss 1` |
| Cooldown | 900 sn | `/earlyv2 set cooldown_s 900` |
| Retry | 0 | `/earlyv2 set retry 0` |
| Slippage tavanı | %0,1 | `/earlyv2 set slippage_pct 0.1` |
| Min RR | 1 | `/earlyv2 set min_rr 1` |
| Fallback TP | %1 | `/earlyv2 set fallback_tp_pct 1` |

`EARLY_V2_FALLBACK_TP_PCT=1.0` ilk kurulum override'ıdır. Sayısal ortam override'ları her başlangıçta menüde kaydedilmiş ayarı ezebilir; kalıcı menü tercihleri için gereksiz sayısal override bırakılmamalıdır.

## İlk DRY → LIVE pilotu

1. `/status` ve `/earlyv2 status`: Early OFF, Profit Lock SHADOW, bekleyen onay/uzlaştırma hatası yok. Premium durumunu ayrıca oku; modunu otomatik değiştirme.
2. Yukarıdaki küçük pilot ayarlarını kontrol et. Fallback akışını simüle etmek için `/earlyv2 profitoff`, sonra `/earlyv2 dry`. DRY+SHADOW mevcut dinamik araştırma simülasyonudur; fallback LIVE davranışıyla eşit değildir.
3. Birkaç gerçek Early adayında V2 skoru, fiyat tavanı/RR, lot/notional reddi, mükerrer sembol koruması, `/earlyv2 positions` ve `/earlyv2 report` kayıtlarını incele. 5 USDT bazı sembollerde borsa minimumunu karşılamayabilir; bot miktarı otomatik artırmaz.
4. Gerçek borsa/testnet doğrulaması bu çalışma kapsamında yapılmadı. Bağımsız V2 değerlendirmesi ve operatör kontrolü tamamlanmadan canlıya hazır/kârlı sonucu çıkarma. LIVE için `EARLY_V2_LIVE_ALLOWED=1` ve `EARLY_V2_SCORE_VALIDATED=1` açık operatör kararıyla gerekir; bu bayraklar burada açılmadı.
5. Kullanıcı LIVE'a geçmeyi seçerse yeniden DRY'den `/earlyv2 live`, ardından 120 saniye içinde `/earlyv2 confirm KOD`. İlk gerçek dolumdan sonra miktar/VWAP ve native SL+TP'yi borsada kontrol et. Profit Lock OFF/SHADOW bırakılabilir; fallback etkindir.
6. Profit Lock LIVE ayrıca bağımsız bayrak, `/earlyv2 profitlive`, `/earlyv2 confirmprofit KOD` ve borsada sabit TP iptal teyidi ister. Bu çalışma onu aktive etmez.
7. Sorunda `/earlyv2 kill`. Belirsiz emir/çıkış çözülmeden yeniden LIVE açılmaz.

API şeması kontrolü: [Binance USD-M Futures Trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade). Bu çalışma gerçek Binance emri veya LIVE aktivasyonu göndermedi.

## Doğrulama

- Odaklı paket: **116/116** (62 pilot + 24 fallback/menü + 30 Position Management).
- Son tam koşu: pilot **371 test / 9 failure / 8 error / 8 skip**; değişmemiş taban **255 / 11 / 8 / 8**. Yeni failure/error kimliği yok. Hata listeleri `early_v2_test_comparison.json` içinde.
- Tam paket yeşil değildir: eski invariant hash'leri, export/snapshot beklentileri, SQLite kaynak-bayt değişmezliği ve startup çıktısının JSON ayrıştırması etkileniyor. Menü beklentisi güncellendi; export testindeki diğer azalma zamanlamaya bağlı olduğundan düzeltilmiş sorun sayılmadı.
- Windows geçici SQLite dosya kilitlerinin karşılaştırmayı bozmasını azaltmak için yalnız koşucuda cleanup öncesi `gc.collect()` kullanıldı. Aynı koşul tabana ve pilota uygulandı; üretim kodunun SQLite yaşam döngüsü değiştirilmedi.
- Derleme ve `git diff --check` geçti. Tüm bot AST karşılaştırması geçti. Referans Position Management motoru/replay/test kaynakları teslimatla aynıdır (Git satır-sonu normalizasyonu dışında).
- Gerçek Binance/demo/testnet emri çalıştırılmadı. Testler geçici SQLite ve sahte borsa kullanır. Yeni CI `Early V2 pilot safety` bu 116 testi ayrı çalıştırır.
