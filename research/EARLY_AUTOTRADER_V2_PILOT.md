# Early AutoTrader V2 — kontrollü pilot

22 Eylül 2026. Taban: `4cbbda39ec1bb3d1bbbf6e79ccfc21d5d2df93b2`.
Kontrollü pilot + fallback TP revizyonu. Güncel doğrulama ve dağıtım bilgileri için EARLY_V2_FALLBACK_TP.md belgesine bakın. LIVE aktivasyonu otomatik yapılmaz.

## Modüller ve korunmuş davranış

- `early_autotrader_v2.py`: yalnız Early durum makinesi, ayrı risk/ayar/işlem tabloları, kalıcı emir niyetleri, çift onay ve kill kilidi.
- `execution_v2.py`: fiyat tavanları, LIMIT IOC ve fill tahsisi/gerçek VWAP doğrulaması.
- `early_v2_adapter.py`: Binance, Telegram ve mevcut tarayıcıyla dar bağlantı.
- `position_management_v2.py`: tamamlanan SHADOW modülünün değiştirilmemiş kopyası.
- `research/profit_lock_shadow.py` ve referans testleri: değiştirilmemiş SHADOW replay aracı.

Early V1 eşikleri, sinyal koşulları, mesajları ve Premium emir/risk/çıkış fonksiyonları değiştirilmedi. Botun mevcut koduna yalnız başlangıç, Early aday kuyruğu, tick, Telegram ve Premium çakışma bağlantıları eklendi. Bu bağlantıların tam AST biçimleri testte tek tek tanınır; çıkarılınca bütün bot AST özeti taban commit ile eşleşir. Premium handler aynı kalır; çağrı sınırındaki ortak kilit aynı sembolde ikinci giriş yarışını önler.

SQLite tabloları: `early_v2_settings`, `early_v2_trades`, `early_v2_events`. Premium tabloları/ayarları bu modül tarafından yazılmaz. Mevcut veritabanı bağlantısı kullanılır; işlem kaynakları ve risk bütçeleri ayrıdır.

## Başlangıç ve bayraklar

Her süreç başlangıcında Early modu **OFF**, Profit Lock **SHADOW** olur. Kaydedilmiş LIVE/DRY otomatik geri yüklenmez. Aktif pozisyonlar ve bekleyen emir kimlikleri tekrar okunur; OFF olsa bile mevcut LIVE pozisyonların koruması/uzlaştırması sürer. Kill kilidi kalıcıdır.

| Ayar | Varsayılan |
|---|---:|
| Early margin | 5 USDT |
| Kaldıraç | 1x |
| Min V2 skor | 90 |
| Eşzamanlı pozisyon | 1 |
| Günlük giriş denemesi | 3 |
| Günlük zarar + açık risk rezervi | 1 USDT |
| Cooldown | 900 saniye |
| Sinyal yaş sınırı | 3 saniye |
| Slippage tavanı | %0,10 |
| Minimum RR | 1 |
| Retry | 0; ayarlanabilir üst sınır 1 |
| İlk kâr kilidi | %0,25; %0,20 varyantı ayarlanabilir |
| Fallback TP | %1; %0,3–%3 arası ayarlanabilir |

Feature flag'ler varsayılan kapalıdır:

```text
EARLY_V2_LIVE_ALLOWED=0
EARLY_V2_SCORE_VALIDATED=0
EARLY_V2_PROFIT_LIVE_ALLOWED=0
```

Sayısal ilk kurulum/deploy override'ları: `EARLY_V2_MIN_SCORE`, `EARLY_V2_MARGIN`, `EARLY_V2_LEVERAGE`, `EARLY_V2_DAILY_LOSS`, `EARLY_V2_DAILY_TRADES`, `EARLY_V2_RETRY`, `EARLY_V2_MAX_POSITIONS`, `EARLY_V2_FALLBACK_TP_PCT`. Telegram ayarları SQLite'da kalır; mevcutsa bu çevre değişkenleri başlangıçta üzerine uygulanır. Geçersiz değerler sessizce kabul edilmez.

Bu çalışma bağımsız, doğrulanmış bir Early V2 modeli bulmadı. Mevcut `ignition_shadow_score` / `FAST_EARLY_V2` araştırma puanı açıkça etiketlenerek kullanıldı; yeni bir model icat edilmedi. Yalnız V1 Early bildirim koşullarını geçen, V2 etiketi FAST_EARLY_V2 olan ve ayarlanmış puan eşiğini geçen adaylar değerlendirilir. `score_gate=false` filtresiz alım açmaz; tüm Early girişlerini durdurur. `execution=false` da girişleri durdurur. Eşik aralığı 85–100; varsayılan 90.

V2 doğrulama bayrağı kapalıyken LIVE açılamaz. `EARLY_V2_SCORE_VALIDATED=1` bir performans kanıtı değildir; ancak bağımsız değerlendirme sonrası operatörün vereceği onayın teknik kilididir. Önceki SHADOW raporunda 1.778 Early / 3.556 kapsam kaydı için gerçek fill/ham yol eksik ve ekonomik karşılaştırmaya uygun çift sayısı sıfırdı. Bu nedenle başarı veya kârlılık iddiası yoktur.

## Execution V2

LONG / USDT / one-way pilotu. Hedge mode veya mevcut sembol pozisyonu/emri varsa giriş reddedilir. Yalnız boş sembolde isolated margin ve Early kaldıraç ayarlanır. Hesap genelinde mod değiştirilmez.

```text
RR_CAP = min((target1 + min_RR × initial_SL) / (1 + min_RR),
             min_RR × initial_SL / (min_RR - fallback_tp_pct/100))
MAX_ENTRY_PRICE = floor_to_tick(min(signal_price × (1+slippage_pct/100), entry_high, RR_CAP))
IOC_LIMIT = ceil_to_tick(fresh_best_ask)
IOC_LIMIT <= MAX_ENTRY_PRICE
QTY = floor_to_LOT_SIZE(margin × leverage / MAX_ENTRY_PRICE)
```

PRICE_FILTER, LIMIT için LOT_SIZE ve MIN_NOTIONAL uygulanır. Minimum lotu tutturmak için miktar yukarı yuvarlanmaz. Book 1,5 saniyeden eskiyse, sinyal süresi geçmişse, fiyat tavanı aşılmışsa veya SL altına inilmişse işlem açılmaz. Fiyatın geri gelmesini bekleyen kuyruk/repricing yoktur.

Emir tipi **BUY LIMIT IOC**. Kısmi fill politikası **KEEP_PROTECT_NO_TOPUP**: gerçekleşen pozitif miktar tutulur, önce o miktara reduce-only STOP_MARKET yerleştirilir, kalan miktar tekrar alınmaz. Varsayılan retry sıfırdır. Bir retry etkinse yalnız terminal ve sıfır-fill teyidi sonrası; aynı sinyal süresi içinde, aynı veya daha düşük limit fiyatıyla yapılabilir. Timeout/kimliği belirsiz cevap sonrası yeni alış POST'u yoktur.

Niyet ve benzersiz client ID gönderimden önce SQLite'a yazılır. Restart'ta aynı ID sorgulanır. Binance `userTrades` tam emir kimliğine göre tahsis edilir, duplicate/conflict ve toplam miktar kontrol edilir. `avgPrice`, sinyal fiyatı veya tahmini miktar gerçek VWAP yerine kullanılmaz. Eksik fill veya 1000 satır sınırına ulaşmış yanıt fail-closed kalır.

Koruyucu ilk stop kurulamazsa sahipliği ve miktarı doğrulanmış Early pozisyonu için tek bir **SELL MARKET reduce-only** acil kapama denenir. MARKET giriş alternatifi değildir. Kapanış ID'si de kalıcıdır; belirsiz cevapta kör tekrar yapılmaz. Borsa erişilemiyorsa kapandığı varsayılmaz; sembol rezervasyonu ve kill kilidi kalır.

## Profit Lock

Mevcut mutlak başlangıç SL korunur. Ölçüm gerçek Binance fill VWAP'ına dayanır; DRY açıkça `DRY_ASK_PROXY` olarak etiketlenir.

- En az 3 farklı aggregate trade ve 200 ms teyit.
- Teyitli +%0,50 → +%0,25 kilit; configurable +%0,20.
- Teyitli +%0,80 → +%0,50 kilit.
- +%1,20 sonrası adaptive gap: `max(%0,40, teyitli tepe / 3)`; kilit en az +%0,50.
- Stop yalnız yukarı gider. Tek wick, tekrar trade ID, eksik/sırasız/eski tick teyit oluşturmaz.
- Yeni exchange stop teyit edildikten sonra eski bot-owned stop iptal edilir. Account-wide cancel/close yoktur.
- Restart'ta son stop tabanı korunur, teyit penceresi yeniden birikir. Profit Lock LIVE otomatik devam etmez.

SHADOW gerçek stopu yükseltmez. **Early LIVE + Profit Lock SHADOW/OFF durumunda başlangıç SL yanında gerçek Binance fill VWAP üzerinden hesaplanan, reduce-only TAKE_PROFIT_MARKET fallback TP bulunur. Varsayılan +%1, ayarlanabilir aralık %0,3–%3.** DRY+OFF yapılandırılan fallback TP çıkışını simüle eder; DRY+SHADOW dinamik araştırma simülasyonunu korur ve gerçek fallback yürütmesiyle eşit değildir. Profit Lock LIVE ikinci onaydan sonra önce mevcut korumayı doğrular, sabit TP iptalini borsadan teyit eder, ardından LIVE olur. Belirsizlikte girişler kilitlenir; yalnız sahipliği teyitli Early miktarı için tek reduce-only acil kapanış denenir.

SHADOW motoru geri dönüşte de kısa teyit uygular; borsadaki native STOP_MARKET tetiklendikten sonra aynı teyidi bekleyemez. Dolayısıyla gerçek LIVE sonuçları replay ile eşit varsayılmaz. Lock yüzdeleri ücret/funding öncesi, kaldıraçsız fiyat yüzdeleridir.

## Risk ve Premium çakışması

Early risk rezervi = gerçekleşmiş zararların toplamı + açık Early riskleri + yeni adayın SL riski ve ücret rezervi. Kazançlar günlük zarar bütçesini yeniden doldurmaz. Gün İstanbul saatine göre değişir; cooldown son giriş denemesi veya kapanıştan başlar. Limit giriş denemelerini sayar; hiç dolmayan IOC de tüketir. Gerçekleşen fill VWAP, komisyon ve funding ledger'da ayrı tutulur.

Early LIVE rezervasyonu/pozisyonu varken aynı sembole Premium gelirse yalnız `EARLY→PREMIUM_CONFIRMED` ve Premium sinyal ID'si yazılır; miktar, kaldıraç, bütçe veya stop profili Premium'a dönüştürülmez. DRY Early gerçek Premium LIVE girişini bastırmaz. Gerçek Early/Premium girişleri aynı kilitten geçer.

Manuel miktar değişikliği veya tahsis edilemeyen çıkışta otomatik sahiplik tahmini yapılmaz. Sembol rezervasyonu korunur ve yeni Early girişleri kilitlenir. Manuel kapama sonucu bilinen bot çıkış emirlerine tahsis edilemiyorsa ledger manuel inceleme bekler. Funding/komisyon USDT dışındaysa otomatik dönüştürme uydurulmaz.

Günlük limit bir risk rezervidir; gap, stop slippage, likidite veya borsa kesintisine karşı mutlak zarar garantisi değildir. Pilot tek bot süreci için tasarlanmıştır; aynı hesabı ve SQLite'ı kullanan paralel bot örnekleri etkinleştirilmemelidir.

## Telegram kullanımı ve sonraki onaylı adımlar

`/earlyv2` ayrı menüyü açar: Mod, Tutar, Kaldıraç, Min V2 Score, Max Pozisyon, Risk Ayarları, Execution V2, Profit Lock, Pozisyonlar, Günlük Rapor, Kill Switch, Fallback TP. Ana menüde Premium ve Early ayrı; `/status` ikisini ayrı gösterir. Kontroller için hem admin chat hem TELEGRAM_ADMIN_USER_ID eşleşmesi gerekir.

Kullanıcı bu revizyon için PR/merge/deploy yetkisi verdi; bu yetki LIVE aktivasyonunu kapsamaz. Onaylı kurulumdan sonra başlangıç OFF kontrol edilir; ilk değerlendirme açık kullanıcı seçimiyle `/earlyv2 dry` olur.

```text
/earlyv2 set margin 5
/earlyv2 set leverage 1
/earlyv2 set min_score 90
/earlyv2 set max_positions 1
/earlyv2 set daily_trades 3
/earlyv2 set daily_loss 1
/earlyv2 set cooldown_s 900
/earlyv2 set retry 0
/earlyv2 set fallback_tp_pct 1
/earlyv2 profitshadow
/earlyv2 dry
/earlyv2 positions
/earlyv2 report
```

LIVE için bağımsız V2 değerlendirmesi, demo/testnet emir–stop–reconciliation doğrulaması ve açık kullanıcı onayı gerekir. Bayrakların bilinçli açılmasından sonra DRY modundayken `/earlyv2 live` yalnız 120 saniyelik kullanıcı/sohbete bağlı tek kullanımlık kod üretir; `/earlyv2 confirm KOD` ikinci onaydır. Ayar değişikliği veya OFF/kill kodu geçersiz kılar.

Profit Lock LIVE ayrıca `EARLY_V2_PROFIT_LIVE_ALLOWED=1`, Early LIVE, `/earlyv2 profitlive` ve `/earlyv2 confirmprofit KOD` gerektirir. Çevre bayrağı tek başına işlem modunu açmaz.

`/earlyv2 kill`: yeni girişleri hemen durdurur, bekleyen onayları iptal eder, Profit Lock'u SHADOW'a alır. Mevcut koruyucu borsa stoplarını iptal etmez ve pozisyonları körlemesine kapatmaz. Gönderilmiş IOC/fill'ler uzlaştırılmaya devam eder. Belirsiz kayıt varken DRY ile kill kilidi kaldırılamaz. Tüm kayıtlar uzlaştırıldıktan sonra kullanıcı açıkça DRY'a geçebilir; LIVE yeniden ikinci onay gerektirir.

## Test ve inceleme

`python -m unittest discover -s tests -p test_early_autotrader_v2.py -v`

`python -m unittest discover -s tests -p test_position_management_v2.py -v`

Testler geçici SQLite ve sahte Binance/Telegram kullanır; `bot.main` başlatılmaz. Genel sonuçlar teslimdeki `DEGISIKLIK_VE_TEST_OZETI.md` ve validation kayıtlarında bulunur. Gerçek hesap/testnet emri bu çalışma sırasında gönderilmedi.

Mevcut değişmezlik hash'leri yenisiyle değiştirilmedi. `tests/early_v2_compat.py` sadece birebir eşleşen yeni bağlantıları çıkarır. Ayrıca bütün bot AST'si 4cbbda3'e karşı ayrı doğrulanır. Güncellenen menü testi yeni iki girişi ve zaten mevcut `/todaypositions` satırını kapsar.

Resmî API referansı (22 Eylül 2026 kontrol edildi):
[Binance USDⓈ-M Futures Trade API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade)

Referans SHADOW raporu: SONUCLAR.md, 22 Eylül 2026; 30/30 birim testi, gerçek tarihsel ekonomik karşılaştırma için 0 uygun çift. Referans modülü kaynak pakete aynen alındı.
