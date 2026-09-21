# Premium / runner araştırma sözleşmesi — 2026-09-21 v1

Amaç daha fazla sinyal değil: güçlü hareketi daha erken seçmek, geç/tepe girişini azaltmak ve masraf sonrası net beklenen değeri artırmak.

## Kaynak ve karar

Kaynak DB SHA256: `e9f1cac685c30e0b7c60c3137124a69b740c548480a137bc10ef122daab771e9`.
374 Premium; zamanında 60m sonucu olan 362: GOOD64 / BAD169 / gri129. TP1/INVALIDATION giriş-teması proxy kohortu341. TARGET_BEFORE_ENTRY32 ve sonuçsuz1 ayrı. Bunlar gerçek exchange fill veya hesap P/L'si değildir.

90.746 eski runner ham snapshot'ında trade/book event timestamp, OI source timestamp, rank source timestamp ve feature-ready timestamp alanları yok. Snapshot yaşı, feature'ın gerçek kaynak zamanı yerine geçirilmez. Yeni strict model eğitimi bu nedenle engellidir. Önceki araştırmanın runner core/compression adayı korunur, fakat kaynak denetiminden geçmiş yeni fit olduğu iddia edilmez.

Üç kayıt adı ayrıdır: `PREMIUM_V2_SHADOW_SCORE`, `RUNNER_OBSERVED_12H10_PROB_T0`, `PREMIUM_STOP3_RISK_T60`. Bu sürüm doğrulanmış fit bulunmadığı için `ABSTAIN_NO_PROVENANCE_VALIDATED_FIT`, null probability ve neden yazar. Deploy edilen bir tahmin modeli değildir. STOP3 metadata etiketi `60m MAE<=-3 research label`; gerçek stop tahmini veya otomatik exit değildir.

## Yeniden üretim

Araştırma bağımlılıkları `research/requirements.txt`. Çalıştırma:

```text
python research/reweight_backtest.py analysis.db --matrix labels_features.csv --out results
```

Matrix, kanıt paketindeki hash'i doğrulanmış analiz tablosudur; script kimliğini audit.json'a yazar. Ham DB read-only/immutable açılır. Matrix bir ölçüm girdisidir; üçüncü taraf tahmini veya sonuç sütunu predictor olamaz. `--provenance` JSONL yalnız doğrulanmış per-feature value/source_ms/available_ms/max_age_ms/trusted kayıtlarını kabul eder. Sadece `trusted=true` yazmak doğrulama yerine geçmez; kaynak üreticisi ve hash ayrıca incelenmelidir.

İlk iki takvim günü eğitim başlangıcı, sonraki günler ileri test. Test günü başından önce olgunlaşmış label şartı, episode/wave purge, ayrı symbol-out. Ön işlemler train içi Pipeline. Eksik güvenilir kaynak değerleri tüm veri medyanıyla doldurulmaz. Model feature ailesi sessizce değiştirilmez. Exact top25 coverage bağlarda kesirli ağırlıkla ölçülür; bu geriye dönük sıralama ölçümüdür, uygulanabilir canlı eşik değildir. Train'de sabitlenen eşik seçimi ayrıca OOF dosyasında tutulur. Raw score olasılık olmadığından Brier N/A; Brier yalnız train-calibrated aday içindir.

Net proxy: giriş temas fiyatı → kayıtlı ilk TP1/stop seviyesi, eksi varsayılan %0,14 roundtrip maliyet ve açık funding varsayımı. Gap/slippage/fill gerçekliği kanıtlanmış değildir. %0,10–0,30 maliyet ve %0–0,03 funding duyarlılığı raporlanır. Gerçek funding eksiktir. `execution_counterfactual.py` tam bid/ask yoluyla ayrı senaryoları çalıştırabilir; mevcut exportta bu yol yoktur. Sonuç üretilmez. Listing age eksiktir; book imbalance gerçek depth bid_ratio yerine kullanılmaz.

FAST/REACQUIRE önce aynı parent'a bağlanır, kind başına ilk watch tutulur. Paired örneklemde FAST ALLOW yoktur; policy üstünlüğü belirlenemez. Anchor/karar zamanı farkı ve tekrar seçimi nedeniyle tablo nedensel değildir. FAST veya REACQUIRE eşikleri değişmez.

## Yeni telemetry

Her bildirilen Early ve Premium için T0/+15/+30/+60/+90/+180, fresh olmasa bile missing/gap kaydı. T0 yeni gözlem anıdır; eski Early radar oluşum anıyla karıştırılmaz. İlk15dk1s, sonra12h1m trade OHLC; bid/ask bucket sonundaki gözlemdir, sürekli executable quote yolu değildir. Eşikler ±0,5/±1/−3/+6/+8/+10/+15 için ilk gözlenen temas zamanı. Downtime teması icat edilmez.

Online wave_key episode kimliğidir; analizde 12h chained symbol grubu yeniden kurulur. Listing age, gerçek depth ve birden fazla kaynağa bağlı feature provenance eksikleri açık kalır. Veri seti %99 fiyat kapsamına erişmeden ekonomik promotion yapılamaz.

Tick callback sadece RAM; SQLite batch flush event-loop dışı. Buffer bounded; overflow açık gap ve log. Batch transaction başarısızsa aynı batch yeniden denenir. Snapshot/touch unique key'leri ve INSERT OR IGNORE retry'ı idempotent yapar. Restart, saklanan cohort'ları ve üretimde kaydolmuş ama flush edilmemiş sinyalleri geri alır; T0'ı geçmişten taze göstermez. Önceki boot downtime'ı gap'tir. Migration yalnız yedi yeni quality_shadow tablosu oluşturur.

## Önceden sabit kabul kapıları

Doğrulanmış model/sürüm dondurulduktan sonra en az28 yeni bağımsız takvim günü; 7/14.gün yalnız veri kalitesi kontrolü. Bugünkü abstention telemetry başlangıcı model doğrulama başlangıcı sayılmaz. Model/eşik değişirse pencere yeniden başlar. Rejim çeşitliliği yoksa42–56gün. Hedef en az100 GOOD,200 BAD,200 observed12h10 ve yeterli bağımsız wave.

- Snapshot kapsamı≥%95; uzun fiyat kapsamı≥%99; kaynak zamanları denetlenebilir; episode/wave overlap0.
- Premium: BAD≥%20 göreli azalma, GOOD capture≥%80; gün ve wave %95 alt sınır fayda>0; gri/coverage ayrı.
- Runner: top25 lift≥1,5; kümeli %95 alt sınır>1,2; mevcut score'a aynı kapsamda üstünlük, çoğu gün aynı yön; train-frozen eşikte de fayda.
- Kalibrasyon: ECE≤0,05, geçmiş prevalans bazına karşı Brier≥%5 iyileşme; kalibrasyon fit/test ayrı.
- Stop3: gerçekten −3 temasından önce uygulanabilir uyarı; recall≥%60, precision≥%50, GOOD kesme maliyeti dahil net fayda alt sınırı>0. −0,5/−1 touch verisi yetersizken production kuralı yok.
- Symbol-out/rejim faydası; başarının missingness veya gözlem yoğunluğundan gelmemesi; ücret/slippage/funding sonrası net proxy üstünlüğü.

Bu PR hiçbir production threshold, notification koşulu, TP/SL, sizing, position-management veya score davranışını değiştirmez. Startup OFF korunur. Shadow modeller otomatik promotion veya exit tüketicisine bağlanmaz.
