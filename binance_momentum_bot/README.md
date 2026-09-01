# Binance Momentum Scanner V5.7

V5.7'nin ana amacı daha fazla Premium üretmek değil, **Premium discovery ile gerçek giriş uygulanabilirliğini birbirinden ayırmak** ve bunu forward veride ölçmektir.

V5.6 `signals0109.db` analizinde yön bulma, işlem uygulanabilirliğinden daha güçlü görünmüştü: birçok sinyal daha sonra hedef bölgesini görürken önemli bir kısmı mevcut invalidation'a önce değiyordu. Bu yüzden V5.7 production Premium seçim mantığını yeniden optimize etmek yerine execution/phase ölçüm katmanını güçlendirir.

## Production'da bilinçli olarak değişmeyenler

- Premium kritik seçim fonksiyonları ve eşikleri
- 3×15 sn süreklilik yapısı
- Momentum / Entry Quality / Rise hesapları
- Premium breakout, flow, aggressive-buy, book ve candidate-runup guard'ları
- Tahmini giriş bölgesi, TP1, TP2 ve geçersizlik formülleri
- Early doğrudan AL sinyaline çevrilmez
- OI için yeni hard gate yok
- 15 sn breakout acceptance için hard gate yok
- Stop genişletilmez
- ML modeli production'a eklenmez

## V5.7'de eklenen ana katmanlar

### 1. Premium micro-execution ölçümü

Her Premium için ilk uygun aggTrade olayıyla şu horizonlarda snapshot saklanır:

`1s, 3s, 5s, 10s, 15s, 20s, 30s, 60s`

Kaydedilenler arasında last/bid/ask/mark, getiri, MFE/MAE, spread, 30/60 sn momentum, flow30, buy30, book imbalance ve BTC-relative bulunur.

### 2. Gerçek breakout reference + 15 sn acceptance Shadow

Premium anında önceki 15 **kapalı** 1 dakikalık mumun tepe referansı saklanır. İlk 15 saniyede:

- breakout üstünde geçirilen süre
- breakout altına penetrasyon
- reclaim sayısı / ilk reclaim zamanı
- sinyalden ve peak'ten maksimum pullback
- yapısal yeni tepe ilerlemesi

ölçülür ve `PASS / WARN / FAIL` olarak **Shadow** etiketlenir. Bu etiket Premium'u engellemez.

### 3. Phase / exhaustion Shadow

Sinyal anında lookahead olmadan şu context kaydedilir:

- 1m / 3m önceki tepe mesafesi
- 15m breakout mesafesi
- canlı 1m range/body/upper-wick
- episode yaşı
- episode low'dan mesafe
- episode peak'ten geri çekilme ve peak yaşı
- OI rejimi / OI ivmesi

Bunlardan `LOW / MEDIUM / HIGH` Phase Risk Shadow üretilir. Production gate değildir.

### 4. OI + funding/mark veri yolu

- OI son 5dk değişimi yanında önceki 5dk ve OI acceleration kaydedilir.
- `POS_GT_005 / NEUTRAL / NEG_LT_M005` OI araştırma rejimi eklenir.
- All-market mark-price stream ile mark price ve latest funding rate toplanır.
- Önceki V5.6 DB'de boş kalan funding alanı artık canlı veri varsa persist edilir.

### 5. Execution status

Premium mesajında artık sinyal fiyatı yanında canlı giriş kalitesi de gösterilir:

- live bid / ask
- signal → live ask drift
- giriş bandına mesafe
- TP1/TP2'ye kalan alan
- yeni stop riski ve live R/R
- `VALID / WAIT_RECLAIM / CHASED / INVALIDATED`

Bu katman kullanıcının geç kalmış bir sinyali kovalamamasına yardımcı olan execution bilgisidir; Premium detector'ın kendisini değiştirmez. Mesaj başlığı da buna göre ayrılır: yalnız `VALID` durumda `ALIM FIRSATI` başlığı kullanılır; `WAIT_RECLAIM / CHASED / INVALIDATED` durumları Premium momentum/setup olarak gösterilir. Bu sınıflandırma yine kural tabanlıdır, olasılık değildir.

### 6. Stop sonrası reclaim + TP2 sonrası runner Shadow

- SKYAI tipi `stop-first → güçlü reclaim` vakaları `RECLAIM_AFTER_STOP` olarak araştırılır.
- Stop otomatik genişletilmez ve ikinci gerçek AL emri üretilmez.
- Eski Shadow Exit'in erken kalma sorununa karşı yeni runner araştırması yalnız **TP2 görüldükten sonra** devreye girer ve adaptive pullback ile `RUNNER_EXIT` Shadow kaydeder.

### 7. Second-Wave research sıkılaştırıldı

Second-Wave artık yalnız eski episode var diye oluşmaz. Önce gerçek pullback, sonra reclaim/re-acceleration, yeterli flow/buy ve yakın prior peak şartı aranır. Hâlâ yalnız araştırma olayıdır.

### 8. Veri kalitesi / operasyon güvenliği

- Premium final teyidinde sembol aggTrade/book verisi açıkça stale ise trade-grade Premium üretimi bekletilir.
- ticker snapshot, 2 saniyeden daha taze aggTrade last price'ını ezmez.
- event time / receive time saklanır; receive lag ölçülebilir.
- `flow_eff60` hesaplamasındaki denominator, `flow30` yerine doğru biçimde `flow60` kullanır.
- Gerçek `premium_ordinal`, bütün Telegram bildirimlerini sayan `daily_notice_no`'dan ayrılır.
- notification log'a `signal_id`, send-start/send-done ms, Telegram message id, canlı bid/ask, drift ve entry status eklenir.

## Yeni DB tabloları

- `premium_context`
- `premium_micro_snapshots`
- `premium_entry_validation`

Mevcut DB'lere migration `ensure_column` üzerinden geriye uyumlu uygulanır.

## Telegram kanal katılım onayı

V5.7, istenirse join-request akışını da yönetebilir. Otomatik kabul YOKTUR.

Railway'e şu değişkenler eklenebilir:

- `TELEGRAM_APPROVAL_CHAT_ID`: onaya tabi kanal/grup ID'si. Bu yoksa özellik kapalıdır.
- `TELEGRAM_ADMIN_CHAT_ID`: opsiyonel ama önerilir; katılım taleplerinin geleceği **senin özel bot sohbetinin** ID'si. Boşsa mevcut `TELEGRAM_CHAT_ID` kullanılır.
- `TELEGRAM_ADMIN_USER_ID`: opsiyonel; yalnız bu Telegram user ID'si Kabul/Reddet butonlarını çalıştırabilsin.
- `JOIN_REQUEST_APPROVAL_ENABLED=1`: `TELEGRAM_APPROVAL_CHAT_ID` ayarlıysa varsayılan açık.

Bot hedef kanalda admin olmalı ve kullanıcı davet etme/onaylama (`can_invite_users`) yetkisine sahip olmalıdır.

Komutlar:

- `/joinstatus`
- `/joinlink` — join request gerektiren bot-owned davet linki oluşturur.

Bir join request gelince bot yönetici sohbetine `✅ KABUL ET / ❌ REDDET` butonları yollar. Kullanıcı ancak sen butona bastığında kabul/reddedilir.

## Yeni / güncellenen komutlar

`/status` `/top` `/gainers` `/funnel` `/stats` `/radarstats` `/shadowstats` `/entrystats` `/latencystats` `/researchstats` `/joinstatus` `/joinlink` `/analiz COIN` `/test`

`/stats` artık TP2-any ile **TP2-before-invalidation** ayrımını açıkça gösterir.

`/entrystats`, 15 sn acceptance gruplarını, OI/phase/execution cohortlarını, micro snapshot coverage'ını, reclaim ve runner Shadow sayılarını gösterir.

`/latencystats`, gerçek signal→Telegram send sürelerini ve 1–60 sn micro path özetini gösterir.

## Railway notu

`signals.db` hâlâ Railway ephemeral dosya sistemindeyse deploy/restart sırasında kaybolabilir. Kalıcı Volume kurulana kadar deploy öncesi DB yedeği almaya devam et.

## Araştırma disiplini

V5.7'nin yeni OI, phase, acceptance, reclaim ve runner özelliklerinin çoğu bilinçli olarak **Shadow** bırakılmıştır. Yeni forward örneklem oluşmadan:

- OI hard gate,
- 15 sn acceptance hard gate,
- stop genişletme,
- yüksek score = olasılık yorumu,
- production ML

yapılmamalıdır.
