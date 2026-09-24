# Early Step Lock V1 — SHADOW

Forward-only araştırma stratejisidir. **Emir açmaz, Telegram bildirimi göndermez ve production Early/Premium eşiklerini değiştirmez.**

## Kural

- Giriş: public Early bildiriminin sinyal fiyatı.
- Başlangıç stopu: **-%2,00**.
- Pozisyonun tamamı tek parçadır; scale-out/parçalı satış yoktur.
- +%0,20 görüldüğünde kâr kilidi +%0,20'ye çıkar.
- Sonraki kilit +%0,50'dir.
- Ardından +%0,75, +%1,00, +%1,25 ... +%4,75 şeklinde her +%0,25 basamakta kilit yukarı taşınır.
- Kilit yalnız yukarı gider; asla gevşetilmez.
- Final hedef **+%5,00**; ulaşıldığında tam pozisyon kapanmış sayılır.
- Araştırma maliyeti toplam round-trip **%0,14** olarak kaydedilir.
- Stop gap'inde gözlenen ilk aggTrade fiyatı kaydedilir; final +%5 hedefi resting target proxy olarak tam +%5 modellenir.

## Veri kalitesi

Her public Early için early_step_lock_shadow_v1 satırı ve early_step_lock_events_v1 olayları tutulur.
AggTrade ID boşluğu, >2 sn gözlem boşluğu, stale/future veya sıra dışı trade ayrı data_flags olarak işaretlenir.
Bu bayraklı kayıtlar kârlılık değerlendirmesinde ayrı tutulmalıdır; favorable ordering uygulanmaz.

## Güvenlik

Bu modül Binance signed endpoint çağırmaz, AutoTrade ile bağlantılı değildir ve Telegram'a shadow mesajı göndermez.
STEP_LOCK_SHADOW_ENABLED=0 ile koleksiyon kapatılabilir; varsayılan açıktır.
