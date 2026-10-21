# STEP Lock — seçilebilir teminat ve kaldıraç paneli

## Teslim kapsamı

Bu değişiklik, mevcut Telegram botuna ayrı **🪜 STEP Lock / `/step`** menüsü
ekler. Kullanıcı, teminat ve hedef kaldıracı seçip özeti onaylayarak **ayar
taslağı** kaydedebilir. Canlı başlatma düğmesi açıklama gösterir; bu sürümde
emir, borsa kaldıraç ayarı, duraklatma veya pozisyon kapatma çağrısı yoktur.

Kullanıcının 21 Ekim 2026 isteği: seçimler **200 USDT teminat** ve **10×
kaldıraç** değerlerine kadar çıkabilsin; başlatma kararı ayrı kalsın.
Bu dal, inceleme için hazırlanmıştır; üretime yüklenmemiştir.

## Boyut seçiminin anlamı

| Alan | Davranış |
| --- | --- |
| Teminat | 0'dan büyük, en fazla 200 USDT; en çok iki ondalık basamak |
| Hazır teminat seçenekleri | 5, 10, 25, 50, 100, 150, 200 USDT |
| Özel tutar | Örneğin `/step margin 12.50` veya `/step margin 12,50` |
| Hedef kaldıraç | 1–10 arasındaki bütün tam sayılar |
| Toplam hedef pozisyon | Teminat × kaldıraç; üst sınır 2.000 USDT |
| En büyük seçim | 200 USDT × 10× = 2.000 USDT toplam hedef pozisyon |

**25 USDT toplam pozisyon / en fazla 5× kaldıraç** olan 19 Ekim referans
profili ilk görünümde korunur. Bu profilde 25 USDT teminat değildir; 5× ise
borsada ölçülmüş gerçek kaldıraç değildir. Kullanıcı teminat veya kaldıraç
seçerek özel taslağa geçtiğinde diğer alan açıkça seçilene kadar boş kalır.
İki alan tamamlanmadan özel taslak kaydedilemez.

Referans profildeki aynı anda 1 işlem, UTC gününde 1 yeni giriş ve 1 USDT
gerçekleşmiş günlük net zarar eşiğinde yeni giriş kilidi, seçim büyüdüğünde
otomatik yükseltilmez. Bu eşik, gerçekleşebilecek kaybın garantili üst sınırı
olarak gösterilmez.

## Telefon akışı

1. **🪜 STEP Lock** menüsünü aç veya `/step` yaz.
2. **Teminat seç** ve **Kaldıraç seç** düğmeleriyle iki değeri belirle.
3. Paneldeki toplam hedef pozisyonu kontrol et.
4. **Taslağı kaydet** ile özeti aç; **Taslağı kaydet** onayı yalnız taslak
   dosyasına yazar. Onay 120 saniye geçerlidir ve tek kullanımlıktır.
5. **Başlatma özeti**, seçimin uygulanması için henüz bağlı olmayan canlı
   yönetim yolunu açıklar. Bu düğme işlem başlatmaz.

Yalnız yönetici sohbeti **ve** `TELEGRAM_ADMIN_USER_ID` ile eşleşen kullanıcı
erişebilir. Eski ekranların revizyonları ve başka oturuma ait onaylar kabul
edilmez. Yeniden başlatma, kayıtlı taslağı okuyabilir; işlem modu oluşturmaz.

## Uygulama sınırı ve kaynak eşleştirme

- Panel modülü: `binance_momentum_bot/step_lock_panel.py`.
- Menü ve yönlendirme: mevcut `telegram_ux.handle` ve `mode_callback` yolları;
  `bot.py` değiştirilmedi.
- Kaydetme: normal `DB_PATH` dosyasının yanında ayrı
  `step_lock_panel_drafts.sqlite3`; sohbet/kullanıcı kapsamında.
- Normal bot ayarları, normal veritabanı, STEP numeric/execution kabul
  dosyaları ve çalışan gateway/worker kodu değiştirilmez.
- Panel, normal hesabın bakiye önbelleğini STEP bakiyesi gibi kullanmaz.
  STEP canlı verisi bağlı olmadığı için bakiye, pozisyon, stop ve canlı veri
  zamanı bilinmiyor olarak gösterilir.

İnceleme tabanı, mevcut normal panelin kaynak referansı olan
`17864116d51483d8bbec23d34376fe837cf1ff4b` commit'idir. 18 Ekim'de üretime
eklenmiş Premium sinyal relay bağlantısının tam üretim paketiyle eşleştirilmesi,
gelecekte yapılacak herhangi bir dağıtımın ayrı gereğidir. Bu dal tek başına
çalışan bütün servislerin birebir üretim paketi veya canlı kabulü değildir.

Çalışan ayrı STEP kaynağında canlı yetki koşulsuz kapalıdır; yönetici için
arm/pause/close API'si ve borsada kaldıraç değiştirme yolu bulunmaz. Bu panel
bunları varmış gibi sunmaz veya mevcut guard'ları kaldırmaz. Gelecekte canlı
yönetim bağlanacaksa yeni limit profili, ayrı hesap kapsamı, gerçek sembol
kaldıracı, miktar/fiyat adımları, minimum tutar ve koruyucu stop yolu birlikte
doğrulanmalıdır. Seçilen tavanı aşmak için miktar sessizce büyütülmemelidir.
Yeni girişleri duraklatmak, mevcut pozisyonun koruma/çıkış yönetimini kesen
servis durdurma işlemiyle aynı anlama gelmez.

## Doğrulama

Odaklı testler gerçek Telegram, Binance veya Railway bağlantısı açmaz.
Geçici dosyalar ve sahte taşıyıcılarla sınırlar, aritmetik, ilk profil,
eksik seçim, sohbet/kullanıcı yetkisi, onay süresi/tek kullanım, eski ekran,
yeniden başlatma, bozuk depo, gönderim hataları ve normal bot ayrımı sınanır.

Son yerel sonuç: **27 panel testi geçti**. Mevcut menü ve seçilmiş normal
bot kontrolleriyle toplam **36 testin 35'i geçti**; kalan tek hata aşağıda
belgelenen ve değişmemiş tabanda da tekrar edilen referans hash uyuşmazlığıdır.

```bash
PYTHONPATH=binance_momentum_bot:tests PYTHON_DOTENV_DISABLED=1 \
python -m unittest -v test_step_lock_panel
```

Mevcut Telegram menüsü ve normal emir/risk fonksiyonları ayrıca kontrol
edildi. `test_telegram_ux.MenuTests.test_production_and_startup_research_baselines_unchanged`
testi, bu değişiklikten önceki tabanda da aynı AST hash uyuşmazlığını verir;
beklenen değer `f914273b…`, gerçek değer `7f31b398…` şeklindedir. Bu eski
referans kaydı değiştirilmedi veya test kaldırılmadı. `bot.py` kaynak dosyası
bu dalda tabanla aynıdır; normal ekonomik fonksiyonların ayrı hash testi geçer.
