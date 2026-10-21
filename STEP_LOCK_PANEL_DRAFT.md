# STEP Lock — boyut seçimi ve başlatma / durdurma paneli

## Teslim durumu

21 Ekim 2026 tarihli kullanıcı onayıyla, ayrı **🪜 STEP Lock / /step**
paneline durumla değişen başlatma, duraklatma, devam etme ve kapatma kontrolleri
eklendi. Telegram paneli, imzalı yönetim istemcisi ve servis kontrol durum
makinesi uygulanmıştır. Yönetim bağlantısı yapılandırılmadığında düğmeler
kilitlidir; gerçek servis yanıtı olmadan LIVE veya kapanış başarısı gösterilmez.

**Bu inceleme dalı üretime yüklenmedi ve canlı işlem başlatılmadı.**
Mevcut ayrı STEP çalıştırıcısındaki koşulsuz FINAL_LIVE_ARM_OFF ve onaylı
sayısal sınırlar korunur. Özel servis uyarlaması ayrı bir aday pakettedir;
hesap ve altyapı kimlikleri içeren özel kaynaklar bu genel depoya eklenmez.

## Telefon akışı

1. **🪜 STEP Lock** menüsünü aç veya **/step** yaz.
2. **Teminat seç** ve **Kaldıraç seç** ile seçimini yap; **Taslağı kaydet**
   üzerinden özeti onayla. Bu işlem emir veya kaldıraç değişikliği göndermez.
3. Yönetim bağlantısı ve çalışma yetkisi doğrulandığında, panel aşağıdaki
   işlemleri servis durumuna göre sunar.

| Doğrulanan durum | Düğme | Sonuç |
| --- | --- | --- |
| OFF; pozisyon ve bekleyen STEP emri yok | ▶️ Canlı başlat | Kayıtlı profil ve kapsam için ayrı onay açar; servis kabul ederse LIVE |
| LIVE | ⏸ Yeni girişleri duraklat | Yeni sinyal kabulünü kapatır; mevcut pozisyonun koruma ve çıkış döngüsü sürer |
| PAUSED | ▶️ Yeni girişlere devam et | Ayrı onaydan sonra giriş kabulünü açar; açık işlem varken profil değiştirilemez |
| LIVE veya PAUSED | ⛔ Pozisyonu kapat ve STEP’i durdur | Geçerli STEP işlem kimliğine bağlı kapatma onayı açar |
| CLOSING | ⏳ Kapanış doğrulanıyor | Yeni girişler kapalı kalır; kapanış tamamlanmış gibi gösterilmez |
| ERROR / bağlantı doğrulanamadı | Durum ve hata açıklaması | Başlatma/kapama başarısı varsayılmaz; kullanıcı durumu yeniler |

Duraklatma, daha önce kabul edilmiş bir LIMIT giriş emrini iptal etmez; bu
emir duraklatmadan sonra da dolabilir ve koruma yönetimi devam eder. **Kapat
ve durdur** akışı, henüz hiç dolmamış STEP giriş emrini de kapsar. Bakiye veya
miktarın sıfır görünmesi tek başına kapanış kanıtı değildir: sahip olunan
giriş/stop/çıkış emirleri de terminal durumda ve hesap doğrulanmış olmalıdır.
Bu doğrulamadan sonra durum OFF olur.

## Seçilebilir boyutlar

| Alan | Sınır / anlam |
| --- | --- |
| Teminat | 0'dan büyük, en fazla 200 USDT; en çok iki ondalık basamak |
| Hazır seçenekler | 5, 10, 25, 50, 100, 150, 200 USDT |
| Özel tutar | Örneğin /step margin 12.50 veya /step margin 12,50 |
| Hedef kaldıraç | 1–10 arasındaki tam sayılar |
| Hedef toplam pozisyon | Teminat × kaldıraç; en fazla 2.000 USDT |
| En büyük seçim | 200 USDT teminat × 10× = 2.000 USDT toplam hedef pozisyon |

İlk görünümdeki 19 Ekim referansı **25 USDT toplam pozisyon / en fazla 5×**
şeklindedir. 25 USDT teminat değildir; 5× gerçekleşmiş kaldıraç gözlemi değildir.
Özel taslağa geçildiğinde diğer alan kullanıcı seçene kadar boş kalır.
Taslakta 200/10 seçilebilmesi, mevcut STEP çalıştırıcısının bu boyutu
uygulayabildiği veya bu boyut için kabul verilmiş olduğu anlamına gelmez.

Referans profilin aynı anda 1 işlem, UTC gününde 1 yeni giriş ve 1 USDT
gerçekleşmiş günlük net zarar eşiğinde yeni giriş kilidi büyütülmez.
Bu eşik, gerçekleşebilecek kaybın garantili üst sınırı değildir.

## Kontrol bağlantısı

- step_lock_panel.py: yönetici sohbeti **ve** kullanıcı kimliği kontrolü;
  kayıtlı taslak; yeni durum düğmeleri ve işlem onayları.
- step_lock_control.py: durum doğrulaması, HTTPS istemcisi, HMAC protokolü
  ve port tabanlı servis kontrolcüsü. Modül içe aktarma sırasında ağ açmaz.
- Menü/yönlendirme mevcut telegram_ux.handle ve mode_callback üzerinden yürür.
  **bot.py değişmedi**; normal bot ayarları ve ekonomik kodu bu kapsamda değildir.
- Taslaklar normal DB_PATH yanında ayrı step_lock_panel_drafts.sqlite3
  dosyasına, sohbet/kullanıcı kapsamında yazılır.
- STEP verisi bilinmiyorsa normal hesabın önbelleği kullanılmaz.
  Bakiye ve stop fiyatı bu protokolde olmadığı için — gösterilir.

İstemci yalnız tam yapılandırmayla açılır:

| Değişken | Amaç |
| --- | --- |
| STEP_CONTROL_URL | HTTPS adresi; yol tam olarak /step-control/v1; yönlendirme kabul edilmez |
| STEP_OPERATOR_CONTROL_KEY | Ayrı 32–64 bayt anahtarın hex değeri; Binance anahtarı değildir |
| STEP_CONTROL_SCOPE | Ayrı STEP hesabına bağlı opak kapsam kimliği |

Bu değişiklik bu değişkenleri üretimde oluşturmaz. Özel uyarlamada dışa açık
worker yalnız imzalı zarfı iletir; operator anahtarı gateway ve yetkili panel
istemcisinde bulunur. Gateway zarfı uçtan uca doğrular. Worker üzerinde anahtar
bulunması yapılandırma hatasıdır. Ortak protokol işlem gönderme yetkisi vermez;
gerçek port kendi hesap, sınır, kaynak ve emir yetkisi kontrollerini uygular.

## Doğrulanmış durum ve tekrar davranışı

Onaylar yönetici, sohbet, taslak revizyonu, kapsam, servis açılış kimliği,
servis revizyonu ve geçerli işlem kimliği/miktarına bağlıdır. Süre 120 saniye;
tek kullanımlıdır. Onay verilirken yeni servis durumu okunur. Arada dolum,
pozisyon değişikliği veya servis yeniden başlatması olmuşsa eski onay reddedilir.

Servis görüntüsü en fazla 15 saniyelik olmalıdır. İmzalı istek süresi
10 saniyedir; nonce tekrarları ve bozuk imzalar reddedilir. Yanıt da imzalıdır
ve aynı istek nonce/kapsamına bağlıdır. En fazla 64 KiB JSON kabul edilir;
tekrarlı JSON anahtarları ve sonlu olmayan sayılar reddedilir.

Her etki öncesinde dayanıklı CONTROL_REQUESTED, sonrasında CONTROL_APPLIED
veya CONTROL_FAILED kaydı oluşur. Aynı işlem kimliği ve içerik tekrarında
etki yinelenmez; farklı içerik aynı kimlikle gönderilemez. Sonucu belirsiz
işlem otomatik tekrar edilmez. Yeniden açılışta giriş kabulü kapanır;
eski başlatma komutu LIVE durumunu geri getirmez. Başarısız bir kapatma,
yeniden açılışta otomatik kapatma denemesine dönüşmez; koruma yönetimi sürer.

## Mevcut servis kaynağının sınırı

Ayrı STEP servisinin kabul edilmiş kaynak sürümünde canlı yetki koşulsuz
kapalıdır; giriş toplamı 25 USDT ve kaldıraç üst sınırı 5× ile sınırlıdır.
Borsa kaldıracını değiştiren bir yol yoktur. Özel aday uyarlama bunları
değiştirmediği için mevcut gerçek port live_capable=false bildirir ve
başlatma/kapatma yoluyla emir yetkisi kazanamaz.

Özel adayda giriş kabulü, ayrı operator-close akışı ve worker→gateway köprüsü
uygulanmıştır. Eski kaynak kabul dosyası byte düzeyinde korunmuştur; değişen
servis dosyaları bu eski kaynak kabulünden geçmez. Aday paketin test ve hash
raporu yeni bir canlı kabulü değildir. Bu paket mevcut kaynak kapısı altında
çalıştırılabilir bir üretim sürümü olarak sunulmaz.

Açık pozisyonla yeniden başlatma desteği ve kontrol isteklerinin koruma
zamanlamasına etkisi bu adayda canlı kullanım için kabul edilmiş değildir.
Mevcut normal üretime 18 Ekim'de eklenmiş Premium sinyal relay paketinin
GitHub tabanıyla tam eşleştirilmesi de gelecekteki dağıtımın gereğidir.
İnceleme tabanı 17864116d51483d8bbec23d34376fe837cf1ff4b commit'idir.

## Testler

Son yerel koşumda **64 STEP testi geçti**: 28 kontrol/protokol testi ve
36 panel testi (27 önceki panel regresyonunun tamamı dahil). Mevcut Telegram
menü kontrolleriyle toplam **71 testin 70'i geçti**; tek hata aşağıda
açıklanan eski AST referans kapısıdır. Ayrı özel servis uyarlamasının
**19 odaklı testi** de geçti. Canlı borsa/Telegram uçtan uca denemesi yapılmadı.

Odaklı testler gerçek Telegram, Binance veya Railway bağlantısı açmaz.
Sahte istemci/port, bellek içi kayıtlar ve geçici taslak dosyaları kullanır.
Durum akışı, bekleyen giriş, korumanın duraklatmada sürmesi, terminal kapanış,
kimlik/revizyon bağları, tek kullanım ve hata/yeniden açılış davranışı sınanır.

```bash
PYTHONPATH=binance_momentum_bot:tests PYTHON_DOTENV_DISABLED=1 \
python -m unittest -v test_step_lock_control test_step_lock_panel
```

Mevcut test_telegram_ux.MenuTests.test_production_and_startup_research_baselines_unchanged
testi, değişiklikten önceki tabanda da AST hash uyuşmazlığı verir:
beklenen f914273b…, gerçek 7f31b398…. Bu referans değiştirilmedi;
test kaldırılmadı. bot.py tabanla aynıdır; normal ekonomik fonksiyonların
ayrı hash kontrolü geçer. Son koşum sayıları teslim raporunda belirtilir.
