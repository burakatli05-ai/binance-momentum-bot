# Failure kayıtları
1. SOURCE_LOOKUP01: railway.toml önce repo root'ta arandı; gerçek yol binance_momentum_bot/railway.toml. SOURCE_LOOKUP02: telegram_ux önce dizin sanıldı; gerçek dosya telegram_ux.py. Okuma hataları; source değişikliği0.
2. MICRO01: episode binding6030 yerine gerçek6028 olmalıydı; aday yeni NameError üretti. Harness M07'de boş read listesinde IndexError ile ilk raporu bitiremedi. İlk kaynaklar failed_attempt01 içinde korunur.
3. DIAG01: tanılama betiğinin inspect.py adı standard-library inspect import'unu gölgeledi; diagnose_probe.py olarak yeniden adlandırıldı. DIAG02: cp1254 stdout UnicodeEncodeError; -X utf8 kullanıldı. Candidate/strategy kriteri değişmedi.
4. MICRO01_COMPLETE: yalnız raporlayıcı empty-list/context guard'ları eklendi; eksik gözlem false olarak kaydedildi. Aynı hatalı aday32unique senaryoda7PASS/25FAIL. Tam JSON/CSV/transcript ve kaynaklar ZIP içindedir.
5. MICRO02_DIAGNOSTIC: yalnız candidate episode anchor6030→6028 düzeltildi.32/32PASS; D duruş kararı kaldırılmadı. Önceki failurelar başarıyla ikame edilmedi.
6. BENCH01_DIAGNOSTIC:50warmup/variant+2000pairedsample=6000timedinvocation; donmuş dört bütçe kontrolü PASS. Production latency PASS değil.

Parent ilk S07 failure ve PREP/STATIC/FORMAT/COMMIT/DOC kayıtları Differential_Test_Evidence.zip içindeki parent pakette byte-identical korunur. Parent T001–T092 run0/notrun92; parent performance_samples0. Bu Work'ün micro testleri parent92 testi yerine geçmez.

7. ARTIFACT_VALIDATION01: ek statik kontrol, gate Return içindeki ayrık ON/OFF AST kopyalarını tek yürütülen yol sandı. Original bytecode cap LOAD_GLOBAL1, candidate bytecode2 (ayrık dallar); runtime senaryolarında yalnız seçilen dal read1. İlk assertion hatası korunur; statik eşlik iddiası geri çekildi. Donmuş runtime read/order kriterleri ve D sonucu değişmedi.
