# Gate → arm bağının sınırı
Öneri yalnız mevcut evaluate() local zincirindedir: bot.py6020 mevcut radar okuması;6022 gate sideband;6028 quality_arm mevcut episode okuması;6042 mevcut radar argümanı ve aynı gate sideband'inden immutable Context. Strategy cap'i context'ten okumaz.

Context nonce yeni object() kimliğidir; global sayaç/registry/DB join veya m'ye yeni witness alanı yoktur. Yalnız observer ON'da token oluşturulur. Test runtime namespace fixture sabitidir; üretim runtime attestation veya restart-proven UUID değildir. Arm'da scorer ve selector sideband'leri nested tuple alanlarıyla taşınır. Nonce sadece process-local object identity'dir; serializer durable exact receipt oluşturmaz.

İlk aday episode için6030 satırına bağlandı; gerçek okuma6028'dir. _w_episode bağlanmadığından arm öncesi NameError oluştu. İlk tam koşu7/32 PASS,25/32 FAIL; kriter gereği D.

Düzeltme yalnız6030→6028 anchor değişimidir. Tanılayıcı koşuda normal zincir, radar mismatch, arm öncesi kesilme, gate reject, reentrant ve iki bağımsız thread fixture'ı incelendi. Radar mismatch/missing context/restart namespace UNKNOWN döner. Başarılı fixture'da nonce, radar17,episode23 ve symbol aynı zincire bağlandı.

Sınır: episode burada gate sırasında okunmaz; quality_arm'ın mevcut okumasından gelir. Gate ile bu okuma arasındaki arbitrary mutation, quality_arm sonrası episode değişimi ve gerçek StepLock state referansının değişmesi için exact runtime bağ KANITLANMADI. M16 paylaşılan config üstünde keyfi schedule ispatı değildir. Bu nedenle tanılayıcı exact örnek production identity exact iddiası değildir; candidate kilitlenmez. Eksik tanığı sonradan DB/outcome ile tamamlama yok.

Serializer kapsamı: mevcut microprototype JSON payload scorer/selector/event/notify operands ve identity alanlarını içerir; gate cap ve nonce immutable context içinde tutulur, JSON alanlarına yazılmaz. ON süreleri bu yerel payload içindir; tam v1/v2 immutable receipt serialization maliyeti veya durable identity kanıtı değildir. Candidate D ile kilitlenmediğinden bu eksikliği başarılı production contract diye sunma yok.
