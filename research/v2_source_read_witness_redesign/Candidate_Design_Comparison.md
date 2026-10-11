# Bir kez değerlendirilen dört sınıf
Tek birleşik aday: exact operand local binding + immutable local context + opt-in result sideband. Yeni sınıf eklenmedi.

|Sınıf|İnceleme|Karar|
|---|---|---|
|A EXACT-SITE OPERAND CAPTURE|Yedi mevcut LOAD etrafında NamedExpr; ek COPY/STORE_FAST; reread yok|Yalnız izole adayın capture parçası|
|B EPHEMERAL WITNESS CONTEXT|Namespace, tek invocation nonce, radar/episode/symbol ve cap immutable context; mevcut lexical zincir|Yalnız izole adayın taşıma parçası|
|C RESULT-SIDEBAND PROPAGATION|Keyword-only _w_on=False / _w_context=None; açık opt-in'de Sideband, defaultta eski sonuç tipi|Public API riski kabul edilmiş değil; gelecekte ayrı code review gerekir|
|D INTERPRETER-LEVEL INTERCEPTION|trace/profile/opcode/native stack, descriptors, monkeypatch/import hook|Reddedildi: loaded-code güvenliği ve pasif noninterference kanıtı yok|

Fixture global dict alt sınıfı ve min_score property yalnız okuma günlüğü ve adversarial test aracıdır. Bunlar önerilen production interception mekanizması değildir. Production module import edilmedi.
Class D için production tracing deneyi yapılmadı; negatif kontrol olarak mekanizmanın gerektirdiği müdahale sınırı incelendi.
A+B+C adayının ilk compiled kopyası yeni NameError üretti. Düzeltme sonrası tanılayıcı PASS, frozen failure kapısını kaldırmaz. Design lock yok.
