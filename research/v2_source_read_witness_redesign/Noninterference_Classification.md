# Noninterference sınıfları
|Sınıf|Sonuç|
|---|---|
|A Bytecode identical|FALSE. NamedExpr COPY/STORE_FAST, yardımcı çağrılar ve keyword/return sideband yolları ekler. Yerel3.12 disassembly teslim edildi; production3.13 bytecode kanıtı değil.|
|B Observable strategy transcript equivalent|İlk tam koşu7PASS/25FAIL.6030→6028 düzeltmesi sonrası32/32 tanılayıcı eşlik.|
|C Semantics-preserving under predeclared model|Sınırlı modelde destekleyen tanılayıcı kanıt var; çalışma düzeyinde regression kapısı başarısız, design lock yok.|
|D Unknown under arbitrary concurrent mutation|UNKNOWN. GIL/wall-clock read timing ve extra references finalizer scheduling eşitliği kanıtlanmadı.|

Kaynak mantıksal evaluation sırası ile gerçek wall-clock instant aynı kavram değildir. Python ifadeleri soldan sağa değerlendirir ve assignment expression orijinal değeri döndürür ([Python reference](https://docs.python.org/3.13/reference/expressions.html#evaluation-order), [assignment expressions](https://docs.python.org/3.13/reference/expressions.html#assignment-expressions)). Local güçlü referans nesnenin ömrünü uzatabilir ([data model](https://docs.python.org/3.13/reference/datamodel.html#object.__del__)). Bu yüzden arbitrary object/concurrency ispatı verilmez.

Ek bytecode bulgusu: gate return ON/OFF koşulu AST ifadesini iki ayrık dala kopyalar; statik cap LOAD_GLOBAL sayısı1→2, seçilen runtime dalında1. Bu yüzden düz bytecode load-list equality de yoktur. Diğer targeted fonksiyonların statik min_score/score load sıraları aynıdır.
