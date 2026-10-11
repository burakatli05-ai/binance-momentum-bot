# Hipotez ve yanlışlama
|Hipotez|Yanlışlama testi|Kanıt ve karar|
|---|---|---|
|Aynı operand load'u local capture ile korunabilir|M01–M11,M25–M27; ilk ve düzeltilmiş transcript|İlk candidate regression ile başarısız; düzeltilmiş7site okumaları modelde eşleşti|
|Gate→arm exact bağ global map gerektirmez|M12–M16,M21,M24,M31|Normal local fixture destekler; missing/gap/restart UNKNOWN; arbitrary episode mutation açık|
|Ayrı min_score değerleri reuse edilmeden korunur|M08–M11|Dört siteye ayrı90/96/97/ABA/same-value tanığı; generation-only kullanılmadı|
|Exception behavior değişmez|M02,M03,M07,M17–M19,M27–M29|İlk anchor NameError hipotezi çalışma düzeyinde yanlışladı; tam failure korunur|
|OFF ek yük bütçede kalır|2000 paired sample|Yerel p99 positive delta71.2µs<500µs; production iddiası değil|
|ON serialization bütçede kalır|2000 paired sample|Yerel p99 positive delta110.7µs<2000µs; production iddiası değil|
|Bytecode aynı kalır|Bytecode_Comparison|Yanlış; local stores ve yardımcı çağrılar eklenir|
|Arbitrary shared mutation altında exact eşlik|Model kapsamı|UNKNOWN; ispat/test iddiası yok|
