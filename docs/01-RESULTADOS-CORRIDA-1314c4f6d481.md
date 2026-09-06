# Resultados de la corrida `RUN_ID_CAP = 1314c4f6d481` — 21 de agosto de 2026

**Bloques corridos:** CAP-0, CAP-0-BIS, CAP-1 v12, CAP-2 v12, CAP-3 v13, CAP-4 v13, CAP-6 v2
**Pendiente:** CAP-5 (dominios) y CAP-7 (saneamiento del outlier)
**CSV exportados a:** `/Volumes/workspace/default/mercado_publico/capstone_salidas/1314c4f6d481/` — 12 tablas y un manifiesto
**Regla de lectura:** §1.16. Cada veredicto va con su magnitud al lado.

---

## 0. Qué es citable y qué no

| Cifra | Estado |
|---|---|
| Brecha real − nulo de CAP-1 (+93,06 pp) | **CITABLE** |
| Composición de universos de licitación (80,08% ≤1 código) | **CITABLE** |
| Cobertura ONU por año y por mecanismo | **CITABLE** |
| Placebo de unidad de medida | **CITABLE** |
| Reconciliación por **número de OC** 2024 (0,9825) | **CITABLE** |
| Cascada en líneas | **CITABLE** |
| Trazabilidad peldaño 2→3 (73,78% / 81,75%) | **CITABLE** |
| Toda cifra en **pesos** | **ESPERA A CAP-7** — contaminada por dos líneas |
| G6 restringido-y-ajustado 99,83% | **NO USAR** — circular |
| Concentración del gasto (49,16%) | **CORREGIR** — saneada es 27,09% |
| Dispersión de precio CAP-4.C | **NO CITABLE AÚN** — falta validación manual |

---

## 1. El outlier, cazado `[MEDIDO]`

```
2022 · SE · doc 5648-84-SE22 · lic 5648-8-LE22 · org 6951 · Unidad
      precio = 6.069.497,00      cantidad = 6.069.497,00
      clp_linea = 36.838.793.833.009
```

**Precio idéntico a cantidad.** Es el error de captura clásico: el monto total escrito en el campo cantidad, y el pipeline multiplicó. La segunda línea del ranking tiene la misma firma: `2018 · 5603-71-SE18 · precio 772.000 · cantidad 772.000 → 595.984.000.000`.

| | CLP |
|---|---|
| Gasto CLP nativo crudo 2017–2026 | 126.889.600.153.921 |
| Línea 5648-84-SE22 sola | 36.838.793.833.009 — **29,03% de la década** |
| Línea 5603-71-SE18 | 595.984.000.000 — 0,47% |
| **Gasto saneado** | **89.454.822.320.912** |

CAP-7 cuenta cuántas líneas comparten la firma `precio == cantidad`. **Si son dos, se declaran y se excluyen; si son miles, es un defecto sistemático del pipeline y ninguna cifra en pesos es reportable.** El bloque imprime siempre las dos versiones, cruda y saneada.

Una tercera clase de línea grande, distinta: once de las veinte del ranking son del organismo 7002, con precios de 130 a 264 mil millones y cantidad 1. `[POR VERIFICAR]` — pueden ser contratos de obra legítimos. No comparten la firma de error y no se excluyen.

**El pipeline reprodujo su propia cifra al peso:** los 126.889.600.153.921 coinciden exactamente con el valor que el proyecto arrastraba de corridas anteriores. Es una validación interna fuerte de que v13 mide lo mismo que se midió antes.

---

## 2. La cascada `[MEDIDO en líneas · en pesos espera a CAP-7]`

Sobre `df_oc_all`, universo completo, 51.451.528 líneas:

| Peldaño | Líneas | % del gasto CLP |
|---|---|---|
| 0 · Universo total | 51.451.528 | — |
| 1 · en CLP nativo | 50.134.474 | 100,00% |
| 2 · y de mecanismo que pasa por licitación | 22.777.963 | 88,53% |
| 3 · y que trae código de licitación | 16.805.709 | 72,37% |
| 4 · y cuya licitación cae dentro del universo | 15.271.439 | 70,33% |
| 5 · y que efectivamente enlaza | 15.245.511 | 70,20% |
| 6 · y con código ONU válido | 15.244.054 | 69,30% |
| 7 · y con cantidad = 1 → auditable | 4.625.369 | 24,69% |

- CLP nativo: **126.889.600.153.921**
- CLP auditable (peldaño 7): **31.325.495.122.510** = 24,69% crudo, **35,02% sobre el total saneado**

### 2.1 · El peldaño 2 es el hallazgo estructural

**El mecanismo licitatorio es el 45,4% de las líneas y el 88,53% del gasto.** Su espejo: Compra Ágil son 10,4 millones de líneas (20,2% del total) y apenas el 3,38% del gasto saneado; Convenio Marco, 17,2 millones de líneas (33,4%) y 9,47%.

Esto **replica desde los datos** lo que ChileCompra publica en su comunicado de 2025 —compra ágil, 42,1% de las órdenes y 5,3% del monto—. Convierte un artefacto de la medición en un hecho documentado del sistema, con fuente externa al lado.

### 2.2 · El peldaño 7 mezcla dos afirmaciones distintas

`cantidad = 1` era el control tosco de la dispersión de precio, adoptado cuando se creía que no había unidad de medida. **Ahora que `_sinclasificar_UnidadMedida` existe, ese filtro ya no pertenece a la definición de "auditable".**

`[OPINIÓN]` Separar en dos:
- **Trazable hasta el producto declarado** = peldaño 6 = **69,30% del gasto**. Es una propiedad del Estado.
- **Comparable en precio** = peldaño 7 = 24,69%. Es una propiedad del control metodológico del proyecto, no del Estado. Presentarlo como "solo el 24,69% es auditable" atribuye al sistema una limitación que puso el analista.

---

## 3. Trazabilidad: qué cifra usar y cuál descartar

### 3.1 · El G6 de 99,83% es circular — NO USAR `[MEDIDO pero inválido]`

CAP-3 v13 reconstruyó el flag de enlace por **clave de licitación** (las claves presentes en `df_oc_linked` con `tiene_licitacion_origen = true`), porque el flag por línea no existe en `df_oc_all`. Y el denominador ya exige `dentro_universo`, que se deriva del año del sufijo del código.

Resultado: se le pregunta a una línea si su licitación está cargada, después de haber exigido que su licitación esté dentro del universo cargado. **Da 99,83% por construcción.** Las dos listas de exclusión —la original y la documentada— dan exactamente la misma cifra y el mismo denominador de 15.359.135, lo que confirma que la cifra no depende de la lista sino del filtro previo.

### 3.2 · La cifra defendible es el peldaño 2→3 `[MEDIDO]`

De las líneas de mecanismo licitatorio, **cuántas traen código de licitación**:

- **por CONTEO: 73,78%** (16.805.709 de 22.777.963)
- **por MONTO: 81,75%**

No es circular: mide si la orden de compra declara de qué licitación viene, que es exactamente la pregunta de trazabilidad. El complemento —26,22% de las líneas licitatorias sin clave— es el hallazgo.

Ancla internacional: Soylu et al. (2022) documentan que en Europa **solo el 9%** de los avisos de adjudicación tiene vínculo explícito con su contrato. El 73,78% chileno se lee muy distinto al lado de ese 9%.

### 3.3 · La discusión sobre TD, CB y R1 quedó cerrada por los datos `[MEDIDO]`

`pct_con_clave` por mecanismo: **TD 0,00% · CB 0,00% · CM 0,0001% · AG 0,0011% · MC 0,02%** contra **SE 73,49% · CC 99,30%**.

Los mecanismos discutidos no tienen clave de licitación de todos modos, así que incluirlos o no en la lista de exclusión **no cambia ninguna cifra**. La corrección documental de `F13` sigue siendo obligatoria para el texto, pero deja de tener consecuencia numérica. Y `R1` no aparece como valor de `tipo_oc` en ninguna línea.

---

## 4. Pregunta 1 — consistencia ONU (CAP-1) `[MEDIDO]`

### 4.1 · El baseline nulo

| | subset_4, universo C_informativo |
|---|---|
| Real | **94,44%** (n = 190.232) |
| Nulo permutado | **1,38%** (n = 190.232) |
| **Brecha** | **+93,06 pp** |

La columna `n` es idéntica entre las dos series en las nueve filas: la permutación no perdió filas. El control de integridad pasó.

La objeción de la segunda opinión —*"contra un azar de 61% no es nada, y hoy no sabes cuál de los dos es"*— queda resuelta: **el azar da 1,38%**, y hay una razón medida (el Estado usa 17.132 códigos distintos, así que el espacio de coincidencia es amplio).

### 4.2 · El sesgo mecánico de cardinalidad no se materializa

`subset_4` real por tramo: 94,64 · 94,62 · 94,66 · 94,85 · 95,16 · 93,20. **Plano.** `iguales_4` sí cae como se predijo: 94,64 · 81,41 · 74,47 · 68,69 · 63,11 · 55,21. La relectura a 2 y 8 dígitos no cambia el signo.

`[OPINIÓN]` Predeclarar una crítica, ponerla a prueba y reportar que no se sostuvo vale más ante un jurado que haber acertado.

### 4.3 · La exclusión es el hallazgo, y tiene número

De **964.171 licitaciones pareadas**: A monolínea legítima 643.562 (**66,75%**) · B testimonial multilínea 128.543 (**13,33%**) · C informativo 190.232 (**19,73%**) · D 1.674 · E 160.

**El 80,08% declara a lo más un código de producto distinto.** La patología real —varias líneas, un solo código— es **13,33%**, no 80%: una licitación de una línea con un código no es una falla.

### 4.4 · Cobertura ponderada

- 15.381.353 líneas enlazadas con licitación presente; 99,99% con ONU válido
- **por LÍNEA: 76,79%** `[MEDIDO]`
- por PESOS: 97,04% — **espera a CAP-7**, el denominador incluye el outlier

---

## 5. Pregunta 2 — la respuesta es no, y el placebo lo prueba `[MEDIDO]`

**85,24% en 2024 contra 85,52% en 2025: 0,28 puntos porcentuales.** El primer año completo bajo el art. 20 bis no se distingue del último año sin él.

**El placebo de unidad de medida es el resultado metodológicamente más fuerte de la corrida.** Cobertura de código ONU contra cobertura de unidad de medida: 2023 → 86,15 y 86,12 · 2024 → 85,24 y 85,23 · 2025 → 85,52 y 85,51 · 2026 → 82,42 y 82,40. Idénticas a dos decimales.

**El artículo 20 bis no manda publicar unidad de medida.** Que las dos series se muevan como una sola demuestra que esto no es cumplimiento normativo: es un mecanismo que dejó de publicar la capa de producto completa. Permite decir *"el nivel no cambió, y además sabemos que lo que mueve la serie no es la norma"* sin usar la palabra efecto.

Los otros dos placebos —cantidad y precio— están en 100,00% los diez años, midiendo validez numérica real. Un placebo en el techo no refuta nada; hay que decirlo así.

**Panel balanceado por organismo:** 828 organismos en los diez años, cubren el 99,6% de las líneas de 2017. Replica la serie (85,43% en 2024, 85,88% en 2025). El descenso no es composición.

**Los organismos que transan pasan de 1.086 (2024) a 1.165 (2025): +79.** No +327 ni +217. El dato medido no respalda un escalón de censo.

**Todo el hueco es Convenio Marco.** Desde 2022, el 100% de las líneas sin ONU válido son de CM. Cobertura dentro de CM: 94,90% (2017) → 61,76% (2021) → 11,76% (2022) → 0,20% (2023) → **0,08% (2024)** → 0,09% (2025). Fuera de CM: Compra Ágil 100,00%, `SE` 99,97–99,99%, Trato Directo 99,96–99,98%. El desplome ocurrió **antes** de la norma y la norma no lo revirtió.

---

## 6. Concentración: la hipótesis era correcta, la vara estaba mal

**Corrección de lo que se escribió antes en este proyecto.** La primera lectura, hecha solo con el ranking por líneas, concluyó que la concentración del catálogo estaba refutada. Estaba mal medida.

| | Por LÍNEAS | Por GASTO |
|---|---|---|
| Top-10 | 10,84% líneas / 1,51% gasto | **1,23% líneas / 49,16% gasto** |
| Top-50 | 26,32% / 10,81% | 8,10% / 62,94% |
| Top-100 | 36,92% / 21,36% | 15,50% / 69,60% |
| Top-300 | 56,56% / 31,18% | 33,55% / 80,86% |

**Diez códigos concentran casi la mitad del gasto con el 1,23% de las líneas.** Pero el top-1 de esa lista es `81101505`, cuyo gasto es 36,87 billones **de los cuales 36,84 son la línea del outlier**: sin ella el código cae a 28.441 millones y sale del ranking.

**Cifra saneada: el top-10 por gasto concentra el 27,09%** del gasto saneado. Diez códigos, un cuarto del dinero del Estado. Sigue siendo el hallazgo, con el número correcto. CAP-7.F lo recalcula formalmente.

El catálogo se usa a fondo —17.132 códigos de 18.881— pero el dinero se concentra en decenas. **Las dos cosas son verdad y juntas son mejor titular que cualquiera sola.**

`[POR VERIFICAR]` La descripción de los 15 códigos de mayor gasto. El número solo no le dice nada a un jurado, y adivinar el rubro por el prefijo es exactamente el tipo de afirmación sin fuente que la Regla 2 prohíbe.

---

## 7. Dispersión de precio — mejor, pero todavía no citable

Con las unidades no comparables excluidas, precio mínimo de $100 y al menos 5 organismos por celda, los ratios bajaron de seis cifras a un rango de 36 a 268 en `p75/p25`.

Las celdas con volumen real: `41116004 UNIDAD 2022` (n=17.351, 163 organismos, p25 2.100 · p50 53.900 · p75 189.000, ratio 90,0) · `85122201 UNIDAD 2024` (n=30.070, **395 organismos**, ratio 44,9) · `41116105 UNIDAD 2024` (n=12.537, 164 organismos, ratio 47,9).

`[OPINIÓN]` Un p25 de 2.100 con un p50 de 53.900 no describe dispersión de precio de un mismo bien: describe un código ONU que agrupa cosas muy distintas. **Esto mide heterogeneidad de producto, no sobreprecio**, y presentarlo como lo segundo sería la afirmación más frágil del paquete.

**No es citable sin la validación manual de F10.** Abrir diez fichas en mercadopublico.cl y ver qué son realmente esas líneas es lo que convierte un ratio en una afirmación defendible. Es media tarde de trabajo y está en el plan.

Dato de contexto: de 51,45 millones de líneas, 12,05 millones no traen unidad de medida y 7,27 millones dicen "UNIDAD NO DEFINIDA".

---

## 8. Reconciliación externa: la buena está en el conteo, no en los pesos

| Vara | 2024 | Razón contra lo oficial |
|---|---|---|
| **Número de órdenes de compra** | **1.996.187** vs 2.031.670 | **0,9825** |
| precio × cantidad, crudo | 12.365.867.778.141 | 0,7426 |
| precio × cantidad, sin líneas gigantes | 12.264.043.965.964 | 0,7364 |
| sin líneas gigantes × 1,19 (con IVA) | 14.594.212.319.497 | 0,8764 |

**El pipeline captura el 98,25% de las órdenes de compra que ChileCompra declara para 2024.** `[MEDIDO]`

`[OPINIÓN]` Ésta es la validación externa que el proyecto buscaba, y es mejor que la de montos: una orden de compra es una orden de compra, y no depende del IVA, del tipo de cambio ni de si el monto oficial incluye cargos y descuentos. Es la lámina 3 del arco narrativo, y se gana temprano.

La brecha en pesos la cierra CAP-7 contra `monto_total_oc_clp`, `_sinclasificar_totalLineaNeto` y `_sinclasificar_totalImpuestos`, que existen en los datos y nadie había usado. CAP-7.B además valida la fórmula `precio × cantidad` contra el propio campo del dato: deja de ser un supuesto del proyecto.

---

## 9. Qué sigue

1. **Correr CAP-7.** Decide si el outlier son dos líneas o un defecto sistemático, y cierra la pregunta del IVA. Sin esto no hay cifra en pesos.
2. **Correr CAP-5** (dominios y sufijos R1/R2/R4) y volver a correr CAP-6 para exportar lo nuevo.
3. **Buscar la descripción de los 15 códigos de mayor gasto** en el catálogo UNSPSC.
4. **Validación manual de 30 casos (F10)**, priorizando los extremos de dispersión. Es lo que hace citable la Pregunta 3.
5. **Aplicar las correcciones de `F13-verificacion-fuentes-21ago2026.md`.** La cita académica cuesta dos minutos.
6. **Retractar la etiqueta "Trato Directo R1/R2/R4"** de la bitácora. Sigue pendiente.
