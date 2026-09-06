# Cierre de las cifras en pesos — CAP-7, corrida `1314c4f6d481`

**21 de agosto de 2026.** Complementa a `01-RESULTADOS-CORRIDA-1314c4f6d481.md` y **lo corrige** en todo lo que toque montos. Pendiente de CAP-8 (nombres de los códigos) y CAP-9 (cascada saneada exacta).

---

## 1. El hallazgo: el outlier no es del pipeline, es de la fuente `[MEDIDO]`

Esto es lo más importante del bloque y cambia el estatus del caso.

| | CLP |
|---|---|
| Suma de `precio × cantidad` (calculada por el pipeline) | 126.889.600.153.921 |
| Suma de `_sinclasificar_totalLineaNeto` (**campo que trae el dato**) | 126.925.942.772.073 |
| Diferencia | +36.342.618.152 (**+0,0286%**) |

Si `totalLineaNeto` no contuviera la línea de 36,8 billones, la diferencia entre ambas sumas sería de **menos 36,8 billones**. Es de más 36 mil millones. **El campo de total de línea publicado por Mercado Público dice lo mismo que la multiplicación del pipeline.**

Dos consecuencias:

**(a) La fórmula `precio × cantidad` queda validada por el propio dato.** 50.134.181 líneas traen `totalLineaNeto`; el 2,66% difiere más de un 1% de la multiplicación (cargos y descuentos de línea), pero las sumas coinciden en 0,029%. Deja de ser un supuesto del proyecto y pasa a ser una línea de metodología con respaldo.

**(b) La línea de 36,8 billones es un hallazgo de calidad de datos, no una nota metodológica.** El Estado publica una línea de orden de compra que registra 36.838.793.833.009 pesos —**el 29% de todo lo transado en la década**— con el precio unitario idéntico a la cantidad (6.069.497 × 6.069.497). Es exactamente el tipo de defecto que este proyecto existe para encontrar, y va al informe como tal.

`[POR VERIFICAR]` Antes de usarlo: abrir el link de la ficha en mercadopublico.cl (CAP-8.B lo imprime) y confirmar a mano que la ficha pública dice lo mismo. **No nombrar al organismo:** describirlo por sector y tramo, según la doctrina "alerta, no infracción".

---

## 2. El filtro de saneamiento `[MEDIDO]`

Firma: `precio == cantidad` y ambos por encima de 1.000 CLP. Umbral arbitrario y declarado.

| | |
|---|---|
| Líneas CLP nativas | 50.134.474 |
| Con la firma | **1.331** (0,002655%) |
| Monto que aportan | 37.442.070.808.807 (**29,51% del gasto crudo**) |
| De ese monto, **dos líneas** | 37.434.777.833.009 — el **99,98%** |
| Las otras 1.329 líneas | 7.292.975.798 — el **0,0057%** del gasto total |

No son 1.331 errores: son dos errores y 1.329 coincidencias legítimas (comprar 5.000 unidades de algo que cuesta 5.000 pesos). **El daño colateral del filtro es el 0,0057% del gasto**, así que el filtro es defendible tal cual.

**Frase para metodología:** *"Se excluyen 1.331 líneas cuyo precio unitario es idéntico a la cantidad; el 99,98% del monto excluido corresponde a dos de ellas."*

| | CLP |
|---|---|
| **Gasto CLP de la década, crudo** | 126.889.600.153.921 |
| **Gasto CLP de la década, saneado** | **89.447.529.345.114** |

Distribución de la firma por año: 199 · 130 · 129 · 165 · 119 · 117 · 120 · 150 · 128 · 74. Frecuencia estable, impacto concentrado en dos años.

---

## 3. `monto_total_oc_clp` no sirve — descartado `[MEDIDO]`

Se intentó cerrar la pregunta del IVA reconciliando contra el monto a nivel de orden de compra. **No funciona.**

| Año | OC-nivel (billones) | Línea-nivel (billones) | Razón |
|---|---|---|---|
| 2017 | 11,44 | 6,54 | 1,75 |
| 2018 | **42,92** | 6,55 | **6,56** |
| 2020 | 9,05 | 7,18 | 1,26 |
| 2023 | 20,34 | 9,65 | 2,11 |
| 2024 | 31,65 | 12,37 | 2,56 |
| 2025 | **45,83** | 14,91 | **3,07** |

La razón oscila entre 1,26 y 6,56 sin patrón. El campo tiene sus propios outliers y probablemente arrastra el problema de doble conteo Histórico/Vigente (riesgo 3). Razón contra la cifra oficial 2024: **1,9007**.

**La pregunta del IVA queda abierta y declarada.** No se cierra con este campo.

---

## 4. Reconciliación externa 2024 — la vara buena es el conteo `[MEDIDO]`

| Vara | Valor | Razón vs. oficial |
|---|---|---|
| **Número de órdenes de compra** | **1.996.187** vs 2.031.670 | **0,9825** |
| precio × cantidad, crudo | 12.365.867.778.141 | 0,7426 |
| precio × cantidad, saneado | 12.365.043.274.621 | 0,7425 |
| saneado × 1,19 (con IVA) | 14.714.401.496.799 | **0,8836** |
| `monto_total_oc_clp` | 31.652.440.518.409 | 1,9007 — descartado |

**El pipeline captura el 98,25% de las órdenes de compra que ChileCompra declara para 2024.**

`[OPINIÓN]` Ésta es la validación externa del proyecto, y hay que liderar con ella en vez de con los montos. Una orden de compra es una orden de compra: no depende del IVA, del tipo de cambio, ni de si el monto oficial incluye cargos y descuentos. Es la lámina 3 del arco narrativo y se gana temprano.

La brecha del 12% que queda en pesos tras el IVA —monedas no CLP excluidas, OC anuladas, cargos y descuentos— se declara en una línea y se sigue. **Salvedad obligatoria junto a toda cifra en pesos:** *"suma de precio neto por cantidad, solo líneas en pesos chilenos nativos; reconcilia con el 88,4% de la cifra oficial de 2024 una vez ajustada por IVA."*

---

## 5. Concentración del gasto, definitiva `[MEDIDO]`

Sobre 45.873.806 líneas CLP con ONU válido, saneadas. **17.117 códigos distintos usados.**

| | % de las líneas | % del gasto |
|---|---|---|
| Top-10 por gasto | 1,26% | **28,35%** |
| Top-50 | 8,51% | 47,06% |
| Top-100 | 15,55% | 56,40% |
| Top-300 | 33,24% | **72,49%** |

**Corrección de dos lecturas previas de este proyecto.** La primera concluyó que la concentración estaba refutada: estaba midiendo por líneas, la vara equivocada. La segunda dio 49,16% para el top-10: incluía el código del outlier. **La cifra buena es 28,35%.**

**El titular:** *el Estado tiene un catálogo de 18.881 códigos, usa 17.117 de ellos, y aun así tres de cada cuatro pesos pasan por trescientos.* Las dos cosas son verdad y juntas dicen más que cualquiera sola.

### Los 15 códigos con más gasto (saneado)

| Código | Líneas | CLP |
|---|---|---|
| 72131702 | 115.203 | 7.737.381.024.169 |
| 93131608 | 135.300 | 5.484.742.757.323 |
| 85121602 | 12.727 | 2.482.256.080.987 |
| 85141701 | 1.632 | 1.727.326.824.696 |
| 76111501 | 120.075 | 1.554.998.395.687 |
| 76121501 | 33.817 | 1.484.014.348.895 |
| 92101501 | 65.464 | 1.346.418.742.415 |
| 32101617 | 1.594 | 995.698.117.654 |
| 93151507 | 82.658 | 713.595.709.492 |
| 70111703 | 11.800 | 686.034.814.036 |
| 50131701 | 214.198 | 680.530.642.996 |
| 85122201 | 425.672 | 675.799.339.121 |
| 51142145 | 3.551 | 658.757.710.500 |
| 81111503 | 812 | 633.242.274.290 |
| 80141607 | 95.914 | 607.436.884.151 |

`[POR VERIFICAR]` **La descripción de cada código.** CAP-8 la resuelve usando `_sinclasificar_NombreroductoGenerico` y `RubroN1/N2/N3`, que vienen en los propios datos: la etiqueta sale de la misma fuente que el código, que es más defendible que un catálogo externo. **Adivinar el rubro por el prefijo de dos dígitos es exactamente el tipo de afirmación sin fuente que la Regla 2 prohíbe.**

Un código con 812 líneas y 633 mil millones (81111503) y otro con 425.672 líneas y 675 mil millones (85122201) son fenómenos completamente distintos. La tabla no se puede resumir en una frase.

---

## 6. La cascada saneada — pendiente de CAP-9

Sanear mueve la cascada mucho más de lo esperado: el outlier vive en los peldaños 1 a 6 pero **no** en el 7, porque su cantidad no es 1. Al excluirlo baja el denominador de los peldaños intermedios y **sube** el porcentaje auditable.

Aproximación desde los porcentajes redondeados de CAP-3 — **no citable**:

| Peldaño | % crudo | % saneado (aprox.) |
|---|---|---|
| 6 · trazable hasta el producto declarado | 69,30% | ~56,5% |
| 7 · comparable en precio (`cantidad = 1`) | 24,69% | **~35,0%** |

CAP-9 los calcula exactos e imprime las dos cascadas lado a lado. **Reportar la cruda junto a la saneada es lo que hace defendible haber excluido algo.**

### La distinción que hay que mantener en el informe

- **Peldaño 6 = trazable hasta el producto declarado.** Es una propiedad del Estado: la orden de compra declara de qué licitación viene y con qué código de producto.
- **Peldaño 7 = comparable en precio.** Es una propiedad del control metodológico de este proyecto: `cantidad = 1` lo puso el analista para poder comparar precios, y ya no hace falta ahora que existe unidad de medida.

Presentar el peldaño 7 como "lo auditable" le atribuye al Estado una limitación que puso el analista. **Reportar los dos y decir cuál es cuál.**

---

## 7. Qué queda

1. **Correr CAP-8 y CAP-9.** Con eso quedan cerradas todas las cifras.
2. **Escribir el acta de congelamiento de métricas** (ficha F3 del plan): la lista cerrada de cifras que pueden entrar al informe, al dashboard y a la presentación. Todo lo demás va a apéndice rotulado "exploratorio".
3. **Abrir la ficha del outlier en mercadopublico.cl** y verificarla a mano.
4. **Validación manual de 30 casos (F10)**, priorizando los extremos de dispersión. Es lo único que hace citable la Pregunta 3.
5. **Correr CAP-5** (dominios y sufijos R1/R2/R4) y volver a exportar con CAP-6.
6. **Aplicar las correcciones de `F13-verificacion-fuentes-21ago2026.md`.**
