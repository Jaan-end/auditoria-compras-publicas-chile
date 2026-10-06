# F13 (adelantado) · Verificación de fuentes primarias — 21 de agosto de 2026

**Estado:** producido por subagente opus y **re-verificado parcialmente en el chat principal** (Regla 5). Lo re-verificado directamente va marcado `[RE-VERIFICADO]`; lo que no, sigue en `[FUENTE·SA]` y **no entra a ningún entregable hasta que lo mires tú**.

**Método:** solo WebSearch/WebFetch. Ninguna página se descargó por otra vía.

---

## Resumen: qué hay que corregir, en orden de urgencia

| # | Qué | Dictamen | Costo de arreglarlo |
|---|---|---|---|
| V3 | La cita académica está mal atribuida | **INEXACTA** | 2 minutos |
| V2 | "El art. 20 bis obliga a clasificar según el estándar ONU" | **INEXACTA en el matiz** | 5 minutos |
| V5 | `TD` y `CB` no existen en la tabla de dominio; `R1` sí y no se excluye | **INEXACTA** | afecta el 66,97% |
| V1 | "+327 organismos ingresaron el 12-dic-2024" | **INEXACTA en el verbo** | 10 minutos + rediseño del control |
| V6 | "18.881 códigos, catálogo vigente" | **NO VERIFICABLE** | reetiquetar como dato interno |
| V4 | Cifras oficiales 2017–2023 | **6 de 7 CONFIRMADAS** | corregir solo la línea de 2018 |

---

## V1 · "+327 organismos ingresaron al sistema el 12-dic-2024" → **INEXACTA**

**Cómo la usa el proyecto.** `[CONFIRMADO]` desde el documento fundacional: *"Entraron más de 327 organismos nuevos al sistema… Cualquier serie agregada mostrará un quiebre que no es conductual, es de censo."*

**Lo que dice la fuente.** `[RE-VERIFICADO]` La cifra 327 **sí es oficial**, pero está en la nota de junio de 2025, no en la del anuncio. Texto literal, https://www.chilecompra.cl/2025/06/montos-transados-en-la-plataforma-mercado-publico-superaron-los-us-17-643-millones-en-2024/ :

> *"a partir de diciembre de 2024 la ley amplió la cobertura del sistema, sumando 327 nuevos organismos compradores"* … *"de los cuales ya tenemos 217 transando en Mercado Público"*

La nota del anuncio (6-dic-2024, https://www.chilecompra.cl/2024/12/nuevos-organismos-compradores-transaran-por-ley-en-mercado-publico/) habla de *"un aumento de cerca de un 35% más de entidades compradoras"* y enumera categorías, **sin lista nominal y sin la cifra 327**. `[FUENTE·SA]`

**Por qué es inexacta.** El número existe; el **verbo y la fecha** no. 327 es *cobertura legal ampliada*, no un ingreso simultáneo. A abril de 2025 —cuatro meses y medio después— solo **217** estaban transando. El proyecto está afirmando un escalón de censo en un día que la propia fuente describe como una rampa.

**Redacción corregida, pegable:**
> Con la entrada en vigencia del nuevo marco el 12 de diciembre de 2024, la cobertura obligatoria del sistema se amplió en **327 nuevos organismos compradores** —*"cerca de un 35% más de entidades compradoras"* según ChileCompra (nota del 6-dic-2024)—. **La incorporación efectiva fue gradual, no simultánea:** ChileCompra reporta que a abril de 2025 solo **217 de esos 327** habían transado efectivamente. ChileCompra no publica lista nominal.

**Consecuencia metodológica, y es la parte importante.** La contaminación del panel no es un escalón en una fecha: es una rampa que cubre todo 2025. Una ventana de exclusión de un día no basta. El panel balanceado sigue siendo obligatorio y ahora hay que decir por qué.

---

## V2 · Texto del artículo 20 bis → **CONFIRMADO el texto, INEXACTO el uso**

**Texto literal, verificado en el Diario Oficial** (Ley 21.634, numeral 29, publicada 11-dic-2023), https://www.diariooficial.interior.gob.cl/publicaciones/2023/12/11/43722/01/2419296.pdf `[FUENTE·SA]`:

> *«En el sistema de información y gestión señalado se deberán clasificar y codificar los bienes y servicios transados a través de él, y permitir el acceso público a la información que señale el reglamento, respecto de la adquisición de cada tipo de bien o servicio, en formato de datos abiertos.»*

Idéntico palabra por palabra al que cita el proyecto. Confirmado también: **no aparecen las palabras "ONU" ni "UNSPSC"**; el artículo 19 pone el sistema **a cargo de la Dirección de Compras y Contratación Pública**; la vigencia del 12-dic-2024 está confirmada por ChileCompra pero **no** por el texto legal (el PDF del Diario Oficial se cortó antes del artículo transitorio).

**bcn.cl/leychile NO se deja leer automáticamente** — es una aplicación JavaScript. `[NO VERIFICABLE por herramienta]`

**La corrección que hay que hacer.** El proyecto dice que el 20 bis *"obliga a clasificar según el estándar ONU"*. **El artículo no dice "ONU".** Quien ata la obligación a ese estándar es ChileCompra en su comunicación, no el legislador. Redacción segura:
> El artículo 20 bis de la Ley 19.886, incorporado por el numeral 29 de la Ley 21.634 (D.O. 11-dic-2023) y vigente desde el 12-dic-2024, obliga a clasificar y codificar los bienes y servicios transados y a publicarlos en formato de datos abiertos. **El texto legal no nombra el estándar ONU/UNSPSC**; es ChileCompra quien lo identifica como el clasificador exigido.

Un jurado que abra la ley y no encuentre "ONU" derriba la frase actual en diez segundos.

---

## V3 · La cita académica → **INEXACTA en la atribución**

`[FUENTE·SA]` https://www.mdpi.com/2078-2489/13/2/99

- Título correcto: *Data Quality Barriers for Transparency in Public Procurement*.
- Autores en orden: **Soylu, Corcho, Elvesæter, Badenes-Olmedo, Yedro-Martínez, Kovacic, Posinkovic, Medvešček, Makgill, Taggart, Simperl, Lech, Roman.** Dumitru Roman es el **último** autor.
- Cita correcta: **Soylu et al. (2022)**, *Information* 13(2):99, https://doi.org/10.3390/info13020099
- Frase irlandesa, literal: *"In a notice from the Irish Republic, 83 CPV codes had been applied, making it harder to recognise exactly what the precise need is."*
- Frase del 9%, literal: *"Our analysis suggest that only 9% of award notices provided an explicit link between tenders and contracts."*

Las dos frases son correctas y citables textualmente. Solo cambia el apellido.

---

## V4 · Cifras oficiales de ChileCompra 2017–2023 → **6 de 7 CONFIRMADAS**

`[FUENTE·SA]` en todas las filas. **Advertencia de forma:** ChileCompra nunca escribe "billones"; escribe **"millones de millones"**. Citar con la palabra de la fuente.

| Año | Cifra del proyecto | Dictamen | Nota |
|---|---|---|---|
| 2017 | CLP 7,8 bill.; >2.363.000 OC | **CONFIRMADA** | dato nuevo: USD 12.229 M |
| 2018 | CLP 8,4 bill. / USD 13.099 M; >2.386.000 OC | Montos **CONFIRMADOS**; **OC INEXACTA** | la propia nota da dos cifras incompatibles: "2 millones 383 mil" y "más de 2 millones 386 mil" |
| 2019 | CLP 8,1 bill. / USD 11.500 M | **CONFIRMADA** | esa nota no publica OC |
| 2020 | USD 12.365 M; 1.622.611 OC | **CONFIRMADA** | |
| 2021 | USD 15.015 M; 1.649.829 OC | **CONFIRMADA** | |
| 2022 | USD 15.023 M | **CONFIRMADA** | Cuenta Pública 2023 |
| 2023 | USD 16.288 M | **CONFIRMADA** | esa nota no publica OC |

**Contradicción abierta:** el proyecto afirma "1.982.154 OC en 2023". Ninguna nota consultada publica esa cifra. Queda `[POR VERIFICAR]`.

**2024 y 2025** ya estaban verificadas. `[RE-VERIFICADO]` la de 2024, textual: *"alcanzaron a US$17.643 millones, equivalentes a $16.653.131 millones de pesos"* y *"durante el año 2024 las entidades emitieron 2.031.670 órdenes de compra"*.

---

## V5 · Tabla de dominio de ChileCompra → **CONFIRMA R1 y destapa tres problemas**

`[FUENTE·SA]` https://www.chilecompra.cl/api/

**Tipo de orden de compra, 14 valores:** OC "Automática" · D1, C1, F3, G1 (tratos directos) · **R1 "Orden de compra menor a 3UTM"** · CA "Orden de compra sin resolución" · SE "Sin emisión automática" · CM "Convenio Marco" · FG "Trato Directo (Art. 8 f y g)" · TL "Convenio Marco – Tienda de Libros (Obsoleto)" · MC "Microcompra" · AG "Compra Ágil" · CC "Compra Coordinada".

**Tipo de Licitación, 23 valores:** L1, LE, LP, LS, A1, B1, J1, F1, E1, CO, B2, A2, D1, E2, C2, C1, F2, F3, G2, G1, **R1**, CA, SE.

**Tres consecuencias, todas accionables:**

1. **`TD` y `CB` NO EXISTEN en la tabla de dominio.** Dos de los seis códigos que el proyecto excluye del denominador del G6 restringido (66,97%) no tienen respaldo documental. `CM`, `AG`, `MC` y `CA` sí lo tienen.
2. **`R1` sí existe, es exactamente lo que se quiere excluir** (compra bajo 3 UTM, sin licitación de origen) **y hoy está dentro del denominador.** Eso sesga el 66,97% a la baja.
3. **La tabla publicada está desactualizada:** no contiene `LR`, `LQ` ni `H2`, ni ninguna fila con "5.000 UTM", pese a que el proyecto mide LR y LQ sobre datos reales. **La tabla de dominio oficial no cubre el universo 2017–2026.**

**R2 a R9 no figuran en ninguna de las dos tablas.** Confirmado. La retractación de la etiqueta "Trato Directo R1/R2/R4" sigue siendo obligatoria.

**Redacción corregida del denominador:**
> El denominador restringido excluye los tipos de orden de compra documentados en la tabla de dominio de ChileCompra que por diseño no provienen de una licitación: **CM, AG, MC, CA** y **R1**. Los códigos **TD** y **CB**, usados en corridas previas, **no figuran en la tabla de dominio oficial** y su exclusión se declara como criterio propio, no documentado. La tabla publicada no incluye LR, LQ ni H2, presentes en los datos del período, por lo que se declara incompleta respecto del universo 2017–2026.

Los tres diccionarios de datos PDF enlazados desde esa página devolvieron **403**. `[NO VERIFICABLE por herramienta]`

---

## V6 · Catálogo UNSPSC de 18.881 códigos → **NO VERIFICABLE**

`[FUENTE·SA]` Revisadas `chilecompra.cl/ley-catalogo-onu/`, `/licitacion-proveedor/`, `/api/` y `datos-abiertos.chilecompra.cl/descargas/complementos`. **Ninguna publica el número de códigos del catálogo ni la versión de UNSPSC.** El portal de datos abiertos requiere JavaScript y no se deja leer.

"Confirmado por el usuario" no es fuente primaria. **Reetiquetar hoy:**
> El catálogo de referencia es `Listado_rubros_ONU_3.xlsx` (18.881 códigos), archivo aportado por el autor. **ChileCompra no publica el tamaño de su catálogo ni la versión de UNSPSC que aplica**, por lo que esta cifra se declara **dato interno del proyecto, no verificado en fuente oficial** (verificación intentada el 21-ago-2026).

Limitación asociada: sin versión declarada, cualquier match exacto puede fallar por deriva de versión.

---

## LO QUE HAY QUE ABRIR A MANO EN UN NAVEGADOR

1. **https://www.bcn.cl/leychile/navegar?idNorma=213004** — versión vigente de la Ley 19.886. Verificar (a) el texto literal del 20 bis; (b) la redacción **consolidada** del artículo 19 tras la Ley 21.634; (c) que "ONU" y "UNSPSC" no aparezcan en ninguna parte, con el buscador interno. Exportar el PDF como respaldo fechado.
2. **Diario Oficial 11-dic-2023, Ley 21.634** — ir a las páginas finales y transcribir el **artículo transitorio** que fija la entrada en vigencia. Es la única forma de citar el 12-dic-2024 desde el texto legal y no desde una nota de prensa.
3. **Los tres diccionarios PDF de la API** (devuelven 403) — descargarlos desde el navegador y buscar dentro: `LR`, `LQ`, `H2`, `R2`, `TD`, `CB`, `5.000 UTM`.
4. **https://datos-abiertos.chilecompra.cl/descargas/complementos** — ver si hay archivo de rubros ONU descargable; anotar nombre, fecha, versión UNSPSC y **contar las filas**. Es lo único que puede convertir el 18.881 en `[FUENTE]`.
5. **https://www.chilecompra.cl/nuevos-compradores/** — dice *"Ya son 58 los nuevos organismos compradores"* sin fecha visible. Anotar la fecha. Con eso queda la serie 58 → 217 (abr-2025) → 327 (universo legal), que es la evidencia dura de que la entrada fue una rampa.
