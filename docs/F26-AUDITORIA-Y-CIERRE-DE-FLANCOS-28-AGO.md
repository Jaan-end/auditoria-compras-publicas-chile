# F26 · Auditoría del proyecto y cierre de flancos — 28-ago-2026

**Sesión 25 · cero cupo consumido en este documento**
**Corridas del día:** `RUN_ID_P0 = c234b182170b` (abortada) · `RUN_ID_P0 = 770618aa84a4` (paso 0)
· paso 0-B, 0-D y 0-E (PC y metadatos, sin acciones de Spark)
**Supersede:** `F24` §2 y §4 · **corrige:** `F25` §2 · **cierra:** `F18` §5.2

---

## 0 · El titular

Se buscaba decidir un cambio de corte. Lo que apareció fue un defecto del corte
**viejo** — una partición inflada al triple — y la pregunta que importaba pasó a
ser otra: **¿cuántas cifras publicadas se caen?**

La respuesta, trazada línea por línea en el código y no por argumento: **ninguna.**
Ni una sola cifra del régimen de evidencia queda contaminada. Las tres razones
están en §2 y cada una es verificable en un archivo concreto del paquete.

Eso convierte el hallazgo en lo que un capstone quiere tener: **un defecto propio,
encontrado por el equipo, medido con tres métodos independientes, y acotado a cero
por inspección del código.** No hay que retractar nada. Hay que declararlo.

---

## 1 · Lo que se midió hoy

### 1.1 · El corte nuevo no es superconjunto del viejo (paso 0, `770618aa84a4`)

| año | lic. viejo | lic. nuevo | delta |
|---|---:|---:|---:|
| 2023 | 149.411 | 149.584 | +173 (+0,12 %) |
| 2024 | 156.470 | 154.142 | −2.328 (−1,49 %) |
| 2025 | 109.398 | 100.532 | −8.866 (−8,10 %) |

Veredicto del protocolo `F23` criterio A4: **NO-GO**. `[MEDIDO]`

### 1.2 · La pérdida es una ventana de descarga, no un defecto del corte nuevo (paso 0-B)

Mes a mes, el déficit ocupa **doce meses contiguos, 2024-11 a 2025-10**, plano en
**−9,80 %** (desviación 1,1 pp), con borde duro a los dos lados. Fuera de esa
ventana los dos cortes calzan: 2023 y 2024 ene-oct dentro de +0,5 %, y 2025-11,
2025-12 y 2026-02 dan delta **exactamente cero**. `[MEDIDO]`

Una tasa constante con bordes duros no es un fenómeno de mercado ni una censura:
es un evento de descarga. Coincide con lo que Gabriel declaró el 28-ago —la
descarga original fue mes a mes entre 2024 y 2025—. Los tramos que calzan vienen
de un bulk posterior, que el propio dato fecha: agosto de 2026 tiene 59
licitaciones en el viejo (≈ un día al ritmo de 270/día) contra 3.050 en el nuevo
(≈ once días). **Corte viejo ≈ 1-ago-2026 · corte nuevo ≈ 11-12-ago-2026.**

Quedan **11.402 licitaciones** presentes en el viejo y ausentes de *todo* el corte
nuevo. Siguen sin explicar (§4.1).

### 1.3 · La partición `lic_historico/year=2025` está inflada ~3× (paso 0-B + 0-E)

Tres métodos independientes, tres veces el mismo resultado:

| método | 2024 | 2025 | cero cupo |
|---|---:|---:|---|
| aritmética por mes (filas viejo ÷ filas nuevo, aislando la ventana) | 2,74× | 3,04× | sí |
| filas por licitación (17-18 es la constante del dato en los dos cortes y los cuatro años) | 19,6 vs 16,3 | **44,0** vs 17,2 | sí |
| bytes por fila declarada del parquet (`dbutils.fs.ls`) | 116,1 | **63,4** (mediana 131,8) | sí |

El 0-E además descarta la explicación fácil: **hay exactamente un parquet por
partición en las veinte particiones**. La duplicación está *dentro* del archivo,
o sea que entró al construirlo, con la lista de CSV de origen repetida. Y es
específica de licitaciones: `oc_historico/2025` da 268,9 bytes/fila, en línea con
2023 (258,6) y 2024 (265,2). `[MEDIDO]`

### 1.4 · `FechaAdjudicacion` y `FechaEstimadaAdjudicacion` son dos columnas distintas (paso 0-D)

El CSV de la re-descarga trae **110 columnas, 18 de fecha**, entre ellas las dos.
En `lic_2023-1.csv`: `FechaAdjudicacion` viene en el 99,9 % de las licitaciones,
**0,00 % en el futuro**, rezago mediano 26 días y p90 75 — que reproduce el
"23-35 días, p90 68-91" que el proyecto ya tenía. `FechaEstimadaAdjudicacion` va
al 100 %, llega a 2029 y tiene 0,01 % en el futuro. `[MEDIDO]`

Dos consecuencias:

1. **`1900-01-01` es el centinela de "sin fecha" de ChileCompra.** Aparece como
   mínimo en cuatro columnas de fecha, y explica los 31 códigos con publicación
   en 1900-01 que salieron en el paso 0-B. **Hoy el pipeline lo parsea como una
   fecha real.** Corrección obligatoria: tratarlo como NULL en todas partes
   (§3.4).
2. La sospecha de `F24` §3 —que el campo de adjudicación fuera una intención y no
   un hecho— **queda descartada para 2023**. Falta el mismo cuadro sobre un mes
   recién cerrado (§4.2): es un `python` de dos minutos y cierra el flanco.

---

## 2 · Radio de daño de la duplicación: **cero cifras publicadas**

No es una opinión tranquilizadora. Son tres hechos verificables, y cada uno se
puede abrir en el archivo que se nombra.

### 2.1 · El dinero no toca la partición afectada `[VERIFICADO]`

Toda la cascada en pesos, la reconciliación 2024 (0,9825), la concentración del
gasto, Q4, el G7 y la Pregunta 2 se calculan sobre **líneas de orden de compra**,
que viven en `oc_historico`. Esa familia de particiones no muestra la anomalía en
ninguno de los diez años (§1.3). El lado licitación entra en el pipeline sólo por
dos puertas, y las dos están cerradas.

> **Corrección de `F25` §2.** Ese documento afirmó que "toda cifra en FILAS del
> corte viejo entre 2024-11 y 2025-10 está inflada ~3×, sumas de montos
> incluidas", y mandó revisar `02-CIERRE-DE-LAS-CIFRAS-EN-PESOS`. **Se
> extralimitó.** La duplicación es de licitaciones; las cifras en pesos son de
> órdenes de compra. `02-CIERRE` no está afectado. Queda anotado acá con el mismo
> peso que la afirmación original, por `§1.20`.

### 2.2 · Puerta 1 — el join usa claves distintas Y lo verifica `[VERIFICADO]`

`etapa2_databricks_v18_COMPLETO.py`, construcción de `df_oc_linked`:

```python
df_lic_keys = (df_lic_all.select(...).filter(...).distinct())
df_oc_linked = df_oc_con_key.join(df_lic_keys, ..., how="left")...
n_linked_filas = df_oc_linked.count()
if n_linked_filas != n_oc_con_key:
    raise AssertionError("El join alteró la cardinalidad de la OC. ...")
```

`.distinct()` colapsa las tres copias de cada licitación en una, y el `assert`
**aborta la corrida** si el join multiplicara filas de OC. Las corridas
`1314c4f6d481`, `70680deaec66`, `fb7c3b125bf7` y `966ab5c4d538` terminaron: el
assert pasó en todas. **No hubo fan-out.** Los peldaños 4-7 de la cascada, el G6
y la cobertura ponderada quedan limpios, y no por suerte: porque alguien puso ese
assert.

### 2.3 · Puerta 2 — la Pregunta 1 cuenta ítems distintos, no filas `[VERIFICADO]`

`celdas_capstone_v12.py`, CAP-1:

```python
c_item_l = cap_resolver(_lic, ["_sinclasificar_Codigoitem", "codigoitem"], "id de línea (lic)")
...
(F.countDistinct("item") if c_item_l else F.count(F.lit(1))).alias("n_lineas_lic"),
```

La columna `_sinclasificar_Codigoitem` **existe con ese nombre exacto** en el
esquema real de licitaciones — está en la lista de 107 columnas que imprime la
prueba de humo (`resultados/cap10_g7_salida_cruda.txt`). `cap_resolver` la
resuelve por coincidencia exacta, sin aviso. Por lo tanto:

- `n_lineas_lic` = `countDistinct(item)` → **inmune** a que la licitación esté tres veces
- `card_lic8` = `size(collect_set(onu8))` → **inmune**
- la `[ADVERTENCIA] no hay id de línea` **nunca se disparó**

Es decir: **el 66,75 % / 13,33 % / 19,73 % y el 80,08 % están intactos**, y
también los tramos de cardinalidad de `01-RESULTADOS` §4.2. Ésta era la cifra
que más se temía perder — la figura 1 del informe es exactamente ese reparto — y
sobrevive por diseño, no por accidente.

### 2.4 · Lo único que sí cambia

`REFERENCIA_FILAS_V1` declara 4.812.550 filas para `lic_historico/2025`. **Ese
número no mide volumen de nada**: es el artefacto. No sirve como denominador y no
se puede citar como "la partición más grande de los diez años". Se corrige en la
prueba de humo o se declara al lado. Ninguna cifra del informe lo usa.

---

## 3 · Flancos cerrados hoy, sin gastar cupo

### 3.1 · `F18` §5.2 — la caída de 2025 del lado licitación **queda explicada** `[MEDIDO]`

`F18` §5.2 dejó abierto: *"106.592 códigos propios en 2025 contra 158.703 en
2024, −33 %, sin explicar. Ninguna cifra del lado licitación desagregada para
2025 es citable mientras no se explique."*

La re-descarga, que es un evento independiente hecho por otra vía y en otra fecha,
**reproduce la caída**: 154.142 licitaciones en 2024 contra 100.532 en 2025. Dos
descargas distintas, la misma caída. **No es un artefacto del corte: es una
propiedad de la fuente.** El asterisco se levanta; lo que corresponde es declarar
la caída como observación y no explicarla con datos que no se tienen.

### 3.2 · La `[ADVERTENCIA]` de `n_lineas_lic` no se disparó — §2.3

### 3.3 · El join no produce fan-out — §2.2

### 3.4 · El centinela `1900-01-01`, y dónde duele `[MEDIDO]`

Hoy `_fecha()` y `pd.to_datetime` lo convierten en una fecha válida de 1900. En
cualquier `min()`, en cualquier rezago publicación→adjudicación y en cualquier
serie por año, ese valor entra como dato. Es un arreglo de una línea y hay que
hacerlo en los tres sitios donde se parsean fechas: `paso0_reconciliacion`,
`auditoria_vintage.py` y la Celda 4 del notebook maestro. `local/fechas_mp.py`
(nuevo, en este paquete) trae la función y el criterio.

### 3.5 · La re-descarga valida el pipeline de conversión `[MEDIDO]`

2023 completo y 2024 ene-oct calzan dentro de +0,5 %, con 17,1 filas por
licitación contra 17,0 del viejo. El esquema, el `latin-1`, el separador `;` y la
asignación a partición **están bien**. Esto vale como validación externa barata y
merece una línea en el informe.

---

## 4 · Flancos abiertos, con quién los cierra y a qué costo

| # | Flanco | Cómo se cierra | Costo |
|---|---|---|---|
| 4.1 | Qué son las 11.402 licitaciones ausentes | `local/paso0c_codigos_perdidos.py`, **sólo sobre 2024-11 y 2024-12** (§4.10) | PC, 0 cupo |
| 4.2 | Si `FechaAdjudicacion` sigue siendo un hecho en un mes recién cerrado | `paso0d --archivo 2026-7` | PC, 0 cupo |
| 4.3 | De dónde salió el ×3 (¿tres CSV por mes en la carpeta de origen?) | inventario PowerShell (`F25` §6), **mirando 2024-11 y 2024-12** (§4.10) | PC, 0 cupo |
| 4.4 | `cap2_serie_onu_por_anio.csv` y `cap9bis_auditoria_year.csv` — **bloquean las figuras 4 y 6** | `notebooks/CAP16_exportar_csv_figuras.py` | 1 corrida |
| 4.5 | `v2_censura_por_anio.csv` — bloquea la figura 7 | `local/auditoria_vintage.py` | PC, 0 cupo |
| 4.6 | CAP-7 (outlier e IVA): sin él no hay cifra en pesos | ya estaba en el plan | 1 corrida |
| 4.7 | Validación manual de 30 casos (F10) | media tarde, sin código | 0 |
| 4.8 | Fecha de descarga del catálogo ONU — bloquea citar el catálogo | preguntarle a quien lo bajó | 0 |
| 4.9 | RUN_ID de las tres cifras del G6 (99,19 / 66,97 / 70,24) — lo exige `§1.21` | buscar en las salidas crudas | 0 |

### 4.10 · Restricción nueva: sólo sobreviven los CSV originales de 2023 y 2024

Gabriel confirmó el 28-ago que los CSV de la descarga original de **2025 y 2026 se
sobrescribieron**; los de 2023 y 2024 se pudieron recuperar. Eso cambia el alcance
de dos flancos y agrega un riesgo:

- **De los doce meses de la ventana, sólo dos tienen su CSV de origen vivo:
  2024-11 y 2024-12.** Son la piedra Rosetta del episodio, porque son los únicos
  donde se puede mirar a la vez el archivo que entró y el parquet que salió.
  Aportan 2.536 de las 11.402 ausentes (22 %) y bastan para contestar la pregunta
  cualitativa —*qué son*—, aunque no para contarlas todas. **2023 sirve de
  control:** ahí no debería faltar ninguna.
- **El ×3 sigue siendo diagnosticable**, porque 2024 también está afectado
  (2,74×) y sus dos meses conservan el origen. Si las carpetas `2024-11` y
  `2024-12` tienen tres CSV y las demás uno, el caso queda cerrado.
- **Riesgo, y es serio:** para 2025 y 2026 el parquet del Volumen es ahora la
  **única copia** de ese dato. No hay de dónde reconstruirlo. Refuerza la regla
  de no borrar nada del Volumen, y agrega una: hacer una copia del parquet de
  `lic_historico/2025` fuera del Volumen antes de cualquier operación sobre esa
  partición.

**4.4 y 4.5 son los que mueven la aguja del informe**: desbloquean tres de las
siete figuras. 4.1 a 4.3 son gratis y cierran el relato del corte.

---

## 5 · La decisión sobre el corte: **no cambiarlo antes de la defensa**

`F23` planteaba reemplazar 2023-2026 con la re-descarga. Con lo medido hoy, la
respuesta es no, y por razones mejores que el NO-GO original:

1. **No compra nada que haga falta.** El motivo del cambio era la censura por
   vintage del lado licitación. Pero ninguna cifra citada hoy depende de eso, y la
   duplicación —el problema real que apareció— no contamina ninguna.
2. **Cuesta todo.** `F22` §1.3 ya lo dice: rehacer la corrida con datos nuevos
   invalida `1314c4f6d481` y arrastra las tres cifras de la Pregunta 1, el
   +0,30 pp de la Pregunta 2 y la reconciliación del 98,25 %. A **una corrida por
   día** (`§1.18`) y con la defensa el **9 de septiembre**, es el peor cambio
   posible de intentar ahora.
3. **Mezcla dos causas.** Un recálculo sobre el corte nuevo confunde duplicación,
   censura por vintage y las 11.402 ausentes en un solo delta ininterpretable.

**Lo que sí hay que hacer con la re-descarga: usarla como instrumento de
validación, no como reemplazo.** Ya rindió tres resultados sin tocar el Volumen —
§3.1, §3.5 y la medición del ×3—. Eso es un capítulo de método, y es más fuerte
que el cambio de corte que se iba a hacer.

El cambio de corte queda como **trabajo futuro declarado**, con su protocolo
(`F23`) y su comparador (`CAP-15`) ya escritos. En un capstone, un protocolo
escrito y deliberadamente no ejecutado, con el motivo dicho, se defiende mejor que
una corrida apurada.

---

## 6 · Qué se declara en el informe (redacción sugerida)

> **Sobre la integridad del corte.** Durante la auditoría del 28-ago-2026 se
> detectó que la partición `lic_historico/year=2025` del corte de trabajo declara
> 4.812.550 filas frente a las ~1,7 millones que su número de licitaciones y la
> densidad observada en el resto del período justifican. Tres estimaciones
> independientes —aritmética mes a mes contra una re-descarga posterior, filas por
> licitación, y bytes por fila del archivo Parquet— sitúan el factor entre 2,7 y
> 3,0 para cada mes del período 2024-11 a 2025-10. La revisión del código
> establece que ninguna cifra reportada se calcula sobre ese conteo: el lado
> licitación entra al análisis únicamente mediante claves deduplicadas —con una
> verificación de cardinalidad que aborta la ejecución si el cruce multiplicara
> filas— y mediante agregados por licitación que cuentan ítems distintos. El
> defecto se documenta por transparencia y su corrección queda declarada como
> trabajo futuro.

> **Sobre la comparabilidad temporal.** El dato abierto de Mercado Público no es
> un archivo estable: el archivo mensual de un mes sigue creciendo después de que
> el mes termina. Comparando dos descargas separadas por once días, el mes con un
> mes de antigüedad creció 23,70 %; con dos meses, 2,95 %; con tres, 1,34 %; con
> cuatro, 0,48 %. Toda serie construida sobre un corte congelado en una fecha
> tiene sus últimos meses subcontados, en una proporción que aquí se mide en vez
> de suponerse.

Las dos declaraciones son `[MEDIDO]` y se sostienen con las salidas crudas que
van en `resultados/corrida_del_28_paso0BDE_salida_cruda.txt`.

---

## 7 · Para el portafolio

El proyecto tiene tres cosas que un revisor técnico busca y casi nunca encuentra:

1. **Un baseline nulo.** La Pregunta 1 no dice "94,44 %, qué alto": dice "94,44 %
   contra un azar medido de 1,38 %". Eso es lo que separa un análisis de una
   descripción.
2. **Placebos que se predeclaran y a veces fallan.** El placebo de unidad de
   medida —un campo que la norma no manda— se mueve igual que el indicador, y por
   eso la conclusión de la Pregunta 2 no dice "efecto". Y CAP-12B se descartó
   porque no replicó su propio criterio predeclarado, teniendo ya el resultado en
   la mano.
3. **Errores propios documentados con su corrección.** `F18` §3 encontró que tres
   de siete errores eran regresiones del propio equipo y escribió una regla nueva
   (`§1.20`) para que no se repitieran. Este documento corrige una afirmación
   escrita hace ocho horas (§2.1).

Para el CV, el titular no es "analicé 51 millones de líneas". Es: **"encontré un
defecto de triplicación en mi propio corte, lo medí con tres métodos
independientes y demostré, trazando el código, que no afectaba ninguna cifra
publicada."** Eso describe a alguien a quien se le pueden confiar datos.

Lo que falta para que el repositorio se defienda solo: un `README` en la raíz que
diga en diez líneas qué se preguntó, qué se encontró y dónde está cada cosa; y que
el régimen de evidencia (`[MEDIDO]`, `[FUENTE]`, `[POR VERIFICAR]`) esté explicado
una vez, arriba, en vez de deducirse de los documentos.
