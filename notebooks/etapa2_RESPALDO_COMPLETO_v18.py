# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# ===========================================================================
# Capstone Big Data — Mercado Público 2017–2026
# ETAPA 2 v10: igual que v9 (join cross-year + G6 completo, CERRADO) + UNA
# CELDA NUEVA AL FINAL (Apéndice C-4, 6D-7/6D-8) que (a) le pone eje de TIEMPO
# a la medición de 6D-5 y mide la antigüedad de la licitación referenciada, y
# (b) prueba la hipótesis de la "línea testimonial" que puede afectar la
# Pregunta 1 del proyecto.
# Autor: Jeancarlo Cuesta — Samsung Innovation Campus 2026
# Plataforma: Databricks Free Edition (serverless)
# Creado: 18-ago-2026 — última revisión: 20-ago-2026 (sesión 10)
# ===========================================================================
#
# QUÉ CAMBIA EN v18 (26-ago-2026, sesión 18) respecto a v17 — ver
# `F19-HALLAZGOS-25-AGO-CELDA9-Y-ANOMALIAS.md` para el detalle y la evidencia:
#   · Celda 4: corregida. Buscaba nombres de columna que no existen
#     (rut_proveedor, fecha_publicacion, fecha_emision_oc, modalidad); las
#     columnas reales llevan prefijo `_sinclasificar_`/PascalCase, y además
#     había un bug de reasignación que anulaba el cast de fecha aunque el
#     nombre calzara. Impacto verificado en cifras congeladas: NINGUNO
#     (nada aguas abajo leía las columnas *_norm que esta celda produce).
#   · Celda 9: REEMPLAZADA por completo. La v17 estaba deshabilitada
#     ("NO CORRER TODAVÍA") y usaba una lógica que nunca generó las cifras
#     congeladas de la Pregunta 1. El código real —CAP-0 + CAP-0-BIS + CAP-1
#     de `celdas_capstone_v12.py`, RUN_ID_CAP=1314c4f6d481— no estaba
#     integrado en este notebook maestro. Gabriel lo recuperó el 25-ago y
#     esta versión lo pega en la Celda 9, en su lugar correcto.
#     CONFIRMADO el 26-ago: Gabriel la corrió (RUN_ID_CAP=70680deaec66) y
#     reprodujo EXACTO 94,44%/1,38%, 80,08% y 13,33%. Ver F19 §2.4.1.
#   · Celda 9-BIS (NUEVA, agregada el mismo 26-ago, después de la Celda 9):
#     dos auditorías baratas — el semi-join del confundidor del 12-dic-2024
#     (mejora nº1 de S17 §5/F18 §4.4) y el conteo de `_year` != año de
#     `fecha_creacion` sobre el universo completo (F19 §2.5). CORRIDAS el
#     26-ago: el 0,28 pp de la Pregunta 2 sobrevive restringido (+0,30 pp);
#     el desborde de `_year` es 0,5982% del universo (307.775/51.451.528),
#     acotado. Ver F19 §2.5.1 y §2.6.
#   · Celda 4 (mismo día, corrección adicional): agregado el par correcto de
#     RUT de la entidad compradora — `_sinclasificar_RutUnidadCompra` (OC) ↔
#     `_sinclasificar_RutUnidad` (Lic) — identificado por Gabriel. `rut_organismo`
#     seguía sin existir con ese nombre; éste es el campo que sí sirve para
#     enlazar lic↔OC por entidad sin pasar por el código de proceso.
# ===========================================================================
#
# QUÉ CAMBIA EN v10 respecto a v9 (20-ago sesión 10) — UNA CELDA NUEVA, NADA MÁS:
#   La corrida real de v9 (`run_id=5fc3ded29565`) entregó 6D-4/5/6. De ahí salen
#   tres resultados [MEDIDO] y una lectura equivocada del propio print:
#     · 6D-6: ausencia CASO A CASO, no hueco estructural (la familia '-LR23'
#       tiene 6.123 códigos y la unidad '5839-' tiene 159; ninguna falta).
#     · 6D-5: el hueco NO castiga a las licitaciones grandes — LR enlaza 94,84%
#       en conteo y 95,89% en monto, MEJOR que LE (91,53%) y LP (84,53%).
#     · 6D-4.D: cada archivo Histórico trae su propio año (99,0% diagonal).
#     · MAL LEÍDO: 6D-4.A imprimió "HIPÓTESIS SOSTENIDA" solo porque el conteo
#       de códigos pre-2026 en el Vigente fue > 0. La magnitud lo desmiente:
#       1.180 códigos de 2025, UNO de 2024, UNO de 2023. Si 'Vigente' retuviera
#       contratos en ejecución habría cientos de LR23/LR24 (existen 6.040 y
#       6.481 códigos de esas familias): hay dos. Eso es forma de "proceso aún
#       abierto", no de "contrato aún vigente" — la hipótesis del corte
#       Histórico/Vigente queda DEBILITADA, no sostenida.
#   La celda nueva (6D-7/6D-8) es la consecuencia directa: 6D-7 repite la
#   medición de 6D-5 pero CON eje de tiempo y con la antigüedad de la licitación
#   referenciada (regla §1.8: 6D-5 promedia 10 años y puede tapar el patrón
#   2024-2026); 6D-8 mide cuántos códigos ONU distintos declara una licitación
#   según su procedimiento, que es lo que decide si la Pregunta 1 es medible en
#   los contratos grandes. DOS acciones de Spark, sin `.persist()`, con guarda de
#   dependencias, y validada corriéndola en PySpark local contra datos sintéticos
#   en SIETE escenarios (incluidos los de borde) antes de entregarse (regla §1.15).
#
#   NINGUNA cifra de G6 ni de reconciliación cambia con esta celda.
#
# QUÉ CAMBIA EN v9 respecto a v8 (20-ago sesión 9) — UNA CELDA NUEVA, NADA MÁS:
#   La corrida real de v8 (`run_id=5fc3ded29565`) hizo lo que tenía que hacer:
#   la Celda 6D-3 corrió limpia (el fix de `.persist()` funcionó) y REFUTÓ la
#   hipótesis de formato. Los CUATRO códigos dieron 0 match exacto, 0 con
#   `trim()` y 0 con `trim()+upper()` — no es un espacio ni una mayúscula: esas
#   licitaciones NO están en la tabla de licitación, bajo ningún formato
#   razonable, en ninguno de los 10 años cargados.
#
#   Además, el usuario aportó la ficha real de `5839-14-LR23` descargada de
#   mercadopublico.cl (PDF, impreso 19-ago-2026 21:57, URL de mercadopublico.cl
#   en el pie), que Claude leyó directamente en el chat. La ficha CONFIRMA:
#   "SUMINISTRO DE FARMACOS", responsable DIRECCION DE SANIDAD DE LA ARMADA,
#   "Tiempo del Contrato = 48 Meses", "Contrato con Renovación: NO". Lo más
#   importante para este diagnóstico: **la ficha existe y está publicada hoy** —
#   o sea que la causa "la licitación fue eliminada/anulada" queda DESCARTADA
#   para este caso. Quedan en pie dos causas posibles, y son distintas:
#     (i)  la licitación nunca entró al export mensual que descargó el usuario
#          (hueco sistemático del universo), o
#     (ii) entró en una categoría/archivo que no se subió al volumen.
#
#   La Celda 6D-4/5/6 (Apéndice C-3, NUEVA) discrimina entre esas dos, y lo
#   hace a escala en vez de con 4 códigos elegidos a mano. Prueba una hipótesis
#   concreta y barata de refutar: el universo cargado tiene `lic_historico`
#   para 2017–2025 y `lic_vigente` SOLO para 2026 ([MEDIDO] Etapa 1,
#   `run_id=d0d8a7bc0329`: `Licitaciones_Nacional_Vigente` = 0 archivos en
#   2017–2025, 8 archivos en 2026). Si el archivo "Vigente" de ChileCompra
#   contiene licitaciones AÚN ACTIVAS publicadas en años anteriores, entonces
#   una licitación de 2023 con contrato de 48 meses —que en 2023 todavía no era
#   "histórica"— pudo quedar fuera de `lic_historico` de su propio año, y su
#   copia "vigente" de 2023 ya no se puede descargar hoy. Eso explicaría, sin
#   invocar ninguna reforma normativa, que falten exactamente las licitaciones
#   grandes y plurianuales.
#
#   El TEST DECISIVO de esa hipótesis es barato y está en 6D-4: mirar si el
#   archivo `lic_vigente` de 2026 (el único "Vigente" que el usuario tiene)
#   contiene códigos con sufijo de años ANTERIORES (LR23, LQ24, ...). Si sí,
#   queda demostrado que "Vigente" arrastra licitaciones de años previos — y por
#   lo tanto que para 2017–2025 el universo cargado carece justamente de esas.
#   Si no, la hipótesis se cae y hay que buscar otra.
#
#   6D-5 mide si la ausencia es SISTEMÁTICA por tipo de procedimiento (¿fallan
#   más las LR/LQ, las licitaciones grandes, que las L1/LE?), en conteo y en
#   monto — porque eso decide si el hueco importa para la Pregunta 1 del
#   proyecto (consistencia ONU lic↔OC): si faltan justo las licitaciones más
#   grandes, esa tasa se mediría sobre una submuestra sesgada, y eso hay que
#   declararlo antes de correr la Celda 9, no después.
#
#   6D-6 aterriza los 4 códigos de 6D-3: pregunta si existe en el universo
#   ALGÚN código de la misma familia de sufijo (`-LR23`, `-LQ24`, ...) y de la
#   misma unidad de compra (`5839-`, `3789-`, ...). Distingue "hueco
#   estructural" (no hay ninguna LR23 en todo el universo) de "ausencia caso a
#   caso" (hay miles de LR23, pero no esta).
#
#   COSTO DE CÓMPUTO: la celda nueva usa exactamente TRES acciones de Spark
#   (dos `.collect()` de tablas agregadas chicas y un `.agg().collect()`), no
#   usa `.persist()` (regla §1.14) y no imprime `.show()` sobre DataFrames
#   grandes. Se escribió así a propósito porque el cupo de Free Edition es
#   finito y se agota con corridas repetidas.
#
#   NINGUNA cifra de G6 ni de reconciliación cambia con esta celda — es
#   puramente exploratoria (Regla 1: no se infiere, se comprueba).
#
# QUÉ CAMBIA EN v8 respecto a v7 (20-ago sesión 8) — FIX DE LA CELDA 6D-3:
#   La corrida real de v7 confirmó que la Celda 7C-5 corregida en v7 SÍ corre
#   limpia (produjo la muestra real de 20 OC de Convenio Marco con ratio
#   extremo, n=577.560 outliers — pendiente (b) con datos reales por primera
#   vez). Pero la Celda 6D-3 (nueva en v7) crasheó con:
#     `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on
#     serverless compute. SQLSTATE: 0A000`
#   Causa raíz: la celda llamaba `.persist()` sobre `df_lic_externo_bruto`.
#   Databricks Free Edition corre exclusivamente en cómputo SERVERLESS (ya
#   documentado en `00-arranque.md`, 18-ago), y ese modo NO soporta
#   `.persist()`/`.cache()` de DataFrame — es una limitación de la
#   plataforma, no un bug de lógica del notebook. FIX: se elimina el
#   `.persist()`/`.unpersist()` y se reemplazan los 3 `.count()` por código
#   (9 pasadas completas por `df_lic_all` solo para esta celda, 12 si se
#   cuentan las 4 iteraciones) por UNA sola agregación (`.agg(...).collect()`)
#   que calcula los tres chequeos (exacto/trim/upper) de los 4 códigos a la
#   vez — una sola pasada por la tabla en vez de doce. Esto además reduce el
#   consumo de cómputo serverless de la corrida, relevante porque el cupo de
#   Free Edition es finito y compartido entre todas las celdas del notebook
#   (ver roadmap 18-ago §7 y `guia-databricks.md`).
#
#   CONTEXTO NUEVO (aportado por el usuario, sesión 8, marcado explícitamente
#   como NO verificado por Claude en este chat — ver nota completa en el
#   docstring de la Celda 6D-3 más abajo): el usuario revisó la ficha de
#   `5839-14-LR23` en mercadopublico.cl y reporta "Tiempo del Contrato = 48
#   Meses". Esto es consistente con lo que ya mostraba la Celda 6D-2 desde la
#   sesión 7 (el mismo `codigo_licitacion` repetido en OC de 2025 Y 2026): en
#   modalidades de suministro o arriendo, una licitación grande puede generar
#   OC durante varios años después de publicada, sin que eso implique ningún
#   error. Se intentó verificar el dato de "48 meses" de forma independiente
#   en este chat (`WebFetch` sobre la ficha pública) y no fue posible: la
#   ficha de mercadopublico.cl se renderiza con JavaScript del lado del
#   cliente y no es legible por las herramientas automáticas de este entorno
#   — la misma limitación ya documentada en
#   `reglas-operativas-proyecto.md` §2.3 para los paneles Power BI del
#   Observatorio. Por eso el dato queda marcado `[POR VERIFICAR]` /
#   "reportado por el usuario, no verificado por Claude", con el mismo
#   estándar de evidencia ya usado antes en este proyecto para observaciones
#   directas del usuario (ver bitácora, 15-ago sesión 4, y 19-ago sesión 7).
#
#   IMPORTANTE — qué SÍ y qué NO explica este dato nuevo: que una licitación
#   dure 48 meses y siga generando OC en años posteriores explica por qué el
#   MISMO `codigo_licitacion` aparece en OC de varios años calendario (ya
#   observado en 6D-2) — eso es normal y no es indicio de nada irregular. Lo
#   que NO explica por sí solo es que el join de la Celda 6 no ENCUENTRE esa
#   licitación: si `5839-14-LR23` se publicó en un año ya cargado (2017–2026),
#   debería existir como fila en `df_lic_all` sin importar cuántos años dure
#   el contrato resultante — la duración del contrato no mueve la fecha de
#   publicación de la licitación. La Celda 6D-3 sigue siendo la prueba que
#   decide si el problema real es de FORMATO (trim/mayúsculas, regla §1.7) o
#   de AUSENCIA del código en el universo cargado (regla §1.13: un dato de
#   contexto no cierra un pendiente por sí solo).
#
#   NINGUNA cifra de G6 ni de reconciliación cambia con este fix — la Celda
#   6D-3 sigue siendo puramente exploratoria (Regla 1: no se infiere, se
#   comprueba).
#
# CÓMO SE USA:
#   1. RECOMENDADO (barato): si ya corriste v8 en esta misma sesión de
#      Databricks y el kernel sigue vivo, NO hace falta correr nada de nuevo —
#      basta pegar la última celda de este archivo (Apéndice C-3, 6D-4/5/6) al
#      final de tu notebook y correr solo esa. Reusa `df_lic_all`,
#      `df_m_linked`, `_key_oc_no_vacia`, `CODIGOS_A_VERIFICAR` y `c_()`, todos
#      ya definidos por las celdas anteriores.
#   2. Si el kernel se reinició o empiezas de cero: File → Import notebook →
#      subir este archivo → correr todas las celdas en orden, igual que
#      v5/v6/v7/v8. En ese caso puedes SALTARTE la Celda 6D-3, que ya cumplió
#      su función (refutó la hipótesis de formato, `run_id=5fc3ded29565`) — se
#      deja en el archivo como documentación de esa prueba, no porque haya que
#      volver a correrla.
#
# QUÉ PRODUCE:
#   - Las mismas tablas y JSON de v5/v6/v7/v8 (sin cambios).
#   - Tablas impresas en consola (NO se guardan a Parquet): muestra de OC CM con
#     ratio extremo (7C-5), muestra de codigo_licitacion del patrón nuevo
#     2024-2026 (6D/6D-2), chequeo de formato del join (6D-3), y —NUEVO en v9—
#     el diagnóstico 6D-4/5/6, cuya salida es la evidencia que decide entre
#     "hueco sistemático del universo de licitación" y "ausencia caso a caso".
#     Esa salida es lo que hay que pegar en el próximo chat.

# COMMAND ----------

# MAGIC %md
# MAGIC # Etapa 2 — Configuración
# MAGIC **Ajustar SOLO esta celda antes de correr el resto.**

# COMMAND ----------

import uuid, datetime
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame
from functools import reduce

# ---------------------------------------------------------------------------
# AJUSTAR AQUÍ — rutas del Volumen
# ---------------------------------------------------------------------------
VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"

# Año para la prueba de humo (recomendado: 2018, tiene [MEDIDO] de referencia)
ANIO_SMOKE = 2018

# Rango completo a procesar después de pasar el smoke test
ANIOS_COMPLETOS = list(range(2017, 2027))

# Ruta de salida — SUBCARPETA dentro del volumen que YA EXISTE
VOLUMEN_SALIDA = f"{VOLUMEN_BASE}/etapa2_output"
# ---------------------------------------------------------------------------

RUN_ID    = uuid.uuid4().hex[:12]
TIMESTAMP = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

print(f"run_id    = {RUN_ID}")
print(f"timestamp = {TIMESTAMP}")
print(f"VOLUMEN_BASE    = {VOLUMEN_BASE}")
print(f"VOLUMEN_SALIDA  = {VOLUMEN_SALIDA}")
print(f"ANIOS_COMPLETOS = {ANIOS_COMPLETOS}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 2 — Prueba de humo
# MAGIC **Si el conteo no calza, DETENER aquí y no avanzar.**

# COMMAND ----------

REFERENCIA_FILAS = {
    2017: {"lic_historico": 3_471_351,  "oc_historico": 6_609_983},  # run_id=fb0230a2b673
    2018: {"lic_historico": 2_976_218,  "oc_historico": 6_614_357},  # run_id=01889c947638
    2019: {"lic_historico": 2_770_490,  "oc_historico": 6_209_651},  # run_id=01889c947638
    2020: {"lic_historico": 1_885_769,  "oc_historico": 4_408_740},  # run_id=649df6092fb4
    2021: {"lic_historico": 1_672_960,  "oc_historico": 4_266_941},  # run_id=649df6092fb4
    2022: {"lic_historico": 2_161_440,  "oc_historico": 4_728_085},  # run_id=6de961bbf738
    2023: {"lic_historico": 2_542_605,  "oc_historico": 5_167_807},  # run_id=6de961bbf738
    2024: {"lic_historico": 3_070_577,  "oc_historico": 5_344_111},  # run_id=8383dcfb2f5e
    2025: {"lic_historico": 4_812_550,  "oc_historico": 5_150_919},  # run_id=8383dcfb2f5e
    2026: {"lic_vigente":     948_445,  "oc_vigente":   2_950_934},  # run_id=d0d8a7bc0329
}

def _cats(anio):
    """Devuelve (cat_lic, cat_oc) según el año."""
    if anio == 2026:
        return "lic_vigente", "oc_vigente"
    return "lic_historico", "oc_historico"

def leer_anio(anio):
    cat_lic, cat_oc = _cats(anio)
    df_lic = spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_lic}/year={anio}")
    df_oc  = spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_oc}/year={anio}")
    return df_lic, df_oc

cat_lic_s, cat_oc_s = _cats(ANIO_SMOKE)
df_lic_s, df_oc_s = leer_anio(ANIO_SMOKE)

n_lic = df_lic_s.count()
n_oc  = df_oc_s.count()

ref_lic = REFERENCIA_FILAS[ANIO_SMOKE][cat_lic_s]
ref_oc  = REFERENCIA_FILAS[ANIO_SMOKE][cat_oc_s]

ok_lic = (n_lic == ref_lic)
ok_oc  = (n_oc  == ref_oc)

print(f"\n{'='*60}")
print(f"PRUEBA DE HUMO — año {ANIO_SMOKE}")
print(f"{'='*60}")
print(f"  Licitaciones → Spark: {n_lic:>12,}  |  pandas ref: {ref_lic:>12,}  {'✅' if ok_lic else '❌'}")
print(f"  OC           → Spark: {n_oc:>12,}  |  pandas ref: {ref_oc:>12,}  {'✅' if ok_oc else '❌'}")
print(f"\n  Columnas lic ({len(df_lic_s.columns)}): {df_lic_s.columns}")
print(f"\n  Columnas OC  ({len(df_oc_s.columns)}):  {df_oc_s.columns}")

if not (ok_lic and ok_oc):
    raise AssertionError(
        "\n❌ PRUEBA DE HUMO FALLÓ.\n"
        "Primera hipótesis: la partición year={} tiene más de un archivo parquet "
        "(duplicación de run_id). Verificar que solo exista un archivo por partición.\n"
        "NO continuar con las celdas siguientes hasta resolver.".format(ANIO_SMOKE)
    )

print(f"\n✅ PRUEBA DE HUMO PASADA — conteos calzan exactamente.")
print("   Puede continuar con las celdas siguientes.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 3 — Cargar universo completo 2017–2026
# MAGIC Solo correr después de pasar la prueba de humo.

# COMMAND ----------

frames_lic = []
frames_oc  = []

for anio in ANIOS_COMPLETOS:
    cat_lic, cat_oc = _cats(anio)
    df_lic = (spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_lic}/year={anio}")
              .withColumn("_year", F.lit(anio))
              .withColumn("_categoria_lic", F.lit(cat_lic)))
    df_oc = (spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_oc}/year={anio}")
             .withColumn("_year", F.lit(anio))
             .withColumn("_categoria_oc", F.lit(cat_oc)))
    frames_lic.append(df_lic)
    frames_oc.append(df_oc)

df_lic_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames_lic)
df_oc_all  = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames_oc)

n_lic_total = df_lic_all.count()
n_oc_total  = df_oc_all.count()

print(f"Total licitaciones 2017–2026 cargadas: {n_lic_total:>14,}")
print(f"Total OC           2017–2026 cargadas: {n_oc_total:>14,}")
print(f"\nEsquema licitaciones ({len(df_lic_all.columns)} columnas):")
df_lic_all.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 4 — Normalización: RUT, fechas, modalidades
# MAGIC **CORREGIDA v18 (26-ago-2026, ver `F19-HALLAZGOS-25-AGO...md` hallazgo 1).**
# MAGIC La versión v17 buscaba nombres de columna "limpios"
# MAGIC (`rut_proveedor`, `fecha_publicacion`, `fecha_emision_oc`, `modalidad`) que
# MAGIC NO existen en el esquema real: las columnas equivalentes SÍ existen, pero
# MAGIC con el prefijo `_sinclasificar_` y en PascalCase
# MAGIC (`_sinclasificar_RutProveedor`, `_sinclasificar_FechaPublicacion`,
# MAGIC `_sinclasificar_Modalidad`) — o, en el caso de la fecha de la OC, con OTRO
# MAGIC nombre ya limpio: `fecha_creacion`, no `fecha_emision_oc`. La celda v17
# MAGIC reportaba "no encontrada" para las cuatro. Además tenía un SEGUNDO bug,
# MAGIC independiente del primero: el bloque de fechas reasignaba `df_ref`
# MAGIC (variable local del `for`) en vez de `df_lic_all`/`df_oc_all`, así que
# MAGIC incluso si el nombre hubiera calzado, el cast a fecha se habría descartado
# MAGIC en silencio. Esta versión resuelve por nombre real (candidatos con y sin
# MAGIC prefijo/guion bajo) y escribe siempre sobre `df_lic_all`/`df_oc_all`.
# MAGIC
# MAGIC **Impacto verificado en las cifras ya congeladas: NINGUNO.** Las columnas
# MAGIC `rut_proveedor_norm`, `rut_organismo_norm` y `modalidad_norm` que esta
# MAGIC celda produce no las lee ninguna otra celda de este notebook ni de
# MAGIC `celdas_capstone_v11/v12` (grep sobre todo el paquete, 25-ago). Corregirla
# MAGIC no cambia ningún resultado citado; se corrige por higiene y porque
# MAGIC `fecha_creacion` (OC) SÍ hace falta más abajo (Apéndice C, `6D-4.A`) y
# MAGIC convenía dejar declarado aquí que la columna existe y cómo se llama.
# MAGIC
# MAGIC `rut_organismo` (con ese nombre exacto) **sigue sin existir** del lado
# MAGIC licitación. **Corrección del 26-ago (Gabriel):** el par correcto para el RUT
# MAGIC de la entidad COMPRADORA —no del proveedor— entre los dos documentos es
# MAGIC `_sinclasificar_RutUnidadCompra` (OC) ↔ `_sinclasificar_RutUnidad` (Lic): es
# MAGIC el mismo concepto (la unidad de compra), con dos nombres distintos porque
# MAGIC vienen de dos endpoints/API distintos de ChileCompra (licitaciones vs.
# MAGIC órdenes de compra). Esta celda ahora resuelve y normaliza también ese par —
# MAGIC es, hoy, el único RUT que permite enlazar la entidad compradora entre lic y
# MAGIC OC sin pasar por el código de proceso.

# COMMAND ----------

def normalizar_rut_udf(rut_raw):
    if rut_raw is None:
        return None
    r = str(rut_raw).strip().upper().replace(".", "").replace(" ", "")
    if "-" not in r and len(r) >= 2:
        r = r[:-1] + "-" + r[-1]
    return r

udf_rut = F.udf(normalizar_rut_udf, T.StringType())


def _c4_resolver(df, candidatos, etiqueta=""):
    """Como cap_resolver (CAP-0), pero normaliza guiones bajos/espacios antes de
    comparar: '_sinclasificar_RutProveedor' y 'rut_proveedor' no comparten
    substring si no se les quita el '_' — ese es el bug de fondo de v17."""
    def _norm(s):
        return s.lower().replace("_", "").replace(" ", "")
    reales = {_norm(c): c for c in df.columns}
    for c in candidatos:
        if _norm(c) in reales:
            return reales[_norm(c)]
    for c in candidatos:
        nc = _norm(c)
        for low, real in reales.items():
            if nc in low:
                return real
    if etiqueta:
        print(f"⚠️  Columna '{etiqueta}' no resuelta. Candidatos: {candidatos}")
    return None


COL_RUT_PROVEEDOR_OC = _c4_resolver(
    df_oc_all, ["rut_proveedor", "RutProveedor"], "rut_proveedor (OC)")
COL_RUT_ORGANISMO_LIC = _c4_resolver(
    df_lic_all, ["rut_organismo", "RutOrganismo"], "rut_organismo (Lic)")
# Par correcto de RUT de la entidad COMPRADORA entre lic y OC (Gabriel, 26-ago):
COL_RUT_UNIDAD_OC = _c4_resolver(
    df_oc_all, ["rut_unidad_compra", "RutUnidadCompra"], "rut_unidad_compra (OC)")
COL_RUT_UNIDAD_LIC = _c4_resolver(
    df_lic_all, ["rut_unidad", "RutUnidad"], "rut_unidad (Lic)")

if COL_RUT_PROVEEDOR_OC:
    df_oc_all = df_oc_all.withColumn("rut_proveedor_norm", udf_rut(F.col(COL_RUT_PROVEEDOR_OC)))
    print(f"RUT proveedor normalizado desde '{COL_RUT_PROVEEDOR_OC}' → columna 'rut_proveedor_norm'")
else:
    print("⚠️  RUT proveedor no encontrado en OC.")

if COL_RUT_ORGANISMO_LIC:
    df_lic_all = df_lic_all.withColumn("rut_organismo_norm", udf_rut(F.col(COL_RUT_ORGANISMO_LIC)))
    print(f"RUT organismo normalizado desde '{COL_RUT_ORGANISMO_LIC}' → columna 'rut_organismo_norm'")
else:
    print("⚠️  RUT organismo (con ese nombre) no existe en Licitaciones. Ver RUT de unidad, abajo.")

if COL_RUT_UNIDAD_OC:
    df_oc_all = df_oc_all.withColumn("rut_unidad_compra_norm", udf_rut(F.col(COL_RUT_UNIDAD_OC)))
    print(f"RUT unidad de compra (OC) normalizado desde '{COL_RUT_UNIDAD_OC}' → columna 'rut_unidad_compra_norm'")
else:
    print("⚠️  RUT de unidad de compra no encontrado en OC.")

if COL_RUT_UNIDAD_LIC:
    df_lic_all = df_lic_all.withColumn("rut_unidad_norm", udf_rut(F.col(COL_RUT_UNIDAD_LIC)))
    print(f"RUT unidad (Lic) normalizado desde '{COL_RUT_UNIDAD_LIC}' → columna 'rut_unidad_norm'")
    print("   NOTA: 'rut_unidad_compra_norm' (OC) y 'rut_unidad_norm' (Lic) son el mismo")
    print("   concepto — la entidad compradora — y hoy son el único RUT que enlaza lic↔OC")
    print("   sin pasar por el código de proceso. Útil como llave alternativa para paneles")
    print("   'por organismo' (CAP-2, Celda 9-BIS) si CodigoOrganismoPublico/CodigoUnidadCompra")
    print("   resultan inestables entre años.")
else:
    print("⚠️  RUT de unidad no encontrado en Licitaciones.")

COL_FECHA_PUBLICACION_LIC = _c4_resolver(
    df_lic_all, ["fecha_publicacion", "FechaPublicacion"], "fecha_publicacion (Lic)")
COL_FECHA_OC = _c4_resolver(
    df_oc_all, ["fecha_creacion", "fecha_emision_oc", "FechaCreacion"], "fecha (OC)")

if COL_FECHA_PUBLICACION_LIC:
    tipo_actual = dict(df_lic_all.dtypes).get(COL_FECHA_PUBLICACION_LIC, "?")
    if tipo_actual not in ("date", "timestamp"):
        print(f"Casteando '{COL_FECHA_PUBLICACION_LIC}' (lic) de '{tipo_actual}' a date")
        df_lic_all = df_lic_all.withColumn(
            "fecha_publicacion_norm", F.to_date(F.col(COL_FECHA_PUBLICACION_LIC)))
    else:
        print(f"'{COL_FECHA_PUBLICACION_LIC}' (lic) ya es {tipo_actual} ✅")
        df_lic_all = df_lic_all.withColumn(
            "fecha_publicacion_norm", F.col(COL_FECHA_PUBLICACION_LIC))
else:
    print("⚠️  Fecha de publicación no encontrada en Licitaciones.")

if COL_FECHA_OC:
    tipo_actual = dict(df_oc_all.dtypes).get(COL_FECHA_OC, "?")
    if tipo_actual not in ("date", "timestamp"):
        print(f"Casteando '{COL_FECHA_OC}' (oc) de '{tipo_actual}' a date")
        df_oc_all = df_oc_all.withColumn("fecha_creacion_norm", F.to_date(F.col(COL_FECHA_OC)))
    else:
        print(f"'{COL_FECHA_OC}' (oc) ya es {tipo_actual} ✅")
        df_oc_all = df_oc_all.withColumn("fecha_creacion_norm", F.col(COL_FECHA_OC))
    print("   NOTA (F19 hallazgo 5): esta es la columna que hay que cruzar contra `_year`")
    print("   antes de leer cualquier tabla 'por año' cerca del borde 2025/2026 — ver Celda 6D.")
else:
    print("⚠️  Fecha de la OC no encontrada.")

COL_MODALIDAD_LIC = _c4_resolver(
    df_lic_all, ["modalidad", "Modalidad"], "modalidad (Lic)")

if COL_MODALIDAD_LIC:
    df_lic_all = df_lic_all.withColumn(
        "modalidad_norm",
        F.when(F.col(COL_MODALIDAD_LIC).isin("LQ", "H2"), F.lit("LP"))
         .otherwise(F.col(COL_MODALIDAD_LIC))
    )
    print(f"Modalidades homologadas (LQ/H2→LP) desde '{COL_MODALIDAD_LIC}' → columna 'modalidad_norm'")
else:
    print("⚠️  Modalidad no encontrada en Licitaciones.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 5 — Normalización de montos (deflactar a CLP real)
# MAGIC **Requiere tabla de IPC empalmado.** Se salta hasta tener el archivo.

# COMMAND ----------

IPC_DISPONIBLE = False

if IPC_DISPONIBLE:
    df_ipc = spark.read.csv(f"{VOLUMEN_BASE}/ipc_mensual.csv", header=True, inferSchema=True)
    COL_MONTO_OC_IPC = "precio_neto_oc"
    COL_FECHA_MES_OC = "fecha_emision_oc"
    df_oc_all = (
        df_oc_all
        .withColumn("_anio_oc", F.year(F.col(COL_FECHA_MES_OC)))
        .withColumn("_mes_oc",  F.month(F.col(COL_FECHA_MES_OC)))
        .join(df_ipc.select(F.col("anio").alias("_anio_oc"), F.col("mes").alias("_mes_oc"), "ipc_empalmado"),
              on=["_anio_oc", "_mes_oc"], how="left")
        .withColumn("precio_neto_clp_real",
                    F.when(F.col("ipc_empalmado").isNotNull(),
                           F.col(COL_MONTO_OC_IPC) / F.col("ipc_empalmado") * 100.0)
                     .otherwise(F.col(COL_MONTO_OC_IPC)))
        .drop("_anio_oc", "_mes_oc", "ipc_empalmado")
    )
    print("✅ Montos deflactados → columna 'precio_neto_clp_real'")
else:
    print("⏭️  Normalización de montos omitida (IPC_DISPONIBLE=False).")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 6 — Enlace OC↔Licitación cross-year (Motor de Cotejo) + estratificación por tipo_oc
# MAGIC Versión FINAL, sin cambios desde el 19-ago sesión 2/3.

# COMMAND ----------

COL_KEY_OC  = "codigo_licitacion"
COL_KEY_LIC = "codigo_externo"
COL_ID_OC   = "codigo_oc"

def c_(nombre):
    return F.col(f"`{nombre}`")

def sanitizar_columnas(df):
    nuevos = []
    for n in df.columns:
        limpio = (n.replace(".", "_").replace(" ", "_").replace(",", "_")
                   .replace(";", "_").replace("{", "_").replace("}", "_")
                   .replace("(", "_").replace(")", "_").replace("=", "_")
                   .replace("\n", "_").replace("\t", "_"))
        nuevos.append(c_(n).alias(limpio))
    return df.select(nuevos)

for _col, _df, _lbl in [(COL_KEY_OC, df_oc_all, "OC"),
                        (COL_KEY_LIC, df_lic_all, "Licitaciones"),
                        (COL_ID_OC, df_oc_all, "OC")]:
    if _col not in _df.columns:
        raise KeyError(f"Columna '{_col}' no encontrada en {_lbl}. Columnas: {_df.columns}")

n_lic_filas  = df_lic_all.count()
n_lic_claves = df_lic_all.select(c_(COL_KEY_LIC)).distinct().count()
n_oc_filas   = df_oc_all.count()
n_oc_docs    = df_oc_all.select(c_(COL_ID_OC)).distinct().count()

print("=" * 66)
print("DIAGNÓSTICO DE GRANULARIDAD (antes de unir)")
print("=" * 66)
print(f"  Licitación — filas:            {n_lic_filas:>14,}")
print(f"  Licitación — codigo_externo:   {n_lic_claves:>14,} distintos")
print(f"  Licitación — filas por clave:  {n_lic_filas / max(n_lic_claves,1):>14,.2f}  (promedio)")
print(f"  OC         — filas:            {n_oc_filas:>14,}")
print(f"  OC         — codigo_oc:        {n_oc_docs:>14,} distintos")
print(f"  OC         — filas por OC:     {n_oc_filas / max(n_oc_docs,1):>14,.2f}  (promedio)")

COL_TIPO_OC        = "tipo_oc"
COL_PROCEDENCIA_OC = "procedencia_oc"

_key_oc_no_vacia_diag = c_(COL_KEY_OC).isNotNull() & (F.trim(c_(COL_KEY_OC)) != "")
tipos_sin_licitacion_por_diseno = []

if COL_TIPO_OC in df_oc_all.columns:
    df_tipo_diag = (
        df_oc_all
        .withColumn("_tiene_clave_oc", _key_oc_no_vacia_diag)
        .groupBy(c_(COL_TIPO_OC).alias("tipo_oc"))
        .agg(F.count("*").alias("n_filas"),
             F.sum(F.col("_tiene_clave_oc").cast("int")).alias("n_con_clave"))
        .withColumn("pct_con_clave", F.round(100 * F.col("n_con_clave") / F.col("n_filas"), 2))
        .orderBy(F.col("n_filas").desc())
    )
    print("=" * 66)
    print("6.0-bis — COBERTURA DE codigo_licitacion POR tipo_oc (universo completo)")
    print("=" * 66)
    df_tipo_diag.show(30, truncate=False)

    UMBRAL_ESTRUCTURAL_PCT = 5.0
    tipos_sin_licitacion_por_diseno = [
        r["tipo_oc"] for r in
        df_tipo_diag.filter(F.col("pct_con_clave") < UMBRAL_ESTRUCTURAL_PCT).collect()
    ]
    print(f"\n  tipo_oc clasificados como 'sin licitación por diseño': {tipos_sin_licitacion_por_diseno}")
else:
    print(f"AVISO: columna '{COL_TIPO_OC}' no encontrada — se omite la estratificación.")

# COMMAND ----------

_key_oc_no_vacia = c_(COL_KEY_OC).isNotNull() & (F.trim(c_(COL_KEY_OC)) != "")

df_oc_con_key = df_oc_all.filter(_key_oc_no_vacia)
df_oc_sin_key = df_oc_all.filter(~_key_oc_no_vacia | c_(COL_KEY_OC).isNull())

n_oc_total_join = n_oc_filas
n_oc_con_key    = df_oc_con_key.count()
n_oc_sin_key    = n_oc_total_join - n_oc_con_key

print()
print("=" * 66)
print("COBERTURA DE CLAVE EN OC (nivel línea)")
print("=" * 66)
print(f"  OC totales (filas):     {n_oc_total_join:>14,}")
print(f"  Con codigo_licitacion:  {n_oc_con_key:>14,}  ({100*n_oc_con_key/n_oc_total_join:.2f}%)")
print(f"  Sin codigo_licitacion:  {n_oc_sin_key:>14,}  ({100*n_oc_sin_key/n_oc_total_join:.2f}%)")

df_lic_keys = (
    df_lic_all
    .select(c_(COL_KEY_LIC).alias("_lic_key"))
    .filter(F.col("_lic_key").isNotNull() & (F.trim(F.col("_lic_key")) != ""))
    .distinct()
)

df_oc_linked = (
    df_oc_con_key
    .join(df_lic_keys, c_(COL_KEY_OC) == F.col("_lic_key"), how="left")
    .withColumn("tiene_licitacion_origen", F.col("_lic_key").isNotNull())
)

n_linked_filas = df_oc_linked.count()
if n_linked_filas != n_oc_con_key:
    raise AssertionError(
        f"\n❌ El join alteró la cardinalidad de la OC.\n"
        f"   Antes: {n_oc_con_key:,} filas — Después: {n_linked_filas:,} filas.\n"
        f"   df_lic_keys no era único. NO usar estas cifras para G6."
    )

n_oc_linked   = df_oc_linked.filter(F.col("tiene_licitacion_origen")).count()
n_oc_unlinked = n_oc_con_key - n_oc_linked

print()
print("=" * 66)
print("ENLACE CROSS-YEAR — resultado (nivel línea)")
print("=" * 66)
print(f"  ✅ Cardinalidad preservada: {n_linked_filas:,} filas (== OC con clave)")
print(f"  Con clave → licitación ENCONTRADA:    {n_oc_linked:>14,}  ({100*n_oc_linked/n_oc_con_key:.2f}%)")
print(f"  Con clave → licitación NO encontrada: {n_oc_unlinked:>14,}  ({100*n_oc_unlinked/n_oc_con_key:.2f}%)")

print()
print("No encontradas, por año de la OC:")
(df_oc_linked
 .groupBy("_year")
 .agg(F.count("*").alias("n_con_clave"),
      F.sum(F.col("tiene_licitacion_origen").cast("int")).alias("n_enlazadas"))
 .withColumn("pct_enlazadas", F.round(100 * F.col("n_enlazadas") / F.col("n_con_clave"), 2))
 .orderBy("_year")
 .show(15, truncate=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 7 — G6 completo: por conteo y por monto (CITABLE COMO [MEDIDO])

# COMMAND ----------

g6_linea_pct = 100 * n_oc_linked / n_oc_total_join
g6_calidad_enlace_pct = 100 * n_oc_linked / n_oc_con_key

n_docs_total   = n_oc_docs
n_docs_linked  = (df_oc_linked
                  .filter(F.col("tiene_licitacion_origen"))
                  .select(c_(COL_ID_OC)).distinct().count())
g6_doc_pct = 100 * n_docs_linked / n_docs_total

print("=" * 66)
print("G6 POR CONTEO — universo 2017–2026 completo")
print(f"run_id = {RUN_ID}   |   timestamp = {TIMESTAMP}")
print("=" * 66)
print(f"  (a) NIVEL LÍNEA — enlazadas / total filas OC")
print(f"      {n_oc_linked:,} / {n_oc_total_join:,} = {g6_linea_pct:.2f}%")
print(f"  (b) NIVEL DOCUMENTO — OC con ≥1 línea enlazada / OC distintas")
print(f"      {n_docs_linked:,} / {n_docs_total:,} = {g6_doc_pct:.2f}%")
print(f"  (c) CALIDAD DEL ENLACE — enlazadas / OC con clave")
print(f"      {n_oc_linked:,} / {n_oc_con_key:,} = {g6_calidad_enlace_pct:.2f}%")

if COL_TIPO_OC in df_oc_all.columns and len(tipos_sin_licitacion_por_diseno) > 0:
    df_oc_relevante_total = df_oc_all.filter(~c_(COL_TIPO_OC).isin(tipos_sin_licitacion_por_diseno))
    n_relevante_total = df_oc_relevante_total.count()
    df_oc_linked_relevante = df_oc_linked.filter(~c_(COL_TIPO_OC).isin(tipos_sin_licitacion_por_diseno))
    n_relevante_enlazadas = df_oc_linked_relevante.filter(F.col("tiene_licitacion_origen")).count()
    g6_restringido_pct = (100 * n_relevante_enlazadas / n_relevante_total if n_relevante_total else float("nan"))
    print(f"  (d) RESTRINGIDO — excluye tipo_oc {tipos_sin_licitacion_por_diseno} del denominador")
    print(f"      {n_relevante_enlazadas:,} / {n_relevante_total:,} = {g6_restringido_pct:.2f}%")
else:
    g6_restringido_pct = float("nan")
    print("  (d) RESTRINGIDO — omitido: falta tipo_oc o no se detectaron tipos estructurales.")

COL_MONTO     = "precio_neto_oc"
COL_CANT_OC   = "cantidad_oc"
COL_MONTO_DOC = "monto_total_oc_clp"
COL_MONEDA    = "moneda_item"
UMBRAL_AMBIGUO_PCT = 0.10

def _clasificar_patron(col):
    v = F.trim(col)
    return (F.when(v.isNull() | (v == ""), "0_nulo_o_vacio")
              .when(v.rlike(r"^-?\d+$"),                    "A_entero_puro")
              .when(v.rlike(r"^-?\d+,\d+$"),                "B_coma_decimal")
              .when(v.rlike(r"^-?\d{1,3}(\.\d{3})+,\d+$"),  "C_punto_miles_coma_decimal")
              .when(v.rlike(r"^-?\d{1,3}(\.\d{3})+$"),      "D_AMBIGUO_solo_punto_grupos3")
              .when(v.rlike(r"^-?\d+\.\d+$"),               "E_AMBIGUO_punto_no_grupo3")
              .when(v.rlike(r"^-?\d+(?:[.,]\d+)?[eE][+-]?\d+$"), "G_cientifica")
              .otherwise("F_OTRO_no_reconocido"))

def _castear_seguro(col):
    v = F.trim(col)
    patron = _clasificar_patron(col)
    seguro = patron.isin("A_entero_puro", "B_coma_decimal",
                          "C_punto_miles_coma_decimal", "G_cientifica")
    return F.when(
        seguro,
        F.regexp_replace(F.regexp_replace(v, r"\.(?=\d{3}(,|$))", ""), ",", ".").cast("double")
    ).otherwise(F.lit(None).cast("double"))

df_oc_m = (df_oc_all
           .withColumn("_monto_num", _castear_seguro(c_(COL_MONTO)))
           .withColumn("_cant_num", _castear_seguro(c_(COL_CANT_OC)))
           .withColumn("_monto_doc_num", _castear_seguro(c_(COL_MONTO_DOC)))
           .withColumn("_monto_linea_total", F.col("_monto_num") * F.col("_cant_num")))

print("\n" + "=" * 66)
print("7.2 — CONTROL DE CALIDAD DEL CASTEO")
print("=" * 66)
for _colname, _numcol in ((COL_MONTO, "_monto_num"), (COL_CANT_OC, "_cant_num"), (COL_MONTO_DOC, "_monto_doc_num")):
    _v = F.trim(c_(_colname))
    _n_no_cast = df_oc_m.filter(F.col(_numcol).isNull() & _v.isNotNull() & (_v != "")).count()
    _pct = 100 * _n_no_cast / n_oc_filas
    print(f"  {_colname}: {_n_no_cast:,} filas no casteables de forma segura ({_pct:.4f}%)")
    if _pct > UMBRAL_AMBIGUO_PCT:
        raise AssertionError(
            f"\n❌ RECHAZADO — {_colname}: {_pct:.4f}% no casteable (umbral: {UMBRAL_AMBIGUO_PCT}%)."
        )
print("  ✅ Las tres columnas bajo el umbral. Continuando con G6 por monto.")

df_m_linked = (df_oc_m
    .join(df_lic_keys, c_(COL_KEY_OC) == F.col("_lic_key"), "left")
    .withColumn("tiene_lic", F.col("_lic_key").isNotNull() & _key_oc_no_vacia))

n_check = df_m_linked.count()
if n_check != n_oc_filas:
    raise AssertionError(f"❌ El join alteró la cardinalidad ({n_check:,} != {n_oc_filas:,}).")
print(f"\n✅ Cardinalidad preservada: {n_check:,} filas.")

print("\n" + "=" * 66)
print("7.3 — G6 POR MONTO")
print("=" * 66)
for _desde in (2017, 2019, 2020):
    print(f"\n  ── Universo {_desde}-2026 ──")
    (df_m_linked.filter(F.col("_year") >= _desde)
     .groupBy(c_(COL_MONEDA).alias("moneda"))
     .agg(F.count("*").alias("n_filas"),
          F.sum(F.col("_monto_linea_total").isNull().cast("int")).alias("n_sin_monto"),
          F.sum("_monto_linea_total").alias("monto_total"),
          F.sum(F.when(F.col("tiene_lic"), F.col("_monto_linea_total"))).alias("monto_enlazado"))
     .withColumn("g6_monto_pct", F.round(100 * F.col("monto_enlazado") / F.col("monto_total"), 2))
     .orderBy(F.col("monto_total").desc())
     .show(20, truncate=False))

print("\n" + "=" * 66)
print("7.4 — G6 POR MONTO, POR tipo_oc")
print("=" * 66)
if COL_TIPO_OC in df_m_linked.columns:
    (df_m_linked
     .filter(c_(COL_MONEDA) == "CLP")
     .groupBy(c_(COL_TIPO_OC).alias("tipo_oc"))
     .agg(F.count("*").alias("n_filas"),
          F.round(F.avg("_monto_linea_total"), 0).alias("monto_promedio_linea"),
          F.sum("_monto_linea_total").alias("monto_total"),
          F.sum(F.when(F.col("tiene_lic"), F.col("_monto_linea_total"))).alias("monto_enlazado"))
     .withColumn("g6_monto_pct", F.round(100 * F.coalesce(F.col("monto_enlazado"), F.lit(0.0)) / F.col("monto_total"), 2))
     .orderBy(F.col("monto_total").desc())
     .show(20, truncate=False))
else:
    print(f"  Se omite: '{COL_TIPO_OC}' no encontrada.")

print()
print(f"[MEDIDO] run_id={RUN_ID} — {TIMESTAMP}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 7C — Reconciliación de escala de precio_neto_oc (EVIDENCIA/AUDITORÍA)

# COMMAND ----------

df_recon = (
    df_oc_m
    .groupBy(c_(COL_ID_OC).alias("_oc_id"))
    .agg(F.sum("_monto_linea_total").alias("suma_precio_x_cantidad"),
         F.first("_monto_doc_num").alias("monto_total_oc_clp"),
         F.count("*").alias("n_lineas"),
         F.first(c_(COL_TIPO_OC)).alias("tipo_oc") if COL_TIPO_OC in df_oc_m.columns else F.lit(None).alias("tipo_oc"))
    .filter(F.col("monto_total_oc_clp").isNotNull() & (F.col("monto_total_oc_clp") != 0)
            & F.col("suma_precio_x_cantidad").isNotNull()
            & (F.col("suma_precio_x_cantidad") != 0))
    .withColumn("ratio", F.col("monto_total_oc_clp") / F.col("suma_precio_x_cantidad"))
)

n_recon = df_recon.count()
print("=" * 70)
print(f"7C — RECONCILIACIÓN precio_neto_oc × cantidad_oc  vs.  monto_total_oc_clp")
print(f"     (n = {n_recon:,})")
print("=" * 70)
df_recon.select(
    F.round(F.mean("ratio"), 4).alias("ratio_promedio_NO_ROBUSTO"),
    F.round(F.expr("percentile_approx(ratio, 0.5)"), 4).alias("ratio_mediana"),
    F.round(F.expr("percentile_approx(ratio, 0.25)"), 4).alias("p25"),
    F.round(F.expr("percentile_approx(ratio, 0.75)"), 4).alias("p75"),
    F.round(F.expr("percentile_approx(ratio, 0.1)"), 4).alias("p10"),
    F.round(F.expr("percentile_approx(ratio, 0.9)"), 4).alias("p90"),
).show(truncate=False)

BANDA_PLAUSIBLE = (0.5, 3.0)
n_outliers = df_recon.filter((F.col("ratio") < BANDA_PLAUSIBLE[0]) | (F.col("ratio") > BANDA_PLAUSIBLE[1])).count()
print(f"\n  OC con ratio fuera de banda plausible {BANDA_PLAUSIBLE}: {n_outliers:,} de {n_recon:,} "
      f"({100*n_outliers/n_recon:.4f}%)")

print("\n" + "=" * 70)
print("7C-4 — DESGLOSE DE OUTLIERS POR tipo_oc")
print("=" * 70)
if COL_TIPO_OC in df_recon.columns:
    (df_recon
     .withColumn("es_outlier", (F.col("ratio") < BANDA_PLAUSIBLE[0]) | (F.col("ratio") > BANDA_PLAUSIBLE[1]))
     .groupBy("tipo_oc")
     .agg(F.count("*").alias("n_oc"), F.sum(F.col("es_outlier").cast("int")).alias("n_outliers"))
     .withColumn("pct_outlier_de_este_tipo", F.round(100 * F.col("n_outliers") / F.col("n_oc"), 4))
     .orderBy(F.col("n_outliers").desc())
     .show(20, truncate=False))

print(f"\n[MEDIDO] run_id={RUN_ID} — {TIMESTAMP}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 8 — Guardar resultados

# COMMAND ----------

df_oc_linked_seguro = sanitizar_columnas(df_oc_linked)

ruta_salida_oc = f"{VOLUMEN_SALIDA}/oc_enlazada/{RUN_ID}"
df_oc_linked_seguro.write.mode("overwrite").partitionBy("_year").parquet(ruta_salida_oc)
print(f"✅ OC enlazada guardada en: {ruta_salida_oc}")

resumen = {
    "run_id":          RUN_ID,
    "timestamp":       TIMESTAMP,
    "n_oc_total":      n_oc_total_join,
    "n_oc_con_key":    n_oc_con_key,
    "n_oc_linked":     n_oc_linked,
    "n_oc_unlinked":   n_oc_unlinked,
    "n_oc_sin_key":    n_oc_sin_key,
    "g6_linea_pct":          round(g6_linea_pct, 4),
    "g6_doc_pct":            round(g6_doc_pct, 4),
    "g6_calidad_enlace_pct": round(g6_calidad_enlace_pct, 4),
    "g6_restringido_pct":    (round(g6_restringido_pct, 4) if g6_restringido_pct == g6_restringido_pct else None),
    "tipos_sin_licitacion_por_diseno": str(tipos_sin_licitacion_por_diseno),
    "n_lic_total":     n_lic_total,
    "anios":           str(ANIOS_COMPLETOS),
    "reconciliacion_precio_neto_oc": "precio_neto_oc es precio unitario neto (confirmado [MEDIDO] "
                                      "run_id=4ec18c3276e3, 19-ago sesion 4; mediana ratio=1.19)",
}

import json
ruta_resumen = f"{VOLUMEN_SALIDA}/resumen_{RUN_ID}.json"
dbutils.fs.put(ruta_resumen, json.dumps(resumen, indent=2), overwrite=True)
print(f"✅ Resumen guardado en: {ruta_resumen}")
print()
print("--- Copiar a la bitácora del proyecto ---")
for k, v in resumen.items():
    print(f"  {k}: {v}")

# MAGIC %md
# MAGIC # Celda 9 — Pregunta 1 del proyecto: consistencia ONU lic↔OC (CORREGIDA v18)
# MAGIC **Reemplaza por completo a la Celda 9 de v17.** Ver
# MAGIC `F19-HALLAZGOS-25-AGO-CELDA9-Y-ANOMALIAS.md`, hallazgo 4.
# MAGIC
# MAGIC **Qué estaba mal.** La Celda 9 de v17 estaba marcada
# MAGIC `NO CORRER TODAVÍA — pendiente rediseño` y usaba una lógica propia,
# MAGIC mucho más simple (comparación directa de 4 dígitos, sin baseline nulo, sin
# MAGIC separar universos, sin parsear formato numérico chileno) y ADEMÁS buscaba
# MAGIC una columna que no existe (`lic_onu_lic`; la real es `onu_lic`). Esa celda
# MAGIC **nunca produjo** las cifras congeladas de la Pregunta 1 que el proyecto cita
# MAGIC en todos los traspasos (94,44% de consistencia real vs. 1,38% de baseline
# MAGIC nulo; 80,08% de licitaciones con a lo más un código de producto; 13,33% de
# MAGIC patología testimonial). Esas cifras salieron de `celdas_capstone_v12.py`
# MAGIC (CAP-0 + CAP-0-BIS + CAP-1 v12), bajo `RUN_ID_CAP = 1314c4f6d481` — un
# MAGIC archivo que no estaba en el paquete de traspaso ni integrado en este
# MAGIC notebook maestro. Gabriel lo recuperó el 25-ago y esta celda lo integra.
# MAGIC
# MAGIC **Qué hace esta celda ahora.** Pega, en una sola celda de Databricks, la
# MAGIC cadena completa CAP-0 (utilidades, de `celdas_capstone_v11.py`, sin cambios)
# MAGIC + CAP-0-BIS (parche de numéricos y resolvedor parlante, `v12`) + CAP-1 v12
# MAGIC (Pregunta 1 con baseline nulo permutado, separación en 5 universos
# MAGIC A–E, y cobertura ponderada por línea y por CLP — la CAP-1.C recomendada
# MAGIC como titular). Usa `df_lic_all` y `df_oc_linked`, que ya existen en el
# MAGIC kernel al llegar aquí (Celdas 3 y 6 de este mismo notebook).
# MAGIC
# MAGIC **No se pudo ejecutar ni verificar en esta sesión** (no hay acceso a
# MAGIC Databricks desde aquí): es texto trasladado del origen verificado, no una
# MAGIC corrida nueva. Antes de citar cualquier número que imprima, correrla y
# MAGIC confirmar que reproduce 1314c4f6d481 — si no reproduce exacto, se declara
# MAGIC como corrida nueva con su propio `RUN_ID_CAP`, nunca se le pone el nombre de
# MAGIC la corrida vieja encima (regla §1.20).
# MAGIC
# MAGIC **Para las Preguntas 2 y 3, la cascada en pesos, G6 y la exportación a CSV:**
# MAGIC pegar a continuación, en celdas propias y en orden, CAP-2 v12, CAP-3 v12,
# MAGIC CAP-4 v12 y CAP-6 de `notebooks/celdas_capstone_v12.py` (incluido en este
# MAGIC paquete). No se copiaron aquí para no encadenar en una sola celda cuatro
# MAGIC acciones caras de Spark que conviene poder correr (o saltar) por separado,
# MAGIC bajo la regla de una corrida de Databricks por día (§1.18).

# COMMAND ----------

# =============================================================================
#  CAP-0 · UTILIDADES COMUNES  (OBLIGATORIA — correr antes que cualquier otra)
#  Fuente: celdas_capstone_v11.py (sin cambios). Es la que generó, junto con
#  CAP-0-BIS y CAP-1 de celdas_capstone_v12.py, las cifras congeladas de la
#  Pregunta 1 bajo RUN_ID_CAP = 1314c4f6d481.
# =============================================================================
import uuid as _uuid

from pyspark.sql import functions as F
from pyspark.sql import Window as W

RUN_ID_CAP = _uuid.uuid4().hex[:12]
print("=" * 78)
print(f"CAP-0 · utilidades cargadas · RUN_ID_CAP = {RUN_ID_CAP}")
print("=" * 78)

# --- Umbrales y parámetros, TODOS declarados y arbitrarios (regla §1.16.b) ----
SEED_NULO = 20260821          # semilla del baseline nulo permutado (reproducible)
TOP_N_PRECIO = 30             # códigos ONU para el análisis de dispersión de precio
BANDA_RECON = (0.98, 1.02)    # banda de reconciliación monto_doc / (precio × cantidad)
UMBRAL_BRECHA_PP = 5.0        # brecha real−nulo que se considera "señal", en pp
ANIO_MIN, ANIO_MAX = 2017, 2026
# Cifra oficial ChileCompra 2024 para reconciliación externa [FUENTE]:
# "alcanzaron a US$17.643 millones, equivalentes a $16.653.131 millones de pesos"
# https://www.chilecompra.cl/2025/06/montos-transados-en-la-plataforma-mercado-publico-superaron-los-us-17-643-millones-en-2024/
OFICIAL_2024_CLP = 16_653_131 * 1_000_000
OFICIAL_2024_NUM_OC = 2_031_670


def cap_resolver(df, candidatos, etiqueta=""):
    """Devuelve el primer nombre de columna que exista en df (case-insensitive).

    Devuelve None si ninguno existe. NUNCA lanza excepción: la celda que llama
    decide si puede seguir sin esa columna o si debe abortar con un mensaje.
    """
    if df is None:
        return None
    reales = {c.lower(): c for c in df.columns}
    for c in candidatos:
        if c.lower() in reales:
            return reales[c.lower()]
    for c in candidatos:                       # segunda pasada: coincidencia parcial
        for low, real in reales.items():
            if c.lower() in low:
                return real
    if etiqueta:
        print(f"   [aviso] no se resolvió la columna '{etiqueta}'. "
              f"Candidatos probados: {candidatos}")
    return None


def cap_global(nombre, default=None):
    """Lee una variable global del notebook sin romper si no existe (regla §1.15.4)."""
    try:
        return eval(nombre)
    except (NameError, SyntaxError):
        return default


def cap_onu_norm(col):
    """Normaliza un código ONU crudo a texto: quita espacios y el sufijo '.0'
    que deja pandas cuando la columna se leyó como float."""
    c = F.trim(col.cast("string"))
    c = F.regexp_replace(c, r"\.0+$", "")
    return F.when((c == "") | (c.isNull()), F.lit(None)).otherwise(c)


def cap_onu_clase(col):
    """Clasifica el código ONU en clases MUTUAMENTE EXCLUYENTES.

    AUSENTE      : null o vacío
    CERO         : '0' / '00000000' → el hueco conocido de Convenio Marco
    VALIDO_8     : exactamente 8 dígitos → código UNSPSC en forma
    NO_NUMERICO  : contiene letras (aquí caen ZGEN y cualquier código propio)
    OTRO_FORMATO : numérico pero de largo distinto de 8
    """
    c = cap_onu_norm(col)
    return (F.when(c.isNull(), F.lit("AUSENTE"))
             .when(F.regexp_replace(c, "0", "") == "", F.lit("CERO"))
             .when(c.rlike(r"^[0-9]{8}$"), F.lit("VALIDO_8"))
             .when(c.rlike(r"^[0-9]+$"), F.lit("OTRO_FORMATO"))
             .otherwise(F.lit("NO_NUMERICO")))


def cap_onu8(col):
    """Devuelve el código ONU solo cuando es válido de 8 dígitos; si no, null."""
    c = cap_onu_norm(col)
    return F.when(c.rlike(r"^[0-9]{8}$"), c).otherwise(F.lit(None))


def cap_unidad_compra(col_codigo_lic):
    """Prefijo de unidad de compra del código de proceso ('5839-14-LR23' → '5839')."""
    c = F.trim(col_codigo_lic.cast("string"))
    return F.when(c.contains("-"), F.split(c, "-").getItem(0)).otherwise(F.lit(None))


def cap_unidad_dual(col_codigo_lic, col_codigo_oc):
    """Unidad de compra tomada del código de licitación y, si no hay, del de la OC."""
    a = cap_unidad_compra(col_codigo_lic) if col_codigo_lic is not None else F.lit(None)
    b = cap_unidad_compra(col_codigo_oc) if col_codigo_oc is not None else F.lit(None)
    return F.coalesce(a, b)


def cap_sufijo_tipo(col_codigo_lic):
    """Tipo de proceso tomado del ÚLTIMO segmento del código ('...-LR23' → 'LR')."""
    c = F.trim(col_codigo_lic.cast("string"))
    ult = F.element_at(F.split(c, "-"), -1)
    return F.when(F.length(ult) >= 3, F.upper(F.substring(ult, 1, 2))).otherwise(F.lit(None))


def cap_anio_sufijo(col_codigo_lic):
    """Año implícito en el sufijo del código ('...-LR23' → 2023)."""
    c = F.trim(col_codigo_lic.cast("string"))
    ult = F.element_at(F.split(c, "-"), -1)
    dd = F.regexp_extract(ult, r"([0-9]{2})$", 1)
    return F.when(dd != "", (F.lit(2000) + dd.cast("int"))).otherwise(F.lit(None))


def cap_bucket_card(col):
    """Agrupa la cardinalidad del conjunto declarado en tramos comparables."""
    return (F.when(col <= 1, F.lit("01_testimonial"))
             .when(col == 2, F.lit("02_dos"))
             .when(col == 3, F.lit("03_tres"))
             .when(col <= 5, F.lit("04_cuatro_cinco"))
             .when(col <= 10, F.lit("05_seis_diez"))
             .otherwise(F.lit("06_once_o_mas")))


def cap_print_tabla(filas, titulo, decimales=2, cols=None):
    """Imprime una lista de Row como tabla de ancho fijo, sin depender de pandas."""
    print("\n" + "-" * 78)
    print(titulo)
    print("-" * 78)
    if not filas:
        print("   (sin filas — no se puede comparar; NO es lo mismo que 'no hay diferencia')")
        return
    disponibles = list(filas[0].asDict().keys())
    cols = [c for c in (cols or disponibles) if c in disponibles]

    def fmt(v):
        if v is None:
            return "—"
        if isinstance(v, float):
            return f"{v:,.{decimales}f}"
        if isinstance(v, int):
            return f"{v:,}"
        return str(v)

    anchos = [max(len(c), max(len(fmt(f[c])) for f in filas)) for c in cols]
    print("  ".join(c.ljust(a) for c, a in zip(cols, anchos)))
    print("  ".join("-" * a for a in anchos))
    for f in filas:
        d = f.asDict()
        print("  ".join(fmt(d[c]).ljust(a) for c, a in zip(cols, anchos)))


def cap_pct(num, den):
    return (100.0 * num / den) if den else None


print(f"   Semilla del baseline nulo : {SEED_NULO}")
print(f"   Umbral de señal real−nulo : {UMBRAL_BRECHA_PP} pp  (ARBITRARIO, declarado)")
print(f"   Banda de reconciliación   : {BANDA_RECON}")
print(f"   Top-N para dispersión     : {TOP_N_PRECIO}")
print("   Listo. Correr ahora CAP-0-BIS y CAP-1 v12.")

# COMMAND ----------

# %% ==========================================================================
# CAP-0-BIS · PARCHE DE NUMÉRICOS Y DE VISIBILIDAD
# Correr DESPUÉS de CAP-0 y ANTES de volver a correr CAP-1, CAP-3 o CAP-4.
#
# QUÉ HACE: (1) define cap_num(), que parsea números escritos en formato chileno
# ('1.112,72975' o '1112,72975') sin abortar y sin convertirlos en NULL en
# silencio; (2) define cap_num_falla(), que MARCA lo que no se pudo parsear para
# poder contarlo en la misma agregación; (3) apaga ANSI como red de seguridad,
# NO como solución; (4) hace parlante al resolvedor de columnas.
#
# POR QUÉ IMPORTA: con ANSI apagado y sin cap_num(), '1112,72975' se convierte en
# NULL y desaparece de toda suma de pesos SIN QUE NADA LO AVISE. Eso no es un
# error de ejecución: es una cifra falsa en el informe. La regla §1.15.2 del
# proyecto (el bug de la rama vacía) es exactamente esto.
# =============================================================================
print("=" * 78)
print(f"CAP-0-BIS · parche de numéricos · RUN_ID_CAP = {RUN_ID_CAP}")
print("=" * 78)

try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    print("   ANSI SQL desactivado (red de seguridad; el parseo real lo hace cap_num).")
except Exception as _e:
    print(f"   [aviso] no se pudo desactivar ANSI ({type(_e).__name__}). cap_num() igual sirve.")

_HAS_TRY_CAST = hasattr(F.lit(1), "try_cast")
print(f"   Column.try_cast disponible : {_HAS_TRY_CAST}")


def _cap_limpia_num(col):
    """Normaliza el TEXTO de un número antes de castear. No castea todavía."""
    s = F.trim(col.cast("string"))
    s = F.regexp_replace(s, r"[\s $]", "")          # espacios, NBSP, signo $
    # Si hay coma, la coma es el separador DECIMAL (formato chileno):
    # entonces los puntos que haya son separadores de miles y se eliminan.
    s = F.when(s.contains(","),
               F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")).otherwise(s)
    return F.when((s == "") | (s.rlike(r"(?i)^(null|na|n/a|s/i|-)$")), F.lit(None)).otherwise(s)


def cap_num(col):
    """Texto o número → double. Formato chileno soportado. Nunca aborta."""
    s = _cap_limpia_num(col)
    return s.try_cast("double") if _HAS_TRY_CAST else s.cast("double")


def cap_num_falla(col):
    """1 si el valor venía con contenido pero NO se pudo parsear a número.

    Es la contrapartida obligatoria de cap_num(): permite contar, dentro de la
    MISMA agregación y sin acciones extra, cuánto universo se está perdiendo.
    """
    s = _cap_limpia_num(col)
    d = s.try_cast("double") if _HAS_TRY_CAST else s.cast("double")
    return F.when(s.isNotNull() & d.isNull(), 1).otherwise(0)


def cap_resolver(df, candidatos, etiqueta=""):
    """Igual que en CAP-0, pero AVISA cuando resuelve por coincidencia parcial.

    La versión original resolvía en silencio: 'ano' puede resolver a
    'OrganoComprador', 'tipo' a 'TipoMoneda'. Un resolvedor mudo convierte un
    error de esquema en una cifra plausible y falsa.
    """
    if df is None:
        return None
    reales = {c.lower(): c for c in df.columns}
    for c in candidatos:
        if c.lower() in reales:
            return reales[c.lower()]
    for c in candidatos:
        for low, real in reales.items():
            if c.lower() in low:
                print(f"   [resolver] '{etiqueta or candidatos[0]}' → '{real}' por coincidencia "
                      f"PARCIAL con '{c}'. VERIFICAR que sea la columna correcta.")
                return real
    if etiqueta:
        print(f"   [aviso] no se resolvió la columna '{etiqueta}'. "
              f"Candidatos probados: {candidatos}")
        print(f"          columnas disponibles: {', '.join(df.columns)}")
    return None


print("   cap_num / cap_num_falla / cap_resolver(parlante) definidos.")
print("   Ahora sí: volver a correr CAP-3 (versión v12) y CAP-4 (versión v12).")

# COMMAND ----------

# %% ==========================================================================
# CAP-1 v12 · PREGUNTA 1 — consistencia ONU lic↔OC CON BASELINE NULO
# Requiere CAP-0 y CAP-0-BIS. Reemplaza por completo al CAP-1 de v11.
#
# QUÉ CAMBIÓ RESPECTO DE v11 (todo verificado contra el esquema real):
#  1. La clave de licitación en df_lic_all es `codigo_externo`. v11 no la
#     resolvía y abortaba.
#  2. df_lic_all trae OFERTAS: hay varias filas por línea de adquisición. Contar
#     filas sobreestima el número de líneas y rompe la definición de
#     "testimonial = >=2 líneas y <=1 código". Ahora se cuenta
#     `_sinclasificar_Codigoitem` DISTINTOS.
#  3. La permutación del baseline nulo se ordena por xxhash64(clave), no por
#     rand(): rand() dentro de una ventana NO es reproducible si cambia el
#     particionamiento, y en serverless el particionamiento no lo controlas.
#  4. Las filas con año NULL ya no se pierden en el join de permutación
#     (coalesce a -1): antes real y nulo comparaban universos distintos.
#  5. Los pares donde NINGÚN lado tiene código ONU válido ya no puntúan
#     "consistencia perfecta": se aíslan en el universo D.
#  6. Los montos se parsean con cap_num() y la cifra en pesos se restringe a
#     moneda CLP nativa (v11 sumaba todas las monedas a valor facial).
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-1 v12 · Pregunta 1 — consistencia ONU con baseline nulo · {RUN_ID_CAP}")
print("=" * 78)

_lic = cap_global("df_lic_all")
_ocl = cap_global("df_oc_linked")

if _lic is None or _ocl is None:
    print("ABORTA: falta df_lic_all y/o df_oc_linked en el kernel.")
else:
    c_lic_key_l = cap_resolver(_lic, ["codigo_externo", "_lic_key", "lic_key",
                                      "codigo_licitacion"], "clave licitación (lic)")
    c_onu_l  = cap_resolver(_lic, ["onu_lic"], "ONU (lic)")
    c_anio_l = cap_resolver(_lic, ["_year", "anio", "año"], "año (lic)")
    c_item_l = cap_resolver(_lic, ["_sinclasificar_Codigoitem", "codigoitem"], "id de línea (lic)")

    c_lic_key_o = cap_resolver(_ocl, ["_lic_key", "codigo_licitacion"], "clave licitación (oc)")
    c_onu_o = cap_resolver(_ocl, ["onu_oc"], "ONU (oc)")
    c_flag  = cap_resolver(_ocl, ["tiene_licitacion_origen"], "flag de enlace")
    c_cant  = cap_resolver(_ocl, ["cantidad_oc"], "cantidad")
    c_prec  = cap_resolver(_ocl, ["precio_neto_oc"], "precio")
    c_mon   = cap_resolver(_ocl, ["moneda_item", "moneda"], "moneda")

    if not all([c_lic_key_l, c_onu_l, c_lic_key_o, c_onu_o]):
        print("ABORTA: no se resolvieron las columnas mínimas.")
        print(f"        lic: clave={c_lic_key_l} onu={c_onu_l}")
        print(f"        oc : clave={c_lic_key_o} onu={c_onu_o}")
    else:
        if not c_item_l:
            print("   [ADVERTENCIA] no hay id de línea en df_lic_all: n_lineas_lic contará FILAS,")
            print("   que incluyen una por oferta. La definición de 'testimonial' NO es citable así.")

        # ---- Lado licitación --------------------------------------------------
        lic = _lic.select(
            F.trim(F.col(c_lic_key_l).cast("string")).alias("k"),
            cap_onu8(F.col(c_onu_l)).alias("onu8"),
            (F.col(c_anio_l).cast("int") if c_anio_l else F.lit(None).cast("int")).alias("anio"),
            (F.col(c_item_l).cast("string") if c_item_l else F.lit(None).cast("string")).alias("item"),
        ).where(F.col("k").isNotNull() & (F.col("k") != ""))

        agg_lic = lic.groupBy("k").agg(
            F.max("anio").alias("anio"),
            F.count(F.lit(1)).alias("n_filas_lic"),
            (F.countDistinct("item") if c_item_l else F.count(F.lit(1))).alias("n_lineas_lic"),
            F.collect_set("onu8").alias("set8_lic"),
        )

        # ---- Lado OC ----------------------------------------------------------
        oc_src = _ocl
        if c_flag:
            oc_src = oc_src.where(F.col(c_flag) == True)  # noqa: E712
        es_clp = (F.upper(F.trim(F.col(c_mon).cast("string"))) == "CLP") if c_mon else F.lit(True)
        monto = ((cap_num(F.col(c_prec)) * cap_num(F.col(c_cant)))
                 if (c_prec and c_cant) else F.lit(None).cast("double"))
        oc = oc_src.select(
            F.trim(F.col(c_lic_key_o).cast("string")).alias("k"),
            cap_onu8(F.col(c_onu_o)).alias("onu8"),
            F.when(es_clp, monto).alias("clp_linea"),          # solo CLP nativo
        ).where(F.col("k").isNotNull() & (F.col("k") != ""))

        agg_oc = oc.groupBy("k").agg(
            F.count(F.lit(1)).alias("n_lineas_oc"),
            F.collect_set("onu8").alias("set8_oc"),
            F.sum("clp_linea").alias("clp_oc"),
        )

        par = agg_lic.join(agg_oc, "k", "inner")

        def _trunc(col, n):
            return F.array_distinct(F.transform(col, lambda x: F.substring(x, 1, n)))

        par = (par
               .withColumn("anio", F.coalesce(F.col("anio"), F.lit(-1)))   # no perder NULL en el join
               .withColumn("set8_lic", F.array_sort(F.array_compact(F.col("set8_lic"))))
               .withColumn("set8_oc",  F.array_sort(F.array_compact(F.col("set8_oc"))))
               .withColumn("card_lic8", F.size("set8_lic"))
               .withColumn("card_oc8",  F.size("set8_oc"))
               .withColumn("bucket", cap_bucket_card(F.col("card_lic8")))
               .withColumn("universo",
                           F.when((F.col("card_lic8") == 0) & (F.col("card_oc8") == 0),
                                  F.lit("D_ninguno_tiene_onu"))
                            .when(F.col("card_lic8") == 0, F.lit("E_lic_sin_onu"))
                            .when(F.col("card_lic8") <= 1,
                                  F.when(F.col("n_lineas_lic") >= 2,
                                         F.lit("B_testimonial_multilinea"))
                                   .otherwise(F.lit("A_monolinea_legitima")))
                            .otherwise(F.lit("C_informativo"))))

        def _metricas(df, sufijo):
            out = df
            for n in (2, 4, 8):
                sl = _trunc(F.col("set8_lic"), n) if n != 8 else F.col("set8_lic")
                so = (_trunc(F.col(f"set8_oc{sufijo}"), n) if n != 8
                      else F.col(f"set8_oc{sufijo}"))
                inter = F.size(F.array_intersect(sl, so))
                union = F.size(F.array_union(sl, so))
                fuera = F.size(F.array_except(so, sl))
                falta = F.size(F.array_except(sl, so))
                out = (out
                       .withColumn(f"overlap_{n}", (inter > 0).cast("int"))
                       .withColumn(f"subset_{n}", (fuera == 0).cast("int"))
                       .withColumn(f"iguales_{n}", ((fuera == 0) & (falta == 0)).cast("int"))
                       .withColumn(f"jacc_{n}",
                                   F.when(union > 0, inter / union).otherwise(F.lit(None))))
            return out

        # ---- Baseline nulo: permutación DETERMINISTA dentro de (año × bucket) --
        w_a = W.partitionBy("anio", "bucket").orderBy(F.xxhash64("k", F.lit(SEED_NULO)))
        w_b = W.partitionBy("anio", "bucket").orderBy(F.xxhash64("k", F.lit(SEED_NULO + 7)))
        izq = par.withColumn("_i", F.row_number().over(w_a))
        der = (par.select("anio", "bucket", "k", F.col("set8_oc").alias("set8_oc_x"))
                  .withColumn("_i", F.row_number().over(w_b)).drop("k"))
        par_perm = izq.drop("set8_oc").join(der, ["anio", "bucket", "_i"], "inner")

        real = _metricas(par, "").withColumn("serie", F.lit("1_real"))
        nulo = _metricas(par_perm, "_x").withColumn("serie", F.lit("2_nulo_permutado"))

        cols_m = [f"{m}_{n}" for n in (2, 4, 8)
                  for m in ("overlap", "subset", "iguales", "jacc")]
        sel = ["serie", "universo", "bucket"] + cols_m
        juntos = real.select(*sel).unionByName(nulo.select(*sel))

        aggs = [F.count(F.lit(1)).alias("n")]
        for c in cols_m:
            aggs.append((100.0 * F.avg(F.col(c))).alias(c) if not c.startswith("jacc")
                        else F.avg(F.col(c)).alias(c))

        # ---- ACCIÓN 1 de 2 ----------------------------------------------------
        res = (juntos.groupBy("serie", "universo", "bucket").agg(*aggs)
                     .orderBy("universo", "bucket", "serie").collect())

        cap_print_tabla(
            res,
            "CAP-1.A · MÉTRICA PRIMARIA (truncamiento 4 dígitos) — REAL vs. NULO PERMUTADO\n"
            "          universos: A_monolinea_legitima | B_testimonial_multilinea |\n"
            "          C_informativo | D_ninguno_tiene_onu | E_lic_sin_onu\n"
            "          CONTROL DE INTEGRIDAD: la columna n debe ser IGUAL entre 1_real y\n"
            "          2_nulo_permutado en cada fila. Si no lo es, la permutación perdió filas\n"
            "          y la brecha NO es comparable.",
            cols=["serie", "universo", "bucket", "n",
                  "overlap_4", "subset_4", "iguales_4", "jacc_4"])
        cap_print_tabla(
            res,
            "CAP-1.A-bis · RELECTURA CONTRA OTRA VARA (§1.16): 2 y 8 dígitos.\n"
            "          Si el resultado cambia de signo al cambiar el truncamiento, la cifra\n"
            "          de 4 dígitos NO es citable sola.",
            cols=["serie", "universo", "bucket", "n",
                  "subset_2", "iguales_2", "subset_8", "iguales_8"])

        d = {}
        for r in res:
            d.setdefault((r["serie"], r["universo"]), []).append(r)
        print("\nCAP-1.B · VEREDICTO — brecha real − nulo en la métrica PRIMARIA "
              f"(subset_4), universo informativo. Umbral declarado: {UMBRAL_BRECHA_PP} pp")
        rr = d.get(("1_real", "C_informativo"), [])
        nn = d.get(("2_nulo_permutado", "C_informativo"), [])
        if not rr or not nn:
            print("   NO SE PUEDE COMPARAR: falta uno de los dos lados.")
            print("   Esto NO significa 'no hay diferencia'. No citar nada de aquí.")
        else:
            n_r = sum(x["n"] for x in rr)
            n_n = sum(x["n"] for x in nn)
            s_r = sum(x["subset_4"] * x["n"] for x in rr) / n_r
            s_n = sum(x["subset_4"] * x["n"] for x in nn) / n_n
            brecha = s_r - s_n
            print(f"   real = {s_r:6.2f}%  (n={n_r:,})")
            print(f"   nulo = {s_n:6.2f}%  (n={n_n:,})")
            if n_r != n_n:
                print(f"   [ALERTA] n real ({n_r:,}) != n nulo ({n_n:,}): universos distintos.")
            print(f"   brecha = {brecha:+6.2f} pp")
            if brecha >= UMBRAL_BRECHA_PP:
                print("   → HAY SEÑAL sobre el azar. Releer contra la tabla por bucket: si la")
                print("     brecha vive en un solo tramo, la cifra agregada no es citable sola.")
            else:
                print("   → NO HAY SEÑAL sobre el azar con este umbral. Si esto pasa,")
                print("     `oc_subconjunto_de_lic` NO sirve como cifra citable y hay que")
                print("     liderar con la cobertura ponderada de CAP-1.C. Decirlo, no esconderlo.")

        # ---- ACCIÓN 2 de 2: cobertura ponderada -------------------------------
        set_lic4 = agg_lic.select(
            "k", _trunc(F.array_compact(F.col("set8_lic")), 4).alias("set4_lic"))
        cob = (oc.join(set_lic4, "k", "inner")
                 .withColumn("onu4", F.substring(F.col("onu8"), 1, 4))
                 .withColumn("declarada",
                             F.when(F.col("onu4").isNull(), F.lit(None))
                              .otherwise(F.array_contains(F.col("set4_lic"),
                                                          F.col("onu4")).cast("int"))))
        cr = cob.agg(
            F.count(F.lit(1)).alias("lineas_oc_enlazadas"),
            F.sum(F.when(F.col("onu4").isNotNull(), 1).otherwise(0)).alias("lineas_con_onu_valido"),
            F.sum(F.col("declarada")).alias("lineas_declaradas"),
            F.sum(F.when(F.col("onu4").isNotNull(), F.col("clp_linea"))).alias("clp_con_onu"),
            F.sum(F.when(F.col("declarada") == 1, F.col("clp_linea"))).alias("clp_declarado"),
        ).collect()

        if cr:
            r = cr[0]
            n_enl = r["lineas_oc_enlazadas"] or 0
            n_val = r["lineas_con_onu_valido"] or 0
            n_dec = r["lineas_declaradas"] or 0
            c_val = r["clp_con_onu"] or 0
            c_dec = r["clp_declarado"] or 0
            print("\n" + "-" * 78)
            print("CAP-1.C · COBERTURA PONDERADA (sin sesgo de cardinalidad)")
            print("-" * 78)
            print("   Denominador declarado: líneas de OC enlazadas CUYA LICITACIÓN ESTÁ")
            print("   PRESENTE en df_lic_all (el borde del universo ya está descontado aquí).")
            print(f"   Líneas de OC enlazadas y con licitación presente  {n_enl:,}")
            print(f"   ... con código ONU válido de 8 dígitos ........... {n_val:,}")
            p_l = cap_pct(n_dec, n_val)
            p_c = cap_pct(c_dec, c_val)
            print("   ... cuya familia (4 díg.) SÍ estaba declarada en la licitación:")
            print(f"       por LÍNEA ..................... "
                  f"{f'{p_l:.2f}%' if p_l is not None else 'NO CALCULABLE (denominador 0)'}")
            print(f"       por PESOS (solo CLP nativo) ... "
                  f"{f'{p_c:.2f}%' if p_c is not None else 'NO CALCULABLE (denominador 0)'}")
            print(f"       CLP con ONU válido: {int(c_val):,}")
            print(f"       CLP declarado     : {int(c_dec):,}")
            print("\n   NOTA: cifra recomendada como TITULAR de la Pregunta 1.")
        print(f"\nCAP-1 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 9-BIS — Mejora nº1 (confundidor 12-dic-2024) + auditoría `_year` (F19 §2.5)
# MAGIC **NUEVA, 26-ago-2026.** Se agrega después de que la Celda 9 corrida por Gabriel
# MAGIC (`RUN_ID_CAP=70680deaec66`) reprodujo EXACTO los tres titulares congelados de la
# MAGIC Pregunta 1 — 94,44% real / 1,38% nulo (brecha +93,06 pp, n=190.232=n.190.232),
# MAGIC 80,08% de licitaciones con a lo más un código (=(643.562+128.543)/964.171) y
# MAGIC 13,33% de patología testimonial (=128.543/964.171) — confirmando que el código
# MAGIC integrado en esta Celda 9 es, en efecto, el que generó `1314c4f6d481`.
# MAGIC
# MAGIC Dos acciones baratas (una sola pasada de Spark cada una, sin `.persist()`),
# MAGIC pensadas para correr JUNTAS en la misma corrida del día:
# MAGIC  1. **Mejora nº1 de `S17` §5 / `F18` §4.4** — recalcula la cobertura ONU 2024 vs.
# MAGIC     2025 restringida a los organismos (o unidades de compra) presentes en AMBOS
# MAGIC     años, para saber si el 0,28 pp del art. 20 bis sobrevive al confundidor de la
# MAGIC     entrada de ~35% más de organismos el 12-dic-2024.
# MAGIC  2. **F19 §2.5** — cuenta, sobre TODO `df_oc_all` (no solo la muestra de 219
# MAGIC     licitaciones), cuántas líneas tienen `año(fecha_creacion) != _year` (la
# MAGIC     partición histórico/vigente de la que se leyó el archivo), por `_year`.
# MAGIC     Requiere que la Celda 4 corregida ya haya corrido (deja `fecha_creacion_norm`).

# COMMAND ----------

print("=" * 78)
print(f"MEJORA Nº1 (S17 §5 / F18 §4.4) · Confundidor del 12-dic-2024 — semi-join · RUN_ID_CAP={RUN_ID_CAP}")
print("=" * 78)

_oc2 = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")

if _oc2 is None:
    print("ABORTA: falta df_oc_all/df_oc_linked en el kernel.")
else:
    c_onu2  = cap_resolver(_oc2, ["onu_oc"], "ONU (oc)")
    c_anio2 = cap_resolver(_oc2, ["_year", "anio", "año"], "año (oc)")
    c_org2  = cap_resolver(_oc2, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")
    c_uni2  = cap_resolver(_oc2, ["_sinclasificar_CodigoUnidadCompra"], "unidad de compra")

    if not c_onu2 or not c_anio2 or not (c_org2 or c_uni2):
        print("ABORTA: faltan columnas mínimas (ONU, año, organismo/unidad).")
    else:
        ent2 = F.col(c_org2) if c_org2 else F.col(c_uni2)
        etiqueta_ent = f"organismo ({c_org2})" if c_org2 else f"unidad de compra ({c_uni2})"
        print(f"   Entidad usada para el semi-join: {etiqueta_ent}")

        base2 = _oc2.select(
            F.col(c_anio2).cast("int").alias("anio"),
            F.trim(ent2.cast("string")).alias("entidad"),
            cap_onu_clase(F.col(c_onu2)).alias("clase_onu"),
        ).where(F.col("anio").isin(2024, 2025))

        orgs_2024 = (base2.where(F.col("anio") == 2024).select("entidad")
                          .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                          .distinct())
        orgs_2025 = (base2.where(F.col("anio") == 2025).select("entidad")
                          .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                          .distinct())
        orgs_ambos = orgs_2024.join(orgs_2025, "entidad", "inner")

        print(f"   Organismos/unidades en 2024 .................. {orgs_2024.count():,}")
        print(f"   Organismos/unidades en 2025 .................. {orgs_2025.count():,}")
        print(f"   Presentes en AMBOS años (semi-join) .......... {orgs_ambos.count():,}")

        restringido = base2.join(orgs_ambos, "entidad", "inner")

        def _serie2(df, etiqueta):
            return (df.groupBy("anio")
                      .agg(F.lit(etiqueta).alias("serie"),
                           F.count(F.lit(1)).alias("lineas"),
                           (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))
                            ).alias("pct_onu_valido"),
                           F.countDistinct("entidad").alias("entidades")))

        filas2 = (_serie2(base2, "1_universo_completo_24_25")
                  .unionByName(_serie2(restringido, "2_restringido_mismos_organismos"))
                  .orderBy("serie", "anio").collect())

        cap_print_tabla(
            filas2,
            "MEJORA Nº1 · Cobertura ONU 2024 vs 2025 — universo completo vs. RESTRINGIDO\n"
            "          a organismos/unidades presentes en AMBOS años (semi-join).\n"
            "          Si el 0,28 pp sobrevive en la serie restringida, el confundidor del\n"
            "          12-dic-2024 NO explica la comparación 2024-2025 y la conclusión se\n"
            "          vuelve más fuerte. Si no sobrevive, el hallazgo era composición.")

        d2 = {(r["serie"], r["anio"]): r for r in filas2}
        print("\nVEREDICTO — indicador restringido a los mismos organismos, 2024 vs 2025")
        r24 = d2.get(("2_restringido_mismos_organismos", 2024))
        r25 = d2.get(("2_restringido_mismos_organismos", 2025))
        if r24 and r25:
            delta = r25["pct_onu_valido"] - r24["pct_onu_valido"]
            print(f"   2024 (restringido) : {r24['pct_onu_valido']:.2f}%")
            print(f"   2025 (restringido) : {r25['pct_onu_valido']:.2f}%")
            print(f"   delta restringido   = {delta:+.2f} pp")
            print("   (cifra congelada SIN restringir, para comparar: +0,28 pp)")
        else:
            print("   NO SE PUEDE COMPARAR: falta 2024 o 2025 en el resultado restringido.")
        print(f"\nMejora Nº1 termina. RUN_ID_CAP={RUN_ID_CAP}")

# COMMAND ----------

print("\n" + "=" * 78)
print(f"F19 §2.5 · Auditoría _year vs. año(fecha_creacion) — universo completo OC · RUN_ID_CAP={RUN_ID_CAP}")
print("=" * 78)

_oc3 = cap_global("df_oc_all")
if _oc3 is None:
    print("ABORTA: falta df_oc_all en el kernel.")
elif "fecha_creacion_norm" not in _oc3.columns:
    print("ABORTA: falta 'fecha_creacion_norm'. Correr la Celda 4 (v18, corregida) antes de esta celda.")
else:
    aud = (_oc3
           .where(F.col("fecha_creacion_norm").isNotNull())
           .withColumn("anio_creacion", F.year(F.col("fecha_creacion_norm")))
           .withColumn("desborde", (F.col("anio_creacion") != F.col("_year")).cast("int"))
           .groupBy("_year")
           .agg(F.count(F.lit(1)).alias("n_total"),
                F.sum("desborde").alias("n_desborde"))
           .withColumn("pct_desborde", F.round(100.0 * F.col("n_desborde") / F.col("n_total"), 4))
           .orderBy("_year")
           .collect())

    cap_print_tabla(
        aud,
        "F19 §2.5 · Líneas de OC donde año(fecha_creacion) != _year (partición), por _year\n"
        "          Cuantifica el borde histórico/vigente sobre el universo COMPLETO de OC,\n"
        "          no solo sobre la muestra curada de 219 licitaciones que trajo Gabriel.\n"
        "          `_year` = partición histórico/vigente (cuándo se descargó el archivo).")

    n_tot3 = sum((r["n_total"] or 0) for r in aud)
    n_desb3 = sum((r["n_desborde"] or 0) for r in aud)
    print(f"\n   Total líneas de OC con fecha_creacion parseable ... {n_tot3:,}")
    print(f"   Con año(fecha_creacion) != _year (partición) ...... {n_desb3:,}  "
          f"({cap_pct(n_desb3, n_tot3) or 0:.4f}% del total con fecha)")
    print("\n   LECTURA: si el % es chico y se concentra en diciembre/enero de cada año,")
    print("   confirma que el efecto es de borde de calendario (lo esperado). Si aparece")
    print("   disperso o grande, hay que investigar antes de seguir citando `_year` como")
    print("   si fuera el año real de creación en cualquier tabla 'por año' del informe.")
    print(f"\nAuditoría §2.5 termina. RUN_ID_CAP={RUN_ID_CAP}")

# COMMAND ----------

# MAGIC %md
# MAGIC # APÉNDICE (opcional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice A — 6C: ¿truncamiento izquierdo o caída real de trazabilidad?

# COMMAND ----------

df_unlinked = df_oc_linked.filter(~F.col("tiene_licitacion_origen"))

print("=" * 70)
print("6C.1 — MUESTRAS CRUDAS: confirmar el formato del codigo_licitacion")
print("=" * 70)
print("\n20 claves de OC NO enlazadas (años 2017-2018):")
(df_unlinked.filter(F.col("_year").isin(2017, 2018))
 .select(c_(COL_KEY_OC).alias("clave_no_enlazada"), "_year")
 .limit(20).show(20, truncate=False))

PATRON_ANIO = r"[A-Za-z]{1,3}(\d{2})$"
df_unl_anio = (
    df_unlinked
    .withColumn("_suf", F.regexp_extract(F.trim(c_(COL_KEY_OC)), PATRON_ANIO, 1))
    .withColumn("_anio_lic_ref",
                F.when(F.col("_suf") == "", F.lit(None))
                 .otherwise(F.when(F.col("_suf").cast("int") >= 90, 1900 + F.col("_suf").cast("int"))
                             .otherwise(2000 + F.col("_suf").cast("int"))))
)

print("\n" + "=" * 70)
print("6C.3 — G6 (calidad del enlace) según el corte temporal")
print("=" * 70)
for _desde in (2017, 2019, 2020, 2021):
    _sub = df_oc_linked.filter(F.col("_year") >= _desde)
    _c = _sub.count()
    _e = _sub.filter(F.col("tiene_licitacion_origen")).count()
    print(f"  {_desde}-2026: {_e:>12,} / {_c:>12,} = {100*_e/_c:.2f}%")
print("\n  [MEDIDO] 19-ago sesión 2: 87,6%-94,1% de las no-enlazadas 2017-2023")
print("  apuntan a pre-2017 — truncamiento del universo, no trazabilidad perdida.")
print("  G6 ajustado acumulado 2017-2026 = 99,19% (ver bitácora para el detalle).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice B — 7B-1: diagnóstico de formato de precio_neto_oc (ya resuelto)

# COMMAND ----------

print("=" * 70)
print("7B-1.3 — CLASIFICACIÓN POR PATRÓN de precio_neto_oc (ya integrada en Celda 7.2)")
print("=" * 70)
(df_oc_all.withColumn("_patron", _clasificar_patron(c_(COL_MONTO)))
 .groupBy("_patron").count()
 .withColumn("pct", F.round(100 * F.col("count") / F.lit(n_oc_filas), 4))
 .orderBy(F.col("count").desc())
 .show(20, truncate=False))
print("  [MEDIDO] 19-ago sesión 2: el 100% del patrón 'no reconocido' resultó ser")
print("  notación científica válida — ya incluida en _clasificar_patron() de la Celda 7.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice C (sesión 6) — 6D: caracterizar el patrón NUEVO de
# MAGIC ## no-enlazadas 2024-2026 (pendiente arrastrado desde sesión 2, [POR VERIFICAR])
# MAGIC
# MAGIC Esta celda NO cambia ninguna cifra de G6 — es exploratoria. Separa las OC
# MAGIC no-enlazadas en (1) las que apuntan a una licitación anterior a 2017
# MAGIC (truncamiento, ya explicado — sesión 2) y (2) las que apuntan a un año SÍ
# MAGIC cargado en `ANIOS_COMPLETOS` pero aun así no encuentran su licitación — el
# MAGIC patrón nuevo que crece en 2024-2026. Imprime una muestra real de
# MAGIC `codigo_licitacion` del grupo (2) para que puedas buscarlos en
# MAGIC mercadopublico.cl y ver si: (a) la licitación fue eliminada/anulada después
# MAGIC de emitida la OC, (b) pertenece a uno de los +327 organismos nuevos que
# MAGIC ingresaron el 12-dic-2024 (posible desfase de indexación), o (c) usa un
# MAGIC formato de código de uno de los procedimientos nuevos de la reforma
# MAGIC (subasta inversa electrónica, diálogos competitivos, contratos para la
# MAGIC innovación) que el regex de extracción de año no reconoce bien.

# COMMAND ----------

df_unl_2024_26 = df_unl_anio.filter(F.col("_year").isin(2024, 2025, 2026))

df_unl_clasificado = df_unl_2024_26.withColumn(
    "_grupo",
    F.when(F.col("_anio_lic_ref").isNull(), F.lit("sin_sufijo_reconocible"))
     .when(F.col("_anio_lic_ref") < 2017, F.lit("truncamiento_pre2017"))
     .when(F.col("_anio_lic_ref").isin(ANIOS_COMPLETOS), F.lit("PATRON_NUEVO_anio_cargado_no_matchea"))
     .otherwise(F.lit("otro_anio_no_cargado"))
)

print("=" * 70)
print("6D — CLASIFICACIÓN DE NO-ENLAZADAS 2024-2026, POR GRUPO Y AÑO")
print("=" * 70)
(df_unl_clasificado
 .groupBy("_year", "_grupo")
 .agg(F.count("*").alias("n"))
 .orderBy("_year", "_grupo")
 .show(30, truncate=False))
print("  Comparar contra lo ya [MEDIDO] en sesión 2 (texto de consola, no archivado en")
print("  archivo): PATRON_NUEVO ≈ 789 (2024), 5.304 (2025), 4.236 (2026). Si el conteo")
print("  de esta corrida difiere, investigar por qué antes de usar cualquiera de las dos cifras.")

print("\n" + "=" * 70)
print("6D-2 — MUESTRA REAL de codigo_licitacion, grupo PATRON_NUEVO, por año")
print("=" * 70)
print("  Copiar 5-10 de estos códigos y buscarlos en mercadopublico.cl para ver si la")
print("  licitación existe, fue eliminada/anulada, o pertenece a un organismo/tipo nuevo.")
(df_unl_clasificado
 .filter(F.col("_grupo") == "PATRON_NUEVO_anio_cargado_no_matchea")
 .select(c_(COL_KEY_OC).alias("codigo_licitacion_de_la_OC"),
         c_(COL_ID_OC).alias("codigo_oc"),
         c_(COL_TIPO_OC).alias("tipo_oc") if COL_TIPO_OC in df_unl_clasificado.columns else F.lit(None).alias("tipo_oc"),
         "_year", "_anio_lic_ref")
 .orderBy(F.rand())
 .limit(20)
 .show(20, truncate=False))

print(f"\n[POR VERIFICAR] run_id={RUN_ID} — esta celda solo junta evidencia, no cierra el pendiente.")
print("Pegar la salida completa en el próximo chat para decidir, con casos reales, la causa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice C-2 (sesión 7, FIX en sesión 8) — 6D-3: ¿son códigos realmente ausentes, o
# MAGIC ## un problema de formato (espacios/mayúsculas) en el join de la Celda 6?
# MAGIC
# MAGIC La muestra 6D-2 de la sesión 6 (`run_id=289bcbf4f33e`) mostró que casi todos
# MAGIC los casos del patrón nuevo son `tipo_oc=SE` (proveniente de licitación), y
# MAGIC que varios `codigo_licitacion` se repiten para muchas OC distintas emitidas
# MAGIC en años diferentes (ej. `"5839-14-LR23"` referenciado por OC de 2025 Y 2026,
# MAGIC `"3789-22-LQ24"` igual) — el patrón de un código repetido durante varios años
# MAGIC es típico de una licitación grande (LR = licitación ≥5.000 UTM) cuyo contrato
# MAGIC resultante sigue generando OC años después de publicada, NO de una licitación
# MAGIC eliminada ni de un organismo nuevo (ninguno de los 20 casos de la muestra usa
# MAGIC un procedimiento nuevo de la reforma — todos son `SE`/`CC`, mecanismos ya
# MAGIC conocidos). Esto DEBILITA la hipótesis de la reforma Ley 21.634 como causa y
# MAGIC abre una hipótesis distinta, más aburrida y más fácil de que sea el problema
# MAGIC real: el join de la Celda 6 compara `codigo_licitacion` (OC) contra
# MAGIC `codigo_externo` (licitación) con `==` exacto, SIN aplicar `trim()`/mayúsculas
# MAGIC al lado de la OC (`df_lic_keys` sí filtra con `trim()` pero conserva el valor
# MAGIC crudo) — si alguno de los dos lados trae un espacio o una diferencia de
# MAGIC mayúsculas/minúsculas para el mismo código lógico, el join falla en silencio
# MAGIC aunque la licitación sí esté cargada. Esta celda prueba esa hipótesis
# MAGIC específicamente para los códigos más repetidos de la muestra 6D-2.
# MAGIC
# MAGIC **CONTEXTO NUEVO, sesión 8 (reportado por el usuario — NO verificado por Claude
# MAGIC en este chat):** el usuario revisó la ficha pública de `5839-14-LR23` en
# MAGIC mercadopublico.cl y reporta "Tiempo del Contrato = 48 Meses", y señala que en
# MAGIC modalidades de suministro o de arriendo de bienes/inmuebles una licitación
# MAGIC puede quedar vigente varios años, generando OC contra el mismo contrato durante
# MAGIC ese período. Se intentó verificar este dato de forma independiente con
# MAGIC `WebFetch` sobre la ficha pública y no fue posible: la página se renderiza con
# MAGIC JavaScript del lado del cliente y no es legible por las herramientas
# MAGIC automáticas de este chat — misma limitación ya documentada en
# MAGIC `reglas-operativas-proyecto.md` §2.3 para los paneles Power BI del Observatorio.
# MAGIC Por eso el dato queda `[POR VERIFICAR]` (reportado por el usuario, no
# MAGIC verificado por Claude), con el mismo estándar de evidencia ya aceptado antes en
# MAGIC este proyecto para observaciones directas del usuario.
# MAGIC
# MAGIC **Qué SÍ explica este dato y qué NO (regla §1.13 — un dato de contexto no cierra**
# MAGIC **un pendiente por sí solo):** que el contrato dure 48 meses explica por qué el
# MAGIC MISMO `codigo_licitacion` sigue generando OC en años posteriores a su
# MAGIC publicación (ya visto en 6D-2) — eso es normal, esperado, y no es indicio de
# MAGIC ninguna irregularidad. Lo que NO explica es que el join de la Celda 6 no
# MAGIC ENCUENTRE esa licitación en `df_lic_all`: si `5839-14-LR23` se publicó en un
# MAGIC año ya cargado (2017-2026), la fila debería existir en la tabla de licitación
# MAGIC sin importar cuántos años dure el contrato resultante — la duración del
# MAGIC contrato no mueve la fecha de publicación de la licitación. La pregunta que
# MAGIC esta celda sigue respondiendo es otra: ¿por qué el join `==` exacto no la
# MAGIC encuentra? Formato (trim/mayúsculas) o ausencia real son las dos hipótesis en
# MAGIC pie — el dato de los 48 meses no descarta ninguna de las dos.
# MAGIC
# MAGIC Esta celda NO cambia ninguna cifra de G6 — es exploratoria.
# MAGIC
# MAGIC **FIX sesión 8:** la versión de la sesión 7 llamaba `.persist()` sobre
# MAGIC `df_lic_externo_bruto`, y la corrida real del usuario falló con
# MAGIC `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
# MAGIC compute` — Databricks Free Edition corre en cómputo serverless, donde
# MAGIC `.persist()`/`.cache()` de DataFrame no está disponible. Corregido: se quita el
# MAGIC `.persist()`/`.unpersist()` y los tres `.count()` por código se reemplazan por
# MAGIC una sola agregación que calcula los tres chequeos (exacto/trim/upper) de los 4
# MAGIC códigos en una sola pasada por la tabla de licitación.

# COMMAND ----------

CODIGOS_A_VERIFICAR = ["5839-14-LR23", "3789-22-LQ24", "1058086-50-LR24", "1057554-79-LQ23"]

print("=" * 70)
print("6D-3 — ¿EXISTEN ESTOS CÓDIGOS EN LA TABLA DE LICITACIÓN (codigo_externo),")
print("       CON UN MATCH EXACTO, TRIMEADO, O SOLO CASE-INSENSITIVE?")
print("=" * 70)
print("  Contexto (usuario, sesión 8, [POR VERIFICAR] — NO comprobado por Claude en este")
print("  chat, ver docstring de esta celda): 5839-14-LR23 tendría 'Tiempo del Contrato =")
print("  48 Meses' según ficha de mercadopublico.cl. Esto es consistente con el patrón de")
print("  códigos repetidos entre años ya visto en 6D-2 (contrato plurianual de suministro/")
print("  arriendo), pero NO explica por sí solo que el join no encuentre la licitación —")
print("  si se publicó en un año cargado, debería existir en df_lic_all igual. Esta celda")
print("  sigue siendo la prueba real de formato vs. ausencia.")

# NOTA (fix v7→v8): SIN .persist() — [NOT_SUPPORTED_WITH_SERVERLESS] "PERSIST TABLE is
# not supported on serverless compute" (confirmado en la corrida real del usuario).
# Compensación: los tres chequeos (exacto/trim/upper) de los 4 códigos se calculan en
# UNA sola agregación (`.agg(...).collect()`) — una sola pasada por `df_lic_all` en vez
# de hasta doce (3 counts x 4 códigos, como en la versión de la sesión 7).
df_lic_externo_bruto = df_lic_all.select(
    c_(COL_KEY_LIC).alias("codigo_externo_crudo"),
    F.trim(c_(COL_KEY_LIC)).alias("codigo_externo_trim"),
    F.upper(F.trim(c_(COL_KEY_LIC))).alias("codigo_externo_trim_upper"),
    "_year"
)

_aggs = []
for _i, _cod in enumerate(CODIGOS_A_VERIFICAR):
    _cod_trim  = _cod.strip()
    _cod_upper = _cod_trim.upper()
    _aggs += [
        F.sum((F.col("codigo_externo_crudo") == _cod).cast("int")).alias(f"n_exacto_{_i}"),
        F.sum((F.col("codigo_externo_trim") == _cod_trim).cast("int")).alias(f"n_trim_{_i}"),
        F.sum((F.col("codigo_externo_trim_upper") == _cod_upper).cast("int")).alias(f"n_upper_{_i}"),
    ]

_fila = df_lic_externo_bruto.agg(*_aggs).collect()[0].asDict()

for _i, _cod in enumerate(CODIGOS_A_VERIFICAR):
    n_exacto = _fila[f"n_exacto_{_i}"] or 0
    n_trim   = _fila[f"n_trim_{_i}"] or 0
    n_upper  = _fila[f"n_upper_{_i}"] or 0
    print(f"\n  Código: '{_cod}'")
    print(f"    match exacto (crudo):              {n_exacto}")
    print(f"    match trim():                      {n_trim}")
    print(f"    match trim()+upper() (case-insens): {n_upper}")
    if n_upper > 0 and n_exacto == 0:
        print("    → ⚠️  EXISTE con formato distinto (espacio/mayúscula) — el join de la")
        print("       Celda 6 SÍ tiene un bug de formato, no es que falte la licitación.")
        _cod_upper = _cod.strip().upper()
        df_lic_externo_bruto.filter(F.col("codigo_externo_trim_upper") == _cod_upper) \
            .select("codigo_externo_crudo", "_year").distinct().show(10, truncate=False)
    elif n_exacto == 0 and n_upper == 0:
        print("    → la licitación NO está en ningún año cargado del universo, bajo ningún")
        print("       formato razonable — consistente con: fue eliminada/anulada, nunca se")
        print("       exportó, o pertenece a un año/categoría que no se subió.")
    else:
        print("    → match exacto encontrado — el join de la Celda 6 debería haberla enlazado;")
        print("       si sigue apareciendo como no-enlazada, revisar si hay más de una OC con")
        print("       el mismo codigo_oc o algún otro filtro previo a este chequeo.")

print(f"\n[POR VERIFICAR] run_id={RUN_ID} — si alguno de los 4 códigos aparece con")
print("match trim()/upper() pero no exacto, es evidencia de un bug de formato en el")
print("join de la Celda 6 (regla §1.7/§1.13) — reportar antes de aceptar cualquier")
print("hipótesis normativa (reforma, contrato plurianual) como causa del patrón nuevo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice D (sesión 6, CORREGIDO sesión 7) — 7C-5: muestra real de
# MAGIC ## OC de Convenio Marco con ratio de reconciliación extremo (pendiente
# MAGIC ## arrastrado desde sesión 4, [POR VERIFICAR])
# MAGIC
# MAGIC **Confirmado en sesión 8:** el fix de la sesión 7 (constante local
# MAGIC `_COL_ONU_OC`, sin depender de la Celda 9) funcionó — la corrida real del
# MAGIC usuario produjo la muestra completa (n=577.560 outliers de Convenio Marco),
# MAGIC sin ningún error. Esta celda queda sin cambios en v8.
# MAGIC
# MAGIC Esta celda NO cambia ninguna cifra de G6 ni de reconciliación — es
# MAGIC exploratoria. Extrae `codigo_oc` reales de tipo Convenio Marco (CM) con
# MAGIC ratio muy por debajo o muy por encima de la banda plausible [0,5-3,0], junto
# MAGIC con su `onu_oc`, para buscarlos en mercadopublico.cl. La sesión 6 encontró
# MAGIC evidencia documental [FUENTE] de que, al menos en el catálogo de Convenio
# MAGIC Marco de Alimentos, el precio de un ítem puede estar expresado por kilo o por
# MAGIC caja de N kilos, mientras que otros catálogos usan precio por unidad — si
# MAGIC `cantidad_oc` cuenta unidades pero `precio_neto_oc` viene en la unidad de
# MAGIC venta del catálogo (kilo/caja), el ratio se dispara. Ver bitácora, sesión 6,
# MAGIC para la cita textual y la fuente.

# COMMAND ----------

_COL_ONU_OC = "onu_oc"   # constante local — NO depende de que la Celda 9 se haya corrido

df_cm_outliers = (
    df_recon
    .filter((F.col("tipo_oc") == "CM") &
            ((F.col("ratio") < BANDA_PLAUSIBLE[0]) | (F.col("ratio") > BANDA_PLAUSIBLE[1])))
)

n_cm_outliers = df_cm_outliers.count()
print("=" * 70)
print(f"7C-5 — MUESTRA REAL DE OC CONVENIO MARCO (CM) CON RATIO EXTREMO (n={n_cm_outliers:,})")
print("=" * 70)

df_cm_outliers_con_onu = (
    df_cm_outliers
    .join(
        df_oc_m.select(c_(COL_ID_OC).alias("_oc_id"),
                        (c_(_COL_ONU_OC).alias("onu_oc_muestra") if _COL_ONU_OC in df_oc_m.columns else F.lit(None).alias("onu_oc_muestra")),
                        c_(COL_CANT_OC).alias("cantidad_oc_muestra"),
                        c_(COL_MONTO).alias("precio_neto_oc_muestra")).dropDuplicates(["_oc_id"]),
        on="_oc_id", how="left"
    )
)

print("\n  10 OC con ratio MÁS BAJO (posible: precio_neto_oc ya viene por lote/caja grande):")
(df_cm_outliers_con_onu
 .orderBy(F.col("ratio").asc())
 .select("_oc_id", "ratio", "suma_precio_x_cantidad", "monto_total_oc_clp", "n_lineas",
         "onu_oc_muestra", "cantidad_oc_muestra", "precio_neto_oc_muestra")
 .limit(10).show(10, truncate=False))

print("\n  10 OC con ratio MÁS ALTO (posible: cantidad_oc cuenta lotes/cajas, precio es por unidad menor):")
(df_cm_outliers_con_onu
 .orderBy(F.col("ratio").desc())
 .select("_oc_id", "ratio", "suma_precio_x_cantidad", "monto_total_oc_clp", "n_lineas",
         "onu_oc_muestra", "cantidad_oc_muestra", "precio_neto_oc_muestra")
 .limit(10).show(10, truncate=False))

print(f"\n[POR VERIFICAR] run_id={RUN_ID} — esta celda solo junta evidencia, no cierra el pendiente.")
print("Buscar 5-10 de estos codigo_oc en mercadopublico.cl y confirmar si el producto (via onu_oc)")
print("corresponde a un catálogo con precio por kilo/caja (ej. Alimentos) — ver bitácora sesión 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice C-3 (NUEVO, sesión 9) — 6D-4/5/6: la hipótesis de formato ya se
# MAGIC ## refutó. ¿Es un hueco SISTEMÁTICO del universo de licitación, o ausencia
# MAGIC ## caso a caso?
# MAGIC
# MAGIC **De dónde viene esta celda.** La Celda 6D-3 (`run_id=5fc3ded29565`,
# MAGIC 20-ago sesión 8) dio 0/0/0 para los cuatro códigos: ni match exacto, ni con
# MAGIC `trim()`, ni con `trim()+upper()`. La hipótesis de formato en el join de la
# MAGIC Celda 6 queda **REFUTADA** — y eso es un resultado, no un fracaso: significa
# MAGIC que el join de la Celda 6 está bien y que **G6 no se mueve** por este
# MAGIC pendiente. Lo que falta explicar es otra cosa: por qué esas licitaciones no
# MAGIC están en la tabla.
# MAGIC
# MAGIC **Qué quedó descartado además.** El usuario aportó la ficha real de
# MAGIC `5839-14-LR23` (PDF impreso de mercadopublico.cl, 19-ago-2026): "SUMINISTRO
# MAGIC DE FARMACOS", DIRECCION DE SANIDAD DE LA ARMADA, "Tiempo del Contrato = 48
# MAGIC Meses". **La ficha existe y está publicada** — así que "la licitación fue
# MAGIC eliminada/anulada" queda descartado para este caso. Quedan dos causas, y son
# MAGIC distintas entre sí:
# MAGIC   - **(i) hueco sistemático:** el export mensual que descargó el usuario no
# MAGIC     incluye cierta clase de licitaciones (por ejemplo, las que al momento de
# MAGIC     generarse el archivo `Histórico` de su año todavía estaban activas).
# MAGIC   - **(ii) ausencia caso a caso:** el export sí trae esa clase, pero estos
# MAGIC     registros puntuales faltan por otra razón.
# MAGIC
# MAGIC **Hipótesis concreta que esta celda prueba (`[POR VERIFICAR]`).** El universo
# MAGIC cargado tiene `lic_historico` para 2017–2025 y `lic_vigente` **solo para
# MAGIC 2026** — eso está `[MEDIDO]` desde la Etapa 1 (`run_id=d0d8a7bc0329`:
# MAGIC `Licitaciones_Nacional_Vigente` = 0 archivos en 2017–2025, 8 archivos en
# MAGIC 2026). Si el dataset "Vigente" de ChileCompra arrastra licitaciones **aún
# MAGIC activas publicadas en años anteriores**, entonces una licitación de 2023 con
# MAGIC contrato de 48 meses (activa hasta ~2027) pudo no entrar nunca en el
# MAGIC `Histórico` de 2023, y su versión "Vigente" de 2023 ya no es descargable hoy.
# MAGIC Eso explicaría —sin invocar la reforma Ley 21.634 ni ningún organismo nuevo—
# MAGIC que falten justamente las licitaciones GRANDES y PLURIANUALES.
# MAGIC
# MAGIC **6D-4 es el test decisivo, y es barato:** ¿el archivo `lic_vigente` de 2026
# MAGIC (el único "Vigente" que existe en el volumen) contiene códigos con sufijo de
# MAGIC años ANTERIORES a 2026 (LR23, LQ24, LE25...)? Si SÍ → queda demostrado que
# MAGIC "Vigente" arrastra años previos, y por lo tanto que para 2017–2025 el
# MAGIC universo carece exactamente de esa clase de licitación. Si NO → la hipótesis
# MAGIC se cae y hay que buscar otra causa (no se fuerza).
# MAGIC
# MAGIC **6D-5 mide si el hueco importa**, en conteo Y en monto, por tipo de
# MAGIC procedimiento. Esto NO es curiosidad: si faltan preferentemente las LR
# MAGIC (≥5.000 UTM) y LQ, la tasa de consistencia ONU de la **Pregunta 1** del
# MAGIC proyecto se mediría sobre una submuestra que excluye los contratos más
# MAGIC grandes — hay que declararlo ANTES de correr la Celda 9, no después.
# MAGIC
# MAGIC **6D-6 aterriza los 4 códigos de 6D-3:** ¿existe en el universo algún código
# MAGIC de la misma familia de sufijo (`-LR23`) y de la misma unidad de compra
# MAGIC (`5839-`)? Distingue hueco estructural de ausencia puntual.
# MAGIC
# MAGIC **Costo y seguridad de ejecución:** UNA sola celda de Databricks, exactamente
# MAGIC 3 acciones de Spark, sin `.persist()` (regla §1.14), sin `.show()` sobre
# MAGIC DataFrames grandes, y sin depender de ninguna variable que solo exista si se
# MAGIC corrió una celda opcional (si `CODIGOS_A_VERIFICAR` no está definida porque
# MAGIC te saltaste la 6D-3, esta celda la define sola — ver el error de la sesión 7,
# MAGIC que fue exactamente eso). Se escribió así a propósito porque el cupo de Free
# MAGIC Edition es finito y se agota con corridas repetidas.
# MAGIC
# MAGIC Esta celda NO cambia ninguna cifra de G6 ni de reconciliación.

# COMMAND ----------

# --- Guarda de dependencias (lección de la sesión 7: no depender de una celda
# --- opcional). Si te saltaste la Celda 6D-3, la lista se define aquí igual.
try:
    CODIGOS_A_VERIFICAR
except NameError:
    CODIGOS_A_VERIFICAR = ["5839-14-LR23", "3789-22-LQ24",
                           "1058086-50-LR24", "1057554-79-LQ23"]
    print("AVISO: CODIGOS_A_VERIFICAR no existía (no se corrió la Celda 6D-3);")
    print("       se define localmente con los 4 códigos de la muestra 6D-2.\n")

# Patrón de sufijo más permisivo que el de la Celda 6C: acepta tipos con dígito
# interno (H2, B2) además de los de solo letras (LR, LQ, LE, LP, L1, CO, TD...).
# Ej: '5839-14-LR23' -> proc='LR', anio='23';  '...-H219' -> proc='H2', anio='19'.
PATRON_PROC_ANIO = r"-([A-Za-z][A-Za-z0-9]{0,2})(\d{2})$"

def _proc_de(col_codigo):
    return F.upper(F.regexp_extract(F.trim(col_codigo), PATRON_PROC_ANIO, 1))

def _anio_suf_de(col_codigo):
    _s = F.regexp_extract(F.trim(col_codigo), PATRON_PROC_ANIO, 2)
    return F.when(_s == "", F.lit(None).cast("int")).otherwise(2000 + _s.cast("int"))

# ===========================================================================
# ACCIÓN 1 de 3 — lado LICITACIÓN: ¿qué códigos existen, por categoría de
# archivo (historico/vigente), año del SUFIJO del código y tipo de
# procedimiento? Se agrega una sola vez y se trae a Python (tabla chica) para
# imprimir tres vistas distintas sin volver a golpear Spark.
# `approx_count_distinct` en vez de `countDistinct` para ahorrar shuffle: acá
# importa el ORDEN DE MAGNITUD (¿ninguno, cientos, cientos de miles?), no la
# cifra exacta — por eso NADA de esta celda se cita como cifra fina.
# ===========================================================================
print("=" * 78)
print("6D-4 — LADO LICITACIÓN: códigos presentes por categoría de archivo,")
print("       año del sufijo del código y tipo de procedimiento (APROXIMADO)")
print("=" * 78)

_lic_diag = (
    df_lic_all
    .select(
        F.col("_categoria_lic").alias("categoria"),
        F.col("_year").alias("anio_archivo"),
        F.trim(c_(COL_KEY_LIC)).alias("cod"),
    )
    .filter(F.col("cod").isNotNull() & (F.col("cod") != ""))
    .withColumn("proc", _proc_de(F.col("cod")))
    .withColumn("anio_suf", _anio_suf_de(F.col("cod")))
    .groupBy("categoria", "anio_archivo", "anio_suf", "proc")
    .agg(F.approx_count_distinct("cod").alias("n_cod_aprox"))
    .collect()
)

_rows_lic = [r.asDict() for r in _lic_diag]
print(f"  filas del diagnóstico traídas a Python: {len(_rows_lic)}")

# --- 6D-4.A — EL TEST DECISIVO ---------------------------------------------
print("\n" + "-" * 78)
print("  6D-4.A — TEST DECISIVO: en el archivo VIGENTE (solo 2026 existe en el")
print("           volumen), ¿hay códigos con sufijo de años ANTERIORES a 2026?")
print("-" * 78)
_vig = [r for r in _rows_lic if str(r["categoria"]).endswith("vigente")]
if not _vig:
    print("  ⚠️  No hay ninguna fila de categoría '*_vigente' — revisar por qué")
    print("      (se esperaba lic_vigente para 2026). Sin esto el test no corre.")
else:
    _por_anio_suf = {}
    for r in _vig:
        _k = r["anio_suf"]
        _por_anio_suf[_k] = _por_anio_suf.get(_k, 0) + (r["n_cod_aprox"] or 0)
    print(f"  {'anio_suf del codigo':>22} | {'n codigos (aprox)':>18}")
    for _k in sorted(_por_anio_suf, key=lambda x: (x is None, x)):
        _et = "sin sufijo reconocible" if _k is None else str(_k)
        print(f"  {_et:>22} | {_por_anio_suf[_k]:>18,}")
    _previos = sum(v for k, v in _por_anio_suf.items() if k is not None and k < 2026)
    _total_v = sum(_por_anio_suf.values())
    print(f"\n  Códigos con sufijo < 2026 dentro del archivo VIGENTE: "
          f"{_previos:,} de {_total_v:,}")
    if _previos > 0:
        print("  → ⚠️  HIPÓTESIS SOSTENIDA: el dataset 'Vigente' SÍ arrastra licitaciones")
        print("     de años anteriores. Implicación directa: como solo se descargó")
        print("     'Vigente' de 2026, para 2017–2025 el universo cargado carece de las")
        print("     licitaciones que en su momento estaban aún activas — justo la clase a")
        print("     la que pertenece 5839-14-LR23 (contrato de 48 meses). NO cerrar el")
        print("     pendiente con esto solo: confirmar contra la documentación de descarga")
        print("     de ChileCompra qué criterio separa Histórico de Vigente (regla §1.13)")
        print("     antes de escribirlo como causa.")
    else:
        print("  → HIPÓTESIS DEBILITADA: el archivo Vigente de 2026 solo trae códigos con")
        print("     sufijo 2026. La ausencia NO se explica por el corte Histórico/Vigente;")
        print("     buscar otra causa (no forzar esta).")

# --- 6D-4.B — ¿existe cada tipo de procedimiento, y en qué volumen? --------
print("\n" + "-" * 78)
print("  6D-4.B — ¿EXISTE cada tipo de procedimiento en el universo cargado?")
print("           (suma sobre todas las categorías y años de archivo)")
print("-" * 78)
_por_proc = {}
for r in _rows_lic:
    _p = r["proc"] if r["proc"] else "(sin sufijo)"
    _por_proc[_p] = _por_proc.get(_p, 0) + (r["n_cod_aprox"] or 0)
print(f"  {'proc':>12} | {'n codigos (aprox)':>18}")
for _p in sorted(_por_proc, key=lambda x: -_por_proc[x]):
    print(f"  {_p:>12} | {_por_proc[_p]:>18,}")

# --- 6D-4.C — cruce proc × año del sufijo, tipos de licitación grande ------
print("\n" + "-" * 78)
print("  6D-4.C — CRUCE proc × año del sufijo (solo tipos de licitación grande:")
print("           LR, LQ, LP, H2 — donde caen los 4 casos de 6D-3)")
print("-" * 78)
_grandes = {"LR", "LQ", "LP", "H2"}
_cruce, _anios_vistos = {}, set()
for r in _rows_lic:
    _p = r["proc"] or ""
    if _p in _grandes:
        _a = r["anio_suf"]
        _anios_vistos.add(_a)
        _cruce[(_p, _a)] = _cruce.get((_p, _a), 0) + (r["n_cod_aprox"] or 0)
_anios_ord = sorted([a for a in _anios_vistos if a is not None])
if not _cruce:
    print("  ⚠️  Ningún código con sufijo LR/LQ/LP/H2 en TODO el universo cargado.")
    print("     Eso ya sería, por sí solo, un hueco estructural grande — antes de")
    print("     creerlo, verificar que el patrón de sufijo sea el correcto (ver 6C).")
else:
    print("  " + f"{'proc':>6} | " + " | ".join(f"{a:>8}" for a in _anios_ord))
    for _p in sorted(_grandes):
        if any(k[0] == _p for k in _cruce):
            print("  " + f"{_p:>6} | " +
                  " | ".join(f"{_cruce.get((_p, a), 0):>8,}" for a in _anios_ord))

# --- 6D-4.D — contraprueba del test decisivo, GRATIS (mismos datos ya
# --- recolectados): ¿el archivo HISTÓRICO también arrastra años anteriores?
# Si `lic_historico` de 2024 trae códigos con sufijo 2023, entonces el export SÍ
# incluye licitaciones de años previos en Histórico, y la hipótesis del corte
# Histórico/Vigente se DEBILITA aunque 6D-4.A haya salido positiva. Este corte
# está justamente para no quedarse con la primera lectura que calce (regla §1.8).
print("\n" + "-" * 78)
print("  6D-4.D — CONTRAPRUEBA: en los archivos HISTÓRICO, ¿el año del archivo")
print("           coincide con el año del sufijo del código, o también arrastra?")
print("           (filas = año del archivo, columnas = año del sufijo)")
print("-" * 78)
_hist = [r for r in _rows_lic if str(r["categoria"]).endswith("historico")]
if not _hist:
    print("  ⚠️  No hay filas de categoría '*_historico'.")
else:
    _m = {}
    _cols, _filas = set(), set()
    for r in _hist:
        _af, _as_ = r["anio_archivo"], r["anio_suf"]
        _filas.add(_af)
        _cols.add(_as_)
        _m[(_af, _as_)] = _m.get((_af, _as_), 0) + (r["n_cod_aprox"] or 0)
    _cols_ord = sorted([a for a in _cols if a is not None]) + \
                ([None] if None in _cols else [])
    _enc = ["s/suf" if a is None else str(a) for a in _cols_ord]
    print("  " + f"{'archivo':>8} | " + " | ".join(f"{e:>7}" for e in _enc))
    _diag, _fuera = 0, 0
    for _af in sorted(_filas):
        print("  " + f"{_af:>8} | " +
              " | ".join(f"{_m.get((_af, a), 0):>7,}" for a in _cols_ord))
        for a in _cols_ord:
            _v = _m.get((_af, a), 0)
            if a is not None and a == _af:
                _diag += _v
            elif a is not None:
                _fuera += _v
    _tot_h = _diag + _fuera
    if _tot_h:
        print(f"\n  Códigos en la diagonal (año archivo == año sufijo): {_diag:,} "
              f"({100.0*_diag/_tot_h:.1f}%)")
        print(f"  Códigos FUERA de la diagonal:                       {_fuera:,} "
              f"({100.0*_fuera/_tot_h:.1f}%)")
        print("  → Si es casi todo diagonal, cada archivo Histórico trae solo su propio")
        print("     año, y una licitación aún activa de 2023 no tendría dónde aparecer:")
        print("     REFUERZA la hipótesis del corte Histórico/Vigente. Si hay bastante")
        print("     fuera de diagonal, el Histórico SÍ arrastra años previos y la")
        print("     hipótesis se DEBILITA — buscar otra causa.")

# ===========================================================================
# ACCIÓN 2 de 3 — lado OC: ¿la falla de enlace es SISTEMÁTICA por tipo de
# procedimiento? Se mide en CONTEO y en MONTO en la misma pasada, solo sobre
# las OC que traen clave (las sin clave no aplican, por diseño normativo).
# `df_m_linked` ya trae `_monto_linea_total` (= precio_neto_oc × cantidad_oc,
# la escala correcta [MEDIDO] sesión 4) y `tiene_lic`.
# ===========================================================================
print("\n\n" + "=" * 78)
print("6D-5 — ¿FALLAN MÁS LAS LICITACIONES GRANDES? tasa de enlace por tipo de")
print("       procedimiento, en conteo y en monto (solo OC CON clave, en CLP)")
print("=" * 78)

_oc_diag = (
    df_m_linked
    .filter(_key_oc_no_vacia & (c_(COL_MONEDA) == "CLP"))
    .withColumn("proc", _proc_de(c_(COL_KEY_OC)))
    .groupBy("proc")
    .agg(F.count("*").alias("n_lineas"),
         F.sum(F.col("tiene_lic").cast("int")).alias("n_enlazadas"),
         F.sum("_monto_linea_total").alias("monto_total"),
         F.sum(F.when(F.col("tiene_lic"), F.col("_monto_linea_total"))).alias("monto_enlazado"))
    .collect()
)

_rows_oc = sorted([r.asDict() for r in _oc_diag],
                  key=lambda r: -(r["monto_total"] or 0))

print(f"  {'proc':>10} | {'n_lineas':>12} | {'% enlaz':>8} | "
      f"{'monto_total_CLP':>22} | {'% monto enl':>11}")
print("  " + "-" * 76)
_no_enl_monto = 0.0
for r in _rows_oc:
    _p = r["proc"] if r["proc"] else "(sin suf)"
    _n = r["n_lineas"] or 0
    _e = r["n_enlazadas"] or 0
    _mt = r["monto_total"] or 0.0
    _me = r["monto_enlazado"] or 0.0
    _pct_n = (100.0 * _e / _n) if _n else float("nan")
    _pct_m = (100.0 * _me / _mt) if _mt else float("nan")
    _no_enl_monto += (_mt - _me)
    print(f"  {_p:>10} | {_n:>12,} | {_pct_n:>7.2f}% | {_mt:>22,.0f} | {_pct_m:>10.2f}%")

print(f"\n  Monto CLP con clave que NO enlaza (todos los proc): {_no_enl_monto:>22,.0f}")
print("  Leer así, y solo así: si el '% enlaz' de LR/LQ/LP es claramente menor que el")
print("  de L1/LE, la ausencia es SISTEMÁTICA en licitaciones grandes, y la tasa de")
print("  consistencia ONU de la Pregunta 1 quedaría medida sobre una submuestra")
print("  sesgada (hay que declararlo en metodología). Si las tasas son parecidas, el")
print("  hueco es parejo y no sesga por tamaño. NO leer esto como tendencia temporal:")
print("  acá no hay eje de tiempo (regla §1.8).")

# ===========================================================================
# ACCIÓN 3 de 3 — los 4 códigos de 6D-3, aterrizados: ¿hueco estructural o
# ausencia caso a caso? Para cada código se pregunta si existe en el universo
# ALGÚN otro código (a) de la misma familia de sufijo ('-LR23') y (b) de la
# misma unidad de compra ('5839-'). Todo en UNA sola agregación.
# ===========================================================================
print("\n\n" + "=" * 78)
print("6D-6 — ¿HUECO ESTRUCTURAL O AUSENCIA CASO A CASO? contexto de los 4")
print("       códigos que 6D-3 no encontró")
print("=" * 78)

_cod_norm = (df_lic_all
             .select(F.trim(c_(COL_KEY_LIC)).alias("cod"))
             .filter(F.col("cod").isNotNull() & (F.col("cod") != "")))

_aggs2, _meta = [], []
for _i, _cod in enumerate(CODIGOS_A_VERIFICAR):
    _c = _cod.strip()
    _sufijo = _c.split("-")[-1] if "-" in _c else _c        # ej. 'LR23'
    _unidad = _c.split("-")[0] if "-" in _c else _c         # ej. '5839'
    _meta.append((_cod, _sufijo, _unidad))
    _aggs2 += [
        F.sum(F.col("cod").endswith("-" + _sufijo).cast("int")).alias(f"n_fam_{_i}"),
        F.approx_count_distinct(
            F.when(F.col("cod").endswith("-" + _sufijo), F.col("cod"))
        ).alias(f"d_fam_{_i}"),
        F.approx_count_distinct(
            F.when(F.col("cod").startswith(_unidad + "-"), F.col("cod"))
        ).alias(f"d_uni_{_i}"),
    ]

_fila2 = _cod_norm.agg(*_aggs2).collect()[0].asDict()

for _i, (_cod, _sufijo, _unidad) in enumerate(_meta):
    _n_fam = _fila2[f"n_fam_{_i}"] or 0
    _d_fam = _fila2[f"d_fam_{_i}"] or 0
    _d_uni = _fila2[f"d_uni_{_i}"] or 0
    print(f"\n  Código ausente: '{_cod}'")
    print(f"    otros códigos de la MISMA familia de sufijo '-{_sufijo}':")
    print(f"        filas: {_n_fam:>12,}   |   códigos distintos (aprox): {_d_fam:>10,}")
    print(f"    códigos de la MISMA unidad de compra '{_unidad}-' (cualquier año/tipo):")
    print(f"        códigos distintos (aprox): {_d_uni:>10,}")
    if _d_fam == 0:
        print("    → HUECO ESTRUCTURAL en esta familia: no existe NINGÚN código")
        print(f"       '-{_sufijo}' en todo el universo cargado. La ausencia no es de este")
        print("       registro, es de la clase completa.")
    elif _d_uni == 0:
        print("    → La familia de sufijo sí existe, pero esta UNIDAD DE COMPRA no aparece")
        print("       con ningún código en todo el universo — apunta a un organismo/unidad")
        print("       ausente del export, no a un registro suelto.")
    else:
        print("    → AUSENCIA CASO A CASO: tanto la familia de sufijo como la unidad de")
        print("       compra existen en el universo; falta este registro puntual. Descarta")
        print("       'hueco estructural' para este caso y deja en pie el corte")
        print("       Histórico/Vigente o una exportación incompleta puntual.")

print("\n" + "=" * 78)
print(f"[POR VERIFICAR] run_id={RUN_ID} — esta celda junta evidencia, NO cierra el")
print("pendiente (c) por sí sola (regla §1.13). Lo que SÍ queda cerrado desde 6D-3:")
print("la hipótesis de formato en el join está REFUTADA (0 match con trim/upper en los")
print("4 códigos) — el join de la Celda 6 no tiene ese bug y G6 no se mueve.")
print("Pegar esta salida completa en el próximo chat para decidir la causa con")
print("evidencia, y contrastarla contra la documentación de descarga de ChileCompra")
print("antes de escribir cualquier mecanismo como confirmado.")
# COMMAND ----------

# MAGIC %md
# MAGIC ## Apéndice C-4 (NUEVO, sesión 10) — 6D-7/6D-8: ¿el hueco de licitaciones
# MAGIC ## tiene FIRMA de contrato plurianual, y afecta a la Pregunta 1?
# MAGIC
# MAGIC **De dónde viene esta celda.** La corrida de 6D-4/5/6 (`run_id=5fc3ded29565`,
# MAGIC 20-ago sesión 9) dejó tres cosas claras y una mal leída:
# MAGIC
# MAGIC 1. `[MEDIDO]` **6D-6 — es ausencia caso a caso, no hueco estructural.** Para
# MAGIC    los 4 códigos, tanto la familia de sufijo (`-LR23`: 6.123 códigos) como la
# MAGIC    unidad de compra (`5839-`: 159 códigos) existen en el universo.
# MAGIC 2. `[MEDIDO]` **6D-5 — el hueco NO castiga a las licitaciones grandes.** LR
# MAGIC    enlaza 94,84% en conteo y 95,89% en monto: MEJOR que LE (91,53%), LQ
# MAGIC    (89,63%) y LP (84,53%). La Pregunta 1 no se mide sobre una submuestra
# MAGIC    sesgada hacia lo chico. *Pero* 6D-5 no tiene eje de tiempo (regla §1.8) y
# MAGIC    el patrón que se investiga es de 2024-2026: el promedio de 10 años puede
# MAGIC    estar tapando un sesgo reciente. **Eso es lo que cierra 6D-7.**
# MAGIC 3. `[MEDIDO]` **6D-4.D — cada archivo `Histórico` trae su propio año** (99,0%
# MAGIC    en la diagonal); lo que sale de la diagonal es goteo de año adyacente.
# MAGIC 4. **Lo mal leído:** 6D-4.A imprimió "HIPÓTESIS SOSTENIDA" porque el conteo de
# MAGIC    códigos pre-2026 en el archivo `Vigente` fue > 0. Pero la MAGNITUD dice otra
# MAGIC    cosa: 1.180 códigos con sufijo **2025**, **1** con sufijo 2024, **1** con
# MAGIC    sufijo 2023, 0 antes. Si `Vigente` retuviera licitaciones con contrato aún
# MAGIC    en ejecución, el Vigente de 2026 debería traer cientos o miles de LR23/LR24
# MAGIC    (existen 6.040 y 6.481 códigos de esas familias) — trae dos en total. La
# MAGIC    forma de la cola (decae en ~1 año) es la de **"proceso todavía abierto"**,
# MAGIC    no la de **"contrato todavía vigente"**. Con eso, la hipótesis del corte
# MAGIC    Histórico/Vigente como causa queda **fuertemente debilitada**, no sostenida.
# MAGIC
# MAGIC **Qué prueba esta celda, y cómo puede caerse.**
# MAGIC
# MAGIC - **6D-7** mide la tasa de enlace cruzando **año de la OC × tipo de
# MAGIC   procedimiento × antigüedad de la licitación referenciada** (`lag` = año de la
# MAGIC   OC − año del sufijo del código). Tres lecturas posibles, todas escritas de
# MAGIC   antemano:
# MAGIC     - Si el no-enlace se concentra en `lag ≥ 2` y en LR/LQ/LP → **firma de
# MAGIC       contrato plurianual**: falta una clase de licitación, y hay que declararlo
# MAGIC       en metodología antes de la Celda 9.
# MAGIC     - Si el no-enlace es parejo por `lag` y por `proc` dentro de 2024-2026 →
# MAGIC       **pérdida difusa del export**, sin sesgo de tamaño ni de plazo.
# MAGIC     - Si se concentra en `lag = 0` → el problema es de **desfase de
# MAGIC       indexación** (la OC se emite antes de que la licitación entre al archivo),
# MAGIC       y es esperable que se cierre solo con el tiempo.
# MAGIC   El corte por `lag` se calcula excluyendo las licitaciones anteriores a 2017,
# MAGIC   que ya están explicadas por el truncamiento izquierdo (`[MEDIDO]` sesión 2) y
# MAGIC   que si no se sacan dominan la tabla y la vuelven ilegible.
# MAGIC
# MAGIC - **6D-8** ataca el otro pendiente, el que sí puede matar la Pregunta 1: la
# MAGIC   **hipótesis de la "línea testimonial"**. La ficha de `5839-14-LR23` (contrato
# MAGIC   de ~CLP 19.560 millones a 4 años) muestra **una sola línea de producto**. Si
# MAGIC   eso es lo normal en las licitaciones grandes, entonces comparar el código ONU
# MAGIC   de la licitación contra el de la OC es estructuralmente imposible justo en los
# MAGIC   contratos que mueven más plata — y eso hay que saberlo ANTES de diseñar la
# MAGIC   regla de comparación de conjuntos de la Celda 9, no después. Se mide, por
# MAGIC   tipo de procedimiento: cuántos códigos ONU DISTINTOS declara cada licitación
# MAGIC   (mediana, p90, promedio), qué fracción declara 1 solo o ninguno, y cuántas
# MAGIC   filas tiene la licitación en el archivo — para distinguir "1 ONU porque tiene
# MAGIC   1 línea" de "muchas líneas, todas con el mismo ONU".
# MAGIC
# MAGIC **Costo y seguridad de ejecución (regla §1.15):** UNA celda de Databricks,
# MAGIC exactamente **2 acciones de Spark** (dos `.collect()` de tablas agregadas
# MAGIC chicas), sin `.persist()`/`.cache()` (regla §1.14), sin `.show()` sobre
# MAGIC DataFrames grandes, y sin depender de ninguna variable que solo exista si se
# MAGIC corrió una celda opcional — incluido `COL_COD_PRODUCTO_LIC`, que se define en
# MAGIC la Celda 9 ("NO CORRER TODAVÍA"): acá la columna ONU del lado licitación se
# MAGIC RESUELVE en tiempo de ejecución contra `df_lic_all.columns`, y si no existe,
# MAGIC 6D-8 se salta sola con un aviso en vez de reventar (lección de la sesión 7).
# MAGIC
# MAGIC Esta celda NO cambia ninguna cifra de G6 ni de reconciliación — es exploratoria.

# COMMAND ----------

# ===========================================================================
# GUARDA DE DEPENDENCIAS — esta celda tiene que poder correr sola (regla §1.15)
# ===========================================================================
try:
    PATRON_PROC_ANIO
except NameError:
    PATRON_PROC_ANIO = r"-([A-Za-z][A-Za-z0-9]{0,2})(\d{2})$"
    print("AVISO: PATRON_PROC_ANIO no existía (no se corrió 6D-4); se define aquí.")

try:
    _proc_de
except NameError:
    def _proc_de(col_codigo):
        return F.upper(F.regexp_extract(F.trim(col_codigo), PATRON_PROC_ANIO, 1))
    print("AVISO: _proc_de no existía; se define aquí.")

try:
    _anio_suf_de
except NameError:
    def _anio_suf_de(col_codigo):
        _s = F.regexp_extract(F.trim(col_codigo), PATRON_PROC_ANIO, 2)
        return F.when(_s == "", F.lit(None).cast("int")).otherwise(2000 + _s.cast("int"))
    print("AVISO: _anio_suf_de no existía; se define aquí.")

try:
    _key_oc_no_vacia
except NameError:
    _key_oc_no_vacia = c_(COL_KEY_OC).isNotNull() & (F.trim(c_(COL_KEY_OC)) != "")
    print("AVISO: _key_oc_no_vacia no existía; se define aquí.")

# Tipos de procedimiento agrupados por TAMAÑO del tramo normativo `[FUENTE]`
# (chilecompra.cl / ayuda.mercadopublico.cl, verificado 19-ago sesión 3):
#   L1 <100 UTM · LE 100-1.000 · LP 1.000-5.000 (absorbe LQ/H2 desde 23-oct-2025)
#   LR >=5.000 UTM.  LQ y H2 existieron hasta 2025.
PROC_GRANDES  = ["LR", "LQ", "LP", "H2"]
PROC_CHICOS   = ["LE", "L1"]
PROC_TABLA    = PROC_GRANDES + PROC_CHICOS
ANIOS_RECIENTES = [2024, 2025, 2026]   # el patrón que se investiga (pendiente c)

# ===========================================================================
# ACCIÓN 1 de 2 — lado OC: tasa de enlace por AÑO DE LA OC × PROC × LAG.
# Una sola agregación; todas las vistas se arman después en Python.
# `df_m_linked` ya trae `_monto_linea_total` (= precio_neto_oc × cantidad_oc,
# la escala correcta [MEDIDO] sesión 4), `tiene_lic` y `_year`.
# ===========================================================================
print("=" * 78)
print("6D-7 — ¿EL HUECO TIENE FIRMA DE CONTRATO PLURIANUAL?")
print("       tasa de enlace por año de la OC × procedimiento × antigüedad de la")
print("       licitación referenciada (solo OC CON clave, moneda CLP)")
print("=" * 78)

_diag7 = (
    df_m_linked
    .filter(_key_oc_no_vacia & (c_(COL_MONEDA) == "CLP"))
    .withColumn("proc", _proc_de(c_(COL_KEY_OC)))
    .withColumn("anio_lic", _anio_suf_de(c_(COL_KEY_OC)))
    .withColumn("lag_anios", F.col("_year") - F.col("anio_lic"))
    .groupBy("_year", "proc", "anio_lic", "lag_anios")
    .agg(F.count("*").alias("n"),
         F.sum(F.col("tiene_lic").cast("int")).alias("n_enl"),
         F.sum("_monto_linea_total").alias("m"),
         F.sum(F.when(F.col("tiene_lic"), F.col("_monto_linea_total"))).alias("m_enl"))
    .collect()
)

_r7 = [r.asDict() for r in _diag7]
for _r in _r7:                      # normalizar Nones para no ensuciar cada suma
    _r["n"]     = _r["n"] or 0
    _r["n_enl"] = _r["n_enl"] or 0
    _r["m"]     = float(_r["m"] or 0.0)
    _r["m_enl"] = float(_r["m_enl"] or 0.0)
    _r["proc"]  = _r["proc"] or "(sin suf)"
print(f"  filas del diagnóstico traídas a Python: {len(_r7)}")


def _pct(num, den):
    return (100.0 * num / den) if den else float("nan")


def _tot(rows, campo):
    return sum(r[campo] for r in rows)


# --- 6D-7.A — matriz año de la OC × proc, % enlazado en CONTEO --------------
print("\n" + "-" * 78)
print("  6D-7.A — % ENLAZADO (conteo de líneas) por AÑO DE LA OC × procedimiento")
print("           Esto es lo que 6D-5 no podía ver: 6D-5 promedia los 10 años.")
print("-" * 78)
_anios_oc = sorted({r["_year"] for r in _r7 if r["_year"] is not None})
print("  " + f"{'anio OC':>8} | " + " | ".join(f"{p:>7}" for p in PROC_TABLA) + " | "
      + f"{'TODOS':>7}")
for _a in _anios_oc:
    _fila = [r for r in _r7 if r["_year"] == _a]
    _celdas = []
    for _p in PROC_TABLA:
        _sub = [r for r in _fila if r["proc"] == _p]
        _n = _tot(_sub, "n")
        _celdas.append(f"{_pct(_tot(_sub, 'n_enl'), _n):>6.2f}%" if _n else f"{'—':>7}")
    _nt = _tot(_fila, "n")
    _celdas.append(f"{_pct(_tot(_fila, 'n_enl'), _nt):>6.2f}%" if _nt else f"{'—':>7}")
    print("  " + f"{_a:>8} | " + " | ".join(_celdas))

# --- 6D-7.B — el corte decisivo: tasa de enlace por LAG ---------------------
# Se excluyen las licitaciones anteriores a 2017 (truncamiento izquierdo, ya
# explicado y cuantificado en la sesión 2) y los lag negativos (OC que citan una
# licitación POSTERIOR a su propio año: existen y son su propia anomalía, se
# cuentan aparte para no esconderlas).
print("\n" + "-" * 78)
print("  6D-7.B — TASA DE ENLACE POR ANTIGÜEDAD de la licitación referenciada")
print("           (lag = año de la OC − año del sufijo del código de licitación)")
print("           Excluye licitaciones pre-2017 (truncamiento ya explicado).")
print("-" * 78)
_val = [r for r in _r7
        if r["anio_lic"] is not None and r["anio_lic"] >= 2017
        and r["lag_anios"] is not None and r["lag_anios"] >= 0]
_neg = [r for r in _r7
        if r["lag_anios"] is not None and r["lag_anios"] < 0]
_sin = [r for r in _r7 if r["anio_lic"] is None]

for _et, _univ in [("TODOS los años de OC (2017-2026)", _val),
                   (f"solo OC de {ANIOS_RECIENTES} (el patrón investigado)",
                    [r for r in _val if r["_year"] in ANIOS_RECIENTES])]:
    print(f"\n    · {_et}")
    if not _univ:
        print("      (sin filas en este universo)")
        continue
    print("      " + f"{'lag':>4} | {'n_lineas':>12} | {'% enlaz':>8} | "
          f"{'monto_CLP':>20} | {'% monto enl':>11}")
    print("      " + "-" * 66)
    for _l in sorted({r["lag_anios"] for r in _univ}):
        _sub = [r for r in _univ if r["lag_anios"] == _l]
        _n, _m = _tot(_sub, "n"), _tot(_sub, "m")
        print("      " + f"{_l:>4} | {_n:>12,} | {_pct(_tot(_sub,'n_enl'), _n):>7.2f}% | "
              f"{_m:>20,.0f} | {_pct(_tot(_sub,'m_enl'), _m):>10.2f}%")

if _neg:
    print(f"\n    · ANOMALÍA APARTE — OC que citan una licitación de año POSTERIOR "
          f"al suyo (lag < 0): {_tot(_neg,'n'):,} línea(s), "
          f"{_pct(_tot(_neg,'n_enl'), _tot(_neg,'n')):.2f}% enlazan.")
    print("      No es el objeto de esta celda; queda anotado para no esconderlo.")
if _sin:
    print(f"\n    · Sin sufijo de año reconocible en el código: {_tot(_sin,'n'):,} línea(s), "
          f"{_pct(_tot(_sin,'n_enl'), _tot(_sin,'n')):.2f}% enlazan.")

# --- 6D-7.C — dónde está el hueco RECIENTE, por procedimiento ---------------
print("\n" + "-" * 78)
print(f"  6D-7.C — EL HUECO RECIENTE: OC de {ANIOS_RECIENTES} que NO enlazan,")
print("           por procedimiento, en conteo y en monto CLP")
print("-" * 78)
_rec = [r for r in _r7 if r["_year"] in ANIOS_RECIENTES]
_by_proc = {}
for r in _rec:
    _d = _by_proc.setdefault(r["proc"], {"n": 0, "n_enl": 0, "m": 0.0, "m_enl": 0.0})
    for _k in ("n", "n_enl", "m", "m_enl"):
        _d[_k] += r[_k]
if not _by_proc:
    print("  (no hay OC en los años recientes en este universo)")
else:
    print("  " + f"{'proc':>10} | {'n_lineas':>12} | {'n NO enl':>11} | {'% enlaz':>8} | "
          f"{'monto NO enl CLP':>20}")
    print("  " + "-" * 74)
    for _p in sorted(_by_proc, key=lambda p: -(_by_proc[p]["n"] - _by_proc[p]["n_enl"])):
        _d = _by_proc[_p]
        print("  " + f"{_p:>10} | {_d['n']:>12,} | {_d['n']-_d['n_enl']:>11,} | "
              f"{_pct(_d['n_enl'], _d['n']):>7.2f}% | {_d['m']-_d['m_enl']:>20,.0f}")

# --- 6D-7.D — veredicto, con los números que lo sostienen a la vista --------
print("\n" + "-" * 78)
print("  6D-7.D — LECTURA (los tres desenlaces estaban escritos antes de correr)")
print("-" * 78)
_rec_val = [r for r in _val if r["_year"] in ANIOS_RECIENTES]
if not _rec_val:
    print("  Sin datos suficientes en los años recientes — no se concluye nada.")
else:
    _g = [r for r in _rec_val if r["proc"] in PROC_GRANDES]
    _c = [r for r in _rec_val if r["proc"] in PROC_CHICOS]
    _p_g = _pct(_tot(_g, "n_enl"), _tot(_g, "n"))
    _p_c = _pct(_tot(_c, "n_enl"), _tot(_c, "n"))
    _l0 = [r for r in _rec_val if r["lag_anios"] == 0]
    _l2 = [r for r in _rec_val if r["lag_anios"] >= 2]
    _p_l0 = _pct(_tot(_l0, "n_enl"), _tot(_l0, "n"))
    _p_l2 = _pct(_tot(_l2, "n_enl"), _tot(_l2, "n"))

    # Una comparación con un lado VACÍO no es "no hay diferencia": es "no se puede
    # comparar". Sin esta guarda, nan > 5.0 da False y el veredicto se iría en
    # silencio a "pérdida difusa" sobre cero datos. (Cazado en el sandbox, §1.15.)
    _ok_tam = _tot(_g, "n") > 0 and _tot(_c, "n") > 0
    _ok_lag = _tot(_l0, "n") > 0 and _tot(_l2, "n") > 0

    def _fmt(p, n):
        return (f"{p:.2f}%  (n={n:,})" if n else f"—  (n=0, no comparable)")

    print(f"    Enlace en OC {ANIOS_RECIENTES}, procedimientos GRANDES {PROC_GRANDES}: "
          + _fmt(_p_g, _tot(_g, "n")))
    print(f"    Enlace en OC {ANIOS_RECIENTES}, procedimientos CHICOS  {PROC_CHICOS}: "
          + _fmt(_p_c, _tot(_c, "n")))
    print(f"    Enlace con lag = 0 (licitación del mismo año): "
          + _fmt(_p_l0, _tot(_l0, "n")))
    print(f"    Enlace con lag >= 2 (licitación de 2+ años antes): "
          + _fmt(_p_l2, _tot(_l2, "n")))
    print()
    _brecha_tam = (_p_c - _p_g) if _ok_tam else 0.0   # >0 = las grandes enlazan PEOR
    _brecha_lag = (_p_l0 - _p_l2) if _ok_lag else 0.0  # >0 = las viejas enlazan PEOR
    if not _ok_tam and not _ok_lag:
        print("    → NO SE PUEDE CONCLUIR: ninguno de los dos contrastes tiene datos en")
        print("      ambos lados. No es evidencia de ausencia de sesgo, es ausencia de")
        print("      evidencia — revisar el filtro de años/procedimientos antes de leer.")
    elif not _ok_tam:
        print(f"    → Contraste por TAMAÑO no comparable (un lado sin filas). Solo se lee")
        print(f"      el de antigüedad: brecha lag0−lag2+ = {_brecha_lag:+.2f} puntos.")
    elif not _ok_lag:
        print(f"    → Contraste por ANTIGÜEDAD no comparable (un lado sin filas). Solo se")
        print(f"      lee el de tamaño: brecha chicos−grandes = {_brecha_tam:+.2f} puntos.")
    elif _brecha_tam > 5.0 and _brecha_lag > 5.0:
        print("    → COMPATIBLE con la firma de CONTRATO PLURIANUAL: las licitaciones")
        print("      grandes y las referenciadas con 2+ años de antigüedad enlazan")
        print("      claramente peor. Declararlo en metodología ANTES de la Celda 9.")
    elif _brecha_lag > 5.0 and _brecha_tam <= 5.0:
        print("    → El plazo pesa, el TAMAÑO no: enlazan peor las licitaciones viejas")
        print("      pero no las grandes. Apunta a un corte por antigüedad del archivo,")
        print("      no a que falte una clase de licitación grande.")
    elif _brecha_tam > 5.0 and _brecha_lag <= 5.0:
        print("    → El TAMAÑO pesa, el plazo no: sesgo por tipo de procedimiento sin")
        print("      relación con la antigüedad. Buscar causa en el procedimiento mismo.")
    elif _p_l0 < _p_l2 - 5.0:
        print("    → COMPATIBLE con DESFASE DE INDEXACIÓN: lo que peor enlaza es la")
        print("      licitación del MISMO año de la OC. Es esperable que se cierre solo.")
    else:
        print("    → PÉRDIDA DIFUSA: ni el tamaño ni la antigüedad separan claramente")
        print("      (brechas por debajo de 5 puntos). No hay clase faltante que")
        print("      declarar; el hueco se reporta como ruido residual del export.")
    print()
    print("    Umbral usado: 5 puntos porcentuales, fijado antes de correr, arbitrario")
    print("    y declarado como tal. Las cifras de arriba están impresas para que")
    print("    cualquiera pueda releer el veredicto con otro umbral.")

# ===========================================================================
# ACCIÓN 2 de 2 — lado LICITACIÓN: la hipótesis de la "LÍNEA TESTIMONIAL".
# ¿Cuántos códigos ONU DISTINTOS declara una licitación, por procedimiento?
# Conteo EXACTO (countDistinct, no aproximado): la afirmación que se quiere
# sostener es "la mediana es 1", y ahí un estimador aproximado no sirve.
# ===========================================================================
print("\n\n" + "=" * 78)
print("6D-8 — HIPÓTESIS DE LA 'LÍNEA TESTIMONIAL': ¿cuántos códigos ONU distintos")
print("       declara una licitación, según su tipo de procedimiento?")
print("=" * 78)

_CAND_ONU_LIC = ["onu_lic", "lic_onu_lic", "codigo_producto_lic", "codigo_producto",
                 "codigo_onu_lic", "codigo_onu", "onu", "CodigoProducto"]
_col_onu_lic = next((_x for _x in _CAND_ONU_LIC if _x in df_lic_all.columns), None)

if _col_onu_lic is None:
    print("  ⚠️  AVISO: no se encontró ninguna columna de código ONU en df_lic_all.")
    print(f"      Se buscaron: {_CAND_ONU_LIC}")
    print(f"      Columnas disponibles ({len(df_lic_all.columns)}): "
          f"{sorted(df_lic_all.columns)[:40]}")
    print("      6D-8 se SALTA (no revienta la celda). Corregir el nombre en")
    print("      _CAND_ONU_LIC y volver a correr solo esta celda.")
else:
    print(f"  Columna ONU del lado licitación resuelta en tiempo de ejecución: "
          f"'{_col_onu_lic}'")

    _onu_col = c_(_col_onu_lic).cast("string")
    _onu_valido = F.when(_onu_col.isNotNull() & (F.trim(_onu_col) != ""),
                         F.trim(_onu_col))

    _diag8 = (
        df_lic_all
        .select(F.trim(c_(COL_KEY_LIC)).alias("cod"), _onu_valido.alias("onu"))
        .filter(F.col("cod").isNotNull() & (F.col("cod") != ""))
        .withColumn("proc", _proc_de(F.col("cod")))
        .groupBy("proc", "cod")
        .agg(F.count("*").alias("n_filas"),
             F.countDistinct("onu").alias("n_onu"))
        .groupBy("proc")
        .agg(F.count("*").alias("n_lic"),
             F.sum(F.when(F.col("n_onu") == 0, 1).otherwise(0)).alias("n_lic_sin_onu"),
             F.sum(F.when(F.col("n_onu") == 1, 1).otherwise(0)).alias("n_lic_1onu"),
             F.expr("percentile_approx(n_onu, 0.5)").alias("p50_onu"),
             F.expr("percentile_approx(n_onu, 0.9)").alias("p90_onu"),
             F.round(F.avg("n_onu"), 2).alias("prom_onu"),
             F.expr("percentile_approx(n_filas, 0.5)").alias("p50_filas"),
             F.round(F.avg("n_filas"), 2).alias("prom_filas"))
        .collect()
    )

    _r8 = {(_r["proc"] or "(sin suf)"): _r.asDict() for _r in _diag8}
    print(f"  filas del diagnóstico traídas a Python: {len(_r8)}")
    print()
    print("  " + f"{'proc':>10} | {'n_lic':>10} | {'s/ONU':>7} | {'1 ONU':>7} | "
          f"{'p50':>5} | {'p90':>6} | {'prom':>8} | {'p50 filas':>9}")
    print("  " + "-" * 82)
    for _p in PROC_TABLA + [_k for _k in sorted(_r8) if _k not in PROC_TABLA]:
        if _p not in _r8:
            continue
        _d = _r8[_p]
        _nl = _d["n_lic"] or 0
        print("  " + f"{_p:>10} | {_nl:>10,} | "
              f"{_pct(_d['n_lic_sin_onu'] or 0, _nl):>6.1f}% | "
              f"{_pct(_d['n_lic_1onu'] or 0, _nl):>6.1f}% | "
              f"{(_d['p50_onu'] if _d['p50_onu'] is not None else -1):>5} | "
              f"{(_d['p90_onu'] if _d['p90_onu'] is not None else -1):>6} | "
              f"{(_d['prom_onu'] if _d['prom_onu'] is not None else float('nan')):>8.2f} | "
              f"{(_d['p50_filas'] if _d['p50_filas'] is not None else -1):>9}")
    print()
    print("  Columnas: 's/ONU' = % de licitaciones sin ningún código ONU no vacío;")
    print("  '1 ONU' = % que declara exactamente UNO; 'p50/p90/prom' = códigos ONU")
    print("  distintos por licitación; 'p50 filas' = filas de esa licitación en el")
    print("  archivo (para separar 'tiene 1 ONU porque tiene 1 línea' de 'tiene muchas")
    print("  líneas, todas con el mismo ONU').")
    print()

    _gr = [_r8[_p] for _p in PROC_GRANDES if _p in _r8]
    _ch = [_r8[_p] for _p in PROC_CHICOS if _p in _r8]
    _ng = sum(_d["n_lic"] or 0 for _d in _gr)
    _nc = sum(_d["n_lic"] or 0 for _d in _ch)
    if not (_ng and _nc):
        print("    → Contraste grandes-vs-chicas NO COMPARABLE: uno de los dos grupos no")
        print(f"      tiene licitaciones en el universo (grandes n={_ng:,}, chicas "
              f"n={_nc:,}). No se concluye nada sobre la línea testimonial.")
    else:
        _1g = _pct(sum((_d["n_lic_1onu"] or 0) + (_d["n_lic_sin_onu"] or 0) for _d in _gr), _ng)
        _1c = _pct(sum((_d["n_lic_1onu"] or 0) + (_d["n_lic_sin_onu"] or 0) for _d in _ch), _nc)
        print(f"    Licitaciones GRANDES {PROC_GRANDES} con 0 o 1 código ONU: "
              f"{_1g:.1f}%  (n={_ng:,})")
        print(f"    Licitaciones CHICAS  {PROC_CHICOS} con 0 o 1 código ONU: "
              f"{_1c:.1f}%  (n={_nc:,})")
        if _1g > _1c + 10.0:
            print("    → HIPÓTESIS DE LÍNEA TESTIMONIAL COMPATIBLE con los datos: las")
            print("      licitaciones grandes declaran capa de producto notoriamente más")
            print("      pobre. La comparación ONU lic↔OC de la Celda 9 sería casi vacía")
            print("      justo donde está la plata: hay que rediseñar la regla o declarar")
            print("      el límite. NO cerrar con esto solo (regla §1.13): confirmar con")
            print("      2-3 fichas reales de licitaciones LR en mercadopublico.cl.")
        elif _1c > _1g + 10.0:
            print("    → Al revés de lo esperado: las licitaciones CHICAS son las de capa")
            print("      de producto más pobre. La ficha de 5839-14-LR23 no sería el caso")
            print("      típico de una LR — no generalizar desde ella.")
        else:
            print("    → HIPÓTESIS DE LÍNEA TESTIMONIAL NO SOSTENIDA: grandes y chicas")
            print("      declaran capa de producto parecida (brecha < 10 puntos). La ficha")
            print("      de 5839-14-LR23 es un caso, no un patrón: no se generaliza.")
        print()
        print("    Umbral usado: 10 puntos porcentuales, fijado antes de correr y")
        print("    declarado como arbitrario. Las cifras están impresas para releerlo.")

print("\n" + "=" * 78)
print(f"[POR VERIFICAR] run_id={RUN_ID} — 6D-7 y 6D-8 juntan evidencia MEDIDA del")
print("propio dataset; ninguna de las dos cierra su pendiente sola (regla §1.13).")
print("6D-7 necesita, además, el criterio documentado que separa 'Histórico' de")
print("'Vigente' en las descargas de ChileCompra (no legible por herramienta: la")
print("página se renderiza con JavaScript — hay que leerla a mano).")
print("6D-8 necesita 2-3 fichas reales de licitaciones LR en mercadopublico.cl para")
print("confirmar que la pobreza de capa de producto es real y no un artefacto del")
print("export. Pegar esta salida completa en el próximo chat.")
print("=" * 78)


# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════════════
# MAGIC # BLOQUES NUEVOS — sesión 14 (23-ago-2026)
# MAGIC # CAP-0-G7 · CAP-10 (G7) · CAP-11 (muestra) · CAP-12 (Spark ML)
# MAGIC # ═══════════════════════════════════════════════════════════════
# MAGIC Ver `00-INSTRUCCIONES-DATABRICKS.md` para el orden exacto de ejecución.

# =============================================================================
#  celdas_capstone_v15.py — sesión 14, 23-ago-2026
#
#  CUATRO BLOQUES NUEVOS, en orden de dependencia:
#    CAP-0-G7 · utilidades AUTOCONTENIDAS   (obligatoria, ~5 s)
#    CAP-10   · G7 — ¿hay texto evaluable?  (obligatoria, ~10 min)
#    CAP-11   · muestra estratificada       (solo si G7 pasa, ~3 min)
#    CAP-12   · Pipeline de Spark ML        (opcional, después de la Clase 10)
#
#  AUTOCONTENIDO A PROPÓSITO. No depende de `celdas_capstone_v14.py`: define
#  sus propias utilidades con prefijo `g7_` y su propio `RUN_ID_G7`. Se puede
#  correr con v14 cargada o sin ella, y nunca pisa a CAP-0.
#
#  LO ÚNICO QUE NECESITA DEL NOTEBOOK: `df_oc_all` en el kernel.
#  Eso lo dejan las celdas 1, 2 y 3 de la Etapa 2. Las celdas 4 a 8 NO hacen
#  falta para nada de aquí.
#
#  REGLA §1.16: ningún veredicto impreso es una conclusión. Cada uno viene con
#  su magnitud al lado. Todo filtro que excluye datos se cuenta y se imprime.
# =============================================================================


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-0-G7 · Utilidades autocontenidas
# MAGIC **Obligatoria.** Correr antes de CAP-10. No pisa el CAP-0 de `celdas_capstone_v14`.

# COMMAND ----------

import uuid as _uuid_g7
import re as _re_g7
from pyspark.sql import functions as F
from pyspark.sql import Window as W

RUN_ID_G7 = _uuid_g7.uuid4().hex[:12]
print("=" * 78)
print(f"CAP-0-G7 · utilidades cargadas · RUN_ID_G7 = {RUN_ID_G7}")
print("=" * 78)

# --- Umbrales, TODOS declarados y arbitrarios (regla §1.16.b) ----------------
MIN_CHARS_G7 = 12            # caracteres útiles mínimos para considerar informativo
TOP_MULETILLAS = 40          # cadenas frecuentes que se imprimen ANTES de filtrar
N_MUESTRA = 300              # 300 piloto · 384 para ±5pp · 1067 para ±3pp
SEED_MUESTRA = 20260823      # declarada: la muestra se reproduce exactamente
LIMITE_PARES_CAP12 = 2_000_000   # tope de pares distintos que procesa CAP-12

RUTA_SALIDA_G7 = "/Volumes/workspace/default/mercado_publico/capstone_salidas"
RUTA_CATALOGO = "/Volumes/workspace/default/mercado_publico/catalogo_onu_18881.csv"

# Lista PROVISORIA de muletillas. El bloque 10.C imprime la lista REAL medida;
# si aparece una frecuente que no está aquí, se agrega y se vuelve a correr 10.D.
MULETILLAS_G7 = [
    r"^segun bases.*", r"^ver bases.*", r"^de acuerdo a bases.*",
    r"^segun adjudicacion.*", r"^segun licitacion.*", r"^segun oferta.*",
    r"^segun contrato.*", r"^segun convenio.*", r"^segun cotizacion.*",
    r"^segun solicitud.*", r"^segun oc.*", r"^segun detalle.*",
    r"^ver detalle.*", r"^ver anexo.*", r"^ver adjunto.*",
    r"^s ?/ ?e$", r"^sin especificacion.*", r"^no aplica$", r"^na$", r"^n a$",
    r"^varios$", r"^otros$", r"^servicio$", r"^servicios$", r"^producto$",
    r"^productos$", r"^item$", r"^items$", r"^bienes$",
    r"^-+$", r"^\.+$", r"^0+$", r"^x+$",
]

_ACENTOS_DE_G7 = "áéíóúüñÁÉÍÓÚÜÑ"
_ACENTOS_A_G7 = "aeiouunAEIOUUN"


def g7_resolver(df, candidatos, etiqueta=""):
    """Primer nombre de columna que exista en df. Avisa si resuelve por parcial."""
    if df is None:
        return None
    reales = {c.lower(): c for c in df.columns}
    for c in candidatos:
        if c.lower() in reales:
            return reales[c.lower()]
    for c in candidatos:
        for low, real in reales.items():
            if c.lower() in low:
                print(f"   [resolver] '{etiqueta or candidatos[0]}' -> '{real}' por coincidencia "
                      f"PARCIAL con '{c}'. VERIFICAR que sea la correcta.")
                return real
    if etiqueta:
        print(f"   [aviso] no se resolvio '{etiqueta}'. Probados: {candidatos}")
    return None


def g7_global(nombre, default=None):
    """Lee una global del notebook sin romper si no existe."""
    try:
        return eval(nombre)
    except (NameError, SyntaxError):
        return default


def g7_num(col):
    """Texto o número -> double. Soporta formato chileno (punto de miles, coma decimal)."""
    s = F.trim(col.cast("string"))
    s = F.regexp_replace(s, r"[\s$]", "")
    s = F.when(s.contains(","),
               F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")).otherwise(s)
    s = F.when((s == "") | s.rlike(r"(?i)^(null|na|n/a|s/i|-)$"), F.lit(None)).otherwise(s)
    return s.cast("double")


def g7_norm(colname_or_col):
    """Texto libre -> forma comparable. Quita acentos, control chars de Excel y puntuación."""
    if colname_or_col is None:
        return F.lit(None).cast("string")
    s = colname_or_col if hasattr(colname_or_col, "cast") else F.col(colname_or_col)
    s = s.cast("string")
    s = F.regexp_replace(s, r"(?i)_x000d_|_x000a_|\r|\n|\t", " ")
    s = F.lower(F.translate(s, _ACENTOS_DE_G7, _ACENTOS_A_G7))
    s = F.regexp_replace(s, r"[^a-z0-9 ]", " ")
    s = F.trim(F.regexp_replace(s, r"\s+", " "))
    return F.when(s == "", F.lit(None)).otherwise(s)


def g7_pc(a, b):
    return f"{(100.0 * a / b):.2f}%" if b else "n/d"


print(f"   umbrales: MIN_CHARS_G7={MIN_CHARS_G7} · N_MUESTRA={N_MUESTRA} · "
      f"SEED_MUESTRA={SEED_MUESTRA} · LIMITE_PARES_CAP12={LIMITE_PARES_CAP12:,}")
print(f"   salida  : {RUTA_SALIDA_G7}/{RUN_ID_G7}")
print("   listo. Siguiente: CAP-10.")


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-10 · G7 — ¿existe texto evaluable para auditar el código ONU?
# MAGIC **Requiere:** celdas 1, 2 y 3 de la Etapa 2 (`df_oc_all`) + CAP-0-G7.
# MAGIC **No requiere** las celdas 4 a 8. Tres pasadas sobre 51M filas; correr con el cluster libre.

# COMMAND ----------

# =============================================================================
# QUÉ RESPONDE. Antes de preguntar "¿el código ONU declarado es correcto?" hay
# que saber sobre cuántas líneas esa pregunta se puede siquiera formular. Esta
# celda mide el DENOMINADOR.
#
# POR QUÉ VALE AUNQUE NO SE HAGA NADA MÁS. El complemento es citable solo:
# "el X% de las líneas de orden de compra no trae una especificación que
# permita verificar el código de producto declarado" es un hallazgo sobre lo
# que el Estado publica. Va a 03-limitaciones.md y a la auditoría del vacío, y
# no depende de ningún juicio ni de ningún modelo.
#
# TRES FILTROS, EN ESTE ORDEN, Y CADA UNO SE CUENTA:
#   1. NO NULA      — el campo existe y no está vacío.
#   2. NO RELLENO   — sobrevive a MULETILLAS_G7 y tiene >= MIN_CHARS_G7.
#   3. NO CIRCULAR  — el texto no es copia del nombre del producto genérico.
#                     Si lo copia, no es evidencia independiente del código:
#                     es el código escrito en palabras.
# EVALUABLE = las tres. Lo demás es NO EVALUABLE — nunca "correcto".
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-10 · G7 — texto evaluable para auditar el código ONU · RUN_ID_G7={RUN_ID_G7}")
print("=" * 78)

_oc = g7_global("df_oc_all")
if _oc is None:
    print("ABORTA: falta df_oc_all. Correr celdas 1, 2 y 3 de la Etapa 2.")
else:
    # --- 10.A · resolución de columnas --------------------------------------
    c_esp_c = g7_resolver(_oc, ["_sinclasificar_EspecificacionComprador",
                                "EspecificacionComprador", "especificacion_comprador",
                                "espec_comprador"], "especificación comprador")
    c_esp_p = g7_resolver(_oc, ["_sinclasificar_EspecificacionProveedor",
                                "EspecificacionProveedor", "especificacion_proveedor",
                                "espec_proveedor"], "especificación proveedor")
    c_gen = g7_resolver(_oc, ["_sinclasificar_NombreroductoGenerico",
                              "NombreroductoGenerico", "NombreProductoGenerico",
                              "nombre_producto_generico", "producto"], "producto genérico")
    c_onu = g7_resolver(_oc, ["onu_oc", "codigo_producto_onu", "codigoProductoONU",
                              "CodigoProductoONU"], "código ONU")
    c_anio = g7_resolver(_oc, ["_year", "anio", "year"], "año")
    c_tipo = g7_resolver(_oc, ["tipo_oc", "_sinclasificar_Tipo"], "tipo de OC")
    c_prec = g7_resolver(_oc, ["precio_neto_oc", "precio_neto", "PrecioNeto"], "precio")
    c_cant = g7_resolver(_oc, ["cantidad_oc", "cantidad", "Cantidad"], "cantidad")

    print("\n### 10.A · columnas resueltas")
    for _e, _v in [("comprador", c_esp_c), ("proveedor", c_esp_p), ("genérico", c_gen),
                   ("ONU", c_onu), ("año", c_anio), ("tipo", c_tipo),
                   ("precio", c_prec), ("cantidad", c_cant)]:
        print(f"   {_e:<12}: {_v}")

    if c_esp_c is None and c_esp_p is None:
        print("\n   VEREDICTO G7: NO CORRE. Ninguna columna de especificación existe en")
        print("   df_oc_all. La pregunta de validez del código ONU es imposible con este")
        print("   Parquet — y eso mismo se documenta como limitación estructural.")
        print(f"\n   COLUMNAS DISPONIBLES (copiar esta lista al chat):\n   {df_oc_all.columns}")
    else:
        # --- 10.B · normalización y banderas ---------------------------------
        _t_c = g7_norm(F.col(c_esp_c)) if c_esp_c else F.lit(None).cast("string")
        _t_p = g7_norm(F.col(c_esp_p)) if c_esp_p else F.lit(None).cast("string")
        _txt = F.coalesce(_t_c, _t_p)          # el comprador manda; el proveedor complementa
        _gen = g7_norm(F.col(c_gen)) if c_gen else F.lit(None).cast("string")

        _rx_mule = "|".join(f"({p})" for p in MULETILLAS_G7)
        _f_nonull = _txt.isNotNull()
        _f_largo = F.length(_txt) >= F.lit(MIN_CHARS_G7)
        _f_nomule = ~_txt.rlike(_rx_mule)
        _f_nocirc = (_gen.isNull()
                     | (~(_txt == _gen) & ~_gen.contains(_txt) & ~_txt.contains(_gen)))
        _f_eval = _f_nonull & _f_largo & _f_nomule & _f_nocirc

        _monto = (g7_num(F.col(c_prec)) * g7_num(F.col(c_cant))) if (c_prec and c_cant) else F.lit(0.0)

        _base = _oc.select(
            (F.col(c_anio).cast("int") if c_anio else F.lit(-1)).alias("anio"),
            (F.col(c_tipo).cast("string") if c_tipo else F.lit("(sin tipo)")).alias("tipo_oc"),
            _txt.alias("txt"),
            _t_c.isNotNull().alias("hay_comprador"),
            _t_p.isNotNull().alias("hay_proveedor"),
            _f_nonull.alias("f1"),
            (_f_nonull & _f_largo & _f_nomule).alias("f2"),
            _f_eval.alias("f3"),
            F.coalesce(_monto, F.lit(0.0)).alias("clp"),
        )

        # --- 10.C · MEDIR las muletillas antes de creerle al filtro ----------
        print(f"\n### 10.C · Las {TOP_MULETILLAS} cadenas más repetidas — así se ve el relleno")
        print("    Si algo marcado PASA es relleno, agrégalo a MULETILLAS_G7 y vuelve a 10.D.")
        print("    No adivines la lista: esta tabla ES la lista.\n")
        _top = (_base.where(F.col("txt").isNotNull())
                     .groupBy("txt").agg(F.count(F.lit(1)).alias("n"))
                     .orderBy(F.col("n").desc()).limit(TOP_MULETILLAS).collect())
        _rx_py = _re_g7.compile(_rx_mule)
        _n_txt = sum(r["n"] for r in _top) or 1
        _n_ya = 0
        for r in _top:
            # FIX v16: 10.D filtra con rlike (busca en cualquier parte); 10.C
            # etiquetaba con .match() (ancla al principio). Con los 33 patrones
            # actuales —todos anclados con ^— da igual, pero si alguna vez se
            # agrega uno sin ^, esta tabla mentiría sobre lo que 10.D hace.
            _cae = bool(_rx_py.search(r["txt"])) or len(r["txt"]) < MIN_CHARS_G7
            _n_ya += r["n"] if _cae else 0
            print(f"    {r['n']:>12,}  {r['txt'][:76]:<76} {'ya filtrada' if _cae else '>>> PASA'}")
        print(f"\n    las {TOP_MULETILLAS} juntas suman {_n_txt:,} líneas; "
              f"el filtro actual atrapa {_n_ya:,} ({100.0 * _n_ya / _n_txt:.1f}%)")

        # --- 10.D · la cascada, en una sola pasada ---------------------------
        _agg = (_base.groupBy("anio", "tipo_oc").agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(F.col("hay_comprador").cast("int")).alias("n_comprador"),
            F.sum(F.col("hay_proveedor").cast("int")).alias("n_proveedor"),
            F.sum(F.col("f1").cast("int")).alias("n1"),
            F.sum(F.col("f2").cast("int")).alias("n2"),
            F.sum(F.col("f3").cast("int")).alias("n3"),
            F.sum("clp").alias("clp"),
            F.sum(F.when(F.col("f3"), F.col("clp")).otherwise(0.0)).alias("clp3"),
        ).collect())

        _T = dict(n=0, n_comprador=0, n_proveedor=0, n1=0, n2=0, n3=0, clp=0.0, clp3=0.0)
        for r in _agg:
            for k in _T:
                _T[k] += (r[k] or 0)

        print("\n### 10.D · CASCADA G7 — universo completo de líneas de OC")
        print(f"    {'líneas totales':<46} {_T['n']:>14,}")
        print(f"    {'· con especificación del COMPRADOR':<46} {_T['n_comprador']:>14,}  {g7_pc(_T['n_comprador'], _T['n'])}")
        print(f"    {'· con especificación del PROVEEDOR':<46} {_T['n_proveedor']:>14,}  {g7_pc(_T['n_proveedor'], _T['n'])}")
        print(f"    {'1. no nula':<46} {_T['n1']:>14,}  {g7_pc(_T['n1'], _T['n'])}")
        print(f"    {'2. + no relleno (>= %d caracteres)' % MIN_CHARS_G7:<46} {_T['n2']:>14,}  {g7_pc(_T['n2'], _T['n'])}")
        print(f"    {'3. + no circular   = EVALUABLE':<46} {_T['n3']:>14,}  {g7_pc(_T['n3'], _T['n'])}")
        print(f"\n    NO EVALUABLE — el complemento, que ES el hallazgo: "
              f"{_T['n'] - _T['n3']:,}  {g7_pc(_T['n'] - _T['n3'], _T['n'])}")
        print(f"\n    Ponderado por CLP (precio x cantidad, SIN sanear): "
              f"evaluable = {g7_pc(_T['clp3'], _T['clp'])}")
        print("    [!] ese denominador incluye el outlier de 36,8 billones. Para citar en")
        print("        pesos, repetir sobre el universo saneado de CAP-7/CAP-9.")

        print("\n### 10.E · Por año")
        print(f"    {'año':>6} {'líneas':>14} {'no nula':>10} {'no relleno':>12} {'EVALUABLE':>12}")
        _pa = {}
        for r in _agg:
            d = _pa.setdefault(r["anio"], dict(n=0, n1=0, n2=0, n3=0))
            for k in d:
                d[k] += (r[k] or 0)
        for a in sorted(_pa):
            d = _pa[a]
            print(f"    {a:>6} {d['n']:>14,} {g7_pc(d['n1'], d['n']):>10} "
                  f"{g7_pc(d['n2'], d['n']):>12} {g7_pc(d['n3'], d['n']):>12}")

        print("\n### 10.F · Por mecanismo (tipo_oc) — top 12 por volumen")
        _pt = {}
        for r in _agg:
            d = _pt.setdefault(r["tipo_oc"], dict(n=0, n3=0))
            d["n"] += (r["n"] or 0)
            d["n3"] += (r["n3"] or 0)
        print(f"    {'tipo':>12} {'líneas':>14} {'EVALUABLE':>12}")
        for t, d in sorted(_pt.items(), key=lambda kv: -kv[1]["n"])[:12]:
            print(f"    {str(t):>12} {d['n']:>14,} {g7_pc(d['n3'], d['n']):>12}")

        # --- 10.G · ¿es pensable el corpus completo? -------------------------
        print("\n### 10.G · Pares distintos (texto normalizado, código ONU)")
        print("    El costo escala con los pares DISTINTOS, no con las líneas.")
        try:
            _dist = (_oc.where(_f_eval)
                        .select(F.approx_count_distinct(
                            F.concat_ws("|", _txt,
                                        F.coalesce(F.col(c_onu).cast("string"), F.lit(""))),
                            0.02).alias("d")).collect()[0]["d"])
            print(f"    pares distintos (aprox., error 2%): {_dist:,}")
            if _dist:
                print(f"    factor de deduplicación: {_T['n3'] / _dist:.1f}x")
                print(f"    corpus deduplicado con Haiku 4.5 batch: US$ {_dist * 0.00042:,.0f}")
        except Exception as _e:
            print(f"    [no se pudo estimar: {type(_e).__name__}]")

        # --- veredicto --------------------------------------------------------
        _p3 = 100.0 * _T["n3"] / _T["n"] if _T["n"] else 0.0
        print("\n### VEREDICTO G7 — leerlo contra las magnitudes de arriba, no solo")
        if _p3 >= 40:
            print(f"    PASA ({_p3:.2f}% evaluable). CAP-11 y CAP-12 tienen base suficiente.")
        elif _p3 >= 15:
            print(f"    PASA CON SALVEDAD ({_p3:.2f}% evaluable). Toda cifra futura se cita")
            print("    con el denominador en la MISMA oración. Nunca 'el X% está mal':")
            print("    siempre 'de las líneas con especificación verificable —el Y% del total—…'")
        else:
            print(f"    NO PASA ({_p3:.2f}% evaluable). No se muestrea. El hallazgo es el")
            print("    complemento: el Estado no publica con qué verificar lo que declara.")
        print(f"\nCAP-10 termina. RUN_ID_G7={RUN_ID_G7}")


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-11 · Muestra estratificada para el juez
# MAGIC **Correr SOLO si CAP-10 imprimió PASA o PASA CON SALVEDAD.** Requiere CAP-10 en el kernel.

# COMMAND ----------

# =============================================================================
# QUÉ HACE. Extrae una muestra estratificada y reproducible de líneas
# EVALUABLES y la exporta. El juicio de corrección NO se hace aquí: se hace
# fuera, con `screener_onu.py` y luego con el juez LLM.
#
# ESTRATIFICACIÓN: rubro N1 (2 primeros dígitos) x mecanismo x tramo de monto.
# Sin estratificar, la muestra describe a los rubros más habladores — que son
# justamente los que mejor especifican.
#
# DOCTRINA, NO NEGOCIABLE: el export NO lleva organismo, unidad de compra,
# proveedor ni RUT. El juicio es sobre el código, no sobre quién lo puso.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-11 · Muestra estratificada · RUN_ID_G7={RUN_ID_G7}")
print("=" * 78)

if g7_global("_f_eval") is None or g7_global("_oc") is None:
    print("ABORTA: correr CAP-10 primero (define _oc, _txt y _f_eval).")
else:
    _ev = (_oc.select(
        F.col(c_onu).cast("string").alias("onu8"),
        _txt.alias("texto_comprador"),
        _t_p.alias("texto_proveedor"),
        _gen.alias("producto_generico"),
        (F.col(c_anio).cast("int") if c_anio else F.lit(-1)).alias("anio"),
        (F.col(c_tipo).cast("string") if c_tipo else F.lit(None)).alias("tipo_oc"),
        F.coalesce(_monto, F.lit(0.0)).alias("clp"),
    ).where(_f_eval).where(F.col("onu8").rlike(r"^\d{8}$")))

    _ev = (_ev.withColumn("rubro_n1", F.substring("onu8", 1, 2))
              .withColumn("tramo", F.when(F.col("clp") < 1e6, "1_bajo")
                                    .when(F.col("clp") < 1e7, "2_medio")
                                    .otherwise("3_alto"))
              .withColumn("_estrato", F.concat_ws("|", "rubro_n1", "tipo_oc", "tramo"))
              .withColumn("_id", F.sha2(F.concat_ws("|", "onu8", "texto_comprador",
                                                    F.col("anio").cast("string"),
                                                    F.col("clp").cast("string")), 256).substr(1, 16)))

    _n_estr = _ev.select("_estrato").distinct().count()
    _tot = _ev.count()
    _por_estrato = max(1, int(round(N_MUESTRA / max(_n_estr, 1))))
    print(f"   universo evaluable : {_tot:,} líneas")
    print(f"   estratos           : {_n_estr}   ->  {_por_estrato} línea(s) por estrato")

    _w = W.partitionBy("_estrato").orderBy(F.col("_id"))
    _muestra = (_ev.withColumn("_rk", F.row_number().over(_w))
                   .where(F.col("_rk") <= _por_estrato)
                   .drop("_rk")
                   .orderBy(F.col("_id"))
                   .limit(N_MUESTRA))

    _m = _muestra.toPandas()
    print(f"\n   muestra obtenida   : {len(_m)} líneas · "
          f"{_m['_estrato'].nunique()} estratos · {_m['rubro_n1'].nunique()} rubros N1")
    print(_m.groupby("tramo").size().to_string())
    if len(_m) < N_MUESTRA:
        print(f"   [!] salieron {len(_m)} de {N_MUESTRA} pedidas — hay estratos con menos")
        print("       líneas que el cupo. Es información, no un error: decláralo.")

    _dest = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}"
    try:
        dbutils.fs.mkdirs(_dest.replace("/Volumes", "dbfs:/Volumes"))  # noqa: F821
    except Exception:
        pass
    try:
        _m.drop(columns=["clp"]).to_csv(f"{_dest}/cap11_muestra_validez_onu.csv", index=False)
        print(f"\n   exportada a: {_dest}/cap11_muestra_validez_onu.csv")
        print("   (sin organismo, sin unidad de compra, sin proveedor, sin RUT — por doctrina)")
    except Exception as _e:
        print(f"\n   [no se pudo escribir: {type(_e).__name__}] — bajarla del display de abajo")
        try:
            display(_muestra)  # noqa: F821
        except Exception:
            pass

    print(f"\nCAP-11 termina. RUN_ID_G7={RUN_ID_G7}")
    print("SIGUIENTE: bajar el CSV y correr `screener_onu.py` en local.")


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-12 · Pipeline de Spark ML — ¿el código declarado es el mejor de su familia?
# MAGIC **v16 · parche serverless (sesión 15, 23-ago-2026).** Requiere CAP-10 (Cmd 42) corrido en el mismo kernel y el catálogo en el Volumen.
# MAGIC Primera corrida con `TOPE_PARES_CAP12 = 200_000` (humo); recién después subir a `2_000_000`.
# MAGIC Cubre Clase 10 (*ML con Apache Spark: pipelines*), Clase 11 (*limpieza y validación*) y módulo A7.

# COMMAND ----------

# =============================================================================
# CAP-12 · PARCHE SERVERLESS · v16 (23-ago-2026, sesión 15)
#
# QUÉ ARREGLA. La corrida del 23-ago murió en la línea `_best.cache()` con:
#     [NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on
#     serverless compute. SQLSTATE: 0A000
# En Databricks Serverless no existe `.cache()` ni `.persist()`. La forma
# equivalente y soportada es MATERIALIZAR a parquet y volver a leer.
#
# QUÉ MÁS CAMBIA, y por qué. La versión anterior recorría el universo de 42M
# líneas CUATRO veces sin darse cuenta (`_pares.count()`, el `sum` de cobertura,
# el `sum` total y luego el pipeline). Ahora `_pares` se materializa una vez y
# las tres cuentas se leen del parquet. Es el mismo resultado, más barato.
#
# CÓMO SE USA. Reemplaza ENTERO el contenido del Cmd 46 por este archivo.
# Requiere CAP-10 (Cmd 42) corrido en el mismo kernel y el catálogo en el
# Volumen.
#
# QUÉ NO CAMBIÓ: los filtros, el pipeline, la métrica y las columnas. Una línea
# de CAP-12 v15 y una de v16 miden exactamente lo mismo.
# QUÉ SÍ CAMBIÓ, y hay que saberlo: (a) el tope por defecto baja a 200.000
# para la corrida de humo; (b) se agregan prints —el % de pares dejados fuera
# y la advertencia de sesgo de cola—; (c) se agrega el bloque 12.H, que
# exporta 200 desacuerdos a CSV.
#
# ORDEN DE CORRIDA RECOMENDADO (no negociable la primera vez):
#   1. TOPE_PARES_CAP12 = 200_000   -> humo, 5-10 min. Confirma que la UDF de
#                                      coseno sobrevive en serverless.
#   2. TOPE_PARES_CAP12 = 2_000_000 -> la corrida real, 30-60 min.
# Si el humo no pasa, no hay nada que ganar corriendo 10x más grande.
# =============================================================================
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, Normalizer
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.sql.types import DoubleType

# --- perilla de esta corrida. Declarada, como todos los umbrales -------------
TOPE_PARES_CAP12 = 200_000        # <-- humo. Subir a 2_000_000 para la real.

# El banner de CAP-0 (Cmd 40) imprimió LIMITE_PARES_CAP12. Este bloque NO lo usa.
# Se declara la discrepancia en vez de dejar que el banner mienta.
_lim_cap0 = g7_global("LIMITE_PARES_CAP12")
if _lim_cap0 is not None and _lim_cap0 != TOPE_PARES_CAP12:
    print(f"   [aviso] el Cmd 40 anunció LIMITE_PARES_CAP12={_lim_cap0:,}; "
          f"esta corrida usa TOPE_PARES_CAP12={TOPE_PARES_CAP12:,}. Manda el tope.")

print("\n" + "=" * 78)
print(f"CAP-12 · Pipeline de Spark ML · RUN_ID_G7={RUN_ID_G7}")
print(f"          [v16 · parche serverless · TOPE_PARES_CAP12={TOPE_PARES_CAP12:,}]")
print("=" * 78)

STOPWORDS_ES = ["de", "la", "el", "los", "las", "un", "una", "unos", "unas", "y", "o",
                "para", "por", "con", "sin", "en", "del", "al", "a", "que", "se", "su",
                "sus", "lo", "es", "mas", "segun", "cada", "tipo", "marca", "modelo",
                "unidad", "unidades", "cantidad", "total", "neto", "iva", "codigo"]

_BASE12 = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}"


def _materializar(df, nombre):
    """Serverless no admite cache()/persist(). Esto es el equivalente soportado:
    escribe una vez a parquet y devuelve el DataFrame leído de ahí. Si el
    Volumen no acepta escritura, devuelve el df original y lo dice — el
    resultado sigue siendo correcto, solo más lento."""
    _p = f"{_BASE12}/_tmp_{nombre}"
    try:
        df.write.mode("overwrite").parquet(_p)
        print(f"   [materializado] {nombre} -> {_p}")
        return spark.read.parquet(_p)          # noqa: F821
    except Exception as _e:
        print(f"   [!] no se pudo materializar {nombre} ({type(_e).__name__}): "
              f"sigo sin materializar. Va a recalcular varias veces.")
        return df


_cat = None
if g7_global("_f_eval") is None:
    print("ABORTA: correr CAP-10 (Cmd 42) primero. CAP-12 no puede seguir.")
else:
    # --- 12.A · catálogo ----------------------------------------------------
    try:
        _cat = (spark.read.csv(RUTA_CATALOGO, header=True, inferSchema=False)  # noqa: F821
                     .withColumn("codigo", F.lpad(F.col("CodigoProducto"), 8, "0"))
                     .withColumn("fam6", F.substring(F.lpad(F.col("CodigoProducto"), 8, "0"), 1, 6))
                     .withColumn("doc", g7_norm(F.concat_ws(" ", F.col("NombreProducto"),
                                                            F.col("Nivel3"), F.col("Nivel2"))))
                     .select("codigo", "fam6", "Nivel1", "NombreProducto", "doc")
                     .where(F.col("doc").isNotNull()))
        _n_cat = _cat.count()
        print(f"   catálogo cargado: {_n_cat:,} códigos")
    except Exception as _e:
        _cat = None
        print(f"   ABORTA: no se pudo leer el catálogo en {RUTA_CATALOGO}")
        print(f"   ({type(_e).__name__}) — subirlo primero: Catalog -> Volumes -> Upload")

if _cat is not None:
    # --- 12.B · pares distintos, con su frecuencia --------------------------
    # CAMBIO v16: se materializa. Antes esta expresión se recalculaba entera
    # tres veces (count, sum de cobertura, sum total) sobre 42M líneas.
    _pares = _materializar(
        (_oc.where(_f_eval)
            .select(_txt.alias("doc"),
                    F.col(c_onu).cast("string").alias("onu8"),
                    F.coalesce(_monto, F.lit(0.0)).alias("clp"))
            .where(F.col("onu8").rlike(r"^\d{8}$"))
            .groupBy("doc", "onu8")
            .agg(F.count(F.lit(1)).alias("n_lineas"), F.sum("clp").alias("clp"))
            .withColumn("fam6", F.substring("onu8", 1, 6))),
        "cap12_pares")

    _n_pares = _pares.count()
    _pares_usa = _pares.orderBy(F.col("n_lineas").desc()).limit(TOPE_PARES_CAP12)
    _cob = _pares_usa.agg(F.sum("n_lineas")).collect()[0][0] or 0
    _tot_l = _pares.agg(F.sum("n_lineas")).collect()[0][0] or 1
    print(f"   pares distintos (texto, código): {_n_pares:,}")
    print(f"   tope aplicado: {TOPE_PARES_CAP12:,} pares "
          f"-> cubren {_cob:,} de {_tot_l:,} líneas evaluables ({100.0 * _cob / _tot_l:.2f}%)")
    if _n_pares > TOPE_PARES_CAP12:
        print(f"   [!] SE DEJARON FUERA {_n_pares - TOPE_PARES_CAP12:,} pares "
              f"({100.0 * (_n_pares - TOPE_PARES_CAP12) / _n_pares:.1f}% de los pares). Declararlo:")
        print("       un tope silencioso se lee como cobertura total, y no lo es.")
        print("       Y el tope NO es aleatorio: toma los pares MÁS REPETIDOS. La cola")
        print("       larga —el texto raro, que es donde más se equivoca cualquiera—")
        print("       queda fuera por construcción. Esa es la salvedad que va al informe.")

    # --- 12.C · el Pipeline de Spark ML -------------------------------------
    _pipe = Pipeline(stages=[
        RegexTokenizer(inputCol="doc", outputCol="tok", pattern=r"\s+", minTokenLength=3),
        StopWordsRemover(inputCol="tok", outputCol="tok2", stopWords=STOPWORDS_ES),
        CountVectorizer(inputCol="tok2", outputCol="tf", minDF=1.0, vocabSize=1 << 16),
        IDF(inputCol="tf", outputCol="tfidf"),
        Normalizer(inputCol="tfidf", outputCol="vec", p=2.0),
    ])
    _model = _pipe.fit(_cat)
    print(f"   pipeline ajustado sobre el catálogo · "
          f"vocabulario = {len(_model.stages[2].vocabulary):,} términos")

    _cat_v = _model.transform(_cat).select(
        F.col("codigo").alias("cand_cod"), F.col("NombreProducto").alias("cand_nom"),
        F.col("fam6"), F.col("vec").alias("cand_vec"))
    _lin_v = _model.transform(_pares_usa).select(
        "doc", "onu8", "fam6", "n_lineas", "clp", F.col("vec").alias("linea_vec"))

    # --- 12.D · similitud coseno contra los hermanos ------------------------
    def _cos(a, b):
        if a is None or b is None:
            return 0.0
        if isinstance(a, SparseVector) and isinstance(b, SparseVector):
            da = dict(zip(a.indices.tolist(), a.values.tolist()))
            return float(sum(da.get(i, 0.0) * v for i, v in zip(b.indices.tolist(), b.values.tolist())))
        return float(DenseVector(a).dot(DenseVector(b)))

    _udf_cos = F.udf(_cos, DoubleType())

    _scored = (_lin_v.join(F.broadcast(_cat_v), on="fam6", how="inner")
                     .withColumn("sim", _udf_cos(F.col("linea_vec"), F.col("cand_vec"))))

    _w12 = W.partitionBy("doc", "onu8").orderBy(F.col("sim").desc(), F.col("cand_cod"))
    _best = (_scored.withColumn("_rk", F.row_number().over(_w12))
                    .where(F.col("_rk") == 1)
                    .select("doc", "onu8", "n_lineas", "clp",
                            F.col("cand_cod").alias("mejor_cod"),
                            F.col("cand_nom").alias("mejor_nom"),
                            F.col("sim").alias("mejor_sim"))
                    .withColumn("declarado_es_mejor", F.col("onu8") == F.col("mejor_cod"))
                    .withColumn("rubro_n1", F.substring("onu8", 1, 2)))

    # ===== EL PARCHE ==========================================================
    # ANTES:  _best.cache()      <-- [NOT_SUPPORTED_WITH_SERVERLESS]
    # AHORA:  materializar a parquet. Misma función, soportado en serverless.
    _best = _materializar(_best, "cap12_best")
    # ==========================================================================

    # --- 12.E · resultado ---------------------------------------------------
    _r = _best.agg(
        F.count(F.lit(1)).alias("pares"),
        F.sum("n_lineas").alias("lineas"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("n_lineas")).otherwise(0)).alias("lineas_ok"),
        F.sum(F.when(F.col("mejor_sim") <= 0.0, F.col("n_lineas")).otherwise(0)).alias("lineas_sim0"),
        F.sum("clp").alias("clp"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("clp")).otherwise(0.0)).alias("clp_ok"),
    ).collect()[0]

    print("\n### 12.E · ¿El código declarado es el mejor de su familia?")
    print(f"    pares evaluados         : {_r['pares']:,}")
    print(f"    líneas que representan  : {_r['lineas']:,}")
    print(f"    declarado ES el mejor   : {g7_pc(_r['lineas_ok'], _r['lineas'])}  (por línea)")
    print(f"                              {g7_pc(_r['clp_ok'], _r['clp'])}  (por CLP, sin sanear)")
    print(f"    similitud 0 con TODOS los hermanos: {g7_pc(_r['lineas_sim0'], _r['lineas'])}")
    print("       ^ ahí el método no opinó: el texto no comparte vocabulario con ningún")
    print("         nombre del catálogo. NO son códigos malos — son casos sin veredicto.")

    print("\n### 12.F · Por rubro N1 — top 15 por volumen")
    _pr = (_best.groupBy("rubro_n1").agg(
        F.sum("n_lineas").alias("lineas"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("n_lineas")).otherwise(0)).alias("ok"))
        .orderBy(F.col("lineas").desc()).limit(15).collect())
    print(f"    {'N1':>4} {'líneas':>14} {'declarado = mejor':>20}")
    for r in _pr:
        print(f"    {r['rubro_n1']:>4} {r['lineas']:>14,} {g7_pc(r['ok'], r['lineas']):>20}")

    print("\n### 12.G · 15 desacuerdos con más peso — para mirarlos a ojo")
    for r in (_best.where(~F.col("declarado_es_mejor") & (F.col("mejor_sim") > 0.15))
                   .orderBy(F.col("n_lineas").desc()).limit(15).collect()):
        print(f"\n    texto     : {r['doc'][:100]}")
        print(f"    declarado : {r['onu8']}")
        print(f"    propone   : {r['mejor_cod']} · {r['mejor_nom']}  (sim {r['mejor_sim']:.3f})")
        print(f"    líneas    : {r['n_lineas']:,}")

    try:
        _best.drop("doc").write.mode("overwrite").parquet(
            f"{_BASE12}/cap12_declarado_vs_mejor")
        print(f"\n   guardado en {_BASE12}/cap12_declarado_vs_mejor")
    except Exception as _e:
        print(f"\n   [no se pudo guardar: {type(_e).__name__}]")

    # --- 12.H · export de 200 desacuerdos para mirar a mano (nuevo en v16) ---
    # Sin esto CAP-12 solo deja una cifra. Con esto deja evidencia mirable, que
    # es lo que el informe necesita y lo que el jurado va a pedir.
    try:
        _muestra12 = (_best.where(~F.col("declarado_es_mejor") & (F.col("mejor_sim") > 0.15))
                           .orderBy(F.col("n_lineas").desc()).limit(200).toPandas())
        _muestra12.to_csv(f"{_BASE12}/cap12_desacuerdos_top200.csv", index=False)
        print(f"   export a mano: {_BASE12}/cap12_desacuerdos_top200.csv "
              f"({len(_muestra12)} filas, sin organismo ni proveedor — por doctrina)")
    except Exception as _e:
        print(f"   [no se pudo exportar los desacuerdos: {type(_e).__name__}]")

    print("\n### LECTURA OBLIGATORIA ANTES DE CITAR NADA DE CAP-12")
    print("    Esta celda NO dice qué códigos están mal. Dice en qué casos un método")
    print("    léxico prefiere otro hermano. El tamiz corrido sobre las 300 líneas de")
    print("    CAP-11 coincide con el código declarado a nivel de HOJA en 7,67% de los")
    print("    casos y no coincide ni al nivel de rubro N1 en 79,00%. Es decir: cuando")
    print("    este método discrepa, la evidencia dice que suele equivocarse ÉL.")
    print("    La cifra de 12.E se cita SOLO junto a esa limitación, y validada contra")
    print("    los 30 casos etiquetados a mano. Sin patrón oro, es diagnóstico del")
    print("    método, no del Estado.")
    print(f"\nCAP-12 termina. RUN_ID_G7={RUN_ID_G7}")

# COMMAND ----------

# MAGIC %md
# MAGIC # ═══════════════════════════════════════════════════════════════
# MAGIC # v17 · sesión 16 (23-ago-2026) — Q1, Q2, Q4, CAP-12B y Q5
# MAGIC # ═══════════════════════════════════════════════════════════════
# MAGIC **Qué cambió respecto de `v16`:** nada de lo anterior. Se AGREGAN al final
# MAGIC las celdas que la sesión 15 dejó sólo en el documento de instrucciones.
# MAGIC Los Cmd 1 a 46 son byte-idénticos al `v16`.
# MAGIC
# MAGIC **Regla nueva §1.17:** el archivo `_COMPLETO` lleva TODAS las celdas
# MAGIC necesarias, aunque sean de prueba. Las de prueba van marcadas `[PRUEBA]` y
# MAGIC se borran en la versión siguiente, no antes.

# =============================================================================
# BLOQUES NUEVOS — sesión 16 (23-ago-2026)
#
# QUÉ SON. Las cuatro consultas que la sesión 15 dejó escritas solo en el
# documento de instrucciones y NO en el notebook — por eso Q1 y Q2 "faltaban"
# en `etapa2_databricks_v16_COMPLETO.py`. Aquí quedan como celdas, más dos
# bloques nuevos (CAP-12B y Q5).
#
# REGLA NUEVA DEL PROYECTO (§1.17): el archivo `_COMPLETO` lleva TODAS las
# celdas necesarias, aunque sean de prueba. Las de prueba se marcan
# `[PRUEBA]` y se borran en la versión siguiente, no antes.
#
# ORDEN: Cmd 3 -> 7 -> 40 -> 42 (-> 44 solo si vas a correr Q2).
# Ninguna de estas celdas es requisito de aprobación.
# =============================================================================

# COMMAND ----------

# MAGIC %md
# MAGIC # Q1 · Circularidad por año **y** mecanismo — 0 segundos de Spark
# MAGIC **La más valiosa.** Decide si el alza de la circularidad (7,43 pp en 2019 →
# MAGIC 22,60 pp en 2026) es **composición** (creció Convenio Marco, que siempre
# MAGIC especificó mal) o **conducta** (dentro de cada mecanismo se especifica peor).
# MAGIC Son dos hallazgos distintos y solo uno dice que el sistema empeoró.
# MAGIC
# MAGIC **Requiere `_agg` vivo** — lo deja el bloque 10.D del Cmd 42.

# COMMAND ----------

# --- Q1 · ¿el alza de la circularidad es composición o conducta? -------------
#     CERO Spark: reusa `_agg`, que el bloque 10.D ya recolectó.
_agg_q1 = g7_global("_agg")
if _agg_q1 is None:
    print("ABORTA Q1: `_agg` no está en memoria. Correr el Cmd 42 (CAP-10) primero.")
else:
    MECS_Q1 = ["CM", "SE", "AG"]          # 98,10% del universo
    _d_q1, _tot_anio_q1, _tot_glob = {}, {}, {}
    for r in _agg_q1:
        a, t = r["anio"], r["tipo_oc"]
        _tot_anio_q1[a] = _tot_anio_q1.get(a, 0) + (r["n"] or 0)
        g = _tot_glob.setdefault(t, [0, 0, 0])
        g[0] += (r["n"] or 0); g[1] += (r["n2"] or 0); g[2] += (r["n3"] or 0)
        if t in MECS_Q1:
            d = _d_q1.setdefault((a, t), [0, 0, 0])
            d[0] += (r["n"] or 0); d[1] += (r["n2"] or 0); d[2] += (r["n3"] or 0)

    def _circ(n, n2, n3):
        """pp del año que se pierden por CIRCULARIDAD (paso 2 -> paso 3)."""
        return (100.0 * (n2 - n3) / n) if n else 0.0

    print("\n### Q1.A · Circularidad DENTRO de cada mecanismo, y peso del mecanismo")
    print("    %circ = pp que ese mecanismo pierde por circularidad ese año")
    print("    %vol  = qué fracción de TODAS las líneas del año es ese mecanismo\n")
    print(f"{'año':>6} " + "".join(f"{t:>10} %circ{t:>9} %vol" for t in MECS_Q1))
    for a in sorted(_tot_anio_q1):
        fila = f"{a:>6} "
        for t in MECS_Q1:
            n, n2, n3 = _d_q1.get((a, t), [0, 0, 0])
            fila += f"{_circ(n, n2, n3):>15.2f}%{(100.0 * n / _tot_anio_q1[a]):>13.1f}%"
        print(fila)

    # --- Q1.B · descomposición formal: ¿cuánto del alza es mezcla y cuánto conducta?
    # Shift-share entre el año base y el último: la variación total se parte en
    #   composición  = sum_t (w_t^fin - w_t^ini) * circ_t^ini
    #   conducta     = sum_t  w_t^ini * (circ_t^fin - circ_t^ini)
    #   interacción  = sum_t (w_t^fin - w_t^ini) * (circ_t^fin - circ_t^ini)
    _anios = sorted(_tot_anio_q1)
    print("\n### Q1.B · Descomposición shift-share del alza de la circularidad")
    print("    (composición = cambió la mezcla · conducta = cambió el comportamiento)\n")
    for _ini in (_anios[0], 2019 if 2019 in _anios else _anios[0]):
        _fin = _anios[-1]
        if _ini == _fin:
            continue
        comp = cond = inter = 0.0
        c_ini_tot = c_fin_tot = 0.0
        for t in MECS_Q1:
            ni, n2i, n3i = _d_q1.get((_ini, t), [0, 0, 0])
            nf, n2f, n3f = _d_q1.get((_fin, t), [0, 0, 0])
            wi = ni / _tot_anio_q1[_ini] if _tot_anio_q1[_ini] else 0.0
            wf = nf / _tot_anio_q1[_fin] if _tot_anio_q1[_fin] else 0.0
            ci = _circ(ni, n2i, n3i); cf = _circ(nf, n2f, n3f)
            comp += (wf - wi) * ci
            cond += wi * (cf - ci)
            inter += (wf - wi) * (cf - ci)
            c_ini_tot += wi * ci; c_fin_tot += wf * cf
        tot = c_fin_tot - c_ini_tot
        print(f"    {_ini} -> {_fin}:  circularidad {c_ini_tot:.2f} pp -> {c_fin_tot:.2f} pp  "
              f"(alza {tot:+.2f} pp)")
        for lab, v in [("composición (mezcla)", comp), ("conducta (dentro del mecanismo)", cond),
                       ("interacción", inter)]:
            print(f"        {lab:<34} {v:>+7.2f} pp   {(100.0 * v / tot if tot else 0):>6.1f}% del alza")
        print()

    print("### Q1.C · Circularidad acumulada 2017-2026, por mecanismo")
    print(f"    {'tipo':>6} {'líneas':>14} {'%circ':>9} {'EVALUABLE':>11}")
    for t, (n, n2, n3) in sorted(_tot_glob.items(), key=lambda kv: -kv[1][0])[:10]:
        print(f"    {t:>6} {n:>14,} {_circ(n, n2, n3):>8.2f}% {g7_pc(n3, n):>11}")
    print("\n    LECTURA: si %circ de CM se mantiene plano y lo que sube es su %vol,")
    print("    el hallazgo es COMPOSICIÓN. Si %circ sube dentro de cada mecanismo,")
    print("    es CONDUCTA. Q1.B lo cuantifica; no lo decidas a ojo desde Q1.A.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Q2 · Pesos de los 911 estratos — 3 min — desbloquea la muestra de 300
# MAGIC El muestreo de CAP-11 es un diseño **bottom-k**: favorece a los estratos
# MAGIC grandes. Sin los tamaños de estrato la muestra es ilustración, no estimación.
# MAGIC
# MAGIC **Requiere `_ev` vivo** — lo deja el Cmd 44 (CAP-11).
# MAGIC **Corrida el 23-ago:** 911 estratos · 41.696.043 líneas · 33.035.469 tuplas.

# COMMAND ----------

# --- Q2 · pesos de los 911 estratos, para poder reponderar la muestra --------
_ev_q2 = g7_global("_ev")
if _ev_q2 is None:
    print("ABORTA Q2: `_ev` no está en memoria. Correr el Cmd 44 (CAP-11) primero.")
else:
    _q2 = (_ev_q2.groupBy("_estrato")
                 .agg(F.count(F.lit(1)).alias("n_lineas"),
                      F.countDistinct("_id").alias("n_tuplas"),   # _id es hash del CONTENIDO
                      F.sum("clp").alias("clp"))
                 .toPandas())
    _ruta_q2 = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}/cap11_pesos_estratos.csv"
    _q2.to_csv(_ruta_q2, index=False)
    print(f"   {len(_q2)} estratos · {_q2.n_lineas.sum():,} líneas · "
          f"{_q2.n_tuplas.sum():,} tuplas distintas")
    print(f"   deduplicación medida: {_q2.n_lineas.sum() / max(_q2.n_tuplas.sum(), 1):.2f}x")
    print(f"   guardado en {_ruta_q2}")
    print(_q2.nlargest(15, "n_lineas").to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC # Q4 · Las 426.515 líneas evaluables con código ONU malformado — 2 min
# MAGIC CAP-10 contó 42.122.558 evaluables; CAP-11 muestreó sobre 41.696.043.
# MAGIC La diferencia (**1,01%**) son líneas evaluables cuyo código ONU **no tiene
# MAGIC ocho dígitos**. Esta celda dice qué son.
# MAGIC
# MAGIC **Requiere `_oc`, `_f_eval`, `c_onu` vivos** — Cmd 42.

# COMMAND ----------

# --- Q4 · los códigos ONU que no tienen forma de código ONU ------------------
if g7_global("_f_eval") is None or g7_global("_oc") is None:
    print("ABORTA Q4: correr el Cmd 42 (CAP-10) primero.")
else:
    _ev_q4 = _oc.where(_f_eval).select(F.col(c_onu).cast("string").alias("onu8"))
    _mal = _ev_q4.where(~F.col("onu8").rlike(r"^\d{8}$"))
    _n_ev, _n_mal = _ev_q4.count(), _mal.count()
    print(f"\n### Q4 · evaluables {_n_ev:,} · malformadas {_n_mal:,}  ({g7_pc(_n_mal, _n_ev)})")

    # clasificación por FORMA, no solo el top-20: un top-20 esconde la cola
    _cls = (_mal.withColumn("_forma",
                F.when(F.col("onu8").isNull() | (F.trim(F.col("onu8")) == ""), "1_nulo_o_vacio")
                 .when(F.col("onu8").rlike(r"^\d{1,7}$"), "2_numerico_menos_de_8")
                 .when(F.col("onu8").rlike(r"^\d{9,}$"), "3_numerico_mas_de_8")
                 .when(F.col("onu8").rlike(r"^0+$"), "4_solo_ceros")
                 .when(F.col("onu8").rlike(r"^\d+\.0+$"), "5_notacion_decimal")
                 .otherwise("6_no_numerico"))
            .groupBy("_forma").count().orderBy("_forma").collect())
    print(f"\n    {'forma':<24} {'líneas':>12} {'% de las malformadas':>22}")
    for r in _cls:
        print(f"    {r['_forma']:<24} {r['count']:>12,} {g7_pc(r['count'], _n_mal):>22}")

    print(f"\n    top 20 valores concretos:")
    for r in (_mal.groupBy("onu8").count().orderBy(F.col("count").desc()).limit(20).collect()):
        print(f"    {str(r['onu8'])[:40]:<40} {r['count']:>12,}")

# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-12B · el mismo cálculo SIN la UDF de Python — [PRUEBA]
# MAGIC **Por qué existe.** La corrida de CAP-12 con `TOPE_PARES_CAP12 = 2_000_000`
# MAGIC pasó de cinco horas. El cuello es la UDF de Python de coseno: el join contra
# MAGIC los hermanos produce **9,2 filas por par** (media medida sobre las 2.049
# MAGIC clases N3 del catálogo; **20,5** en los pares de mayor volumen), o sea
# MAGIC **~18–40 millones de invocaciones de Python** en serverless, cada una
# MAGIC serializando dos vectores dispersos. No es un cuelgue: es el costo real.
# MAGIC
# MAGIC **Antes de correr esto, lee la recomendación:** con 17 días para el informe,
# MAGIC el dashboard y la presentación, **CAP-12 es lo primero que la regla de
# MAGIC sacrificio bota.** La corrida de 200.000 ya está hecha y ya responde la
# MAGIC pregunta. Esta celda existe por si sobra tiempo, no porque haga falta.
# MAGIC
# MAGIC **Qué cambia.** El TF-IDF y el coseno se calculan enteros en SQL: tokenizar,
# MAGIC explotar a `(documento, término)`, IDF sobre el catálogo, normalizar L2 y
# MAGIC producto punto por `join` + `groupBy`. **Cero Python por fila.** Replica la
# MAGIC fórmula de `CountVectorizer + IDF + Normalizer` de Spark ML:
# MAGIC `idf(t) = ln((m+1)/(df(t)+1))`, `tf` = conteo crudo, términos fuera del
# MAGIC vocabulario del catálogo descartados — igual que hace `CountVectorizer`.
# MAGIC
# MAGIC **Validación obligatoria:** correr con 200.000 y comparar contra 12.E del
# MAGIC 23-ago (30,57% por línea · 62,28% sim 0). Si no replica, la celda está mal
# MAGIC y se descarta — **no** se cita. Sólo si replica tiene sentido subir a 2M.

# COMMAND ----------

# --- CAP-12B · TF-IDF y coseno nativos, sin una sola llamada a Python --------
TOPE_PARES_CAP12B = 200_000     # <-- validación. Subir a 2_000_000 sólo si 12B.E replica 12.E.
MIN_TOK_12B = 3                 # igual que RegexTokenizer(minTokenLength=3) del Cmd 46

if g7_global("_f_eval") is None:
    print("ABORTA CAP-12B: correr el Cmd 42 (CAP-10) primero.")
else:
    print("\n" + "=" * 78)
    print(f"CAP-12B · coseno nativo (sin UDF) · TOPE={TOPE_PARES_CAP12B:,} · RUN_ID_G7={RUN_ID_G7}")
    print("=" * 78)
    _B12B = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}"

    STOPWORDS_12B = ["de", "la", "el", "los", "las", "un", "una", "unos", "unas", "y", "o",
                     "para", "por", "con", "sin", "en", "del", "al", "a", "que", "se", "su",
                     "sus", "lo", "es", "mas", "segun", "cada", "tipo", "marca", "modelo",
                     "unidad", "unidades", "cantidad", "total", "neto", "iva", "codigo"]

    def _tokenizar(df, id_cols, doc_col="doc"):
        """doc -> filas (id..., term). Mismo criterio que RegexTokenizer + StopWordsRemover."""
        return (df.select(*id_cols, F.explode(F.split(F.col(doc_col), r"\s+")).alias("term"))
                  .where((F.length("term") >= MIN_TOK_12B)
                         & (~F.col("term").isin(STOPWORDS_12B))))

    # --- 12B.A · catálogo -> vocabulario e IDF -------------------------------
    _catB = (spark.read.csv(RUTA_CATALOGO, header=True, inferSchema=False)   # noqa: F821
                  .withColumn("codigo", F.lpad(F.col("CodigoProducto"), 8, "0"))
                  .withColumn("fam6", F.substring(F.lpad(F.col("CodigoProducto"), 8, "0"), 1, 6))
                  .withColumn("doc", g7_norm(F.concat_ws(" ", F.col("NombreProducto"),
                                                         F.col("Nivel3"), F.col("Nivel2"))))
                  .select("codigo", "fam6", "NombreProducto", "doc")
                  .where(F.col("doc").isNotNull()))
    _m_cat = _catB.count()
    _catTok = _tokenizar(_catB, ["codigo", "fam6"])
    # IDF de Spark ML:  idf(t) = ln((m + 1) / (df(t) + 1))
    _idf = (_catTok.groupBy("term").agg(F.countDistinct("codigo").alias("df"))
                   .withColumn("idf", F.log((F.lit(_m_cat) + 1.0) / (F.col("df") + 1.0))))
    print(f"   catálogo: {_m_cat:,} códigos · vocabulario: {_idf.count():,} términos")

    def _vectorizar(tok_df, id_cols):
        """(id..., term) -> (id..., term, val) con val = tf*idf normalizado L2."""
        _tf = tok_df.groupBy(*id_cols, "term").agg(F.count(F.lit(1)).cast("double").alias("tf"))
        _v = (_tf.join(F.broadcast(_idf.select("term", "idf")), on="term", how="inner")
                 .withColumn("v", F.col("tf") * F.col("idf")))
        _n2 = _v.groupBy(*id_cols).agg(F.sqrt(F.sum(F.col("v") * F.col("v"))).alias("norma"))
        return (_v.join(_n2, on=id_cols, how="inner")
                  .where(F.col("norma") > 0)
                  .select(*id_cols, "term", (F.col("v") / F.col("norma")).alias("val")))

    _catVec = _vectorizar(_catTok, ["codigo", "fam6"]).withColumnRenamed("val", "val_c")

    # --- 12B.B · pares distintos, con el mismo tope declarado ----------------
    _paresB = (_oc.where(_f_eval)
                  .select(_txt.alias("doc"), F.col(c_onu).cast("string").alias("onu8"),
                          F.coalesce(_monto, F.lit(0.0)).alias("clp"))
                  .where(F.col("onu8").rlike(r"^\d{8}$"))
                  .groupBy("doc", "onu8")
                  .agg(F.count(F.lit(1)).alias("n_lineas"), F.sum("clp").alias("clp"))
                  .withColumn("fam6", F.substring("onu8", 1, 6)))
    _rp = f"{_B12B}/_tmp_cap12b_pares"
    _paresB.write.mode("overwrite").parquet(_rp)
    _paresB = spark.read.parquet(_rp)                                        # noqa: F821
    _nB = _paresB.count()
    _usaB = (_paresB.orderBy(F.col("n_lineas").desc()).limit(TOPE_PARES_CAP12B)
                    .withColumn("par_id", F.sha2(F.concat_ws("|", "doc", "onu8"), 256).substr(1, 20)))
    _rp2 = f"{_B12B}/_tmp_cap12b_usa"
    _usaB.write.mode("overwrite").parquet(_rp2)
    _usaB = spark.read.parquet(_rp2)                                         # noqa: F821
    _cobB = _usaB.agg(F.sum("n_lineas")).collect()[0][0] or 0
    _totB = _paresB.agg(F.sum("n_lineas")).collect()[0][0] or 1
    print(f"   pares distintos: {_nB:,} · tope {TOPE_PARES_CAP12B:,} -> "
          f"cubre {_cobB:,}/{_totB:,} líneas ({100.0 * _cobB / _totB:.2f}%)")
    print(f"   [!] quedan fuera {_nB - TOPE_PARES_CAP12B:,} pares "
          f"({100.0 * (_nB - TOPE_PARES_CAP12B) / _nB:.1f}%), y NO al azar: el tope toma los")
    print("       pares MÁS REPETIDOS. La cola larga —el texto raro, donde más se")
    print("       equivoca cualquier método— queda fuera por construcción.")

    # --- 12B.C · coseno = producto punto sobre términos comunes de la familia -
    _linVec = _vectorizar(_tokenizar(_usaB.select("par_id", "fam6", "doc"), ["par_id", "fam6"]),
                          ["par_id", "fam6"]).withColumnRenamed("val", "val_l")

    _sim = (_linVec.join(_catVec, on=["fam6", "term"], how="inner")
                   .groupBy("par_id", "codigo")
                   .agg(F.sum(F.col("val_l") * F.col("val_c")).alias("sim")))

    _w12b = W.partitionBy("par_id").orderBy(F.col("sim").desc(), F.col("codigo"))
    _top1 = (_sim.withColumn("_rk", F.row_number().over(_w12b)).where(F.col("_rk") == 1)
                 .select("par_id", F.col("codigo").alias("mejor_cod"), F.col("sim").alias("mejor_sim")))

    # left join: los pares SIN ningún término en común con sus hermanos no aparecen
    # en _sim, y esos son justamente la cifra "similitud 0 con todos los hermanos".
    _bestB = (_usaB.select("par_id", "doc", "onu8", "n_lineas", "clp")
                   .join(_top1, on="par_id", how="left")
                   .join(F.broadcast(_catB.select(F.col("codigo").alias("mejor_cod"),
                                                  F.col("NombreProducto").alias("mejor_nom"))),
                         on="mejor_cod", how="left")
                   .withColumn("mejor_sim", F.coalesce(F.col("mejor_sim"), F.lit(0.0)))
                   .withColumn("declarado_es_mejor",
                               F.coalesce(F.col("onu8") == F.col("mejor_cod"), F.lit(False)))
                   .withColumn("rubro_n1", F.substring("onu8", 1, 2)))
    _rp3 = f"{_B12B}/_tmp_cap12b_best"
    _bestB.write.mode("overwrite").parquet(_rp3)
    _bestB = spark.read.parquet(_rp3)                                        # noqa: F821

    # --- 12B.E · resultado, con la MISMA métrica del Cmd 46 ------------------
    _rB = _bestB.agg(
        F.count(F.lit(1)).alias("pares"), F.sum("n_lineas").alias("lineas"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("n_lineas")).otherwise(0)).alias("ok"),
        F.sum(F.when(F.col("mejor_sim") <= 0.0, F.col("n_lineas")).otherwise(0)).alias("sim0"),
        F.sum("clp").alias("clp"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("clp")).otherwise(0.0)).alias("clp_ok"),
    ).collect()[0]
    print("\n### 12B.E · ¿El código declarado es el mejor de su familia?")
    print(f"    pares evaluados         : {_rB['pares']:,}")
    print(f"    líneas que representan  : {_rB['lineas']:,}")
    print(f"    declarado ES el mejor   : {g7_pc(_rB['ok'], _rB['lineas'])}  (por línea)")
    print(f"                              {g7_pc(_rB['clp_ok'], _rB['clp'])}  (por CLP, sin sanear)")
    print(f"    similitud 0 con TODOS los hermanos: {g7_pc(_rB['sim0'], _rB['lineas'])}")
    print("\n    VALIDACIÓN — con TOPE=200.000 esto tiene que dar 30,57% y 62,28%.")
    print("    Si no replica, la celda está mal: se descarta y no se cita nada de ella.")

    _opino = (_rB["lineas"] or 0) - (_rB["sim0"] or 0)
    print("\n### 12B.E-bis · LA LECTURA CONDICIONAL — la que va al informe")
    print(f"    el método sólo opinó en {g7_pc(_opino, _rB['lineas'])} de las líneas.")
    print(f"    entre ésas, el código declarado ES el mejor de su familia en "
          f"{g7_pc(_rB['ok'], _opino)}.")
    print("    CAP-12 compara SÓLO dentro de la clase N3 (join por fam6): mide 'dada la")
    print("    familia correcta, ¿acertó la hoja?'. El tamiz de CAP-11 busca en los 18.881")
    print("    códigos y mide otra cosa. Las dos cifras NO se contradicen — y por eso la")
    print("    nota del Cmd 46 que usa el 7,67% para acotar el 30,57% está mal planteada.")

    print("\n### 12B.F · Por rubro N1 — top 15 por volumen")
    print(f"    {'N1':>4} {'líneas':>14} {'declarado = mejor':>20}")
    for r in (_bestB.groupBy("rubro_n1").agg(
                F.sum("n_lineas").alias("lineas"),
                F.sum(F.when(F.col("declarado_es_mejor"), F.col("n_lineas")).otherwise(0)).alias("ok"))
              .orderBy(F.col("lineas").desc()).limit(15).collect()):
        print(f"    {r['rubro_n1']:>4} {r['lineas']:>14,} {g7_pc(r['ok'], r['lineas']):>20}")

    try:
        (_bestB.where(~F.col("declarado_es_mejor") & (F.col("mejor_sim") > 0.15))
               .orderBy(F.col("n_lineas").desc()).limit(200).toPandas()
               .to_csv(f"{_B12B}/cap12b_desacuerdos_top200.csv", index=False))
        print(f"\n   export: {_B12B}/cap12b_desacuerdos_top200.csv")
        print("   [declarar] el export filtra mejor_sim > 0.15. Los desacuerdos por debajo")
        print("   de ese umbral NO están en el CSV; el umbral es arbitrario y va declarado.")
    except Exception as _e:
        print(f"   [no se pudo exportar: {type(_e).__name__}]")

# COMMAND ----------

# MAGIC %md
# MAGIC # Q5 · Reponderación bottom-k de la muestra de 300 — 0 segundos de Spark
# MAGIC Convierte la muestra de **ilustración** en **estimación**. Corre en el driver,
# MAGIC con pandas, sobre dos CSV que ya existen. No toca el cluster.
# MAGIC
# MAGIC **Ya corrida fuera de Databricks el 23-ago (sesión 16).** Queda aquí para que
# MAGIC el notebook sea autosuficiente y el resultado reproducible.

# COMMAND ----------

# --- Q5 · pesos Horvitz-Thompson para la muestra de 300 ----------------------
import pandas as _pd, numpy as _np

_BASE_Q5 = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}"
try:
    _m5 = _pd.read_csv(f"{_BASE_Q5}/cap11_muestra_validez_onu.csv", dtype={"_id": str})
    _p5 = _pd.read_csv(f"{_BASE_Q5}/cap11_pesos_estratos.csv")
except Exception as _e:
    _m5 = _p5 = None
    print(f"ABORTA Q5: falta un CSV ({type(_e).__name__}). Correr CAP-11 y Q2 primero.")

if _m5 is not None:
    # t = umbral del diseño bottom-k = el _id MÁS GRANDE de la muestra, normalizado
    _t5 = int(_m5["_id"].max(), 16) / 2 ** 64
    print(f"\n### Q5 · diseño bottom-k · t = {_t5:.6e}  (punto de quiebre 1/t = {1/_t5:,.0f} tuplas)")

    # ¿qué base ajusta mejor el diseño, tuplas distintas o líneas? Se decide con
    # datos, no con teoría: la que reproduzca el tamaño de muestra observado.
    for _b in ["n_tuplas", "n_lineas"]:
        _pi = 1 - (1 - _t5) ** _p5[_b]
        _sd = float(_np.sqrt((_pi * (1 - _pi)).sum()))
        print(f"    base={_b:<9} E[tamaño muestra]={_pi.sum():7.1f}  sd={_sd:5.1f}  "
              f"observado={len(_m5)}  z={(len(_m5)-_pi.sum())/_sd:+.2f}")

    _p5["pi"] = 1 - (1 - _t5) ** _p5["n_tuplas"]
    _p5["en_muestra"] = _p5["_estrato"].isin(set(_m5["_estrato"]))
    _tl5, _tc5 = _p5.n_lineas.sum(), _p5.clp.sum()
    print(f"\n    cobertura: los {int(_p5.en_muestra.sum())} estratos muestreados cubren "
          f"{100*_p5[_p5.en_muestra].n_lineas.sum()/_tl5:.2f}% de las líneas y "
          f"{100*_p5[_p5.en_muestra].clp.sum()/_tc5:.2f}% del CLP")
    _excl = _p5[_p5.pi < 0.01]
    print(f"    los {len(_excl)} estratos con pi<1% (prácticamente excluidos) son "
          f"{100*_excl.n_lineas.sum()/_tl5:.2f}% de las líneas y {100*_excl.clp.sum()/_tc5:.2f}% del CLP")

    _d5 = _m5.merge(_p5[["_estrato", "n_lineas", "n_tuplas", "clp", "pi"]], on="_estrato", how="left")
    _d5["w_lineas"] = _d5.n_lineas / _d5.pi
    _d5["w_clp"] = _d5.clp / _d5.pi
    print(f"\n    CALIBRACIÓN Horvitz-Thompson — esto es lo que valida la reponderación:")
    print(f"      suma de pesos-línea = {_d5.w_lineas.sum():,.0f}  vs universo {_tl5:,}  "
          f"error {100*(_d5.w_lineas.sum()/_tl5-1):+.2f}%")
    print(f"      suma de pesos-CLP   = {_d5.w_clp.sum():,.0f}  vs universo {_tc5:,.0f}  "
          f"error {100*(_d5.w_clp.sum()/_tc5-1):+.2f}%")
    _w5 = _d5.w_lineas.values
    print(f"      n = {len(_d5)}  ->  n_efectivo (Kish) = {_w5.sum()**2/(_w5**2).sum():.1f}")
    print("      ^ ESA es la salvedad grande: 300 líneas reponderadas pesan como ~49.")
    _d5.to_csv(f"{_BASE_Q5}/cap11c_pesos_muestra.csv", index=False)
    print(f"\n    guardado en {_BASE_Q5}/cap11c_pesos_muestra.csv")
    print("    Cruzar con `cap11b_tamiz_resultado.csv` por `_id` para reponderar cualquier cifra:")
    print("      estimador = sum(w_lineas * y) / sum(w_lineas)")
