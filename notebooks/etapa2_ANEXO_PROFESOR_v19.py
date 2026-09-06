# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# =============================================================================
#  CAPSTONE BIG DATA — MERCADO PUBLICO DE CHILE, 2017-2026
#  Anexo tecnico reproducible
#
#  Autor      : Jeancarlo Cuesta
#  Programa   : Samsung Innovation Campus 2026
#  Plataforma : Databricks Free Edition (serverless)
#  Version    : v19-ANEXO · 29-ago-2026
# =============================================================================
#
#  QUE ES ESTE CUADERNO
#  Contiene, y solo contiene, el codigo que produce las cifras citadas en el
#  informe. Se lee de arriba a abajo y se corre en ese orden. Cada bloque
#  imprime sus propios resultados y su RUN_ID.
#
#  CORRIDAS DE REFERENCIA (las cifras del informe salen de estas)
#     RUN_ID_CAP = 1314c4f6d481    Pregunta 1, cascada y cifras en pesos
#     RUN_ID_CAP = 70680deaec66    re-corrida de verificacion, reprodujo exacto
#     RUN_ID_G7  = fb7c3b125bf7    evaluabilidad del texto (Pregunta 3)
#     RUN_ID_G7b = 966ab5c4d538    consultas complementarias Q1-Q5
#
#  ORDEN DE EJECUCION
#     PARTE A   Preparacion del dato          A1 - A8
#     PARTE B   Pregunta 1                    B1 - B3
#     PARTE C   Cifras en pesos               C1 - C6
#     PARTE D   Auditorias del universo       D1
#     PARTE E   Pregunta 3 — codigo ONU       E1 - E4
#     PARTE F   Consultas complementarias     F1 - F4
#     PARTE G   Exportacion de tablas         G1 - G2
#
#  CONVENCIONES DE ETIQUETADO usadas en todo el proyecto
#     [MEDIDO]        cifra que salio de una corrida, con RUN_ID trazable
#     [FUENTE]        dato externo, con referencia publicada
#     [POR VERIFICAR] afirmacion que aun no tiene respaldo suficiente
#     [OPINION]       lectura del autor, no un resultado
#
#  QUE NO ESTA AQUI, A PROPOSITO
#     Los apendices de diagnostico (6C, 6D-3 a 6D-8, 7B-1, 7C-5), el bloque
#     CAP-12B marcado [PRUEBA] y CAP-5, que nunca corrio. Son trabajo de
#     investigacion interna, no sostienen ninguna cifra del informe y estan
#     completos en la version de respaldo del cuaderno.
# =============================================================================

# COMMAND ----------
# MAGIC # PARTE A — Preparacion del dato
# MAGIC
# MAGIC Carga del universo 2017-2026, normalizacion y enlace orden de compra ↔ licitacion.
# MAGIC Todo lo que viene despues depende de esta parte.
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
# MAGIC # PARTE B — Pregunta 1: consistencia del codigo ONU entre licitacion y orden de compra
# MAGIC
# MAGIC Mide si el codigo de producto declarado en la licitacion es el mismo que aparece en la
# MAGIC orden de compra, contra un baseline nulo permutado que fija que esperariamos del azar.
# MAGIC
# MAGIC **Cifras congeladas:** 94,44 % real contra 1,38 % del azar · 80,08 % · 13,33 %.
# MAGIC RUN_ID_CAP = `1314c4f6d481`, reproducidas exacto en `70680deaec66`.
# COMMAND ----------
# MAGIC ## B1 · CAP-0 — utilidades comunes
# MAGIC
# MAGIC Obligatoria. Declara todos los umbrales del proyecto, cada uno con su justificacion.
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
# MAGIC ## B2 · CAP-0-BIS — parseo numerico chileno
# MAGIC
# MAGIC Sin esto, `1.112,72975` se convierte en NULL en silencio y desaparece de toda suma.
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
# MAGIC ## B3 · CAP-1 — la medicion, con su baseline nulo
# MAGIC
# MAGIC La permutacion se ordena por `xxhash64`, no por `rand()`, para que sea reproducible.
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
# MAGIC # PARTE C — Cifras en pesos: cascada, outlier y reconciliacion externa
# MAGIC
# MAGIC **Cifras congeladas:** gasto saneado de la decada 89.447.529.345.114 CLP ·
# MAGIC top-10 de codigos = 28,35 % del gasto · reconciliacion externa 2024 = **0,9825**
# MAGIC en numero de ordenes de compra.
# MAGIC
# MAGIC Salvedad obligatoria junto a toda cifra en pesos: *suma de precio neto por cantidad,
# MAGIC solo lineas en pesos chilenos nativos; reconcilia con el 88,4 % de la cifra oficial
# MAGIC de 2024 una vez ajustada por IVA.*
# COMMAND ----------
# MAGIC ## C1 · CAP-2 — Pregunta 2: cobertura y validez del codigo ONU por año
# MAGIC
# MAGIC La serie 2017-2026. El quiebre 2019→2022 de −10,21 pp es el hallazgo; el +0,30 pp es el control.
# COMMAND ----------
# %% ==========================================================================
# CAP-2 v12 · PREGUNTA 2 — Cobertura y validez del código ONU por año
# Requiere CAP-0 y CAP-0-BIS. Reemplaza al CAP-2 de v11.
#
# POR QUÉ HAY QUE VOLVER A CORRERLO aunque v11 no abortó:
# En v11 los placebos dieron 100,00% en los diez años, y eso NO era un resultado.
# Todas las columnas numéricas del Parquet son `string`; bajo ANSI, Spark
# reescribe `cast(x AS DOUBLE) IS NOT NULL` como `x IS NOT NULL`, así que
# placebo_pct_cant y placebo_pct_prec midieron "el string no está vacío", no
# "es un número válido". Un placebo pegado al techo no puede refutar nada: si no
# se mueve nunca, no informa sobre si el salto de 2025 es de norma o de plataforma.
#
# QUÉ CAMBIÓ:
#  1. Los placebos usan cap_num(): miden validez NUMÉRICA real.
#  2. Se agrega un tercer placebo: cobertura de UNIDAD DE MEDIDA, campo que el
#     art. 20 bis no manda y que existe en los datos.
#  3. El panel se puede balancear por ORGANISMO (`CodigoOrganismoPublico`), no
#     por el prefijo del código de proceso. Elimina el riesgo 5 del documento de
#     segunda opinión (unidades que se fusionan y renombran).
#  4. Imprime qué años están realmente presentes antes de exigir presencia en todos.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-2 v12 · Pregunta 2 — cobertura del código ONU por año · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")

if _oc is None:
    print("ABORTA: falta df_oc_all / df_oc_linked en el kernel.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year", "anio", "año"], "año (oc)")
    c_tipo = cap_resolver(_oc, ["tipo_oc"], "tipo_oc")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_um   = cap_resolver(_oc, ["_sinclasificar_UnidadMedida"], "unidad de medida")
    c_org  = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")
    c_uni  = cap_resolver(_oc, ["_sinclasificar_CodigoUnidadCompra"], "unidad de compra")

    if not c_onu or not c_anio:
        print(f"ABORTA: no se resolvieron ONU ({c_onu}) o año ({c_anio}).")
    else:
        # ENTIDAD del panel: organismo si existe; si no, unidad de compra.
        if c_org:
            ent, ent_nombre = F.col(c_org), f"organismo ({c_org})"
        elif c_uni:
            ent, ent_nombre = F.col(c_uni), f"unidad de compra ({c_uni})"
        else:
            ent, ent_nombre = F.lit(None).cast("string"), "NINGUNA (panel no calculable)"
        print(f"   Entidad del panel balanceado: {ent_nombre}")

        base = _oc.select(
            F.col(c_anio).cast("int").alias("anio"),
            cap_onu_clase(F.col(c_onu)).alias("clase_onu"),
            (F.col(c_tipo) if c_tipo else F.lit("NA")).alias("tipo_oc"),
            F.trim(ent.cast("string")).alias("entidad"),
            (cap_num(F.col(c_cant)).isNotNull().cast("int")
             if c_cant else F.lit(None).cast("int")).alias("placebo_cant_num"),
            (cap_num(F.col(c_prec)).isNotNull().cast("int")
             if c_prec else F.lit(None).cast("int")).alias("placebo_prec_num"),
            ((F.trim(F.col(c_um).cast("string")).isNotNull() &
              (F.trim(F.col(c_um).cast("string")) != "")).cast("int")
             if c_um else F.lit(None).cast("int")).alias("placebo_um"),
        ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

        _anios = sorted(r["anio"] for r in base.select("anio").distinct().collect()
                        if r["anio"] is not None)
        print(f"   Años presentes en los datos: {_anios}")
        print(f"   El panel exigirá presencia en los {len(_anios)} años listados.")
        print("   OJO: 2026 es un año PARCIAL. Exigir presencia en 2026 sesga el panel hacia")
        print("   entidades grandes y activas. Considerar además un panel 2017-2025.")

        panel = (base.where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                     .groupBy("entidad").agg(F.countDistinct("anio").alias("na"))
                     .where(F.col("na") == len(_anios)).select("entidad"))

        def _serie(df, etiqueta):
            return (df.groupBy("anio")
                      .agg(F.lit(etiqueta).alias("serie"),
                           F.count(F.lit(1)).alias("lineas"),
                           (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct_onu_valido"),
                           (100.0 * F.avg((F.col("clase_onu") == "CERO").cast("int"))).alias("pct_onu_cero"),
                           (100.0 * F.avg((F.col("clase_onu") == "AUSENTE").cast("int"))).alias("pct_ausente"),
                           (100.0 * F.avg((F.col("clase_onu") == "NO_NUMERICO").cast("int"))).alias("pct_no_num"),
                           (100.0 * F.avg(F.col("placebo_cant_num"))).alias("plac_cant"),
                           (100.0 * F.avg(F.col("placebo_prec_num"))).alias("plac_prec"),
                           (100.0 * F.avg(F.col("placebo_um"))).alias("plac_unidad_medida"),
                           F.countDistinct("entidad").alias("entidades")))

        filas = (_serie(base, "1_universo_completo")
                 .unionByName(_serie(base.join(panel, "entidad", "inner"), "2_panel_balanceado"))
                 .orderBy("serie", "anio").collect())
        cap_print_tabla(
            filas,
            "CAP-2.A v12 · Cobertura y validez del código ONU por año\n"
            "          plac_cant / plac_prec / plac_unidad_medida: campos que el art. 20 bis\n"
            "          NO manda. Ahora miden validez NUMÉRICA real, no 'string no vacío'.\n"
            "          Si estos placebos se mueven igual que pct_onu_valido, el cambio es de\n"
            "          plataforma o de composición, NO de la norma.\n"
            "          Un placebo pegado a 100,00 en los diez años NO refuta nada: dilo así.")

        print("\nCAP-2.B · LECTURA OBLIGADA (§1.16)")
        print("   El corte legal es el 12-dic-2024. Con granularidad ANUAL 2024 tiene 19 días")
        print("   tratados y 347 no tratados, por eso 2024 se lee como PRE y 2025 es el primer")
        print("   año post. 2026 es PARCIAL. Con solo el año NO se puede afirmar 'efecto'.")
        print("   PERO: `fecha_creacion` EXISTE en estos datos. Si parsea, este bloque puede")
        print("   rehacerse con granularidad mensual y el diseño deja de ser un pre-post ciego.")

        comp = (base.groupBy("anio", "tipo_oc")
                    .agg(F.count(F.lit(1)).alias("lineas"),
                         (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct_onu_valido"))
                    .orderBy("anio", F.col("lineas").desc()).limit(200).collect())
        cap_print_tabla(comp,
                        "CAP-2.C · Cobertura ONU por año y mecanismo de compra\n"
                        "          Si la cobertura global se mueve porque cambió la MEZCLA de\n"
                        "          mecanismos, se ve aquí y no en la serie agregada.")
        print(f"\nCAP-2 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## C2 · CAP-3 — cascada sobre el universo completo y caza del outlier
# MAGIC
# MAGIC Aqui aparece la linea de 36,8 billones con `precio == cantidad`.
# COMMAND ----------
# %% ==========================================================================
# CAP-3 v13 · CASCADA SOBRE EL UNIVERSO COMPLETO + CAZA DEL OUTLIER
# Requiere CAP-0 y CAP-0-BIS. Reemplaza a CAP-3 v12.
#
# POR QUÉ v13. v12 eligió df_oc_linked porque ahí vive el flag de enlace, pero
# df_oc_linked tiene 16.901.188 filas contra 51.451.528 de df_oc_all: es un
# SUBCONJUNTO ya filtrado a filas con clave de licitación. Sobre él, los peldaños
# 2 y 3 de la cascada dan 100% por construcción y el G6 mide la tasa de enlace
# dentro de un universo que solo contiene enlazables. Tautológico.
#
# v13 usa df_oc_all para TODO y reconstruye el flag de enlace por join contra
# las claves de df_oc_linked que traen tiene_licitacion_origen = true.
#
# ADEMÁS: caza el outlier de 2022 (42,3 billones CLP contra 4-5 billones en los
# años vecinos) imprimiendo las 20 líneas de mayor monto y el gasto recalculado
# sin ellas. Regla §1.16: la magnitud antes que el veredicto.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-3 v13 · Cascada sobre el universo COMPLETO · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all")
_lnk = cap_global("df_oc_linked")

if _oc is None:
    print("ABORTA: falta df_oc_all en el kernel. Este bloque NO acepta df_oc_linked.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year"], "año (oc)")
    c_tipo = cap_resolver(_oc, ["tipo_oc"], "tipo_oc")
    c_klic = cap_resolver(_oc, ["codigo_licitacion"], "código licitación")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_doc  = cap_resolver(_oc, ["codigo_oc"], "código OC")
    c_org  = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")
    c_um   = cap_resolver(_oc, ["_sinclasificar_UnidadMedida"], "unidad de medida")

    # DOS listas de exclusión, y se reportan las DOS cifras.
    # ORIGINAL: la que el proyecto usó para medir el 66,97%. Se mantiene para
    #   que la cifra nueva sea comparable con la vieja.
    # DOCUMENTADA: solo códigos que figuran en la tabla de dominio oficial de
    #   ChileCompra (https://www.chilecompra.cl/api/, verificada el 21-ago-2026).
    #   TD y CB NO figuran ahí; R1 sí, y el proyecto no lo excluía.
    SIN_LIC_ORIGINAL   = ["CM", "AG", "MC", "TD", "CA", "CB"]
    SIN_LIC_DOCUMENTADA = ["CM", "AG", "MC", "CA", "R1"]

    if not c_anio or not (c_prec and c_cant):
        print(f"ABORTA: faltan año ({c_anio}) o precio/cantidad ({c_prec}/{c_cant}).")
    else:
        b = _oc.select(
            F.col(c_anio).cast("int").alias("anio"),
            (F.col(c_tipo) if c_tipo else F.lit(None)).alias("tipo_oc"),
            (F.trim(F.col(c_klic).cast("string")) if c_klic else F.lit(None)).alias("k_lic"),
            (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
            (F.col(c_doc) if c_doc else F.lit(None)).alias("doc"),
            (F.col(c_org) if c_org else F.lit(None)).alias("org"),
            (F.col(c_um) if c_um else F.lit(None)).alias("um"),
            (cap_onu_clase(F.col(c_onu)) if c_onu else F.lit("NA")).alias("clase_onu"),
            cap_num(F.col(c_prec)).alias("precio"),
            cap_num(F.col(c_cant)).alias("cant"),
            cap_num_falla(F.col(c_prec)).alias("falla_prec"),
            cap_num_falla(F.col(c_cant)).alias("falla_cant"),
        ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

        # Flag de enlace reconstruido: claves que df_oc_linked marcó como enlazadas.
        if _lnk is not None:
            c_lk = cap_resolver(_lnk, ["_lic_key"], "clave (linked)")
            c_fl = cap_resolver(_lnk, ["tiene_licitacion_origen"], "flag (linked)")
            claves_ok = (_lnk.where(F.col(c_fl) == True)                      # noqa: E712
                             .select(F.trim(F.col(c_lk).cast("string")).alias("k_lic"))
                             .distinct().withColumn("_enl", F.lit(True)))
            b = b.join(claves_ok, "k_lic", "left")
            print("   Flag de enlace reconstruido desde df_oc_linked por clave de licitación.")
            print("   SALVEDAD: el enlace se atribuye a la CLAVE, no a la línea. Si en")
            print("   df_oc_linked una misma licitación tiene líneas enlazadas y no enlazadas,")
            print("   esta reconstrucción es OPTIMISTA. Declararlo junto a la cifra.")
        else:
            b = b.withColumn("_enl", F.lit(None).cast("boolean"))
            print("   [aviso] df_oc_linked no está: el peldaño 5 quedará NO CALCULABLE.")

        b = (b.withColumn("clp_linea", F.col("precio") * F.col("cant"))
              .withColumn("es_clp", F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP")
              .withColumn("tipo_norm", F.upper(F.trim(F.col("tipo_oc").cast("string"))))
              .withColumn("tipo_nulo", F.col("tipo_norm").isNull() | (F.col("tipo_norm") == ""))
              .withColumn("lic_orig",
                          F.when(F.col("tipo_nulo"), F.lit(False))
                           .otherwise(~F.col("tipo_norm").isin(SIN_LIC_ORIGINAL)))
              .withColumn("lic_doc",
                          F.when(F.col("tipo_nulo"), F.lit(False))
                           .otherwise(~F.col("tipo_norm").isin(SIN_LIC_DOCUMENTADA)))
              .withColumn("tiene_clave", F.col("k_lic").isNotNull() & (F.col("k_lic") != ""))
              .withColumn("dentro_universo",
                          F.coalesce(cap_anio_sufijo(F.col("k_lic")) >= ANIO_MIN, F.lit(False)))
              .withColumn("enlazada", F.coalesce(F.col("_enl"), F.lit(False))))

        p1 = F.col("es_clp")
        p2 = p1 & F.col("lic_orig")
        p3 = p2 & F.col("tiene_clave")
        p4 = p3 & F.col("dentro_universo")
        p5 = p4 & F.col("enlazada")
        p6 = p5 & (F.col("clase_onu") == "VALIDO_8")
        p7 = p6 & (F.col("cant") == 1.0)
        d_o = F.col("lic_orig") & F.col("tiene_clave") & F.col("dentro_universo")
        d_d = F.col("lic_doc")  & F.col("tiene_clave") & F.col("dentro_universo")

        def _cn(c, a): return F.sum(F.when(c, 1).otherwise(0)).alias(a)
        def _cc(c, a): return F.coalesce(F.sum(F.when(c, F.col("clp_linea"))), F.lit(0.0)).alias(a)

        # ---- ACCIÓN 1: la pasada única ----------------------------------------
        k = b.agg(
            F.count(F.lit(1)).alias("n_total"),
            F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("facial"),
            F.sum("falla_prec").alias("n_falla_prec"),
            F.sum("falla_cant").alias("n_falla_cant"),
            _cn(F.col("tipo_nulo"), "n_tipo_nulo"),
            _cn(F.col("precio").isNull() | F.col("cant").isNull(), "n_monto_null"),
            _cn(p1, "n_1"), _cc(p1, "c_1"), _cn(p2, "n_2"), _cc(p2, "c_2"),
            _cn(p3, "n_3"), _cc(p3, "c_3"), _cn(p4, "n_4"), _cc(p4, "c_4"),
            _cn(p5, "n_5"), _cc(p5, "c_5"), _cn(p6, "n_6"), _cc(p6, "c_6"),
            _cn(p7, "n_7"), _cc(p7, "c_7"),
            _cn(d_o, "n_do"), _cc(d_o, "c_do"),
            _cn(d_o & F.col("enlazada"), "n_no"), _cc(d_o & F.col("enlazada"), "c_no"),
            _cn(d_d, "n_dd"), _cc(d_d, "c_dd"),
            _cn(d_d & F.col("enlazada"), "n_nd"), _cc(d_d & F.col("enlazada"), "c_nd"),
        ).collect()[0]

        nt = k["n_total"] or 0
        print("\n" + "-" * 78)
        print("CAP-3.0 · SALUD DEL PARSEO (leer ANTES que cualquier cifra en pesos)")
        print("-" * 78)
        for et, kk in [("precio no parseable", "n_falla_prec"),
                       ("cantidad no parseable", "n_falla_cant"),
                       ("precio o cantidad NULL", "n_monto_null"),
                       ("tipo_oc nulo o vacio", "n_tipo_nulo")]:
            v = k[kk] or 0
            print(f"   {et:<44} {v:>13,}  ({cap_pct(v, nt) or 0:.4f}%)")

        base_c = k["c_1"] or 0
        print("\n" + "-" * 78)
        print("CAP-3.A · CASCADA ANIDADA sobre df_oc_all (UNIVERSO COMPLETO)")
        print("-" * 78)
        print(f"{'PELDANO':<54}{'LINEAS':>13}{'% de p1':>11}")
        print(f"{'0 · Universo total de lineas de OC':<54}{nt:>13,}{'—':>11}")
        for et, kn, kc in [
            ("1 · ... en pesos chilenos nativos (CLP)", "n_1", "c_1"),
            ("2 · ... y de mecanismo que pasa por licitacion", "n_2", "c_2"),
            ("3 · ... y que trae codigo de licitacion", "n_3", "c_3"),
            ("4 · ... y cuya licitacion cae dentro del universo", "n_4", "c_4"),
            ("5 · ... y que efectivamente enlaza", "n_5", "c_5"),
            ("6 · ... y con codigo ONU valido de 8 digitos", "n_6", "c_6"),
            ("7 · ... y con cantidad = 1  -> AUDITABLE", "n_7", "c_7"),
        ]:
            print(f"{et:<54}{k[kn] or 0:>13,}{cap_pct(k[kc] or 0, base_c) or 0:>10.2f}%")
        print(f"\n   Suma FACIAL todas las monedas (NO es CLP): {int(k['facial'] or 0):,}")
        print(f"   CLP nativo (peldano 1) ..................: {int(base_c):,}")
        print(f"   CLP auditable (peldano 7) ...............: {int(k['c_7'] or 0):,}")

        print("\n" + "-" * 78)
        print("CAP-3.B · G6 RESTRINGIDO-Y-AJUSTADO, con las DOS listas de exclusion")
        print("-" * 78)
        for et, dn, nn, dc, nc, lst in [
            ("lista ORIGINAL del proyecto", "n_do", "n_no", "c_do", "c_no", SIN_LIC_ORIGINAL),
            ("lista DOCUMENTADA en la tabla oficial", "n_dd", "n_nd", "c_dd", "c_nd", SIN_LIC_DOCUMENTADA)]:
            gn = cap_pct(k[nn] or 0, k[dn] or 0)
            gc = cap_pct(k[nc] or 0, k[dc] or 0)
            print(f"   {et}  {lst}")
            print(f"      denominador {k[dn] or 0:,} lineas · numerador {k[nn] or 0:,}")
            print(f"      por CONTEO {f'{gn:.2f}%' if gn is not None else 'NO CALCULABLE'}"
                  f"   ·   por MONTO {f'{gc:.2f}%' if gc is not None else 'NO CALCULABLE'}")
        print("   Si las dos cifras difieren mucho, REPORTAR LAS DOS y decir cual es cual.")

        # ---- ACCIÓN 2: mecanismos ---------------------------------------------
        mec = (b.groupBy("tipo_norm")
                .agg(F.count(F.lit(1)).alias("lineas"),
                     F.coalesce(F.sum(F.when(F.col("es_clp"), F.col("clp_linea"))), F.lit(0.0)).alias("clp"),
                     (100.0 * F.avg(F.col("tiene_clave").cast("int"))).alias("pct_con_clave"),
                     (100.0 * F.avg(F.col("enlazada").cast("int"))).alias("pct_enlazada"),
                     (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct_onu_ok"))
                .orderBy(F.col("lineas").desc()).limit(30).collect())
        cap_print_tabla(mec,
                        "CAP-3.B-bis · MECANISMOS EN EL UNIVERSO COMPLETO\n"
                        f"          Lista original: {SIN_LIC_ORIGINAL}\n"
                        f"          Lista documentada: {SIN_LIC_DOCUMENTADA}\n"
                        "          Un mecanismo con pct_con_clave ~0 pertenece a la lista de exclusion;\n"
                        "          uno con pct_con_clave alto NO deberia estar excluido.")

        # ---- ACCIÓN 3: CAZA DEL OUTLIER ---------------------------------------
        top = (b.where(F.col("es_clp") & F.col("clp_linea").isNotNull())
                .select("anio", "tipo_norm", "doc", "k_lic", "org", "um",
                        "precio", "cant", "clp_linea")
                .orderBy(F.col("clp_linea").desc_nulls_last()).limit(20).collect())
        cap_print_tabla(top,
                        "CAP-3.E · LAS 20 LINEAS DE MAYOR MONTO DEL UNIVERSO (caza del outlier)\n"
                        "          El ano 2022 imprimio 42,3 billones contra 4-5 en los vecinos.\n"
                        "          Si una sola linea explica el salto, se ve aqui. Mirar `precio` y\n"
                        "          `cant`: un precio con separador de miles mal leido, o una cantidad\n"
                        "          absurda, aparecen a simple vista.",
                        cols=["anio", "tipo_norm", "doc", "org", "um", "precio", "cant", "clp_linea"])

        # ---- ACCIÓN 4: reconciliación por año, con y sin cola ------------------
        UMBRAL_LINEA = 1e11   # 100.000 millones CLP en UNA linea: declarado y arbitrario
        rec = (b.where(F.col("es_clp"))
                .groupBy("anio")
                .agg(F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp_pipeline"),
                     F.coalesce(F.sum(F.when(F.col("clp_linea") < UMBRAL_LINEA,
                                             F.col("clp_linea"))), F.lit(0.0)).alias("clp_sin_cola"),
                     _cn(F.col("clp_linea") >= UMBRAL_LINEA, "n_lineas_gigantes"),
                     (F.countDistinct("doc") if c_doc else F.lit(None)).alias("n_oc_distintas"),
                     F.count(F.lit(1)).alias("n_lineas"))
                .orderBy("anio").collect())
        cap_print_tabla(rec,
                        "CAP-3.C · Gasto por ano, con y sin las lineas gigantes\n"
                        f"          Umbral declarado y ARBITRARIO: una linea de {UMBRAL_LINEA:,.0f} CLP.\n"
                        "          Si clp_sin_cola se parece a la cifra oficial y clp_pipeline no,\n"
                        "          el problema son unas pocas lineas, no el pipeline.")

        r24 = [r for r in rec if r["anio"] == 2024]
        print("\nCAP-3.D · RECONCILIACION CONTRA LA CIFRA OFICIAL 2024  [FUENTE]")
        print("   ChileCompra 09-jun-2025: 'US$17.643 millones, equivalentes a $16.653.131")
        print("   millones de pesos' y '2.031.670 ordenes de compra'.")
        if not r24:
            print("   NO SE PUEDE COMPARAR: no hay filas de 2024.")
        else:
            p, ps = r24[0]["clp_pipeline"] or 0, r24[0]["clp_sin_cola"] or 0
            for et, v in [("Pipeline 2024 completo", p), ("Pipeline 2024 sin lineas gigantes", ps),
                          ("Pipeline sin cola x 1,19 (con IVA)", ps * 1.19)]:
                print(f"   {et:<38}: {int(v):>22,}   razon {v / OFICIAL_2024_CLP:.4f}")
            print(f"   {'Oficial 2024':<38}: {OFICIAL_2024_CLP:>22,}")
            if r24[0]["n_oc_distintas"]:
                q = r24[0]["n_oc_distintas"]
                print(f"   OC distintas: pipeline {q:,} · oficial {OFICIAL_2024_NUM_OC:,} "
                      f"· razon {q / OFICIAL_2024_NUM_OC:.4f}")
            print("   LECTURA: el pipeline suma precio_NETO x cantidad, solo CLP nativo. Una razon")
            print("   cercana a 0,84 es compatible con el solo efecto del IVA. Explicar la brecha")
            print("   ANTES de citar cualquier cifra en pesos.")
        print(f"\nCAP-3 v13 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## C3 · CAP-4 — concentracion del catalogo y dispersion de precio
# COMMAND ----------
# %% ==========================================================================
# CAP-4 v13 · CONCENTRACIÓN DEL CATÁLOGO + DISPERSIÓN DE PRECIO SANEADA
# Requiere CAP-0 y CAP-0-BIS. Reemplaza a CAP-4 v12.
#
# POR QUÉ v13. La corrida de v12 dejó dos cosas a la vista:
#  (a) La concentración del catálogo NO es la esperada. El Estado usa 17.132
#      códigos distintos de los 18.881 del catálogo de referencia (90,74%), y el
#      top-10 concentra apenas el 10,84% de las líneas y el 1,51% del gasto. La
#      hipótesis de "usa unas pocas decenas de códigos" queda REFUTADA por los
#      datos. v13 agrega el top-N por GASTO, que es la vara que faltaba.
#  (b) La dispersión estaba dominada por basura. Los ratios extremos venían de
#      `um = GLOBAL`, que no es una unidad de medida sino suma alzada: un precio
#      por el total de un servicio. Con p10 = $1,00 y p90 = $3.389.780, el ratio
#      de 3.389.780 no mide dispersión de precio, mide que "GLOBAL" no compara.
#
# CONTROLES NUEVOS, todos declarados y arbitrarios:
#  - Se excluyen las unidades de medida que no permiten comparar (GLOBAL, NO
#    DEFINIDA, vacías). La lista se imprime.
#  - Precio mínimo, para sacar los $1 que son error de captura.
#  - Se reporta p75/p25 como métrica PRIMARIA (robusta) y p90/p10 como
#    secundaria. Un ratio de colas es muy sensible a diez filas malas.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-4 v13 · Concentracion y dispersion saneada · {RUN_ID_CAP}")
print("=" * 78)

# --- Parámetros declarados y ARBITRARIOS -------------------------------------
UM_NO_COMPARABLES = ["GLOBAL", "UNIDAD NO DEFINIDA", "NO DEFINIDA", "SIN INFORMACION",
                     "S/I", "OTROS", "OTRO", "N/A", "NA", ""]
PRECIO_MIN = 100          # CLP. Debajo de esto se asume error de captura.
N_MIN_CELDA = 100         # filas mínimas por (codigo x unidad x anio)

_oc = cap_global("df_oc_all")
if _oc is None:
    print("ABORTA: falta df_oc_all. Este bloque NO acepta df_oc_linked (es un subconjunto).")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year"], "año (oc)")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_um   = cap_resolver(_oc, ["_sinclasificar_UnidadMedida"], "unidad de medida")
    c_org  = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")

    b = _oc.select(
        F.col(c_anio).cast("int").alias("anio"),
        cap_onu8(F.col(c_onu)).alias("onu8"),
        cap_num(F.col(c_prec)).alias("precio"),
        cap_num(F.col(c_cant)).alias("cant"),
        (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
        (F.upper(F.trim(F.col(c_um).cast("string"))) if c_um else F.lit(None).cast("string")).alias("um"),
        (F.trim(F.col(c_org).cast("string")) if c_org else F.lit(None).cast("string")).alias("org"),
    ).withColumn("clp_linea", F.col("precio") * F.col("cant"))

    conv = b.where(F.col("onu8").isNotNull())

    # ---- ACCIÓN 1: ranking por LÍNEAS -------------------------------------
    por_lineas = (conv.groupBy("onu8")
                      .agg(F.count(F.lit(1)).alias("lineas"),
                           F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp"))
                      .orderBy(F.col("lineas").desc()).limit(300).collect())
    # ---- ACCIÓN 2: ranking por GASTO (la vara que faltaba) -----------------
    por_gasto = (conv.groupBy("onu8")
                     .agg(F.count(F.lit(1)).alias("lineas"),
                          F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp"))
                     .orderBy(F.col("clp").desc_nulls_last()).limit(300).collect())
    # ---- ACCIÓN 3: totales -------------------------------------------------
    rt = conv.agg(F.count(F.lit(1)).alias("n"),
                  F.countDistinct("onu8").alias("codigos"),
                  F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp")).collect()[0]
    n_tot, k_tot, clp_tot = rt["n"] or 0, rt["codigos"] or 0, rt["clp"] or 0

    def _p(num, den):
        v = cap_pct(num, den)
        return f"{v:6.2f}%" if v is not None else "    n/c"

    print("\n" + "-" * 78)
    print("CAP-4.A · CONCENTRACION DEL CATALOGO EFECTIVAMENTE USADO")
    print("-" * 78)
    print(f"   Lineas con codigo ONU de 8 digitos ....... {n_tot:,}")
    print(f"   Codigos distintos EN FORMA de 8 digitos .. {k_tot:,}")
    print("   Catalogo de referencia ................... 18.881 codigos")
    print("     [DATO INTERNO, no [FUENTE]: Listado_rubros_ONU_3.xlsx aportado por el autor.")
    print("      ChileCompra no publica el tamano de su catalogo ni la version UNSPSC.]")
    print(f"   Razon contra ese catalogo ................ {_p(k_tot, 18881)}")
    print("\n   CONCENTRACION POR LINEAS          |  CONCENTRACION POR GASTO")
    for n in (10, 50, 100, 300):
        sl, sg = por_lineas[:n], por_gasto[:n]
        print(f"   Top-{n:<4} {_p(sum(r['lineas'] for r in sl), n_tot)} lineas / "
              f"{_p(sum(r['clp'] or 0 for r in sl), clp_tot)} gasto  |  "
              f"{_p(sum(r['lineas'] for r in sg), n_tot)} lineas / "
              f"{_p(sum(r['clp'] or 0 for r in sg), clp_tot)} gasto")
    print("\n   LECTURA OBLIGADA (§1.16). Si estos porcentajes son bajos, la hipotesis de")
    print("   'el Estado usa unas pocas decenas de codigos' esta REFUTADA por los datos, y")
    print("   hay que decirlo asi. Un catalogo ampliamente usado NO es un mal resultado:")
    print("   es un resultado distinto del que el proyecto esperaba, y cambia el hallazgo.")
    cap_print_tabla(por_gasto[:15],
                    "CAP-4.B · Los 15 codigos ONU que concentran mas GASTO")

    # ---- ACCIÓN 4: qué unidades de medida existen --------------------------
    ums = (b.where(F.col("onu8").isNotNull())
             .groupBy("um").agg(F.count(F.lit(1)).alias("lineas"))
             .orderBy(F.col("lineas").desc()).limit(40).collect())
    cap_print_tabla(ums,
                    "CAP-4.B-bis · VALORES REALES DE UNIDAD DE MEDIDA (top 40 por volumen)\n"
                    f"          Excluidos del analisis de precio: {UM_NO_COMPARABLES}\n"
                    "          MIRAR ESTA TABLA antes de citar cualquier ratio: si 'UN', 'Unidad'\n"
                    "          y 'c/u' aparecen como valores distintos, hay que homologarlos o\n"
                    "          declarar que no se homologaron.")

    # ---- ACCIÓN 5: dispersión saneada --------------------------------------
    top_codes = [r["onu8"] for r in por_lineas[:TOP_N_PRECIO]]
    disp = (b.where(F.col("onu8").isin(top_codes) &
                    (F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP") &
                    (F.col("precio") >= PRECIO_MIN) &
                    F.col("um").isNotNull() &
                    (~F.col("um").isin(UM_NO_COMPARABLES)))
             .groupBy("onu8", "um", "anio")
             .agg(F.count(F.lit(1)).alias("n"),
                  F.countDistinct("org").alias("organismos"),
                  F.expr("percentile_approx(precio, 0.25, 1000)").alias("p25"),
                  F.expr("percentile_approx(precio, 0.50, 1000)").alias("p50"),
                  F.expr("percentile_approx(precio, 0.75, 1000)").alias("p75"),
                  F.expr("percentile_approx(precio, 0.10, 1000)").alias("p10"),
                  F.expr("percentile_approx(precio, 0.90, 1000)").alias("p90"))
             .where((F.col("n") >= N_MIN_CELDA) & (F.col("organismos") >= 5))
             .withColumn("r_p75_p25", F.when(F.col("p25") > 0, F.col("p75") / F.col("p25")))
             .withColumn("r_p90_p10", F.when(F.col("p10") > 0, F.col("p90") / F.col("p10")))
             .orderBy(F.col("r_p75_p25").desc_nulls_last()).limit(40).collect())
    cap_print_tabla(
        disp,
        "CAP-4.C · DISPERSION DE PRECIO UNITARIO — SANEADA\n"
        f"          Filtros DECLARADOS y ARBITRARIOS: top-{TOP_N_PRECIO} codigos por volumen ·\n"
        "          moneda CLP nativa · precio >= " + f"{PRECIO_MIN:,} CLP · unidad de medida\n"
        f"          comparable · n >= {N_MIN_CELDA} y al menos 5 organismos por celda.\n"
        "          METRICA PRIMARIA: r_p75_p25 (robusta). r_p90_p10 va al lado como\n"
        "          secundaria: un ratio de colas se mueve con diez filas malas.\n"
        "          SALVEDAD 1: la unidad de medida es texto libre del comprador y NO se\n"
        "          homologo. SALVEDAD 2: dentro de un codigo ONU de 8 digitos siguen\n"
        "          conviviendo productos distintos. Antes de citar: revisar a mano los\n"
        "          diez casos extremos y mirar las fichas en mercadopublico.cl.",
        cols=["onu8", "um", "anio", "n", "organismos", "p25", "p50", "p75",
              "r_p75_p25", "r_p90_p10"])
    print("\n   Los montos NO estan deflactados: no comparar entre anos sin llevarlos a")
    print("   CLP real (IPC) o a UTM.")
    print(f"\nCAP-4 v13 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## C4 · CAP-7 — saneamiento del gasto y reconciliacion definitiva
# MAGIC
# MAGIC Decide que el outlier son dos lineas, no un defecto sistematico. El daño colateral del
# MAGIC filtro es el 0,0057 % del gasto.
# COMMAND ----------
# %% ==========================================================================
# CAP-7 · SANEAMIENTO DEL GASTO Y RECONCILIACIÓN DEFINITIVA
# Requiere CAP-0 y CAP-0-BIS. Se corre DESPUÉS de CAP-3 v13.
#
# QUÉ RESUELVE
# CAP-3.E cazó el outlier y su firma es inconfundible: `precio == cantidad`.
#   2022 · 5648-84-SE22 · precio 6.069.497 y cantidad 6.069.497 → 36,8 billones
#   2018 · 5603-71-SE18 · precio 772.000   y cantidad 772.000   → 596 mil millones
# Es el error de captura clásico: el monto total escrito en el campo cantidad.
# Esa sola línea de 2022 es el 29,03% del gasto de toda la década.
#
# Este bloque:
#  1. Mide cuántas líneas tienen esa firma. Si son dos, es anécdota; si son miles,
#     es un defecto sistemático del pipeline y hay que decirlo en metodología.
#  2. Recalcula la cascada con y sin saneamiento, para reportar las DOS.
#  3. Cierra la pregunta del IVA reconciliando contra `monto_total_oc_clp` y
#     `_sinclasificar_totalLineaNeto`, que existen en los datos y nadie usó.
#  4. Recalcula la concentración del gasto sin el outlier.
#
# NINGÚN FILTRO DE ESTE BLOQUE SE APLICA EN SILENCIO. Todos se cuentan y se
# imprimen. Un dato excluido sin declarar es peor que un dato malo declarado.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-7 · Saneamiento del gasto y reconciliacion · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all")
if _oc is None:
    print("ABORTA: falta df_oc_all.")
else:
    c_anio = cap_resolver(_oc, ["_year"], "año")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_doc  = cap_resolver(_oc, ["codigo_oc"], "codigo OC")
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU")
    c_tot  = cap_resolver(_oc, ["monto_total_oc_clp"], "monto total OC en CLP")
    c_lnet = cap_resolver(_oc, ["_sinclasificar_totalLineaNeto"], "total linea neto")
    c_imp  = cap_resolver(_oc, ["_sinclasificar_totalImpuestos"], "total impuestos")

    # Umbral de la firma de error: precio == cantidad y ambos por encima de esto.
    # ARBITRARIO y declarado. Bajo este valor, precio == cantidad puede ser
    # legitimo (comprar 5 unidades de algo que cuesta 5 pesos).
    UMBRAL_FIRMA = 1000.0

    b = _oc.select(
        F.col(c_anio).cast("int").alias("anio"),
        cap_num(F.col(c_prec)).alias("precio"),
        cap_num(F.col(c_cant)).alias("cant"),
        (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
        (F.col(c_doc) if c_doc else F.lit(None)).alias("doc"),
        (cap_onu8(F.col(c_onu)) if c_onu else F.lit(None).cast("string")).alias("onu8"),
        (cap_num(F.col(c_lnet)) if c_lnet else F.lit(None).cast("double")).alias("linea_neto"),
    ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

    b = (b.withColumn("clp_linea", F.col("precio") * F.col("cant"))
          .withColumn("es_clp", F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP")
          .withColumn("firma_error",
                      (F.col("precio") == F.col("cant")) &
                      (F.col("precio") > UMBRAL_FIRMA))
          .withColumn("clp_saneado",
                      F.when(F.col("firma_error"), F.lit(None)).otherwise(F.col("clp_linea"))))

    # ---- ACCIÓN 1: ¿cuántas líneas tienen la firma, y cuánto pesan? ----------
    f = b.where(F.col("es_clp")).agg(
        F.count(F.lit(1)).alias("n"),
        F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp_crudo"),
        F.coalesce(F.sum("clp_saneado"), F.lit(0.0)).alias("clp_saneado"),
        F.sum(F.col("firma_error").cast("int")).alias("n_firma"),
        F.coalesce(F.sum(F.when(F.col("firma_error"), F.col("clp_linea"))), F.lit(0.0)).alias("clp_firma"),
        # Control cruzado: la multiplicacion precio x cantidad contra el campo
        # `totalLineaNeto` que el propio dato trae. Si no coinciden, el problema
        # no es el outlier: es la formula.
        F.coalesce(F.sum("linea_neto"), F.lit(0.0)).alias("suma_linea_neto"),
        F.sum(F.when(F.col("linea_neto").isNotNull() &
                     (F.abs(F.col("clp_linea") - F.col("linea_neto")) >
                      0.01 * F.abs(F.col("linea_neto"))), 1).otherwise(0)).alias("n_discrepan_5pct"),
        F.sum(F.when(F.col("linea_neto").isNotNull(), 1).otherwise(0)).alias("n_con_linea_neto"),
    ).collect()[0]

    n, crudo, san = f["n"] or 0, f["clp_crudo"] or 0, f["clp_saneado"] or 0
    print("\n" + "-" * 78)
    print("CAP-7.A · LA FIRMA DE ERROR  `precio == cantidad`")
    print(f"          Umbral ARBITRARIO y declarado: ambos > {UMBRAL_FIRMA:,.0f} CLP")
    print("-" * 78)
    print(f"   Lineas CLP nativas ..................... {n:,}")
    print(f"   ... con la firma de error .............. {f['n_firma'] or 0:,} "
          f"({cap_pct(f['n_firma'] or 0, n) or 0:.6f}%)")
    print(f"   Monto que aportan esas lineas .......... {int(f['clp_firma'] or 0):,}")
    print(f"   ... como % del gasto CRUDO ............. {cap_pct(f['clp_firma'] or 0, crudo) or 0:.2f}%")
    print(f"\n   Gasto CLP CRUDO   (2017-2026) .......... {int(crudo):,}")
    print(f"   Gasto CLP SANEADO (2017-2026) .......... {int(san):,}")
    print("\n   LECTURA (§1.16): si el conteo de la firma es de unas pocas lineas, esto es")
    print("   una anecdota que se declara en metodologia y se excluye. Si son miles, es un")
    print("   defecto sistematico del pipeline y NO se puede reportar ninguna cifra en")
    print("   pesos hasta entenderlo. El numero de arriba decide cual de las dos es.")

    print("\n" + "-" * 78)
    print("CAP-7.B · CONTROL CRUZADO: precio x cantidad  vs  totalLineaNeto del dato")
    print("-" * 78)
    if not c_lnet or (f["n_con_linea_neto"] or 0) == 0:
        print("   NO CALCULABLE: la columna totalLineaNeto no se resolvio o viene vacia.")
    else:
        nc = f["n_con_linea_neto"] or 0
        nd = f["n_discrepan_5pct"] or 0
        print(f"   Lineas con totalLineaNeto ................ {nc:,}")
        print(f"   ... que difieren mas de 1% de precio x cant  {nd:,} "
              f"({cap_pct(nd, nc) or 0:.4f}%)")
        print(f"   Suma de totalLineaNeto ................... {int(f['suma_linea_neto'] or 0):,}")
        print(f"   Suma de precio x cantidad (crudo) ........ {int(crudo):,}")
        print("   Si la discrepancia es ~0%, la formula precio x cantidad esta validada por")
        print("   el propio dato y deja de ser un supuesto del proyecto.")

    # ---- ACCIÓN 2: reconciliación con el monto de la OC (cierra el IVA) ------
    print("\n" + "-" * 78)
    print("CAP-7.C · RECONCILIACION CONTRA `monto_total_oc_clp` — cierra la pregunta del IVA")
    print("-" * 78)
    if not (c_tot and c_doc):
        print("   NO CALCULABLE: falta monto_total_oc_clp o codigo_oc.")
        oc_anual = []
    else:
        oc_anual = (_oc.select(
                        F.col(c_anio).cast("int").alias("anio"),
                        F.col(c_doc).alias("doc"),
                        cap_num(F.col(c_tot)).alias("monto_oc"))
                      .where(F.col("anio").between(ANIO_MIN, ANIO_MAX))
                      .dropDuplicates(["doc"])          # una fila por OC, no por linea
                      .groupBy("anio")
                      .agg(F.count(F.lit(1)).alias("n_oc"),
                           F.coalesce(F.sum("monto_oc"), F.lit(0.0)).alias("monto_oc_total"))
                      .orderBy("anio").collect())
        cap_print_tabla(oc_anual,
                        "   Monto declarado a nivel de ORDEN DE COMPRA (deduplicado por codigo_oc)")

    # ---- ACCIÓN 3: gasto anual saneado --------------------------------------
    anual = (b.where(F.col("es_clp"))
              .groupBy("anio")
              .agg(F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("crudo"),
                   F.coalesce(F.sum("clp_saneado"), F.lit(0.0)).alias("saneado"),
                   F.sum(F.col("firma_error").cast("int")).alias("n_firma"),
                   (F.countDistinct("doc") if c_doc else F.lit(None)).alias("n_oc"))
              .orderBy("anio").collect())
    cap_print_tabla(anual, "CAP-7.D · Gasto anual CRUDO vs SANEADO, y lineas con firma de error")

    _oc_map = {r["anio"]: r for r in oc_anual}
    print("\nCAP-7.E · LAS TRES VARAS CONTRA LA CIFRA OFICIAL 2024  [FUENTE]")
    print("   Oficial: $16.653.131 millones y 2.031.670 ordenes de compra.")
    a24 = [r for r in anual if r["anio"] == 2024]
    if not a24:
        print("   NO SE PUEDE COMPARAR: no hay 2024.")
    else:
        r = a24[0]
        varas = [("precio x cantidad, crudo", r["crudo"] or 0),
                 ("precio x cantidad, saneado", r["saneado"] or 0),
                 ("saneado x 1,19 (si lo oficial fuese con IVA)", (r["saneado"] or 0) * 1.19)]
        if 2024 in _oc_map:
            varas.append(("monto_total_oc_clp (nivel OC)", _oc_map[2024]["monto_oc_total"] or 0))
        for et, v in varas:
            print(f"   {et:<46}: {int(v):>21,}  razon {v / OFICIAL_2024_CLP:.4f}")
        if r["n_oc"]:
            print(f"   {'N de OC distintas':<46}: {r['n_oc']:>21,}  razon "
                  f"{r['n_oc'] / OFICIAL_2024_NUM_OC:.4f}")
        print("\n   LECTURA: la vara que mas se acerque a 1,00 es la que hay que usar en el")
        print("   informe, y hay que DECIR cual es y por que. Si `monto_total_oc_clp` cuadra")
        print("   y `precio x cantidad` no, la diferencia son impuestos, cargos y descuentos,")
        print("   y eso se declara en una linea de metodologia en vez de discutirse.")

    # ---- ACCIÓN 4: concentración del gasto, saneada -------------------------
    conc_s = (b.where(F.col("onu8").isNotNull() & F.col("es_clp") & ~F.col("firma_error"))
                .groupBy("onu8")
                .agg(F.count(F.lit(1)).alias("lineas"),
                     F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp"))
                .orderBy(F.col("clp").desc_nulls_last()).limit(300).collect())
    tot_s = b.where(F.col("onu8").isNotNull() & F.col("es_clp") & ~F.col("firma_error")).agg(
        F.count(F.lit(1)).alias("n"),
        F.countDistinct("onu8").alias("k"),
        F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp")).collect()[0]
    nS, kS, cS = tot_s["n"] or 0, tot_s["k"] or 0, tot_s["clp"] or 0

    print("\n" + "-" * 78)
    print("CAP-7.F · CONCENTRACION DEL GASTO, SANEADA (sin las lineas con firma de error)")
    print("-" * 78)
    print(f"   Lineas CLP con ONU valido: {nS:,} · codigos distintos: {kS:,}")
    for nn in (10, 50, 100, 300):
        sub = conc_s[:nn]
        print(f"   Top-{nn:<4} por gasto: {cap_pct(sum(r['lineas'] for r in sub), nS) or 0:6.2f}% "
              f"de las lineas y {cap_pct(sum(r['clp'] or 0 for r in sub), cS) or 0:6.2f}% del gasto")
    cap_print_tabla(conc_s[:15], "CAP-7.G · Los 15 codigos ONU con mas gasto, SANEADO")
    print("\n   Estos 15 codigos son el material del hallazgo en pesos. Antes de nombrarlos")
    print("   en el informe hay que buscar su descripcion en el catalogo UNSPSC: el numero")
    print("   solo no le dice nada a un jurado, y adivinar el rubro es exactamente el tipo")
    print("   de afirmacion sin fuente que la Regla 2 prohibe.")
    print(f"\nCAP-7 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## C5 · CAP-8 — que son los codigos que concentran el gasto
# MAGIC
# MAGIC La etiqueta de cada codigo sale de los propios datos, no de un catalogo externo.
# COMMAND ----------
# %% ==========================================================================
# CAP-8 · QUÉ SON LOS CÓDIGOS QUE CONCENTRAN EL GASTO + LA FICHA DEL OUTLIER
# Requiere CAP-0 y CAP-0-BIS. Se corre después de CAP-7.
#
# POR QUÉ. CAP-7.G entregó los 15 códigos ONU que concentran el gasto, pero un
# código de ocho dígitos no le dice nada a un jurado, y adivinar el rubro por el
# prefijo es exactamente el tipo de afirmación sin fuente que la Regla 2 prohíbe.
# No hace falta el catálogo externo: los propios datos traen
# `_sinclasificar_NombreroductoGenerico` y `RubroN1/N2/N3`. La descripción sale
# de la misma fuente que el código, que es la forma más defendible de obtenerla.
#
# Además imprime la FICHA COMPLETA de las dos líneas del outlier. Es material de
# informe: `totalLineaNeto` del propio dato confirma esos montos, así que no es
# un artefacto del pipeline — es lo que Mercado Público publica.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-8 · Nombres de los codigos y ficha del outlier · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all")
if _oc is None:
    print("ABORTA: falta df_oc_all.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU")
    c_nom  = cap_resolver(_oc, ["_sinclasificar_NombreroductoGenerico"], "nombre generico")
    c_r1   = cap_resolver(_oc, ["_sinclasificar_RubroN1"], "rubro 1")
    c_r2   = cap_resolver(_oc, ["_sinclasificar_RubroN2"], "rubro 2")
    c_r3   = cap_resolver(_oc, ["_sinclasificar_RubroN3"], "rubro 3")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_doc  = cap_resolver(_oc, ["codigo_oc"], "codigo OC")
    c_klic = cap_resolver(_oc, ["codigo_licitacion"], "codigo licitacion")
    c_anio = cap_resolver(_oc, ["_year"], "año")
    c_org  = cap_resolver(_oc, ["_sinclasificar_OrganismoPublico"], "organismo (nombre)")
    c_sec  = cap_resolver(_oc, ["_sinclasificar_sector"], "sector")
    c_desc = cap_resolver(_oc, ["_sinclasificar_Descripcion/Obervaciones"], "descripcion OC")
    c_lnk  = cap_resolver(_oc, ["_sinclasificar_Link"], "link")
    c_lnet = cap_resolver(_oc, ["_sinclasificar_totalLineaNeto"], "total linea neto")
    c_nomoc= cap_resolver(_oc, ["_sinclasificar_Nombre"], "nombre OC")

    # Lista de códigos: la de CAP-7 si sigue viva, si no la de respaldo.
    _cs = cap_global("conc_s")
    if _cs:
        CODIGOS = [r["onu8"] for r in _cs[:30]]
        print(f"   Codigos tomados de conc_s (CAP-7): {len(CODIGOS)}")
    else:
        CODIGOS = ["72131702", "93131608", "85121602", "85141701", "76111501",
                   "76121501", "92101501", "32101617", "93151507", "70111703",
                   "50131701", "85122201", "51142145", "81111503", "80141607"]
        print("   [aviso] conc_s no esta en el kernel: se usa la lista de respaldo de 15.")

    b = _oc.select(
        cap_onu8(F.col(c_onu)).alias("onu8"),
        (F.trim(F.col(c_nom).cast("string")) if c_nom else F.lit(None).cast("string")).alias("nombre"),
        (F.trim(F.col(c_r1).cast("string")) if c_r1 else F.lit(None).cast("string")).alias("rubro1"),
        (F.trim(F.col(c_r2).cast("string")) if c_r2 else F.lit(None).cast("string")).alias("rubro2"),
        (F.trim(F.col(c_r3).cast("string")) if c_r3 else F.lit(None).cast("string")).alias("rubro3"),
        cap_num(F.col(c_prec)).alias("precio"),
        cap_num(F.col(c_cant)).alias("cant"),
        (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
    ).where(F.col("onu8").isin(CODIGOS) &
            (F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP"))

    # ---- ACCIÓN 1: la etiqueta mas frecuente de cada codigo -----------------
    et = (b.where(~((F.col("precio") == F.col("cant")) & (F.col("precio") > 1000)))
           .groupBy("onu8", "nombre", "rubro1", "rubro2", "rubro3")
           .agg(F.count(F.lit(1)).alias("lineas"),
                F.coalesce(F.sum(F.col("precio") * F.col("cant")), F.lit(0.0)).alias("clp"))
           .collect())

    # Se elige, por codigo, la etiqueta que mas LINEAS tiene, y se suma el gasto total.
    por_cod = {}
    for r in et:
        d = por_cod.setdefault(r["onu8"], {"lineas": 0, "clp": 0.0, "etqs": []})
        d["lineas"] += r["lineas"]
        d["clp"] += r["clp"] or 0.0
        d["etqs"].append((r["lineas"], r["nombre"], r["rubro1"], r["rubro2"], r["rubro3"]))

    orden = sorted(por_cod.items(), key=lambda kv: -kv[1]["clp"])
    print("\n" + "-" * 78)
    print("CAP-8.A · QUE SON LOS CODIGOS QUE CONCENTRAN EL GASTO")
    print("   Etiqueta = el valor de NombreroductoGenerico mas frecuente del propio dato.")
    print("   Se excluyen las lineas con firma de error para no distorsionar el gasto.")
    print("-" * 78)
    for cod, d in orden:
        d["etqs"].sort(key=lambda t: -t[0])
        n, nom, r1, r2, r3 = d["etqs"][0]
        cuota = 100.0 * n / d["lineas"] if d["lineas"] else 0.0
        print(f"\n   {cod}   {d['lineas']:>9,} lineas   CLP {int(d['clp']):>20,}")
        print(f"      nombre generico : {nom}   (en el {cuota:.1f}% de sus lineas)")
        print(f"      rubro N1 / N2 / N3 : {r1} / {r2} / {r3}")
        if len(d["etqs"]) > 1:
            otras = ", ".join(f"{t[1]} ({t[0]:,})" for t in d["etqs"][1:4] if t[1])
            if otras:
                print(f"      otras etiquetas : {otras}")
        if cuota < 60:
            print("      [OJO] ninguna etiqueta domina: este codigo agrupa productos distintos.")
            print("      No describirlo con una sola frase en el informe.")

    # ---- ACCIÓN 2: la ficha del outlier -------------------------------------
    o = _oc.select(
        (F.col(c_anio).cast("int") if c_anio else F.lit(None)).alias("anio"),
        (F.col(c_doc) if c_doc else F.lit(None)).alias("codigo_oc"),
        (F.col(c_klic) if c_klic else F.lit(None)).alias("codigo_lic"),
        (F.col(c_nomoc) if c_nomoc else F.lit(None)).alias("nombre_oc"),
        (F.col(c_org) if c_org else F.lit(None)).alias("organismo"),
        (F.col(c_sec) if c_sec else F.lit(None)).alias("sector"),
        (F.col(c_nom) if c_nom else F.lit(None)).alias("producto"),
        (F.col(c_desc) if c_desc else F.lit(None)).alias("descripcion"),
        (F.col(c_lnk) if c_lnk else F.lit(None)).alias("link"),
        cap_onu8(F.col(c_onu)).alias("onu8"),
        cap_num(F.col(c_prec)).alias("precio"),
        cap_num(F.col(c_cant)).alias("cant"),
        (cap_num(F.col(c_lnet)) if c_lnet else F.lit(None).cast("double")).alias("linea_neto"),
    ).where((F.col("precio") == F.col("cant")) & (F.col("precio") > 1000) &
            ((F.col("precio") * F.col("cant")) > 1e11)).limit(10).collect()

    print("\n" + "-" * 78)
    print("CAP-8.B · FICHA DE LAS LINEAS DEL OUTLIER (firma precio==cantidad, > 100 mil millones)")
    print("-" * 78)
    if not o:
        print("   (ninguna) — revisar el umbral.")
    for r in o:
        d = r.asDict()
        print(f"\n   OC {d['codigo_oc']}   ano {d['anio']}   licitacion {d['codigo_lic']}")
        print(f"      organismo   : {d['organismo']}")
        print(f"      sector      : {d['sector']}")
        print(f"      nombre OC   : {str(d['nombre_oc'])[:120]}")
        print(f"      producto    : {d['producto']}   (ONU {d['onu8']})")
        print(f"      descripcion : {str(d['descripcion'])[:200]}")
        print(f"      precio {d['precio']:,.2f}   x   cantidad {d['cant']:,.2f}")
        print(f"      = {d['precio'] * d['cant']:,.2f} CLP")
        print(f"      totalLineaNeto del propio dato: "
              f"{d['linea_neto']:,.2f}" if d['linea_neto'] is not None else "      totalLineaNeto: null")
        print(f"      link : {d['link']}")
    print("\n   LECTURA: si `totalLineaNeto` coincide con precio x cantidad, el monto NO es un")
    print("   artefacto del pipeline: es lo que la fuente publica. Eso convierte el caso en un")
    print("   HALLAZGO de calidad de datos, no en una nota metodologica. Antes de usarlo:")
    print("   abrir el link y verificar a mano que la ficha publica dice lo mismo.")
    print("   Y NO nombrar al organismo en el informe: describirlo por sector y por tramo.")
    print(f"\nCAP-8 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## C6 · CAP-9 — la cascada saneada, cruda y saneada lado a lado
# MAGIC
# MAGIC Reportar las dos es lo que hace defendible haber excluido algo.
# COMMAND ----------
# %% ==========================================================================
# CAP-9 · LA CASCADA SANEADA — cifras exactas para el acta de congelamiento
# Requiere CAP-0 y CAP-0-BIS. Es CAP-3.A con el filtro de la firma de error.
#
# POR QUÉ. Sanear mueve la cascada mucho más de lo que parece: el outlier vive en
# los peldaños 1 a 6 pero NO en el 7 (su cantidad no es 1), así que al excluirlo
# baja el denominador de todos los peldaños intermedios y SUBE el porcentaje
# auditable. Aproximado: el peldaño 6 pasa de 69,30% a ~56%, y el 7 de 24,69% a
# ~35%. Una aproximación no se cita: esta celda da los números exactos.
#
# Imprime SIEMPRE las dos cascadas, cruda y saneada, una al lado de la otra.
# Reportar las dos es lo que hace defendible haber excluido algo.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-9 · Cascada CRUDA vs SANEADA · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all")
_lnk = cap_global("df_oc_linked")
if _oc is None:
    print("ABORTA: falta df_oc_all.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU")
    c_anio = cap_resolver(_oc, ["_year"], "año")
    c_tipo = cap_resolver(_oc, ["tipo_oc"], "tipo_oc")
    c_klic = cap_resolver(_oc, ["codigo_licitacion"], "codigo licitacion")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")

    SIN_LICITACION = ["CM", "AG", "MC", "TD", "CA", "CB"]
    UMBRAL_FIRMA = 1000.0

    b = _oc.select(
        F.col(c_anio).cast("int").alias("anio"),
        (F.col(c_tipo) if c_tipo else F.lit(None)).alias("tipo_oc"),
        (F.trim(F.col(c_klic).cast("string")) if c_klic else F.lit(None)).alias("k_lic"),
        (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
        (cap_onu_clase(F.col(c_onu)) if c_onu else F.lit("NA")).alias("clase_onu"),
        cap_num(F.col(c_prec)).alias("precio"),
        cap_num(F.col(c_cant)).alias("cant"),
    ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

    if _lnk is not None:
        c_lk = cap_resolver(_lnk, ["_lic_key"], "clave (linked)")
        c_fl = cap_resolver(_lnk, ["tiene_licitacion_origen"], "flag (linked)")
        claves = (_lnk.where(F.col(c_fl) == True)                      # noqa: E712
                      .select(F.trim(F.col(c_lk).cast("string")).alias("k_lic"))
                      .distinct().withColumn("_enl", F.lit(True)))
        b = b.join(claves, "k_lic", "left")
    else:
        b = b.withColumn("_enl", F.lit(None).cast("boolean"))

    b = (b.withColumn("clp", F.col("precio") * F.col("cant"))
          .withColumn("es_clp", F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP")
          .withColumn("tn", F.upper(F.trim(F.col("tipo_oc").cast("string"))))
          .withColumn("lic", F.when(F.col("tn").isNull() | (F.col("tn") == ""), F.lit(False))
                              .otherwise(~F.col("tn").isin(SIN_LICITACION)))
          .withColumn("clave", F.col("k_lic").isNotNull() & (F.col("k_lic") != ""))
          .withColumn("dentro", F.coalesce(cap_anio_sufijo(F.col("k_lic")) >= ANIO_MIN, F.lit(False)))
          .withColumn("enl", F.coalesce(F.col("_enl"), F.lit(False)))
          .withColumn("ok", ~((F.col("precio") == F.col("cant")) &
                              (F.col("precio") > UMBRAL_FIRMA))))   # ok = NO tiene la firma

    P = {}
    P[1] = F.col("es_clp")
    P[2] = P[1] & F.col("lic")
    P[3] = P[2] & F.col("clave")
    P[4] = P[3] & F.col("dentro")
    P[5] = P[4] & F.col("enl")
    P[6] = P[5] & (F.col("clase_onu") == "VALIDO_8")
    P[7] = P[6] & (F.col("cant") == 1.0)

    aggs = [F.count(F.lit(1)).alias("n_universo")]
    for i in range(1, 8):
        aggs += [
            F.sum(F.when(P[i], 1).otherwise(0)).alias(f"n{i}"),
            F.coalesce(F.sum(F.when(P[i], F.col("clp"))), F.lit(0.0)).alias(f"c{i}"),
            F.sum(F.when(P[i] & F.col("ok"), 1).otherwise(0)).alias(f"ns{i}"),
            F.coalesce(F.sum(F.when(P[i] & F.col("ok"), F.col("clp"))), F.lit(0.0)).alias(f"cs{i}"),
        ]
    k = b.agg(*aggs).collect()[0]

    ET = ["", "en CLP nativo",
          "y de mecanismo que pasa por licitacion",
          "y que trae codigo de licitacion",
          "y cuya licitacion cae dentro del universo",
          "y que efectivamente enlaza",
          "y con codigo ONU valido de 8 digitos",
          "y con cantidad = 1  -> AUDITABLE"]

    base_c, base_s = k["c1"] or 0, k["cs1"] or 0
    print(f"\n   Universo total de lineas: {k['n_universo'] or 0:,}")
    print(f"   Gasto CLP CRUDO   : {int(base_c):,}")
    print(f"   Gasto CLP SANEADO : {int(base_s):,}")
    print(f"   Excluido por la firma precio==cantidad: {int(base_c - base_s):,} "
          f"({cap_pct(base_c - base_s, base_c) or 0:.2f}% del gasto crudo)")
    print("\n" + "-" * 78)
    print(f"{'PELDANO':<46}{'% CRUDO':>10}{'% SANEADO':>12}{'LINEAS SAN.':>14}")
    print("-" * 78)
    for i in range(1, 8):
        print(f"{str(i) + ' · ' + ET[i]:<46}"
              f"{cap_pct(k[f'c{i}'] or 0, base_c) or 0:>9.2f}%"
              f"{cap_pct(k[f'cs{i}'] or 0, base_s) or 0:>11.2f}%"
              f"{k[f'ns{i}'] or 0:>14,}")
    print("-" * 78)
    print(f"\n   CLP trazable hasta el producto declarado (peldano 6, saneado):")
    print(f"      {int(k['cs6'] or 0):,}   = {cap_pct(k['cs6'] or 0, base_s) or 0:.2f}% del gasto")
    print(f"   CLP comparable en precio (peldano 7, saneado):")
    print(f"      {int(k['cs7'] or 0):,}   = {cap_pct(k['cs7'] or 0, base_s) or 0:.2f}% del gasto")
    print("\n   OJO AL LEER: el peldano 6 es una propiedad del ESTADO (la OC declara de que")
    print("   licitacion viene y con que codigo de producto). El peldano 7 es una propiedad")
    print("   del CONTROL METODOLOGICO de este proyecto: `cantidad = 1` lo puso el analista")
    print("   para poder comparar precios. Presentar el 7 como 'lo auditable' le atribuye al")
    print("   Estado una limitacion que puso el analista. Reportar los dos, y decir cual es cual.")
    print(f"\n   Trazabilidad no circular (peldano 2 -> 3, saneado):")
    t_n = cap_pct(k["ns3"] or 0, k["ns2"] or 0)
    t_c = cap_pct(k["cs3"] or 0, k["cs2"] or 0)
    print(f"      por CONTEO {t_n:.2f}%   ·   por MONTO {t_c:.2f}%")
    print(f"\nCAP-9 termina. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC # PARTE D — Auditorias del universo
# MAGIC
# MAGIC Dos controles baratos: el semi-join que descarta composicion como causa del +0,30 pp
# MAGIC de la Pregunta 2, y el desborde entre el año de la particion y el año de la fecha de
# MAGIC creacion (0,5982 % del universo, acotado).
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
# MAGIC # PARTE E — Pregunta 3: ¿el codigo ONU declarado es el correcto?
# MAGIC
# MAGIC Primero se mide si existe texto evaluable (CAP-10); recien despues se juzga el codigo.
# MAGIC La muestra estratificada de CAP-11 es la que hace citable esta pregunta.
# MAGIC RUN_ID_G7 = `fb7c3b125bf7`.
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
# MAGIC # PARTE F — Consultas complementarias
# MAGIC
# MAGIC Q1 circularidad por mecanismo · Q2 pesos de los estratos · Q4 codigos ONU malformados ·
# MAGIC Q5 reponderacion Horvitz-Thompson de la muestra de 300.
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
# COMMAND ----------
# MAGIC # PARTE G — Exportacion de las tablas citadas
# MAGIC
# MAGIC G1 no gasta computo. G2 trae su propio control: si el delta 2024→2025 no reproduce el
# MAGIC +0,30 pp congelado, **no escribe el CSV** y lo dice.
# COMMAND ----------
# MAGIC ## G1 · CAP-6 — exportar a CSV las tablas citadas
# MAGIC
# MAGIC Cero computo de Spark.
# COMMAND ----------
# %% ==========================================================================
# CAP-6 v2 · EXPORTAR A CSV LAS TABLAS CITADAS — CERO cómputo de Spark
#
# POR QUÉ v2. v1 asumió que el volumen `/Volumes/workspace/default/capstone`
# existía. No existe, y `os.makedirs` NO puede crearlo: en Unity Catalog el
# segmento que sigue al esquema es el NOMBRE de un volumen, que se crea con SQL,
# no con el sistema de archivos. De ahí el Errno 95 "Operation not supported".
#
# QUÉ HACE v2
#  1. Lista los volúmenes que realmente existen y los imprime.
#  2. Intenta escribir en varios destinos, en orden, y se queda con el primero
#     que funcione: volumen existente → volumen creado por SQL → archivos del
#     workspace → disco local del driver.
#  3. PASE LO QUE PASE, imprime todas las tablas como CSV en la salida de la
#     celda. Aunque los cuatro destinos fallen, la corrida no se pierde: copias
#     la salida y la pegas en un archivo. Esa es la garantía que importa.
#
# No lanza ninguna acción de Spark sobre los datos: lee las variables de Python
# que quedaron vivas en el kernel. Por eso no consume cupo, y por eso hay que
# correrla en la MISMA sesión que CAP-1 a CAP-5.
# =============================================================================
import csv as _csv
import io as _io
import os as _os
import datetime as _dt

MAX_FILAS_IMPRESAS = 400      # tope por tabla al imprimir en pantalla

print("=" * 78)
print(f"CAP-6 v2 · exportación · RUN_ID_CAP = {RUN_ID_CAP}")
print("=" * 78)

# ---- 1 · Qué existe realmente ------------------------------------------------
_user = None
try:
    _user = spark.sql("SELECT current_user()").collect()[0][0]
    print(f"   Usuario actual: {_user}")
except Exception as _e:
    print(f"   [aviso] no se pudo leer current_user(): {type(_e).__name__}")

for _q in ("SHOW CATALOGS", "SHOW SCHEMAS IN workspace", "SHOW VOLUMES IN workspace.default"):
    try:
        _r = spark.sql(_q).collect()
        print(f"   {_q}: " + ", ".join(str(x[-1]) if len(x) == 1 else str(tuple(x)) for x in _r[:20]))
    except Exception as _e:
        print(f"   {_q}: falló ({type(_e).__name__})")

# ---- 2 · Encontrar un destino escribible -------------------------------------
_CANDIDATOS = []

# (a) volúmenes que ya existen en workspace.default
try:
    for _row in spark.sql("SHOW VOLUMES IN workspace.default").collect():
        _d = _row.asDict()
        _vol = _d.get("volume_name") or _d.get("volumeName") or list(_d.values())[-1]
        _CANDIDATOS.append(f"/Volumes/workspace/default/{_vol}/capstone_salidas")
except Exception:
    pass

# (b) crear el volumen con SQL — esta es la vía correcta, no makedirs
try:
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.capstone")
    print("   Volumen workspace.default.capstone creado o ya existente.")
    _CANDIDATOS.append("/Volumes/workspace/default/capstone/salidas_cap")
except Exception as _e:
    print(f"   [aviso] no se pudo crear el volumen: {type(_e).__name__}: {str(_e)[:160]}")

# (c) archivos del workspace  (d) disco local del driver
if _user:
    _CANDIDATOS.append(f"/Workspace/Users/{_user}/capstone_salidas")
_CANDIDATOS.append("/tmp/capstone_salidas")

_DEST = None
for _c in _CANDIDATOS:
    _ruta = f"{_c}/{RUN_ID_CAP}"
    try:
        _os.makedirs(_ruta, exist_ok=True)
        with open(f"{_ruta}/.probe", "w") as _fh:
            _fh.write("ok")
        _os.remove(f"{_ruta}/.probe")
        _DEST = _ruta
        print(f"\n   DESTINO ELEGIDO: {_DEST}")
        break
    except Exception as _e:
        print(f"   [no sirve] {_c} → {type(_e).__name__}: {str(_e)[:90]}")

if _DEST is None:
    print("\n   NINGÚN DESTINO ESCRIBIBLE. Se imprime todo abajo: copiar la salida.")

# ---- 3 · Inventario de tablas vivas en el kernel ------------------------------
_TABLAS = [
    ("res",       "cap1_real_vs_nulo",        "CAP-1.A · consistencia ONU real vs. nulo permutado"),
    ("filas",     "cap2_cobertura_anual",     "CAP-2.A · cobertura y validez del ONU por año, con placebos"),
    ("comp",      "cap2_cobertura_mecanismo", "CAP-2.C · cobertura ONU por año y mecanismo"),
    ("mec",       "cap3_mecanismos",          "CAP-3.B-bis · mecanismos, tasa de clave y de enlace"),
    ("rec",       "cap3_gasto_anual",         "CAP-3.C · gasto por año, solo líneas CLP nativas"),
    ("top",       "cap3_lineas_mayores",      "CAP-3.E · las 20 líneas de mayor monto (caza del outlier)"),
    ("por_lineas","cap4_top_por_lineas",      "CAP-4.A · códigos ONU ordenados por número de líneas"),
    ("por_gasto", "cap4_top_por_gasto",       "CAP-4.A · códigos ONU ordenados por gasto"),
    ("ums",       "cap4_unidades_medida",     "CAP-4.B-bis · valores reales de unidad de medida"),
    ("disp",      "cap4_dispersion_precio",   "CAP-4.C · dispersión de precio saneada"),
    ("conc",      "cap4_concentracion_v12",   "CAP-4.A (v12) · concentración por líneas"),
    ("dispA",     "cap4_dispersion_v12",      "CAP-4.C (v12) · dispersión sin sanear — NO CITABLE"),
]


def _tomar(nombre):
    try:
        return eval(nombre)
    except (NameError, SyntaxError):
        return None


def _a_dicts(filas):
    if not filas:
        return []
    try:
        return [r.asDict() for r in filas]
    except AttributeError:
        return [dict(r) if not isinstance(r, dict) else r for r in filas]


def _csv_texto(datos):
    buf = _io.StringIO()
    w = _csv.DictWriter(buf, fieldnames=list(datos[0].keys()))
    w.writeheader()
    w.writerows(datos)
    return buf.getvalue()


_manifiesto = []
_pendientes = []

for _var, _nombre, _desc in _TABLAS:
    _datos = _a_dicts(_tomar(_var))
    if not _datos:
        continue
    _pendientes.append((_nombre, _desc, _datos))
    if _DEST:
        try:
            _ruta = f"{_DEST}/{_nombre}__{RUN_ID_CAP}.csv"
            with open(_ruta, "w", newline="", encoding="utf-8") as _fh:
                _fh.write(_csv_texto(_datos))
            print(f"   [ok] {_nombre:<28} {len(_datos):>6,} filas → {_ruta}")
            _manifiesto.append({"archivo": _os.path.basename(_ruta), "tabla": _nombre,
                                "filas": len(_datos), "descripcion": _desc,
                                "run_id_cap": RUN_ID_CAP,
                                "fecha_corrida": _dt.date.today().isoformat()})
        except Exception as _e:
            print(f"   [FALLÓ] {_nombre}: {type(_e).__name__}: {str(_e)[:90]}")

# La cascada de CAP-3 es una sola Row
for _v, _n in (("k", "cap3_cascada_v13"), ("casc", "cap3_cascada_v12")):
    _row = _tomar(_v)
    if _row is not None and not isinstance(_row, (list, tuple)):
        try:
            _d = [_row.asDict()]
            _pendientes.append((_n, "CAP-3.A · cascada del gasto, contadores y montos", _d))
            if _DEST:
                with open(f"{_DEST}/{_n}__{RUN_ID_CAP}.csv", "w", newline="", encoding="utf-8") as _fh:
                    _fh.write(_csv_texto(_d))
                print(f"   [ok] {_n:<28} {1:>6,} filas")
                _manifiesto.append({"archivo": f"{_n}__{RUN_ID_CAP}.csv", "tabla": _n,
                                    "filas": 1, "descripcion": "CAP-3.A · cascada",
                                    "run_id_cap": RUN_ID_CAP,
                                    "fecha_corrida": _dt.date.today().isoformat()})
        except Exception:
            pass

if _DEST and _manifiesto:
    try:
        with open(f"{_DEST}/00_MANIFIESTO__{RUN_ID_CAP}.csv", "w", newline="", encoding="utf-8") as _fh:
            _fh.write(_csv_texto(_manifiesto))
        print(f"\n   [ok] manifiesto → {_DEST}/00_MANIFIESTO__{RUN_ID_CAP}.csv")
        print(f"   {len(_manifiesto)} tablas escritas en disco.")
        print("   Descargarlas desde Catalog → Volumes (o desde el explorador de archivos")
        print("   del workspace) y subirlas al repositorio junto con el manifiesto.")
    except Exception as _e:
        print(f"   [FALLÓ] manifiesto: {type(_e).__name__}")

# ---- 4 · GARANTÍA: imprimir todo, pase lo que pase ---------------------------
print("\n" + "=" * 78)
print("VOLCADO CSV EN PANTALLA — la copia de seguridad que no depende de nada")
print("Cada bloque va entre marcas ###CSV. Copiar entre marcas y guardar como .csv")
print("=" * 78)
for _nombre, _desc, _datos in _pendientes:
    _n = len(_datos)
    print(f"\n###CSV_INICIO {_nombre} | filas={_n} | run_id={RUN_ID_CAP}")
    print(f"### {_desc}")
    if _n > MAX_FILAS_IMPRESAS:
        print(f"### TRUNCADO: se imprimen las primeras {MAX_FILAS_IMPRESAS} de {_n} filas.")
        print("### Las filas omitidas NO están en pantalla — si el archivo en disco no se")
        print("### escribió, esta tabla queda incompleta. Declararlo si se usa.")
    print(_csv_texto(_datos[:MAX_FILAS_IMPRESAS]).rstrip())
    print(f"###CSV_FIN {_nombre}")

print(f"\nCAP-6 v2 termina. {len(_pendientes)} tablas. RUN_ID_CAP={RUN_ID_CAP}")
# COMMAND ----------
# MAGIC ## G2 · CAP-16 — series para las figuras 4 y 6
# COMMAND ----------
# =============================================================================
# CAP-16 · EXPORTADOR DE LOS CSV QUE LES FALTAN A LAS FIGURAS
# Capstone Big Data · Mercado Publico · sesion 25 (28-ago-2026)
#
# CELDA DE DATABRICKS. Pegar al final del notebook maestro v18, DESPUES de que
# hayan corrido la Celda 1-3 (carga) y CAP-0/CAP-0-BIS (utilidades).
#
# COSTO: DOS acciones de Spark. Es la corrida del dia mejor gastada que queda:
#        desbloquea TRES de las siete figuras del informe.
#
# -----------------------------------------------------------------------------
# POR QUE ESTA CELDA Y NO OTRA
# -----------------------------------------------------------------------------
# `graficos_capstone.py` corre fuera de Databricks y no cuesta cupo, pero tres
# de sus siete figuras se saltan solas porque les falta su CSV:
#
#   figura 4 · Pregunta 2, serie interrumpida   <- cap2_serie_onu_por_anio.csv
#   figura 6 · auditoria _year                  <- cap9bis_auditoria_year.csv
#   figura 2 · embudo de trazabilidad           <- cap9_cascada.csv  (YA RESUELTO:
#              va escrito a mano en csv/, copiado de la salida cruda de
#              01-RESULTADOS seccion 2. No cuesta corrida.)
#
# Las dos primeras se calculan aca. Las cifras NO son nuevas: son exactamente
# las que ya imprime la Celda 9-BIS del v18. Lo unico que agrega esta celda es
# escribirlas en CSV con los nombres de columna que `graficos_capstone.py`
# espera, para que la figura salga sin que nadie transcriba numeros a mano
# (que es como se producen las regresiones de la regla §1.20).
#
# UNA DIFERENCIA DELIBERADA CON LA CELDA 9-BIS
# La 9-BIS restringe a 2024 y 2025 porque su pregunta era el confundidor del
# 12-dic-2024. La figura 4 dibuja la serie COMPLETA con la restringida encima,
# asi que aca el semi-join de organismos (los presentes en 2024 Y en 2025) se
# aplica a TODOS los anios. Es el mismo conjunto de organismos; cambia el rango
# sobre el que se lo evalua. El veredicto de 2024 vs 2025 tiene que reproducir
# el +0,30 pp de `70680deaec66`: si no lo reproduce, hay algo mal y NO se
# escribe el CSV.
# =============================================================================

import os

MEDIR_DUPLICACION_LIC = False   # ver el bloque 16.C antes de ponerlo en True

RUN_ID_16 = globals().get("RUN_ID_CAP") or "sin_run_id"
DIR_SALIDA = f"/Volumes/workspace/default/mercado_publico/capstone_salidas/{RUN_ID_16}"

print("=" * 78)
print(f"CAP-16 · exportador de CSV para las figuras · RUN_ID_CAP={RUN_ID_16}")
print("=" * 78)

_falta = [n for n in ("cap_resolver", "cap_onu_clase", "cap_global")
          if n not in globals()]
if _falta:
    print(f"ABORTA: faltan utilidades en el kernel: {_falta}.")
    print("        Correr CAP-0 y CAP-0-BIS antes de esta celda.")
else:
    def _escribir_csv(nombre, cabecera, filas):
        """Escribe un CSV chico desde el driver. No usa Spark: son 10-20 filas."""
        os.makedirs(DIR_SALIDA, exist_ok=True)
        ruta = os.path.join(DIR_SALIDA, nombre)
        with open(ruta, "w", encoding="utf-8") as fh:
            fh.write(";".join(cabecera) + "\n")
            for f in filas:
                fh.write(";".join("" if v is None else str(v) for v in f) + "\n")
        print(f"   [OK] escrito {ruta}  ({len(filas)} filas)")
        print("        Descargarlo a la carpeta csv/ del repositorio "
              "(Catalog -> Volumes) y correr graficos_capstone.py.")
        return ruta

    # =========================================================================
    # 16.A · cap2_serie_onu_por_anio.csv  ->  FIGURA 4
    # =========================================================================
    print("\n" + "-" * 78)
    print("16.A · Serie de cobertura ONU por anio: completa y restringida")
    print("-" * 78)

    _oc = cap_global("df_oc_all") or cap_global("df_oc_linked")   # noqa: F821
    if _oc is None:
        print("ABORTA 16.A: falta df_oc_all / df_oc_linked en el kernel.")
    else:
        c_onu = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")                    # noqa: F821
        c_anio = cap_resolver(_oc, ["_year", "anio", "año"], "año (oc)")     # noqa: F821
        c_org = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"],  # noqa: F821
                             "organismo")
        c_uni = cap_resolver(_oc, ["_sinclasificar_CodigoUnidadCompra"],      # noqa: F821
                             "unidad de compra")

        if not c_onu or not c_anio or not (c_org or c_uni):
            print(f"ABORTA 16.A: ONU={c_onu} anio={c_anio} org={c_org} uni={c_uni}")
        else:
            ent = F.col(c_org) if c_org else F.col(c_uni)
            print(f"   Entidad del semi-join: {c_org or c_uni}")

            base = _oc.select(
                F.col(c_anio).cast("int").alias("anio"),
                F.trim(ent.cast("string")).alias("entidad"),
                cap_onu_clase(F.col(c_onu)).alias("clase_onu"),   # noqa: F821
            ).where(F.col("anio").isNotNull())

            _e = (F.col("entidad").isNotNull() & (F.col("entidad") != ""))
            orgs_ambos = (base.where(_e & (F.col("anio") == 2024)).select("entidad").distinct()
                          .join(
                              base.where(_e & (F.col("anio") == 2025)).select("entidad").distinct(),
                              "entidad", "inner"))

            def _serie(df, etiqueta):
                return (df.groupBy("anio")
                          .agg(F.lit(etiqueta).alias("serie"),
                               F.count(F.lit(1)).alias("lineas"),
                               (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))
                                ).alias("pct_onu_valido"),
                               F.countDistinct("entidad").alias("entidades")))

            # >>> ACCION DE SPARK 1 de 2 <<<
            filas = (_serie(base, "1_completa")
                     .unionByName(_serie(base.join(orgs_ambos, "entidad", "inner"),
                                         "2_restringida"))
                     .orderBy("serie", "anio").collect())

            d = {(r["serie"], r["anio"]): r for r in filas}
            anios = sorted({r["anio"] for r in filas})

            print(f"\n   {'anio':<7}{'lineas':>14}{'completa':>11}"
                  f"{'restringida':>13}{'organismos':>12}")
            print("   " + "-" * 57)
            salida = []
            for a in anios:
                rc = d.get(("1_completa", a))
                rr = d.get(("2_restringida", a))
                pc = round(rc["pct_onu_valido"], 4) if rc else None
                pr = round(rr["pct_onu_valido"], 4) if rr else None
                print(f"   {a:<7}{(rc['lineas'] if rc else 0):>14,}"
                      f"{(f'{pc:.2f}%' if pc is not None else '-'):>11}"
                      f"{(f'{pr:.2f}%' if pr is not None else '-'):>13}"
                      f"{(rr['entidades'] if rr else 0):>12,}")
                salida.append((a, pc, pr))

            # --- CONTROL: tiene que reproducir el +0,30 pp de 70680deaec66 ---
            r24 = d.get(("2_restringida", 2024))
            r25 = d.get(("2_restringida", 2025))
            print("\n   CONTROL (§1.16) — el delta restringido 2024->2025 tiene que dar")
            print("   +0,30 pp, que es la cifra congelada de RUN_ID_CAP=70680deaec66.")
            if r24 and r25:
                delta = r25["pct_onu_valido"] - r24["pct_onu_valido"]
                print(f"      medido ahora = {delta:+.2f} pp   "
                      f"(2024 {r24['pct_onu_valido']:.2f}% -> 2025 {r25['pct_onu_valido']:.2f}%)")
                ok = abs(delta - 0.30) <= 0.02
                print(f"      organismos en ambos anios = {r25['entidades']:,} "
                      f"(congelado: 1.079)")
                if ok:
                    print("      -> REPRODUCE. Se escribe el CSV.")
                    _escribir_csv("cap2_serie_onu_por_anio.csv",
                                  ("anio", "pct_completa", "pct_restringida"), salida)
                else:
                    print("      -> NO REPRODUCE. NO se escribe el CSV.")
                    print("      No es un problema del grafico: o cambio el universo cargado,")
                    print("      o la entidad del semi-join no es la misma. Mirar eso ANTES")
                    print("      de dibujar nada. Una figura que contradice una cifra")
                    print("      congelada sin explicacion es exactamente lo que la regla")
                    print("      §1.20 prohibe.")
            else:
                print("      NO SE PUEDE COMPARAR: falta 2024 o 2025 en la serie restringida.")

    # =========================================================================
    # 16.B · cap9bis_auditoria_year.csv  ->  FIGURA 6
    # =========================================================================
    print("\n" + "-" * 78)
    print("16.B · Auditoria _year vs anio(fecha_creacion)  (F19 §2.5)")
    print("-" * 78)

    _oc3 = cap_global("df_oc_all")            # noqa: F821
    if _oc3 is None:
        print("ABORTA 16.B: falta df_oc_all en el kernel.")
    elif "fecha_creacion_norm" not in _oc3.columns:
        print("ABORTA 16.B: falta 'fecha_creacion_norm'. Correr la Celda 4 (v18,")
        print("             corregida) antes de esta celda.")
    else:
        # >>> ACCION DE SPARK 2 de 2 <<<
        aud = (_oc3
               .where(F.col("fecha_creacion_norm").isNotNull())
               .withColumn("anio_creacion", F.year(F.col("fecha_creacion_norm")))
               .withColumn("desborde", (F.col("anio_creacion") != F.col("_year")).cast("int"))
               .groupBy("_year")
               .agg(F.count(F.lit(1)).alias("n_total"),
                    F.sum("desborde").alias("n_desborde"))
               .withColumn("pct_desborde",
                           F.round(100.0 * F.col("n_desborde") / F.col("n_total"), 4))
               .orderBy("_year")
               .collect())

        print(f"\n   {'_year':<8}{'n_total':>15}{'n_desborde':>13}{'pct':>10}")
        print("   " + "-" * 46)
        salida6 = []
        for r in aud:
            print(f"   {r['_year']:<8}{r['n_total']:>15,}{r['n_desborde']:>13,}"
                  f"{r['pct_desborde']:>9.4f}%")
            salida6.append((r["_year"], r["pct_desborde"]))

        n_tot = sum((r["n_total"] or 0) for r in aud)
        n_des = sum((r["n_desborde"] or 0) for r in aud)
        pct_glob = 100.0 * n_des / n_tot if n_tot else 0.0
        print(f"\n   GLOBAL: {n_des:,} de {n_tot:,} = {pct_glob:.4f}%")
        print("   CONTROL (§1.16): la cifra congelada es 0,5982% "
              "(307.775 / 51.451.528, RUN_ID_CAP=70680deaec66).")
        if abs(pct_glob - 0.5982) <= 0.01:
            print("   -> REPRODUCE. Se escribe el CSV.")
            _escribir_csv("cap9bis_auditoria_year.csv",
                          ("anio", "pct_discrepante"), salida6)
        else:
            print("   -> NO REPRODUCE. NO se escribe el CSV. Averiguar por que antes")
            print("      de dibujar la figura 6.")

    # =========================================================================
    # 16.C · OPCIONAL — medir la duplicacion de lic_historico/2025
    # =========================================================================
    # NO se corre por defecto. La duplicacion ya esta establecida por tres
    # metodos que no cuestan cupo (F26 §1.3), y ninguna cifra publicada depende
    # de ella (F26 §2). Esto solo agrega el numero exacto para el informe.
    #
    # Cuesta UN escaneo completo de licitaciones, que es la tabla grande. Si se
    # activa, hacerlo en una corrida propia, no encima de las dos de arriba.
    if MEDIR_DUPLICACION_LIC:
        print("\n" + "-" * 78)
        print("16.C · Duplicacion en el lado licitacion  (F26 §1.3)")
        print("-" * 78)
        _lic = cap_global("df_lic_all")      # noqa: F821
        if _lic is None:
            print("ABORTA 16.C: falta df_lic_all.")
        else:
            c_k = cap_resolver(_lic, ["codigo_externo"], "clave lic")     # noqa: F821
            c_i = cap_resolver(_lic, ["_sinclasificar_Codigoitem"], "item")  # noqa: F821
            c_of = cap_resolver(_lic, ["_sinclasificar_Nombre de la Oferta",  # noqa: F821
                                       "_sinclasificar_RutProveedor"], "oferta")
            if not c_k or not c_i:
                print(f"ABORTA 16.C: clave={c_k} item={c_i}")
            else:
                # La clave natural de una fila de licitacion es
                # (licitacion, item, oferta). Si count(*) supera al distinct de
                # esa terna, las filas sobrantes son copias literales.
                llaves = [F.col(c_k), F.col(c_i)] + ([F.col(c_of)] if c_of else [])
                dup = (_lic.groupBy("_year")
                       .agg(F.count(F.lit(1)).alias("filas"),
                            F.countDistinct(*llaves).alias("filas_distintas"),
                            F.countDistinct(F.col(c_k)).alias("licitaciones"))
                       .withColumn("factor", F.round(F.col("filas") /
                                                     F.col("filas_distintas"), 3))
                       .orderBy("_year").collect())
                print(f"\n   {'_year':<8}{'filas':>14}{'distintas':>14}"
                      f"{'licitac.':>12}{'factor':>9}")
                print("   " + "-" * 57)
                for r in dup:
                    print(f"   {r['_year']:<8}{r['filas']:>14,}{r['filas_distintas']:>14,}"
                          f"{r['licitaciones']:>12,}{r['factor']:>9.3f}")
                print("\n   LECTURA: factor ~1,0 = sin copias. factor ~3,0 en 2025 (y ~1,2")
                print("   en 2024, que solo tiene dos meses de la ventana) confirma F26 §1.3")
                print("   con el numero exacto. Si el factor da 1,0 en 2025, la duplicacion")
                print("   NO es de filas identicas: son vintages distintos del mismo mes, y")
                print("   entonces la terna (lic, item, oferta) no alcanza para deduplicar.")

print(f"\nCAP-16 termina. RUN_ID_CAP={RUN_ID_16}")
print("Pegar esta salida completa en el chat.")


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-3-BIS · G6 AJUSTADO — cálculo real (reemplaza el literal "99,19%")
# MAGIC **Nueva, sesión 31 (31-ago-2026).** Requiere `df_oc_linked`, `ANIO_MIN` y `cap_anio_sufijo`
# MAGIC en el kernel (Celda 6 y CAP-0, más arriba en ESTE mismo notebook). No depende de CAP-1 a CAP-9.

# COMMAND ----------

# =============================================================================
# QUÉ CORRIGE. El Apéndice A (6C) de `etapa2_RESPALDO_COMPLETO_v18.py` (histórico,
# archivado, NO se entrega ni se edita) imprimía dos cifras como TEXTO FIJO, sin
# calcularlas en ninguna variable:
#     "87,6%-94,1% de las no-enlazadas 2017-2023 apuntan a pre-2017"
#     "G6 ajustado acumulado 2017-2026 = 99,19% (ver bitácora para el detalle)"
# Verificado el 31-ago-2026 (sesión 31), leyendo el código fuente línea por
# línea: NINGUNA de las dos aparece calculada en ese cuaderno ni en ningún otro
# archivo del paquete. Son literales escritos a mano (violan la regla del
# proyecto §3.14 — nada se cita sin que su propia celda lo calcule). Esta celda
# es NUEVA y no existía en ninguna versión anterior del notebook.
#
# QUÉ HACE. Para cada línea de OC con código de licitación (`df_oc_linked`),
# extrae el año implícito en el SUFIJO de ese código con `cap_anio_sufijo`
# (función ya definida en este notebook: '...-LR23' -> 2023) y separa las NO
# enlazadas en dos grupos:
#   (a) su código de licitación apunta a un año ANTERIOR a ANIO_MIN (2017) ->
#       la licitación de origen nunca fue cargada al universo: no puede
#       enlazar por diseño, no es trazabilidad perdida.
#   (b) apunta a un año DENTRO del universo (>= ANIO_MIN), o el sufijo no se
#       pudo leer -> sigue contando como enlace fallido real.
# G6 AJUSTADO = enlazadas / (con_clave - grupo_a). El grupo (a) sale del
# denominador; el grupo (b) —con o sin año legible— se queda adentro: es la
# lectura CONSERVADORA (un sufijo no legible nunca se asume pre-2017).
#
# QUÉ IMPRIME. La tabla por año (reemplaza el "87,6%-94,1%" citado sin celda) y
# la cifra agregada 2017-2026 (reemplaza el "99,19%"), las dos con su propio
# `RUN_ID_CAP`. Ninguna cifra de esta celda se cita en el informe sin pegar
# antes esta salida completa en el chat para verificarla (regla §1).
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-3-BIS · G6 AJUSTADO — cálculo real · RUN_ID_CAP={RUN_ID_CAP}")
print("=" * 78)

try:
    _lnk_bis = df_oc_linked
except NameError:
    _lnk_bis = cap_global("df_oc_linked")

if _lnk_bis is None:
    print("ABORTA: falta df_oc_linked en el kernel. Correr la Celda 6 de este notebook antes.")
else:
    _base_adj = (
        _lnk_bis
        .withColumn("_anio_lic_ref", cap_anio_sufijo(c_(COL_KEY_OC)))
        .withColumn("_pre_universo",
                    F.coalesce(F.col("_anio_lic_ref") < ANIO_MIN, F.lit(False)))
    )

    # ---- (1) tabla por año de la OC, solo entre las NO enlazadas ------------
    _por_anio_bis = (
        _base_adj
        .where(~F.col("tiene_licitacion_origen"))
        .groupBy("_year")
        .agg(F.count(F.lit(1)).alias("no_enlazadas"),
             F.sum(F.col("_pre_universo").cast("int")).alias("apuntan_pre_universo"),
             F.sum((F.col("_anio_lic_ref").isNull()).cast("int")).alias("sufijo_no_legible"))
        .withColumn("pct_pre_universo",
                    F.round(100.0 * F.col("apuntan_pre_universo") /
                            F.when(F.col("no_enlazadas") > 0, F.col("no_enlazadas")), 2))
        .orderBy("_year")
        .collect()
    )
    cap_print_tabla(
        _por_anio_bis,
        "CAP-3-BIS.A · No-enlazadas por año de la OC: ¿cuántas apuntan a una\n"
        "              licitación anterior a 2017 (fuera del universo cargado)?\n"
        "              Reemplaza el '87,6%-94,1%' que se venía citando sin celda.",
        cols=["_year", "no_enlazadas", "apuntan_pre_universo", "sufijo_no_legible",
              "pct_pre_universo"])

    # ---- (2) G6 ajustado 2017-2026, con el denominador corregido ------------
    _tot_bis = _base_adj.agg(
        F.count(F.lit(1)).alias("con_clave"),
        F.sum(F.col("tiene_licitacion_origen").cast("int")).alias("enlazadas"),
        F.sum((~F.col("tiene_licitacion_origen") & F.col("_pre_universo")).cast("int")
              ).alias("estructuralmente_no_enlazable"),
    ).collect()[0]

    _con_clave_bis = _tot_bis["con_clave"] or 0
    _enlazadas_bis = _tot_bis["enlazadas"] or 0
    _estruct_bis = _tot_bis["estructuralmente_no_enlazable"] or 0
    _den_ajustado_bis = _con_clave_bis - _estruct_bis
    _g6_raw_bis = cap_pct(_enlazadas_bis, _con_clave_bis)
    _g6_adj_bis = cap_pct(_enlazadas_bis, _den_ajustado_bis)

    print("\n" + "-" * 78)
    print("CAP-3-BIS.B · G6 AJUSTADO 2017-2026 (reemplaza el literal '99,19%')")
    print("-" * 78)
    print(f"   Con código de licitación (denominador crudo) ....... {_con_clave_bis:,}")
    print(f"   Enlazadas ............................................ {_enlazadas_bis:,}")
    print(f"   De las NO enlazadas, apuntan a pre-{ANIO_MIN} .............. {_estruct_bis:,}")
    print(f"   Denominador AJUSTADO (crudo − estructural) .......... {_den_ajustado_bis:,}")
    print(f"   G6 SIN ajustar (91,01% ya congelado, control) ....... "
          f"{f'{_g6_raw_bis:.2f}%' if _g6_raw_bis is not None else 'NO CALCULABLE'}")
    print(f"   G6 AJUSTADO (ESTA CELDA — cítese con este run_id) ... "
          f"{f'{_g6_adj_bis:.2f}%' if _g6_adj_bis is not None else 'NO CALCULABLE'}")
    print("\n   LECTURA. Si este número no cae cerca de 99,19%, el literal anterior era")
    print("   incorrecto y se cita ESTE en su lugar, con este run_id. Si cae cerca, el")
    print("   literal 'adivinó bien', pero hasta esta celda no cumplía la regla §3.14 —")
    print("   se empieza a citar ahora, con run_id, no antes.")
    print("\n   NOTA sobre el control (91,01%): es el mismo G6 bruto ya congelado en")
    print("   `hechos-verificados.md` §6 (`run_id=86a9f215712a`). Si `_con_clave_bis` y")
    print("   `_enlazadas_bis` de esta celda no coinciden con 16.901.188 y 15.381.353,")
    print("   detener la lectura: algo cambió en `df_oc_linked` entre esa corrida y ésta.")
    print(f"\nCAP-3-BIS termina. RUN_ID_CAP={RUN_ID_CAP}")

# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-11-D · Enriquecer la muestra de 30 casos: código de licitación, código de OC,
# MAGIC # concordancia ONU lic↔OC y precio real de línea
# MAGIC **Nueva, sesión 31 (31-ago-2026).** Correr en el MISMO kernel, DESPUÉS de CAP-10 y CAP-11
# MAGIC (más arriba en este notebook) — reutiliza sus variables (`_oc`, `_txt`, `_t_p`, `_gen`,
# MAGIC `c_onu`, `c_anio`, `c_tipo`, `_monto`, `N_MUESTRA`, `_muestra`). Requiere además
# MAGIC `df_lic_all` cargado (Celdas 1-3).

# COMMAND ----------

# =============================================================================
# QUÉ CORRIGE. `cap11_muestra_validez_onu.csv` (la salida de CAP-11, más arriba)
# es ciego por doctrina: sin organismo, unidad, proveedor, RUT — Y sin
# `codigo_licitacion` ni `codigo_oc`. Correcto para el tamiz léxico/semántico
# (el "juez" no debe saber quién puso el código), pero significa que la
# reponderación posterior (`local/reponderar_muestra.py` -> resultado
# `cap11c_muestra_reponderada.csv`) tampoco los tiene — y ese es el archivo que
# el protocolo de los 30 casos (`docs/00-PLAN-19-DIAS-Y-FICHAS-DE-TAREA.md`,
# sección "F10") usa para construir la tabla final. Verificado el 31-ago-2026
# (sesión 31), inspeccionando el CSV real: sin código de licitación, código de
# OC, o el conjunto de códigos ONU de la licitación de origen, esa tabla no se
# puede construir, y las columnas `n_lineas`/`n_tuplas`/`clp` que SÍ trae son
# totales del ESTRATO (para la reponderación Horvitz-Thompson), no de la línea
# muestreada — tampoco sirven para el grupo de "extremos de dispersión de
# precio" del protocolo.
#
# QUÉ HACE ESTA CELDA. Reconstruye EXACTAMENTE la misma tabla evaluable y el
# mismo muestreo estratificado de CAP-11 (mismo `_f_eval`, mismo `_estrato`,
# MISMA fórmula de `_id` — no cambia: sigue siendo sha2(onu8|texto_comprador|
# anio|clp), así que agregar columnas "pasajeras" no puede alterar qué 300
# líneas salen elegidas), pero además de las columnas de siempre carga:
#   - `codigo_licitacion` y `codigo_oc` de la propia línea de OC.
#   - el conjunto de códigos ONU (8 díg.) que declaró la licitación de origen
#     (mismo criterio que CAP-1 v12 en la Celda de la Pregunta 1: `set8_lic`),
#     para saber si el código que declaró la OC SÍ estaba o NO estaba en su
#     licitación de origen.
#   - cuántas líneas y cuántos códigos ONU distintos declaró esa misma
#     licitación (para poder marcar "testimonial": >=2 líneas, <=1 ONU).
#   - el CLP REAL de la línea muestreada (ya vivía en memoria en `_ev`/
#     `_muestra` de CAP-11; el CSV que CAP-11 exporta lo descarta por doctrina
#     — aquí SÍ se exporta, porque este archivo no es el que ve el tamiz).
#
# VERIFICACIÓN OBLIGATORIA (regla §3.14). Antes de exportar nada, esta celda
# compara el conjunto de `_id` que resulta AQUÍ contra el de `_muestra` (CAP-11,
# calculada en este mismo run) — deben ser IDÉNTICOS, 300 de 300. Si no lo son,
# algo en el filtro, el orden o `N_MUESTRA` cambió entre CAP-11 y esta celda —
# NO usar el resultado hasta resolverlo. Un segundo cruce, fuera de Databricks
# (contra los `_id` que ya están en `cap11c_muestra_reponderada.csv`, calculado
# en una corrida anterior), se hace después con
# `local/enriquecer_muestra_30_casos.py`.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-11-D · Enriquecer la muestra para los 30 casos · RUN_ID_G7={RUN_ID_G7}")
print("=" * 78)

_faltan_d = [n for n in ("_oc", "_txt", "_t_p", "_gen", "c_onu", "c_anio", "c_tipo",
                         "_monto", "N_MUESTRA", "_muestra")
             if n not in dir()]
_lic_d = cap_global("df_lic_all")
if _faltan_d or _lic_d is None:
    _faltan_msg = _faltan_d + (["df_lic_all"] if _lic_d is None else [])
    print(f"ABORTA: falta(n) en el kernel: {_faltan_msg}.")
    print("Correr CAP-10 y CAP-11 (más arriba en ESTE notebook) y tener df_lic_all cargado.")
else:
    # ---- 1) las claves de licitación/OC, resueltas igual que en Celda 6/CAP-1 --
    c_klic_d = cap_resolver(_oc, ["codigo_licitacion"], "código licitación (oc)")
    c_koc_d  = cap_resolver(_oc, ["codigo_oc"], "código OC")
    c_klic_l = cap_resolver(_lic_d, ["codigo_externo", "codigo_licitacion"], "clave licitación (lic)")
    c_onu_l_d = cap_resolver(_lic_d, ["onu_lic"], "ONU (lic)")
    c_item_l_d = cap_resolver(_lic_d, ["_sinclasificar_Codigoitem", "codigoitem"], "id de línea (lic)")

    if not all([c_klic_d, c_koc_d, c_klic_l, c_onu_l_d]):
        print(f"ABORTA: no se resolvieron columnas mínimas. "
              f"oc: klic={c_klic_d} koc={c_koc_d} | lic: klic={c_klic_l} onu={c_onu_l_d}")
    else:
        # ---- 2) misma _ev de CAP-11, más las columnas pasajeras -----------------
        _ev_d = (_oc.select(
            F.col(c_onu).cast("string").alias("onu8"),
            _txt.alias("texto_comprador"),
            _t_p.alias("texto_proveedor"),
            _gen.alias("producto_generico"),
            (F.col(c_anio).cast("int") if c_anio else F.lit(-1)).alias("anio"),
            (F.col(c_tipo).cast("string") if c_tipo else F.lit(None)).alias("tipo_oc"),
            F.coalesce(_monto, F.lit(0.0)).alias("clp"),
            F.trim(F.col(c_klic_d).cast("string")).alias("codigo_licitacion"),
            F.trim(F.col(c_koc_d).cast("string")).alias("codigo_oc"),
        ).where(_f_eval).where(F.col("onu8").rlike(r"^\d{8}$")))

        _ev_d = (_ev_d.withColumn("rubro_n1", F.substring("onu8", 1, 2))
                     .withColumn("tramo", F.when(F.col("clp") < 1e6, "1_bajo")
                                          .when(F.col("clp") < 1e7, "2_medio")
                                          .otherwise("3_alto"))
                     .withColumn("_estrato", F.concat_ws("|", "rubro_n1", "tipo_oc", "tramo"))
                     # MISMA fórmula de _id que CAP-11 — no cambia con columnas nuevas.
                     .withColumn("_id", F.sha2(F.concat_ws(
                         "|", "onu8", "texto_comprador",
                         F.col("anio").cast("string"), F.col("clp").cast("string")), 256
                     ).substr(1, 16)))

        _n_estr_d = _ev_d.select("_estrato").distinct().count()
        _por_estrato_d = max(1, int(round(N_MUESTRA / max(_n_estr_d, 1))))
        _w_d = W.partitionBy("_estrato").orderBy(F.col("_id"))
        _muestra_d = (_ev_d.withColumn("_rk", F.row_number().over(_w_d))
                          .where(F.col("_rk") <= _por_estrato_d)
                          .drop("_rk")
                          .orderBy(F.col("_id"))
                          .limit(N_MUESTRA))

        # ---- 3) verificación INMEDIATA contra la _muestra de CAP-11 (este run) --
        _ids_cap11 = set(r["_id"] for r in _muestra.select("_id").collect())
        _ids_d = set(r["_id"] for r in _muestra_d.select("_id").collect())
        _solo_cap11 = _ids_cap11 - _ids_d
        _solo_d = _ids_d - _ids_cap11
        print(f"\n   _id en CAP-11    (este run) ......... {len(_ids_cap11):,}")
        print(f"   _id en CAP-11-D  (este run) ......... {len(_ids_d):,}")
        print(f"   presentes en ambos ................... {len(_ids_cap11 & _ids_d):,}")
        if _solo_cap11 or _solo_d:
            print(f"   ❌ DIFERENCIA: {len(_solo_cap11)} solo en CAP-11, {len(_solo_d)} solo en CAP-11-D.")
            print("   NO USAR el resultado de esta celda: la selección no es la misma. Revisar")
            print("   primero si `_f_eval` o `N_MUESTRA` cambiaron entre CAP-11 y esta celda.")
        else:
            print("   ✅ IDÉNTICOS — agregar columnas no alteró la selección de 300 líneas.")
        print("\n   Pendiente fuera de Databricks: comparar el CSV que resulte de esta celda")
        print("   contra los `_id` de `cap11c_muestra_reponderada.csv` (ya en disco, reponderado")
        print("   en una corrida anterior) con `local/enriquecer_muestra_30_casos.py`.")

        # ---- 4) concordancia ONU lic<->OC + conteos de la licitación de origen --
        _lic_agg_d = (
            _lic_d.select(
                F.trim(F.col(c_klic_l).cast("string")).alias("codigo_licitacion"),
                cap_onu8(F.col(c_onu_l_d)).alias("onu8_lic"),
                (F.col(c_item_l_d).cast("string") if c_item_l_d
                 else F.lit(None).cast("string")).alias("item_lic"),
            ).where(F.col("codigo_licitacion").isNotNull() & (F.col("codigo_licitacion") != ""))
             .groupBy("codigo_licitacion")
             .agg(F.array_compact(F.collect_set("onu8_lic")).alias("set8_lic"),
                  (F.countDistinct("item_lic") if c_item_l_d
                   else F.count(F.lit(1))).alias("n_lineas_lic"))
             .withColumn("n_onu_distintos_lic", F.size(F.col("set8_lic")))
        )

        _enriquecida = (
            _muestra_d.join(_lic_agg_d, "codigo_licitacion", "left")
                      .withColumn("onu_declarado_en_licitacion",
                                  F.when(F.col("set8_lic").isNull(), F.lit(None))
                                   .otherwise(F.array_contains(F.col("set8_lic"), F.col("onu8"))))
        )

        _resumen_d = _enriquecida.agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(F.col("codigo_licitacion").isNotNull().cast("int")).alias("con_codigo_licitacion"),
            F.sum(F.col("set8_lic").isNotNull().cast("int")).alias("con_licitacion_encontrada"),
            F.sum(F.when(F.col("onu_declarado_en_licitacion") == True, 1).otherwise(0)  # noqa: E712
                  ).alias("onu_si_estaba"),
            F.sum(F.when(F.col("onu_declarado_en_licitacion") == False, 1).otherwise(0)  # noqa: E712
                  ).alias("onu_no_estaba"),
        ).collect()[0]
        print("\n" + "-" * 78)
        print("CAP-11-D · resumen de concordancia ONU lic<->OC sobre la muestra de 300")
        print("-" * 78)
        print(f"   con codigo_licitacion no vacío ............ {_resumen_d['con_codigo_licitacion'] or 0:,}")
        print(f"   con licitación de origen encontrada ....... {_resumen_d['con_licitacion_encontrada'] or 0:,}")
        print(f"   ONU declarado SÍ estaba en la licitación .. {_resumen_d['onu_si_estaba'] or 0:,}")
        print(f"   ONU declarado NO estaba ..................... {_resumen_d['onu_no_estaba'] or 0:,}")
        print("   (el resto: sin codigo_licitacion, o con código pero licitación de origen no")
        print("    encontrada — el mismo fenómeno de enlace incompleto que mide G6, aplicado")
        print("    a esta muestra de 300. Si sale chico, avisa que los grupos 1-2 del protocolo")
        print("    F10 pueden no llenarse a 10+10 y hay que decirlo, no forzarlo.)")

        # ---- 5) exportar -------------------------------------------------------
        _out_d = _enriquecida.select(
            "_id", "onu8", "codigo_licitacion", "codigo_oc",
            "onu_declarado_en_licitacion", "n_onu_distintos_lic", "n_lineas_lic",
            "clp", "tipo_oc", "anio", "tramo", "rubro_n1", "producto_generico", "_estrato",
        ).toPandas()

        _dest_d = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}"
        try:
            dbutils.fs.mkdirs(_dest_d.replace("/Volumes", "dbfs:/Volumes"))  # noqa: F821
        except Exception:
            pass
        try:
            _out_d.to_csv(f"{_dest_d}/cap11d_enriquecido_30_casos.csv", index=False)
            print(f"\n   exportada a: {_dest_d}/cap11d_enriquecido_30_casos.csv")
            print("   ESTE archivo SÍ lleva codigo_licitacion y codigo_oc — fuera de la doctrina")
            print("   ciega de CAP-11 A PROPÓSITO: es insumo para construir la tabla de los 30")
            print("   casos, NO la muestra que ve el tamiz léxico/semántico. No reemplaza a")
            print("   `cap11_muestra_validez_onu.csv` para ese propósito.")
        except Exception as _e:
            print(f"\n   [no se pudo escribir: {type(_e).__name__}] — bajarla del display de abajo")
            try:
                display(_enriquecida)  # noqa: F821
            except Exception:
                pass

        print(f"\nCAP-11-D termina. RUN_ID_G7={RUN_ID_G7}")
        print("SIGUIENTE: bajar este CSV a `resultados/cap11d_enriquecido_30_casos.csv` y correr")
        print("`local/enriquecer_muestra_30_casos.py` para cruzarlo contra")
        print("`cap11c_muestra_reponderada.csv` y armar los 4 grupos del protocolo F10.")
