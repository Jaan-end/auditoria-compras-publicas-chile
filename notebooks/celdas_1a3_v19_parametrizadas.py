# -*- coding: utf-8 -*-
# =============================================================================
# CELDAS 1-3 · v19 — PARAMETRIZADAS POR CORTE
# Capstone Big Data · Mercado Publico · sesion 25 (28-ago-2026)
#
# Reemplazan tal cual a las Celdas 1, 2 y 3 de `etapa2_databricks_v18_COMPLETO.py`.
# NINGUNA otra celda del notebook cambia: de la Celda 4 en adelante todo sigue
# viendo `df_lic_all` / `df_oc_all` con el mismo esquema y los mismos nombres.
#
# QUE APORTAN
#   1. `CORTE_ACTIVO` elige el substrato de datos. Revertir es cambiar UN string
#      y volver a correr — no hay que restaurar ningun archivo, porque el corte
#      v1 nunca se toco. Ese es todo el diseño del rollback.
#   2. La prueba de humo (Celda 2) sigue siendo bloqueante, con su propia tabla
#      de referencia por corte. Sin eso, el primer error de conteo del corte v2
#      pasaria inadvertido hasta la Celda 9.
#   3. Cada corrida estampa `CORTE_ACTIVO` en su `RUN_ID`, en el resumen y en la
#      salida. Un `run_id` sin corte declarado no se puede citar: es la regla
#      §1.21 aplicada al nuevo grado de libertad que estamos introduciendo.
#
# ADVERTENCIA QUE NO SE PUEDE SALTAR
#   Con dos cortes conviviendo, "la cifra" deja de estar definida por el
#   `run_id` solo. TODA cifra nueva se cita como `run_id + corte`. Las cifras
#   congeladas (1314c4f6d481, 70680deaec66, fb7c3b125bf7, 966ab5c4d538) son
#   todas CORTE_ACTIVO='v1' por definicion, aunque se generaran antes de que
#   esta variable existiera.
# =============================================================================

import uuid, datetime, json
from pyspark.sql import functions as F
from functools import reduce

# ---------------------------------------------------------------------------
# AJUSTAR AQUI — lo unico que se toca para cambiar o revertir el corte
# ---------------------------------------------------------------------------
CORTE_ACTIVO = "v1"      # "v1" | "v2" | "mixto"
#   v1     : todo el corte congelado. Reproduce 1314c4f6d481. ES EL DEFAULT.
#   mixto  : 2017-2022 de v1 + 2023-2026 de v2. Es lo que corresponde a una
#            re-descarga que solo cubre 2023-2026.
#   v2     : todo del corte nuevo. Solo tiene sentido si algun dia re-descargas
#            los diez anios; con la re-descarga parcial dejaria 2017-2022 vacio.

VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"
ANIO_SMOKE = 2018
ANIOS_COMPLETOS = list(range(2017, 2027))
ANIOS_REDESCARGA = [2023, 2024, 2025, 2026]     # los que v2 puede servir
VOLUMEN_SALIDA = f"{VOLUMEN_BASE}/etapa2_output"
# ---------------------------------------------------------------------------

RUN_ID = uuid.uuid4().hex[:12]
TIMESTAMP = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")

if CORTE_ACTIVO not in ("v1", "v2", "mixto"):
    raise ValueError(f"CORTE_ACTIVO={CORTE_ACTIVO!r} no es v1/v2/mixto")


def corte_de(anio):
    """Que corte sirve este anio, segun CORTE_ACTIVO."""
    if CORTE_ACTIVO == "v1":
        return "v1"
    if CORTE_ACTIVO == "v2":
        return "v2"
    return "v2" if anio in ANIOS_REDESCARGA else "v1"


def _cats(anio):
    """(cat_lic, cat_oc) FISICAS, con sufijo _v2 donde corresponda."""
    suf = "" if corte_de(anio) == "v1" else "_v2"
    if anio == 2026:
        return f"lic_vigente{suf}", f"oc_vigente{suf}"
    return f"lic_historico{suf}", f"oc_historico{suf}"


def _cats_logicas(anio):
    """(cat_lic, cat_oc) SIN sufijo. Es lo que se estampa en `_categoria_lic` /
    `_categoria_oc`, para que el valor de esa columna signifique lo mismo en los
    dos cortes y ninguna celda de aguas abajo cambie de comportamiento."""
    return ("lic_vigente", "oc_vigente") if anio == 2026 else ("lic_historico", "oc_historico")


print(f"run_id       = {RUN_ID}")
print(f"CORTE_ACTIVO = {CORTE_ACTIVO}")
print(f"timestamp    = {TIMESTAMP}")
print(f"VOLUMEN_BASE = {VOLUMEN_BASE}")
print("\ncorte efectivo por anio:")
print("   " + "  ".join(f"{a}:{corte_de(a)}" for a in ANIOS_COMPLETOS))
if CORTE_ACTIVO != "v1":
    print("\n*** ESTA CORRIDA NO ES COMPARABLE DIRECTAMENTE CON LAS CIFRAS CONGELADAS.")
    print("*** Toda cifra que salga de aqui se cita como `run_id + corte`, nunca sola.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 2 — Prueba de humo (v19: una tabla de referencia por corte)
# MAGIC **Si el conteo no calza, DETENER aqui y no avanzar.**

# COMMAND ----------

# Conteos [MEDIDO] del corte v1. NO TOCAR: son la huella del substrato que
# genero 1314c4f6d481.
REFERENCIA_FILAS_V1 = {
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

# PEGAR AQUI la salida de REFERENCIA_FILAS_V2_lic.txt y REFERENCIA_FILAS_V2_oc.txt
# que produce `preparar_corte_v2.py`. Mientras este vacio, la prueba de humo del
# corte v2 se declara NO VERIFICABLE y avisa — no se salta en silencio.
REFERENCIA_FILAS_V2 = {
    # 2023: {"lic_historico_v2": ..., "oc_historico_v2": ...},
    # 2024: {"lic_historico_v2": ..., "oc_historico_v2": ...},
    # 2025: {"lic_historico_v2": ..., "oc_historico_v2": ...},
    # 2026: {"lic_vigente_v2":   ..., "oc_vigente_v2":   ...},
}


def referencia(anio, cat_fisica):
    tabla = REFERENCIA_FILAS_V2 if cat_fisica.endswith("_v2") else REFERENCIA_FILAS_V1
    return tabla.get(anio, {}).get(cat_fisica)


def leer_anio(anio):
    cat_lic, cat_oc = _cats(anio)
    return (spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_lic}/year={anio}"),   # noqa: F821
            spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_oc}/year={anio}"))    # noqa: F821


# La prueba de humo corre sobre ANIO_SMOKE y, si el corte activo trae datos
# nuevos, tambien sobre un anio de la re-descarga: un smoke test que solo mira
# 2018 no prueba nada del material que acabas de subir.
ANIOS_SMOKE = [ANIO_SMOKE]
if CORTE_ACTIVO != "v1":
    ANIOS_SMOKE.append(2024)   # anio de CONTROL: el que menos deberia moverse

print(f"\n{'='*66}")
print(f"PRUEBA DE HUMO — corte '{CORTE_ACTIVO}' — anios {ANIOS_SMOKE}")
print(f"{'='*66}")

fallos_smoke, sin_referencia = [], []
for anio in ANIOS_SMOKE:
    cat_lic, cat_oc = _cats(anio)
    df_l, df_o = leer_anio(anio)
    n_l, n_o = df_l.count(), df_o.count()
    for cat, n in ((cat_lic, n_l), (cat_oc, n_o)):
        ref = referencia(anio, cat)
        if ref is None:
            sin_referencia.append((anio, cat, n))
            print(f"  {anio} {cat:<20} Spark: {n:>12,}  |  ref: (sin referencia)  ??")
            continue
        ok = (n == ref)
        print(f"  {anio} {cat:<20} Spark: {n:>12,}  |  ref: {ref:>12,}  "
              f"{'OK' if ok else 'FALLA'}")
        if not ok:
            fallos_smoke.append((anio, cat, n, ref))
    print(f"       columnas lic: {len(df_l.columns)}   columnas oc: {len(df_o.columns)}")

if sin_referencia:
    print("\n  AVISO: hay particiones sin conteo de referencia.")
    print("  Pegar los conteos en REFERENCIA_FILAS_V2 (los emite preparar_corte_v2.py)")
    print("  ANTES de citar cualquier cifra de esta corrida. Sin referencia, un")
    print("  archivo parquet duplicado en una particion no se detecta.")
if fallos_smoke:
    raise AssertionError(
        "PRUEBA DE HUMO FALLO en " + str([(a, c) for a, c, _n, _r in fallos_smoke]) +
        "\\nPrimera hipotesis: la particion tiene mas de un archivo parquet "
        "(al subir al Volumen se dejo el anterior). Verificar que haya EXACTAMENTE "
        "uno por particion.\\nSegunda hipotesis: la subida quedo incompleta.\\n"
        "NO continuar con las celdas siguientes hasta resolver.")
print("\n  PRUEBA DE HUMO PASADA.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Celda 3 — Cargar universo completo 2017–2026 (v19)
# MAGIC Solo correr despues de pasar la prueba de humo.

# COMMAND ----------

frames_lic, frames_oc, procedencia = [], [], []
for anio in ANIOS_COMPLETOS:
    cat_lic, cat_oc = _cats(anio)
    log_lic, log_oc = _cats_logicas(anio)
    df_lic = (spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_lic}/year={anio}")  # noqa: F821
              .withColumn("_year", F.lit(anio))
              .withColumn("_categoria_lic", F.lit(log_lic))
              .withColumn("_corte", F.lit(corte_de(anio))))
    df_oc = (spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_oc}/year={anio}")    # noqa: F821
             .withColumn("_year", F.lit(anio))
             .withColumn("_categoria_oc", F.lit(log_oc))
             .withColumn("_corte", F.lit(corte_de(anio))))
    frames_lic.append(df_lic)
    frames_oc.append(df_oc)
    procedencia.append((anio, cat_lic, cat_oc, corte_de(anio)))

df_lic_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames_lic)
df_oc_all = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames_oc)

n_lic_total = df_lic_all.count()
n_oc_total = df_oc_all.count()

print(f"Total licitaciones 2017-2026: {n_lic_total:>14,}")
print(f"Total OC           2017-2026: {n_oc_total:>14,}")
print(f"\nPROCEDENCIA POR ANIO (va al informe como anexo de reproducibilidad):")
print(f"  {'anio':<6}{'corte':<8}{'categoria lic':<24}{'categoria oc':<24}")
for anio, cl, co, c in procedencia:
    print(f"  {anio:<6}{c:<8}{cl:<24}{co:<24}")

print(f"\nEsquema licitaciones ({len(df_lic_all.columns)} columnas):")
df_lic_all.printSchema()

# Sello de procedencia. Sin esto, dentro de tres dias no vas a saber que corte
# produjo que run_id — y es exactamente el problema que la regla §1.21 creo
# para no repetir.
_sello = {"run_id": RUN_ID, "timestamp": TIMESTAMP, "corte_activo": CORTE_ACTIVO,
          "n_lic": n_lic_total, "n_oc": n_oc_total,
          "procedencia": [{"anio": a, "corte": c, "cat_lic": cl, "cat_oc": co}
                          for a, cl, co, c in procedencia]}
try:
    with open(f"{VOLUMEN_SALIDA}/sello_corte_{RUN_ID}.json", "w", encoding="utf-8") as fh:
        json.dump(_sello, fh, ensure_ascii=False, indent=2)
    print(f"\nSello de procedencia: {VOLUMEN_SALIDA}/sello_corte_{RUN_ID}.json")
except Exception as e:                                              # noqa: BLE001
    print(f"\n[aviso] no se pudo escribir el sello ({type(e).__name__}). "
          f"Anotar a mano: run_id={RUN_ID} corte={CORTE_ACTIVO}")
