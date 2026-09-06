# -*- coding: utf-8 -*-
# =============================================================================
# CAP-15 · COMPARADOR DE CORTES  (v1 congelado  vs  v2 re-descarga)
# Capstone Big Data · Mercado Publico · sesion 25 (28-ago-2026)
#
# CELDA DE DATABRICKS. UNA SOLA ACCION DE SPARK en toda la celda.
# Correrla SOLA, sin el resto del notebook. No escribe nada al Volumen.
#
# -----------------------------------------------------------------------------
# QUE ES Y QUE NO ES
# -----------------------------------------------------------------------------
# ES el bloque V-3 de `auditoria_vintage.py` hecho contra el substrato REAL de
# RUN_ID_CAP=1314c4f6d481 — el parquet del Volumen — en vez de contra los CSV
# viejos, que ya no existen. Es el numero mas fuerte disponible del proyecto:
# no es una reconstruccion contrafactual, es el conteo directo de registros que
# cambiaron de contenido entre dos descargas del mismo periodo.
#
# NO ES una re-corrida del pipeline. Aqui NO se recalcula P1, ni P2, ni G6, ni
# G7. Esta celda contesta una sola pregunta: **¿el corte v2 es usable?**
# La respuesta a "¿que le pasa a las cifras?" viene despues, y es otra corrida.
#
# -----------------------------------------------------------------------------
# LOS CRITERIOS ESTAN PREDECLARADOS Y NO SE TOCAN DESPUES DE VER EL RESULTADO
# -----------------------------------------------------------------------------
# Fijados el 28-ago-2026, ANTES de la primera corrida, en F23 §4. Son criterios
# de CALIDAD DE DATO, no de resultado: ninguno mira P1 ni P2. Elegir el corte
# por la cifra que produce seria escoger el dato que da la respuesta que gusta,
# y en la defensa eso se llama por su nombre.
# =============================================================================

from pyspark.sql import functions as F
from functools import reduce
import uuid, datetime

RUN_ID_CAP15 = uuid.uuid4().hex[:12]
VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"
ANIOS = [2023, 2024, 2025, 2026]

# Anios de CONTROL: publicados y ya adjudicados mucho antes de cualquiera de las
# dos descargas. El corte nuevo casi no deberia moverlos. Si aqui hay movimiento
# grande, el problema es el build, no el vintage.
ANIOS_CONTROL = [2023, 2024]
# Anios de TRATAMIENTO: donde el vintage muerde.
ANIOS_TRATAMIENTO = [2025, 2026]

# --- criterios predeclarados (F23 §4) ---------------------------------------
A2_MAX_DESAPARECIDAS_PCT = 0.10   # % de lic de v1 ausentes en v2, anios control
A3_MAX_PERDIDA_CAMPO_PCT = 0.05   # % de comunes que PIERDEN un campo ya publicado
A4_TOLERANCIA_FILAS = 0           # filas_v2 >= filas_v1 en anios control
A5_MIN_GANANCIA_2025 = 1000       # lic de 2025 que ganan fecha de adjudicacion

print("=" * 78)
print(f"CAP-15 · COMPARADOR DE CORTES    RUN_ID_CAP15 = {RUN_ID_CAP15}")
print(f"timestamp = {datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}")
print("=" * 78)


def _cat(tipo, anio, corte):
    base = f"{tipo}_{'vigente' if anio == 2026 else 'historico'}"
    return base if corte == "v1" else base + "_v2"


def _ruta(tipo, anio, corte):
    return f"{VOLUMEN_BASE}/categoria={_cat(tipo, anio, corte)}/year={anio}"


# =============================================================================
# A1 · CONFORMIDAD DE ESQUEMA — metadatos, CERO acciones de Spark
# =============================================================================
print("\n" + "=" * 78)
print("A1 · CONFORMIDAD DE ESQUEMA  (criterio BLOQUEANTE)")
print("=" * 78)

a1_ok = True
esquemas = {}
for tipo in ("lic", "oc"):
    for anio in ANIOS:
        for corte in ("v1", "v2"):
            try:
                sch = spark.read.parquet(_ruta(tipo, anio, corte)).schema  # noqa: F821
                esquemas[(tipo, anio, corte)] = [(f.name, f.dataType.simpleString())
                                                 for f in sch.fields]
            except Exception as e:                                        # noqa: BLE001
                print(f"   [FALTA] {_ruta(tipo, anio, corte)}  ({type(e).__name__})")
                esquemas[(tipo, anio, corte)] = None
                a1_ok = False

for tipo in ("lic", "oc"):
    for anio in ANIOS:
        e1, e2 = esquemas.get((tipo, anio, "v1")), esquemas.get((tipo, anio, "v2"))
        if e1 is None or e2 is None:
            continue
        if e1 == e2:
            print(f"   [OK]   {tipo} year={anio}: {len(e1)} columnas, nombres/orden/tipos identicos")
            continue
        a1_ok = False
        n1, n2 = {n for n, _ in e1}, {n for n, _ in e2}
        print(f"   [FALLA] {tipo} year={anio}: esquemas DISTINTOS")
        if n1 - n2:
            print(f"           solo en v1 ({len(n1-n2)}): {sorted(n1-n2)[:10]}")
        if n2 - n1:
            print(f"           solo en v2 ({len(n2-n1)}): {sorted(n2-n1)[:10]}")
        t1, t2 = dict(e1), dict(e2)
        difs = [(n, t1[n], t2[n]) for n in sorted(n1 & n2) if t1[n] != t2[n]]
        for n, x, y in difs[:10]:
            print(f"           tipo distinto: {n}  v1={x}  v2={y}")
        if [n for n, _ in e1] != [n for n, _ in e2] and not (n1 ^ n2) and not difs:
            print("           mismas columnas y tipos, ORDEN distinto (no rompe "
                  "unionByName, pero anotarlo)")

if not a1_ok:
    print("\n   A1 FALLA -> PARAR. `unionByName` con esquemas distintos mete NULL en")
    print("   silencio en la mitad nueva de la serie. Volver a preparar_corte_v2.py.")
    raise AssertionError("CAP-15 A1: conformidad de esquema no alcanzada. No seguir.")
print("\n   A1 PASA. Los dos cortes tienen el mismo esquema.")


# =============================================================================
# RESOLUCION DE COLUMNAS (metadatos)
# =============================================================================
def _norm(s):
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def _res(cols, cands):
    reales = {_norm(c): c for c in cols}
    for c in cands:
        if _norm(c) in reales:
            return reales[_norm(c)]
    return None


COLS_LIC = [n for n, _ in esquemas[("lic", 2024, "v1")]]
COLS_OC = [n for n, _ in esquemas[("oc", 2024, "v1")]]

# Nombres canonicos PRIMERO: asi los deja la capa de esquema comun
# (ESQUEMA_LIC/ESQUEMA_OC de esquema_comun.py). El `_sinclasificar_*` es
# el fallback para las columnas que NO son canonicas.
L_COD = _res(COLS_LIC, ["codigo_externo", "CodigoExterno", "_sinclasificar_CodigoExterno"])
L_ADJ = _res(COLS_LIC, ["_sinclasificar_FechaAdjudicacion", "FechaAdjudicacion",
                        "fecha_adjudicacion"])
L_PRV = _res(COLS_LIC, ["_sinclasificar_RutProveedor", "RutProveedor", "rut_proveedor"])
L_OFE = _res(COLS_LIC, ["numero_oferentes", "NumeroOferentes",
                        "_sinclasificar_NumeroOferentes"])
O_COD = _res(COLS_OC, ["codigo_oc", "Codigo", "_sinclasificar_Codigo"])
O_LIC = _res(COLS_OC, ["codigo_licitacion", "CodigoLicitacion",
                       "_sinclasificar_NroLicitacion", "NroLicitacion"])

print("\n   columnas resueltas:")
for nom, val in [("lic.codigo", L_COD), ("lic.fecha_adj", L_ADJ), ("lic.rut_prov", L_PRV),
                 ("lic.n_oferentes", L_OFE), ("oc.codigo", O_COD), ("oc.nro_licitacion", O_LIC)]:
    print(f"     [{'OK ' if val else 'NO '}] {nom:<18} -> {val}")
if not L_COD:
    raise AssertionError("Sin columna de codigo de licitacion no hay diff posible. "
                         "Mirar COLS_LIC a mano; no forzar un sustituto.")


def _lleno(c):
    """1 si la columna trae algo utilizable, 0 si es NULL o cadena vacia."""
    if c is None:
        return F.lit(0)
    x = F.col(c).cast("string")
    return F.when(x.isNotNull() & (F.trim(x) != ""), 1).otherwise(0)


def tabla_lic(anio, corte):
    """Una fila por licitacion: si el archivo trae varias filas por linea y por
    oferta, `max` deja 1 si CUALQUIERA de ellas trae el campo. Es la misma
    deduplicacion que hace la clase Corte de auditoria_vintage.py."""
    df = spark.read.parquet(_ruta("lic", anio, corte))          # noqa: F821
    return (df.select(F.col(L_COD).cast("string").alias("cod"),
                      _lleno(L_ADJ).alias("adj"),
                      _lleno(L_PRV).alias("prv"),
                      _lleno(L_OFE).alias("ofe"))
              .groupBy("cod").agg(F.max("adj").alias("adj"),
                                  F.max("prv").alias("prv"),
                                  F.max("ofe").alias("ofe"),
                                  F.count(F.lit(1)).alias("filas")))


LARGO = ["bloque", "entidad", "anio", "metrica", "valor"]


def _fila(bloque, entidad, anio, aggs):
    return aggs.select(F.lit(bloque).alias("bloque"), F.lit(entidad).alias("entidad"),
                       F.lit(anio).cast("int").alias("anio"),
                       F.col("metrica"), F.col("valor").cast("bigint"))


piezas = []
for anio in ANIOS:
    v1, v2 = tabla_lic(anio, "v1"), tabla_lic(anio, "v2")
    j = (v1.alias("a").join(v2.alias("b"), on="cod", how="full_outer")
           .select(
               F.when(F.col("a.filas").isNull(), 1).otherwise(0).alias("solo_v2"),
               F.when(F.col("b.filas").isNull(), 1).otherwise(0).alias("solo_v1"),
               F.when(F.col("a.filas").isNotNull() & F.col("b.filas").isNotNull(), 1)
                .otherwise(0).alias("comun"),
               F.coalesce(F.col("a.adj"), F.lit(0)).alias("a_adj"),
               F.coalesce(F.col("b.adj"), F.lit(0)).alias("b_adj"),
               F.coalesce(F.col("a.prv"), F.lit(0)).alias("a_prv"),
               F.coalesce(F.col("b.prv"), F.lit(0)).alias("b_prv"),
               F.coalesce(F.col("a.ofe"), F.lit(0)).alias("a_ofe"),
               F.coalesce(F.col("b.ofe"), F.lit(0)).alias("b_ofe"),
               F.coalesce(F.col("a.filas"), F.lit(0)).alias("a_filas"),
               F.coalesce(F.col("b.filas"), F.lit(0)).alias("b_filas"),
           ))
    m = j.agg(
        F.sum("solo_v1").alias("lic_solo_v1"),
        F.sum("solo_v2").alias("lic_solo_v2"),
        F.sum("comun").alias("lic_comunes"),
        F.sum("a_filas").alias("filas_v1"),
        F.sum("b_filas").alias("filas_v2"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_adj=0 AND b_adj=1 THEN 1 ELSE 0 END")).alias("gano_fecha_adj"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_prv=0 AND b_prv=1 THEN 1 ELSE 0 END")).alias("gano_proveedor"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_ofe=0 AND b_ofe=1 THEN 1 ELSE 0 END")).alias("gano_oferentes"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_adj=1 AND b_adj=0 THEN 1 ELSE 0 END")).alias("perdio_fecha_adj"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_prv=1 AND b_prv=0 THEN 1 ELSE 0 END")).alias("perdio_proveedor"),
        F.sum(F.expr("CASE WHEN comun=1 AND a_ofe=1 AND b_ofe=0 THEN 1 ELSE 0 END")).alias("perdio_oferentes"),
    )
    cols = [c for c in m.columns]
    largo = reduce(lambda x, y: x.unionByName(y),
                   [m.select(F.lit(c).alias("metrica"), F.col(c).alias("valor")) for c in cols])
    piezas.append(_fila("DIFF", "lic", anio, largo))

    if O_COD:
        for corte in ("v1", "v2"):
            o = spark.read.parquet(_ruta("oc", anio, corte))     # noqa: F821
            mo = o.agg(F.count(F.lit(1)).alias(f"oc_filas_{corte}"),
                       F.countDistinct(F.col(O_COD).cast("string")).alias(f"oc_distintas_{corte}"),
                       F.sum(_lleno(O_LIC)).alias(f"oc_con_nrolic_{corte}"))
            largo_o = reduce(lambda x, y: x.unionByName(y),
                             [mo.select(F.lit(c).alias("metrica"), F.col(c).alias("valor"))
                              for c in mo.columns])
            piezas.append(_fila("OC", "oc", anio, largo_o))

tabla = reduce(lambda a, b: a.unionByName(b), piezas)

# >>>>>>>>>>>>>>>>>> LA UNICA ACCION DE SPARK DE LA CELDA <<<<<<<<<<<<<<<<<<<<<
print("\n   Lanzando la unica accion de Spark de la celda...")
res = tabla.collect()
print(f"   ...lista. {len(res)} metricas recibidas en el driver.")

M = {}
for r in res:
    M[(r["entidad"], r["anio"], r["metrica"])] = r["valor"] or 0


def g(ent, anio, met):
    return M.get((ent, anio, met), 0)


# =============================================================================
# A2-A5 · LA TABLA, Y RECIEN DESPUES EL VEREDICTO
# =============================================================================
print("\n" + "=" * 78)
print("15.A · DIFF DIRECTO DE LICITACIONES  (esta es la tabla que se lee primero)")
print("=" * 78)
print(f"  {'anio':<6}{'lic v1':>11}{'lic v2':>11}{'solo v1':>10}{'solo v2':>10}"
      f"{'comunes':>11}{'filas v1':>13}{'filas v2':>13}")
print("  " + "-" * 86)
for a in ANIOS:
    lv1 = g("lic", a, "lic_solo_v1") + g("lic", a, "lic_comunes")
    lv2 = g("lic", a, "lic_solo_v2") + g("lic", a, "lic_comunes")
    print(f"  {a:<6}{lv1:>11,}{lv2:>11,}{g('lic',a,'lic_solo_v1'):>10,}"
          f"{g('lic',a,'lic_solo_v2'):>10,}{g('lic',a,'lic_comunes'):>11,}"
          f"{g('lic',a,'filas_v1'):>13,}{g('lic',a,'filas_v2'):>13,}")

print("\n  DE LAS COMUNES: ¿cuantas GANARON dato que el corte viejo no tenia?")
print(f"  {'anio':<6}{'gano f.adj':>13}{'gano prov':>12}{'gano oferent':>14}"
      f"{'% comunes':>12}")
print("  " + "-" * 60)
for a in ANIOS:
    com = g("lic", a, "lic_comunes") or 1
    print(f"  {a:<6}{g('lic',a,'gano_fecha_adj'):>13,}{g('lic',a,'gano_proveedor'):>12,}"
          f"{g('lic',a,'gano_oferentes'):>14,}{100.0*g('lic',a,'gano_fecha_adj')/com:>11.2f}%")
print("\n  ^ ESTE ES EL NUMERO PARA EL INFORME. No es estimacion: es el conteo")
print("    directo de registros que cambiaron de contenido entre dos descargas")
print("    del mismo periodo. Sustituye a la reconstruccion contrafactual de V-2.")

print("\n  DE LAS COMUNES: ¿cuantas PERDIERON dato? (un dato publicado no se despublica)")
print(f"  {'anio':<6}{'perdio f.adj':>15}{'perdio prov':>14}{'perdio oferent':>16}")
print("  " + "-" * 60)
for a in ANIOS:
    print(f"  {a:<6}{g('lic',a,'perdio_fecha_adj'):>15,}{g('lic',a,'perdio_proveedor'):>14,}"
          f"{g('lic',a,'perdio_oferentes'):>16,}")

if O_COD:
    print("\n" + "=" * 78)
    print("15.B · ORDENES DE COMPRA — cobertura estructural")
    print("=" * 78)
    print(f"  {'anio':<6}{'filas v1':>14}{'filas v2':>14}{'OC dist v1':>13}{'OC dist v2':>13}"
          f"{'c/NroLic v1':>13}{'c/NroLic v2':>13}")
    print("  " + "-" * 86)
    for a in ANIOS:
        print(f"  {a:<6}{g('oc',a,'oc_filas_v1'):>14,}{g('oc',a,'oc_filas_v2'):>14,}"
              f"{g('oc',a,'oc_distintas_v1'):>13,}{g('oc',a,'oc_distintas_v2'):>13,}"
              f"{g('oc',a,'oc_con_nrolic_v1'):>13,}{g('oc',a,'oc_con_nrolic_v2'):>13,}")
    print("\n  ^ Este bloque es el canal por el que la Pregunta 2 se contaminaria:")
    print("    `evaluable_g7` se calcula sobre LINEAS DE OC, no sobre adjudicaciones.")
    print("    Si las filas de OC de 2025 crecen mucho, el +0,28 pp esta en juego.")

# =============================================================================
# 15.C · VEREDICTO CONTRA LOS CRITERIOS PREDECLARADOS
# =============================================================================
print("\n" + "=" * 78)
print("15.C · VEREDICTO contra los criterios PREDECLARADOS (F23 §4)")
print("=" * 78)
fallos = []


def chequeo(nombre, ok, detalle):
    print(f"    [{'OK  ' if ok else 'FALLA'}] {nombre} — {detalle}")
    if not ok:
        fallos.append(nombre)


for a in ANIOS_CONTROL:
    base = g("lic", a, "lic_solo_v1") + g("lic", a, "lic_comunes")
    pct = 100.0 * g("lic", a, "lic_solo_v1") / base if base else 0.0
    chequeo(f"A2 · no desaparicion {a}", pct <= A2_MAX_DESAPARECIDAS_PCT,
            f"{g('lic',a,'lic_solo_v1'):,} lic de v1 ausentes en v2 = {pct:.4f}% "
            f"(umbral {A2_MAX_DESAPARECIDAS_PCT}%)")

for a in ANIOS:
    com = g("lic", a, "lic_comunes")
    peor = max(g("lic", a, "perdio_fecha_adj"), g("lic", a, "perdio_proveedor"))
    pct = 100.0 * peor / com if com else 0.0
    chequeo(f"A3 · no perdida de campo {a}", pct <= A3_MAX_PERDIDA_CAMPO_PCT,
            f"peor caso {peor:,} de {com:,} comunes = {pct:.4f}% "
            f"(umbral {A3_MAX_PERDIDA_CAMPO_PCT}%)")

for a in ANIOS_CONTROL:
    d = g("lic", a, "filas_v2") - g("lic", a, "filas_v1")
    chequeo(f"A4 · monotonia de filas {a}", d >= A4_TOLERANCIA_FILAS,
            f"filas_v2 - filas_v1 = {d:+,}")

gan = g("lic", 2025, "gano_fecha_adj")
chequeo("A5 · la re-descarga aporta algo", gan >= A5_MIN_GANANCIA_2025,
        f"{gan:,} licitaciones de 2025 ganan fecha de adjudicacion "
        f"(umbral {A5_MIN_GANANCIA_2025:,})")

print("\n  " + "-" * 74)
if not fallos:
    print("  VEREDICTO: CORTE v2 USABLE. Pasa A1-A5.")
    print("  Siguiente paso: correr el notebook maestro con CORTE_ACTIVO='mixto' y")
    print("  comparar P1/P2/G6/G7 contra las cifras congeladas. Ese resultado NO")
    print("  decide cual corte se usa — eso ya quedo decidido aqui, con criterios de")
    print("  calidad de dato. El informe reporta LAS DOS series.")
else:
    print(f"  VEREDICTO: CORTE v2 NO USABLE. Falla {fallos}.")
    print("  NO se cambia el notebook. Revertir es no hacer nada: el corte v1 sigue")
    print("  intacto en su sitio y `1314c4f6d481` no se toco.")
    print("  El fallo se documenta: 'la re-descarga no cumplio el criterio X' es un")
    print("  hallazgo publicable, no un fracaso.")

print("\n  RECORDATORIO: esta celda NO recalculo P1, P2, G6 ni G7. Ninguna cifra")
print("  congelada cambio. Lo unico que se decidio aqui es si el corte v2 es")
print("  material valido para una corrida futura.")
print(f"\nCAP-15 termina. RUN_ID_CAP15={RUN_ID_CAP15}")
