# -*- coding: utf-8 -*-
# =============================================================================
# PASO 0 · RECONCILIACION DE CORTES — go / no-go ANTES de construir nada
# Capstone Big Data · Mercado Publico 2017-2026 · sesion 25 (28-ago-2026)
# v2 · 28-ago: parche try_to_date. La corrida c234b182170b murio en el collect
#      con CANNOT_PARSE_TIMESTAMP: un literal '0' en una columna de fecha.
#
# CELDA DE DATABRICKS. Pegar como celda nueva al final del notebook maestro
# (v18) o en un notebook aparte apuntando al mismo Volumen.
#
# COSTO: UNA sola accion de Spark (un collect de una tabla agregada chica).
#        Todo lo demas es lectura de metadatos (schema), que no lanza job.
#
# -----------------------------------------------------------------------------
# QUE CONTESTA, Y POR QUE VA ANTES QUE TODO LO DEMAS
# -----------------------------------------------------------------------------
# La auditoria de vintage local (v2_censura_por_anio.csv) dice que el corte
# nuevo trae 100.532 licitaciones publicadas en 2025. El notebook maestro dice
# que el corte VIEJO tiene 4.812.550 FILAS de licitacion en la particion
# year=2025 — la particion mas grande de los diez anios. Esas dos cosas no se
# comparan directamente (filas != licitaciones), y hasta que se comparen NO SE
# SABE si el corte nuevo es un superconjunto del viejo o un producto distinto.
#
#   · Si el corte nuevo tiene MAS licitaciones 2025 que el viejo -> es lo que
#     esperabamos: trae adjudicaciones que antes no estaban. Seguir.
#   · Si tiene MENOS -> el corte nuevo NO es un superconjunto. Reemplazar
#     destruiria universo en vez de completarlo. PARAR, y averiguar que
#     descargaste antes de convertir un solo archivo a parquet.
#
# Ademas reconstruye algo que el proyecto no tiene y que la tabla V-2 supone mal:
# EL CALENDARIO DE DESCARGA, MES A MES.
#
# V-2 asume UNA fecha de corte para todo el universo. Gabriel confirmo el
# 28-ago que la descarga original no fue asi: fue MES A MES entre 2024 y 2025,
# bajando cada vez el mes que faltaba. Eso significa que cada archivo mensual
# tiene su propia fecha de corte y su propio grado de madurez, y que la censura
# no es un acantilado en una fecha sino una rampa que sigue el calendario de
# descarga — no la realidad del mercado.
#
# Se reconstruye asi, y es una MEDICION, no un supuesto: para cada mes de
# publicacion, la fecha de adjudicacion MAXIMA observada en el corte viejo es
# una cota inferior del dia en que se bajo ese mes. Un archivo no puede traer
# un registro posterior al dia de la descarga.
#
#   · Si la descarga fue un corte unico, max(fecha_adj) es CASI CONSTANTE en
#     todos los meses: todos se bajaron el mismo dia.
#   · Si fue rodante, max(fecha_adj) SUBE mes a mes, siguiendo la descarga.
#
# Esa sola tabla distingue empiricamente las dos historias, y da la fecha por
# mes que `auditoria_vintage.py` necesita para recalcular V-2 bien.
# =============================================================================

from pyspark.sql import functions as F
from functools import reduce
import uuid, datetime

RUN_ID_P0 = uuid.uuid4().hex[:12]
VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"

# Anios que la re-descarga va a reemplazar. 2017-2022 no se tocan.
ANIOS_EN_JUEGO = [2023, 2024, 2025, 2026]   # los que reemplaza la re-descarga

print("=" * 78)
print(f"PASO 0 · RECONCILIACION DE CORTES   RUN_ID_P0 = {RUN_ID_P0}")
print(f"timestamp = {datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')}")
print("=" * 78)


def _cats(anio):
    return ("lic_vigente", "oc_vigente") if anio == 2026 else ("lic_historico", "oc_historico")


# -----------------------------------------------------------------------------
# 0.A · RESOLUCION DE COLUMNAS (metadatos, sin accion de Spark)
# -----------------------------------------------------------------------------
def _norm(s):
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


CANDIDATOS = {
    "codigo_externo":     ["codigo_externo", "CodigoExterno", "_sinclasificar_CodigoExterno"],
    "fecha_publicacion":  ["_sinclasificar_FechaPublicacion", "FechaPublicacion", "fecha_publicacion"],
    "fecha_adjudicacion": ["_sinclasificar_FechaAdjudicacion", "FechaAdjudicacion", "fecha_adjudicacion"],
    "fecha_cierre":       ["_sinclasificar_FechaCierre", "FechaCierre", "fecha_cierre"],
    "rut_proveedor":      ["_sinclasificar_RutProveedor", "RutProveedor", "rut_proveedor"],
    "numero_oferentes":   ["numero_oferentes", "NumeroOferentes", "_sinclasificar_NumeroOferentes"],
    "estado":             ["_sinclasificar_Estado", "Estado", "estado", "_sinclasificar_CodigoEstado"],
}


def resolver(cols, clave):
    reales = {_norm(c): c for c in cols}
    for cand in CANDIDATOS.get(clave, []):
        if _norm(cand) in reales:
            return reales[_norm(cand)]
    return None


# El schema se lee de UNA particion; es metadato, no dispara job.
_esq_lic = spark.read.parquet(f"{VOLUMEN_BASE}/categoria=lic_historico/year=2024")  # noqa: F821
COLS_LIC = _esq_lic.columns
RES = {k: resolver(COLS_LIC, k) for k in CANDIDATOS}

print("\n0.A · COLUMNAS RESUELTAS EN EL CORTE VIEJO (lic_historico/year=2024)")
print(f"     total de columnas: {len(COLS_LIC)}")
for k, v in RES.items():
    print(f"     [{'OK ' if v else 'NO '}] {k:<20} -> {v}")

_faltan = [k for k in ("codigo_externo", "fecha_publicacion") if not RES[k]]
if _faltan:
    raise AssertionError(
        f"PARAR: no se resolvio {_faltan} en el esquema real. Mirar COLS_LIC a mano "
        "y agregar el nombre correcto a CANDIDATOS antes de seguir. No inventar un sustituto."
    )

C_COD = RES["codigo_externo"]
C_FPUB = RES["fecha_publicacion"]
C_FADJ = RES["fecha_adjudicacion"]
C_PROV = RES["rut_proveedor"]
C_OFER = RES["numero_oferentes"]


# -----------------------------------------------------------------------------
# PARCHE 28-AGO (corrida c234b182170b): to_date -> try_to_date
#
# La primera corrida murio en el collect con
#   [CANNOT_PARSE_TIMESTAMP] Text '0' could not be parsed at index 0
# Eso NO es un bug de logica: es el dato. Alguna columna de fecha del corte
# viejo trae el literal "0" (celda vacia exportada como cero por ChileCompra).
# to_date() en Databricks LANZA excepcion con texto invalido en vez de devolver
# NULL, y como esta dentro de un coalesce, la excepcion se propaga y mata el
# job entero antes de que el coalesce llegue a probar el segundo formato.
# F.coalesce NO protege: evalua argumentos, no atrapa errores.
#
# try_to_date() es la version que devuelve NULL en vez de tirar. Es lo que la
# propia Databricks sugiere en el mensaje de error, y es exactamente lo que
# esta funcion decia querer hacer ("NULL si no parsea; NO inventa").
#
# El "0" no se descarta en silencio: la fila queda con fpub NULL y cae en el
# grupo (aniom=NULL, mes=NULL) de la agregacion, que ahora se imprime en 0.B-bis.
# -----------------------------------------------------------------------------

# ¿Existe try_to_date en este runtime? La prueba es sobre un literal, sin leer
# ni un byte del Volumen: no consume cupo de escaneo.
try:
    spark.sql("SELECT try_to_date('2020-01-01', 'yyyy-MM-dd')").collect()  # noqa: F821
    _HAY_TRY = True
except Exception as _e:
    _HAY_TRY = False
    print(f"\n   AVISO: try_to_date no disponible ({type(_e).__name__}). Se cae a")
    print("   timeParserPolicy=LEGACY, donde to_date devuelve NULL en vez de tirar.")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")  # noqa: F821

_FORMATOS = ["yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd"]


def _fecha(col):
    """Cast tolerante a fecha. Devuelve NULL si no parsea; NO inventa.

    Prueba los formatos sobre el string completo y sobre sus primeros 10
    caracteres (para 'yyyy-MM-dd HH:mm:ss'). Devuelve el primero que parsea.
    """
    if _HAY_TRY:
        c = f"CAST(`{col}` AS STRING)"
        partes = []
        for f in _FORMATOS:
            partes.append(f"try_to_date({c}, '{f}')")
            partes.append(f"try_to_date(substring({c}, 1, 10), '{f}')")
        return F.expr("COALESCE(" + ", ".join(partes) + ")")

    c = F.col(col).cast("string")
    alts = []
    for f in _FORMATOS:
        alts.append(F.to_date(c, f))
        alts.append(F.to_date(F.substring(c, 1, 10), f))
    return F.coalesce(*alts)


# -----------------------------------------------------------------------------
# 0.B · UNA SOLA AGREGACION EN FORMATO LARGO -> UN SOLO collect()
# -----------------------------------------------------------------------------
ANIOS_CALENDARIO = [2022, 2023, 2024, 2025, 2026]   # ampliar si hace falta;
# V-2 da censura 0,00% hasta 2021, asi que la rampa empieza despues. Cada anio
# extra es mas escaneo, y el cupo diario es el recurso escaso.

frames = []
for anio in sorted(set(ANIOS_CALENDARIO) | set(ANIOS_EN_JUEGO)):
    cat_lic, _ = _cats(anio)
    df = spark.read.parquet(f"{VOLUMEN_BASE}/categoria={cat_lic}/year={anio}")  # noqa: F821
    frames.append(df.select(
        F.lit(anio).alias("part"),
        F.col(C_COD).cast("string").alias("cod"),
        _fecha(C_FPUB).alias("fpub"),
        (_fecha(C_FADJ) if C_FADJ else F.lit(None).cast("date")).alias("fadj"),
        (F.col(C_PROV).cast("string") if C_PROV else F.lit(None).cast("string")).alias("prov"),
        (F.col(C_OFER).cast("string") if C_OFER else F.lit(None).cast("string")).alias("ofer"),
    ))

viejo = (reduce(lambda a, b: a.unionByName(b), frames)
         .withColumn("aniom", F.year("fpub"))
         .withColumn("mes", F.month("fpub")))

agg = (viejo.groupBy("part", "aniom", "mes").agg(
    F.count(F.lit(1)).alias("filas"),
    F.countDistinct("cod").alias("licitaciones"),
    F.countDistinct(F.when(F.col("fadj").isNotNull(), F.col("cod"))).alias("lic_con_fadj"),
    F.countDistinct(F.when((F.col("prov").isNotNull()) & (F.trim(F.col("prov")) != ""),
                           F.col("cod"))).alias("lic_con_prov"),
    F.countDistinct(F.when((F.col("ofer").isNotNull()) & (F.trim(F.col("ofer")) != ""),
                           F.col("cod"))).alias("lic_con_ofer"),
    F.max("fpub").alias("max_fpub"),
    F.max("fadj").alias("max_fadj"),
))

# >>> LA UNICA ACCION DE SPARK DE TODA LA CELDA <<<
print("\n   Lanzando la unica accion de Spark de la celda...")
filas = agg.orderBy("part", "aniom", "mes").collect()
print(f"   ...lista. {len(filas)} celdas (particion x anio x mes) en el driver.")

# -----------------------------------------------------------------------------
# 0.B-bis · FILAS SIN FECHA DE PUBLICACION PARSEABLE  (sale del mismo collect)
#
# Aqui aterriza el "0" que hizo caer la corrida anterior. No cuesta una accion
# extra: es el grupo (aniom=NULL, mes=NULL) de la agregacion que ya se trajo.
# -----------------------------------------------------------------------------
_sin_fecha = [r for r in filas if r["aniom"] is None or r["mes"] is None]
_tot_filas = sum(r["filas"] for r in filas)
print("\n" + "=" * 78)
print("0.B-bis · FILAS CON fecha_publicacion NO PARSEABLE")
print("=" * 78)
if not _sin_fecha:
    print("  Ninguna. Todas las filas tienen fecha de publicacion legible.")
else:
    print(f"  {'particion':<12}{'filas':>14}{'licitaciones':>15}{'% de la part.':>15}")
    print("  " + "-" * 56)
    _por_part = {}
    for r in filas:
        _por_part[r["part"]] = _por_part.get(r["part"], 0) + r["filas"]
    _f_sf = _l_sf = 0
    for r in sorted(_sin_fecha, key=lambda x: x["part"]):
        _f_sf += r["filas"]
        _l_sf += r["licitaciones"]
        _den = _por_part.get(r["part"], 0)
        print(f"  {r['part']:<12}{r['filas']:>14,}{r['licitaciones']:>15,}"
              f"{(100.0 * r['filas'] / _den if _den else 0):>14.2f}%")
    print("  " + "-" * 56)
    print(f"  {'TOTAL':<12}{_f_sf:>14,}{_l_sf:>15,}"
          f"{(100.0 * _f_sf / _tot_filas if _tot_filas else 0):>14.2f}%")
    print("\n  LECTURA: estas filas NO entran en el calendario 0.C ni en el conteo")
    print("  0.D, porque no se sabe a que mes pertenecen. Si el porcentaje es")
    print("  decimas, es basura de exportacion y no cambia ningun veredicto. Si")
    print("  pasa de ~1% de una particion, el conteo 'viejo' de 0.D esta")
    print("  subestimado por ese lado y el delta contra el corte nuevo se lee")
    print("  como cota, no como cifra.")

# -----------------------------------------------------------------------------
# 0.C · CALENDARIO DE DESCARGA RECONSTRUIDO
# -----------------------------------------------------------------------------
print("\n" + "=" * 78)
print("0.C · CALENDARIO DE DESCARGA RECONSTRUIDO  (una fecha por mes, no una sola)")
print("=" * 78)
print("  `corte>=` es la fecha de adjudicacion maxima vista en ese mes: cota")
print("  INFERIOR del dia en que se bajo el archivo. `madurez` son los dias entre")
print("  el fin del mes publicado y esa fecha: cuanto tiempo tuvo el mes para")
print("  cerrar procesos ANTES de que lo congelaras.\n")
print(f"  {'mes pub':<10}{'licitac.':>11}{'c/f.adj':>10}{'% c/adj':>9}"
      f"{'corte >=':>13}{'madurez d':>11}")
print("  " + "-" * 66)

import datetime as _dt
calendario = []
for r in filas:
    if r["aniom"] is None or r["mes"] is None:
        continue
    if r["aniom"] < min(ANIOS_CALENDARIO):
        continue
    lic, adj = r["licitaciones"], r["lic_con_fadj"]
    fin_mes = _dt.date(r["aniom"] + (r["mes"] == 12), (r["mes"] % 12) + 1, 1) - _dt.timedelta(days=1)
    mad = (r["max_fadj"] - fin_mes).days if r["max_fadj"] else None
    print(f"  {r['aniom']}-{r['mes']:02d}   {lic:>11,}{adj:>10,}"
          f"{(100.0*adj/lic if lic else 0):>8.1f}%{str(r['max_fadj']):>13}"
          f"{(str(mad) if mad is not None else '-'):>11}")
    calendario.append((f"{r['aniom']}-{r['mes']:02d}", r["max_fadj"], mad, lic, adj))

mads = [m for _k, _f, m, _l, _a in calendario if m is not None]
if mads:
    print("\n  LECTURA (releer el veredicto contra la tabla, regla §1.16):")
    print(f"    madurez minima  = {min(mads):>6} dias")
    print(f"    madurez maxima  = {max(mads):>6} dias")
    if max(mads) - min(mads) > 180:
        print("    -> El rango es ANCHO: la descarga NO fue un corte unico. Confirma la")
        print("       descarga rodante mes a mes. Consecuencia directa: la censura de")
        print("       cada mes depende de CUANDO lo bajaste, no de nada del mercado, y")
        print("       cualquier serie 'por anio' compara anios con madurez distinta.")
        print("       La tabla V-2 (una sola fecha) queda SUPERADA por esta.")
    else:
        print("    -> El rango es ESTRECHO: compatible con un corte unico. En ese caso la")
        print("       tabla V-2 recalculada con la fecha de arriba si sirve.")
    print(f"\n    Rezago mediano publicacion->adjudicacion (V-2): 23-35 dias, p90 68-91.")
    print(f"    Todo mes con madurez por debajo del p90 de su anio esta CENSURADO de")
    print(f"    forma material, no marginal.")

# -----------------------------------------------------------------------------
# 0.D · VIEJO vs NUEVO — ¿es el corte nuevo un SUPERCONJUNTO?
# -----------------------------------------------------------------------------
viejo_lic = {}
for r in filas:
    if r["part"] in ANIOS_EN_JUEGO and r["aniom"] == r["part"]:
        viejo_lic[r["part"]] = viejo_lic.get(r["part"], 0) + r["licitaciones"]

NUEVO_LIC = {2023: 149_584, 2024: 154_142, 2025: 100_532, 2026: None}

print("\n" + "=" * 78)
print("0.D · VIEJO vs NUEVO — ¿es el corte nuevo un SUPERCONJUNTO?")
print("=" * 78)
print("  (viejo = licitaciones con fecha de publicacion del MISMO anio que su")
print("   particion; nuevo = columna `licitaciones` de v2_censura_por_anio.csv)\n")
print(f"  {'anio':<6}{'lic viejo':>12}{'lic nuevo':>12}{'delta':>12}{'delta %':>10}   veredicto")
print("  " + "-" * 74)
bloqueo = []
for a in ANIOS_EN_JUEGO:
    v, n = viejo_lic.get(a), NUEVO_LIC.get(a)
    if not v or n is None:
        print(f"  {a:<6}{(v or 0):>12,}{'(s/d)':>12}{'':>12}{'':>10}   sin dato comparable")
        continue
    d = n - v
    ver = "OK  agrega" if d >= 0 else "BLOQUEA  el nuevo PIERDE licitaciones"
    if d < 0:
        bloqueo.append(a)
    print(f"  {a:<6}{v:>12,}{n:>12,}{d:>+12,}{100.0*d/v:>+9.2f}%   {ver}")

print("\n  " + "-" * 74)
if bloqueo:
    print(f"  VEREDICTO: NO-GO. En {bloqueo} el corte nuevo tiene MENOS licitaciones")
    print("  que el viejo. Reemplazar destruiria universo. Antes de convertir un solo")
    print("  archivo a parquet, revisar QUE se descargo — y en particular si se bajo")
    print("  la carpeta Historico donde antes habia Vigente, o al reves:")
    print("  `esquema_comun.py` documenta que son CUATRO carpetas distintas de")
    print("  ChileCompra (Licitaciones_Nacional_Historico / _Vigente y las dos de OC),")
    print("  no una sola. Un proceso abierto vive en Vigente y se muda a Historico al")
    print("  cerrar: bajar solo Historico de un anio reciente deja fuera justo los")
    print("  procesos largos. Esa, y no la censura de adjudicacion, es la explicacion")
    print("  mas simple de los cuatro codigos de la Celda 6D-3.")
else:
    print("  VEREDICTO: GO por conteo. Seguir con preparar_corte_v2.py.")
    print("  Superconjunto por CONTEO no es superconjunto por CONTENIDO: eso lo")
    print("  prueba CAP-15, que es el que detecta licitaciones que desaparecen.")

print(f"\nPASO 0 termina. RUN_ID_P0={RUN_ID_P0}")
print("Pegar esta salida completa en el chat antes de construir nada.")
