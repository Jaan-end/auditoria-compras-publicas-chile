# -*- coding: utf-8 -*-
"""
preparar_corte_v2.py — Capstone Big Data · Mercado Publico
===========================================================
Convierte la RE-DESCARGA 2023-2026 de ChileCompra a parquet, con el MISMO
esquema, los MISMOS nombres de columna y los MISMOS tipos que el corte viejo
que alimento RUN_ID_CAP=1314c4f6d481.

CORRE EN TU PC. Cero Databricks, cero cupo. Requiere pandas y pyarrow.

    pip install pandas pyarrow

-----------------------------------------------------------------------------
POR QUE EXISTE
-----------------------------------------------------------------------------
El notebook maestro une los diez anios con `unionByName`. Si el corte v2 trae
una columna con otro nombre, o la misma columna con otro tipo, pasa una de dos
cosas y las dos son peores que un error:

  · la union mete NULL en silencio para toda la mitad nueva de la serie, o
  · un `cap_resolver()` engancha una columna que no es la que crees.

Eso ya paso una vez en este proyecto (Celda 4 de v17, F19 hallazgo 1) y costo
semanas de cifras citadas sobre codigo que no era. Este script NO adivina: lee
el esquema real del corte viejo desde `esquema_v1.json` y aborta si no lo puede
reproducir exactamente.

-----------------------------------------------------------------------------
COMO SE USA — SIEMPRE EN DOS PASADAS
-----------------------------------------------------------------------------
1) DIAGNOSTICO (por defecto, no escribe nada; lee solo cabeceras). Rapido.

   python preparar_corte_v2.py --nuevo "D:/descarga_2026" \
                               --esquema-v1 esquema_v1.json \
                               --tipo lic

   Leer la salida entera. Si hay columnas de v1 sin equivalente, o sobrantes,
   se decide QUE hacer con cada una ANTES de escribir. No hay default silencioso.

2) ESCRITURA (solo despues de leer el diagnostico):

   python preparar_corte_v2.py --nuevo "D:/descarga_2026" \
                               --esquema-v1 esquema_v1.json \
                               --tipo lic --anios 2023 2024 2025 2026 \
                               --salida "D:/corte_v2" \
                               --escribir --rellenar-faltantes --descartar-sobrantes

Salida (lista para subir al Volumen tal cual):

   D:/corte_v2/categoria=lic_historico_v2/year=2023/parte-00000.parquet
   D:/corte_v2/categoria=lic_historico_v2/year=2024/parte-00000.parquet
   D:/corte_v2/categoria=lic_historico_v2/year=2025/parte-00000.parquet
   D:/corte_v2/categoria=lic_vigente_v2/year=2026/parte-00000.parquet
   D:/corte_v2/manifiesto_v2_lic.json
   D:/corte_v2/REFERENCIA_FILAS_V2_lic.txt   <- bloque para pegar en la Celda 2

UN SOLO archivo parquet por particion, a proposito: la prueba de humo del
notebook falla si una particion tiene mas de uno (duplicacion de run_id).

-----------------------------------------------------------------------------
LA DECISION DE PARTICION, QUE NO ES INOCENTE
-----------------------------------------------------------------------------
`_year` en el notebook NO es un ano del registro: es el nombre de la carpeta
`year=NNNN` de la que se leyo el archivo, o sea una decision tuya al construir
el parquet. Por eso F19 §2.5 midio 0,5982% de lineas donde `_year` != ano de
`fecha_creacion`.

Para que el corte v2 sea COMPARABLE con el viejo, hay que asignar la particion
con el MISMO criterio con que se asigno la primera vez: por el ano que viene en
el NOMBRE DEL ARCHIVO de origen (`--particion archivo`, default). La opcion
`--particion fecha` existe para medir, no para producir: el script imprime
siempre la tabla cruzada de las dos asignaciones, que es la version v2 de la
auditoria `_year`. Si cambias el criterio, cambias la serie por ano y ya no
sabes si el movimiento es del dato o de la carpeta.
"""

import argparse
import csv
import io
import json
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import datetime

try:
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.exit("Falta pandas o pyarrow.  pip install pandas pyarrow")

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

CHUNK = 200_000  # filas por trozo; bajalo a 50_000 si el PC tiene poca RAM
SNIFF = 8 << 20  # bytes que se miran para adivinar encoding y separador (8 MB)

# Mapeo categoria del corte viejo -> categoria v2. Se conserva la particion
# historico/vigente EXACTAMENTE como estaba, para que la comparacion sea limpia.
CATEGORIA = {
    ("lic", "historico"): "lic_historico_v2",
    ("lic", "vigente"):   "lic_vigente_v2",
    ("oc",  "historico"): "oc_historico_v2",
    ("oc",  "vigente"):   "oc_vigente_v2",
}
CAT_V1 = {"lic": {"historico": "lic_historico", "vigente": "lic_vigente"},
          "oc":  {"historico": "oc_historico",  "vigente": "oc_vigente"}}

# 2026 es la unica particion `vigente` en el corte viejo. Se respeta.
ANIOS_VIGENTE = {2026}

# Columnas por las que se reconoce si un archivo es de licitacion o de OC.
CLAVE_LIC = ["codigoexterno", "sinclasificarcodigoexterno"]
CLAVE_OC = ["codigo", "sinclasificarcodigo", "nrolicitacion", "sinclasificarnrolicitacion",
            "codigooc", "sinclasificarcodigooc"]

# Columnas candidatas para la asignacion por fecha (solo diagnostico).
FECHA_PARTICION = ["_sinclasificar_FechaPublicacion", "FechaPublicacion",
                   "_sinclasificar_FechaCreacion", "FechaCreacion",
                   "_sinclasificar_FechaEnvio", "FechaEnvio"]


# =============================================================================
# UTILIDADES
# =============================================================================

def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def decodificar(raw):
    """Adivina el encoding mirando los primeros SNIFF bytes.

    SALVEDAD que hay que conocer: si esos 8 MB no traen ningun byte acentuado,
    un archivo cp1252 se detecta como utf-8. Por eso la lectura posterior va con
    `encoding_errors="replace"`: un byte raro a mitad de un archivo de 500 MB
    degrada un caracter en vez de abortar un trabajo de 40 minutos. Si ves
    caracteres de reemplazo en el texto de una OC, el encoding se adivino mal y
    hay que forzarlo."""
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace"), "latin-1/replace"


def detectar_sep(linea):
    if linea.count(";") >= linea.count(",") and linea.count(";") > 0:
        return ";"
    if linea.count(",") > 0:
        return ","
    if linea.count("\t") > 0:
        return "\t"
    return ";"


def anio_mes_de_ruta(ruta):
    """Devuelve (anio, mes, como) o (None, None, motivo).

    La descarga no siempre tiene la misma forma, y la asignacion a la particion
    `year=NNNN` depende de acertarle. Dos estructuras vistas en este proyecto:

        A) <Categoria>/2023-01/lic_2023-1.csv      <- 2020-2023 (manifiestos)
        B) <Categoria>/2023/lic_2023-1.csv         <- 2025 viejo y la re-descarga

    Se prueban en orden y se DECLARA cual gano, archivo por archivo. Ninguna
    heuristica corre en silencio: si el ano de la carpeta y el del nombre no
    coinciden, se avisa y se usa el de la CARPETA, que es el que definio la
    particion del corte viejo (`_year` = carpeta, F19 §2.5).
    """
    norm_ruta = str(ruta).replace("\\", "/")
    partes = [x for x in norm_ruta.split("/") if x]

    # --- ano: componente de ruta que sea exactamente `20NN` o `20NN-MM` ------
    anio_carpeta = mes_carpeta = None
    for parte in partes[:-1]:                      # el ultimo es el archivo
        m = re.fullmatch(r"(20[0-9]{2})", parte)
        if m:
            anio_carpeta = int(m.group(1))
            mes_carpeta = None
        m = re.fullmatch(r"(20[0-9]{2})[-_](0?[1-9]|1[0-2])", parte)
        if m:
            anio_carpeta = int(m.group(1))
            mes_carpeta = int(m.group(2))

    # --- ano y mes desde el nombre del archivo ------------------------------
    base = os.path.basename(norm_ruta)
    anio_nombre = mes_nombre = None
    m = re.search(r"(20[0-9]{2})[-_](0?[1-9]|1[0-2])(?!\d)", base)
    if m:
        anio_nombre, mes_nombre = int(m.group(1)), int(m.group(2))
    else:
        m = re.search(r"(20[0-9]{2})(?!\d)", base)
        if m:
            anio_nombre = int(m.group(1))
        # ultimo recurso: archivo numerado suelto dentro de la carpeta del ano
        # (`1.csv`, `lic_1.csv`) — solo vale si el ano vino de la carpeta
        if mes_nombre is None and anio_carpeta is not None:
            m = re.fullmatch(r"(?:lic[-_]?)?(0?[1-9]|1[0-2])", os.path.splitext(base)[0])
            if m:
                mes_nombre = int(m.group(1))

    anio = anio_carpeta if anio_carpeta is not None else anio_nombre
    mes = mes_carpeta if mes_carpeta is not None else mes_nombre

    if anio is None:
        return None, None, "sin ano ni en la carpeta ni en el nombre"
    aviso = ""
    if (anio_carpeta is not None and anio_nombre is not None
            and anio_carpeta != anio_nombre):
        aviso = (" *** DISCREPANCIA: carpeta dice %d, nombre dice %d; se usa la "
                 "CARPETA" % (anio_carpeta, anio_nombre))
    if mes_carpeta is not None:
        como = "carpeta YYYY-MM"
    elif anio_carpeta is not None and mes_nombre is not None:
        como = "carpeta YYYY + mes del nombre"
    elif anio_carpeta is not None:
        como = "carpeta YYYY (sin mes)"
    else:
        como = "nombre del archivo"
    return anio, mes, como + aviso


def anio_de_nombre(ruta):
    """Extrae el ano del NOMBRE del archivo (o del zip que lo contiene).
    Toma el ultimo 20xx que aparezca; si hay varios, gana el ultimo, que en
    los nombres de ChileCompra es el del periodo (p.ej. `2025-3.csv`)."""
    cands = re.findall(r"(20[0-9]{2})", ruta.replace("\\", "/"))
    return int(cands[-1]) if cands else None


def descubrir(raiz):
    """[(etiqueta, ruta_fisica, interno_o_None)] para cada csv suelto o en zip."""
    out = []
    rutas = []
    if os.path.isfile(raiz):
        rutas = [raiz]
    else:
        for dp, _dn, fns in os.walk(raiz):
            for fn in fns:
                if fn.lower().endswith((".csv", ".zip", ".txt")):
                    rutas.append(os.path.join(dp, fn))
    for r in sorted(rutas):
        if r.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(r) as z:
                    for n in z.namelist():
                        if n.lower().endswith((".csv", ".txt")):
                            out.append((f"{os.path.basename(r)}::{n}", r, n))
            except (zipfile.BadZipFile, OSError) as e:
                print(f"   [aviso] zip ilegible {r}: {e}")
        else:
            out.append((os.path.basename(r), r, None))
    return out


def leer_bytes(ruta, interno):
    if interno is None:
        with open(ruta, "rb") as fh:
            return fh.read()
    with zipfile.ZipFile(ruta) as z:
        return z.read(interno)


def cabecera_de(ruta, interno):
    """Lee SOLO el primer bloque del archivo: un csv de 500 MB no se decodifica
    entero para mirarle la cabecera."""
    if interno is None:
        with open(ruta, "rb") as fh:
            raw = fh.read(SNIFF)
    else:
        with zipfile.ZipFile(ruta) as z, z.open(interno) as src:
            raw = src.read(SNIFF)
    txt, enc = decodificar(raw)
    prim = txt.split("\n", 1)[0]
    sep = detectar_sep(prim)
    cab = next(csv.reader(io.StringIO(prim), delimiter=sep))
    cab = [c.strip().lstrip("\ufeff") for c in cab]
    return cab, enc, sep, None


def _fuente_legible(ruta, interno, enc):
    """Devuelve (ruta_para_pandas, temporal_a_borrar_o_None). Los .csv sueltos se
    leen en su sitio; los que vienen dentro de un .zip se extraen a un temporal.
    Nunca se sostiene el archivo entero como texto en RAM."""
    if interno is None:
        return ruta, None
    import tempfile
    with zipfile.ZipFile(ruta) as z, \
            tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as th:
        with z.open(interno) as src:
            while True:
                buf = src.read(1 << 22)
                if not buf:
                    break
                th.write(buf)
        return th.name, th.name


def tipo_de_archivo(cab):
    n = {norm(c) for c in cab}
    if n & set(CLAVE_LIC):
        return "lic"
    if n & set(CLAVE_OC):
        return "oc"
    return None


# =============================================================================
# TIPOS: del simpleString de Spark al dtype de pandas y al tipo de pyarrow
# =============================================================================

def a_pyarrow(tipo_spark):
    t = (tipo_spark or "string").lower()
    if t.startswith("decimal"):
        return pa.float64()
    return {
        "string": pa.string(), "boolean": pa.bool_(),
        "tinyint": pa.int32(), "smallint": pa.int32(),
        "int": pa.int32(), "integer": pa.int32(), "bigint": pa.int64(),
        "float": pa.float64(), "double": pa.float64(),
        "date": pa.date32(), "timestamp": pa.timestamp("us"),
    }.get(t, pa.string())


FORMATOS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
            "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y"]


def convertir(serie, tipo_spark):
    """Convierte una Series de texto al tipo de v1. Devuelve (serie, n_fallos).
    Los valores que NO convierten quedan NULL pero se CUENTAN y se imprimen al
    final: un cast que falla en silencio es exactamente como se pierde media
    serie sin enterarse."""
    fallos = 0
    t = (tipo_spark or "string").lower()
    s = serie.astype("string")
    s = s.where(~s.isin(["", "NA", "N/A", "NULL", "None", "S/I", "-"]), other=pd.NA)
    if t == "string":
        return s, 0
    if t == "boolean":
        m = s.str.strip().str.lower().map(
            {"true": True, "false": False, "1": True, "0": False,
             "si": True, "no": False, "t": True, "f": False})
        fallos += int((s.notna() & m.isna()).sum())
        return m.astype("boolean"), fallos
    if t in ("date", "timestamp"):
        out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        pend = s.notna()
        for f in FORMATOS:
            if not pend.any():
                break
            try:
                cand = pd.to_datetime(s[pend], format=f, errors="coerce")
            except (ValueError, TypeError):
                continue
            ok = cand.notna()
            out.loc[cand.index[ok]] = cand[ok]
            pend.loc[cand.index[ok]] = False
        fallos += int(pend.sum())
        return out, fallos
    # numericos: tolera el formato chileno 1.234,56 y el ingles 1,234.56
    lim = (s.str.replace(r"\s", "", regex=True)
            .str.replace(r"\.(?=\d{3}(\D|$))", "", regex=True)
            .str.replace(",", ".", regex=False))
    num = pd.to_numeric(lim, errors="coerce")
    fallos += int((s.notna() & num.isna()).sum())
    if t in ("tinyint", "smallint", "int", "integer"):
        return num.round().astype("Int32"), fallos
    if t == "bigint":
        return num.round().astype("Int64"), fallos
    return num.astype("float64"), fallos


# =============================================================================
# DIAGNOSTICO DE ESQUEMA
# =============================================================================

def cargar_esquema_comun(ruta):
    """Importa TU esquema_comun.py. No se reimplementa el mapeo aqui: el propio
    archivo dice 'un solo lugar para el mapeo evita que dos scripts se
    desincronicen', y ese es exactamente el riesgo de esta operacion."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("esquema_comun", ruta)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for req in ("ESQUEMA_LIC", "ESQUEMA_OC", "resolver_alias"):
        if not hasattr(mod, req):
            sys.exit(f"{ruta} no define {req}. ¿Es el esquema_comun.py del proyecto?")
    return mod


def mapa_de_capa(cols_raw, tipo, EC):
    """Reproduce la regla de nombres de `construir_capa_esquema_comun.py`:

      · una columna cruda que calza un alias de ESQUEMA_LIC/ESQUEMA_OC toma el
        NOMBRE CANONICO (codigo_externo, onu_lic, numero_oferentes, ...);
      · cualquier otra viaja con el prefijo `_sinclasificar_` + su nombre crudo
        (_sinclasificar_MontoEstimado, _sinclasificar_RutProveedor, ...).

    Se comprueba contra la salida real de CAP-14 (`run_id=4b4680c66397`), que
    imprime justo esa mezcla: `onu_lic`/`numero_oferentes`/`codigo_licitacion`
    sin prefijo y `_sinclasificar_MontoEstimado`/`_sinclasificar_sector` con el.

    Devuelve {nombre_en_la_capa: columna_cruda}.
    """
    esquema = EC.ESQUEMA_LIC if tipo == "lic" else EC.ESQUEMA_OC
    mapa, usados = {}, set()
    for canon, aliases in esquema.items():
        real = EC.resolver_alias(cols_raw, aliases)
        if real is not None and real not in usados:
            mapa[canon] = real
            usados.add(real)
    for c in cols_raw:
        if c not in usados:
            mapa["_sinclasificar_" + str(c)] = c
    return mapa


def detectar_prefijo(cols_v1):
    """El corte viejo lleva columnas con prefijo `_sinclasificar_`. Ese prefijo NO
    viene del CSV de ChileCompra: se lo puso la ingesta original. Al reconstruir
    hay que volver a ponerlo, o `unionByName` deja en NULL toda la mitad nueva de
    la serie sin decir nada.

    Se detecta en vez de hardcodearse, y se IMPRIME: si el corte viejo no lo
    tuviera, el script no debe inventarlo."""
    cand = Counter()
    for c in cols_v1:
        m = re.match(r"^(_[a-z]+_)", c)
        if m:
            cand[m.group(1)] += 1
    if not cand:
        return None, 0
    pref, n = cand.most_common(1)[0]
    return (pref, n) if n >= max(3, 0.25 * len(cols_v1)) else (None, 0)


def diagnostico_por_capa(cols_v1, cab_nueva, tipo, EC):
    """Empareja las columnas de v1 contra el CSV nuevo PASANDO POR la capa de
    esquema comun, que es como se construyo v1. Devuelve
    (mapa v1->col_cruda, faltantes, sobrantes, n_canonicas)."""
    capa = mapa_de_capa(cab_nueva, tipo, EC)          # nombre_capa -> col_cruda
    idx_capa = {norm(k): (k, v) for k, v in capa.items()}
    mapa, faltan = {}, []
    for c1 in cols_v1:
        hit = idx_capa.get(norm(c1))
        if hit is None:
            faltan.append(c1)
        else:
            mapa[c1] = hit[1]
    usados = {norm(v) for v in mapa.values()}
    sobran = [c for c in cab_nueva if norm(c) not in usados]
    n_canon = sum(1 for k in capa if not k.startswith("_sinclasificar_"))
    return mapa, faltan, sobran, n_canon


def diagnostico_esquema(cols_v1, cab_nueva, prefijo=None):
    """Devuelve (mapa v1->nueva, faltantes, sobrantes, n_por_prefijo).

    Dos pasadas, en este orden y sin una tercera:
      1. nombre normalizado identico (mayusculas/guiones/underscores no cuentan);
      2. nombre de v1 SIN el prefijo de ingesta contra el nombre nuevo.
    No hay emparejamiento por coincidencia parcial: eso es como la Celda 4 de v17
    engancho una columna que no era."""
    idx_nueva = {}
    for c in cab_nueva:
        idx_nueva.setdefault(norm(c), c)
    mapa, faltan, por_prefijo = {}, [], 0
    for c1 in cols_v1:
        real = idx_nueva.get(norm(c1))
        if real is None and prefijo and c1.startswith(prefijo):
            real = idx_nueva.get(norm(c1[len(prefijo):]))
            if real is not None:
                por_prefijo += 1
        if real is None:
            faltan.append(c1)
        else:
            mapa[c1] = real
    usados = {norm(v) for v in mapa.values()}
    sobran = [c for c in cab_nueva if norm(c) not in usados]
    return mapa, faltan, sobran, por_prefijo


# =============================================================================
# MAIN
# =============================================================================

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nuevo", required=True, help="carpeta de la re-descarga 2023-2026")
    p.add_argument("--esquema-v1", required=True,
                   help="esquema_v1.json producido por exportar_esquema_v1.py")
    p.add_argument("--tipo", choices=["lic", "oc"], required=True)
    p.add_argument("--anios", nargs="*", type=int, default=[2023, 2024, 2025, 2026])
    p.add_argument("--salida", default="corte_v2")
    p.add_argument("--encoding", default="latin-1",
                   help="encoding de lectura. DEFAULT latin-1 porque los "
                        "manifiestos del build original (run_id 649df6092fb4 y "
                        "6de961bbf738) declaran latin-1 en los 96 archivos. "
                        "Pasar 'auto' para volver a adivinarlo por archivo.")
    p.add_argument("--sep", default=";",
                   help="separador. DEFAULT ';' por la misma razon. 'auto' para adivinar.")
    p.add_argument("--esquema-comun", default="esquema_comun.py",
                   help="ruta a TU esquema_comun.py (el del proyecto). Define el "
                        "mapeo alias->nombre canonico; sin el, la reconstruccion "
                        "no reproduce los nombres de columna de v1.")
    p.add_argument("--particion", choices=["archivo", "fecha"], default="archivo",
                   help="criterio de asignacion a year=NNNN. 'archivo' reproduce "
                        "el criterio del corte viejo. NO cambiar sin declararlo.")
    p.add_argument("--escribir", action="store_true",
                   help="sin esto solo diagnostica; no escribe ningun parquet")
    p.add_argument("--rellenar-faltantes", action="store_true",
                   help="columnas de v1 que no existen en el CSV nuevo se crean NULL")
    p.add_argument("--descartar-sobrantes", action="store_true",
                   help="columnas del CSV nuevo que no existen en v1 se descartan")
    a = p.parse_args(argv)

    print("=" * 78)
    print("PREPARAR CORTE v2 — %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("modo: %s | particion por: %s | tipo: %s"
          % ("ESCRITURA" if a.escribir else "DIAGNOSTICO (no escribe)",
             a.particion, a.tipo))
    print("encoding: %s | separador: %r" % (a.encoding, a.sep))
    if a.encoding != "latin-1":
        print("   *** OJO: el build original leyo TODO en latin-1 (96/96 archivos en")
        print("   *** los manifiestos 649df6092fb4 y 6de961bbf738). Decodificar con")
        print("   *** otro encoding cambia caracteres del TEXTO, y G7 mide texto:")
        print("   *** la comparacion v1 vs v2 quedaria contaminada por la decodificacion.")
    print("=" * 78)

    if not os.path.exists(a.esquema_comun):
        sys.exit(f"No encuentro {a.esquema_comun!r}. Es el esquema_comun.py del "
                 "proyecto; sin el no se pueden reproducir los nombres de v1.")
    EC = cargar_esquema_comun(a.esquema_comun)
    print(f"\nesquema_comun.py: {len(EC.ESQUEMA_LIC)} campos canonicos de licitacion, "
          f"{len(EC.ESQUEMA_OC)} de OC")
    for nom, tabla in (("LIC", getattr(EC, "DRIFT_CONOCIDO_LIC", {})),
                       ("OC", getattr(EC, "DRIFT_CONOCIDO_OC", {}))):
        for campo, info in (tabla or {}).items():
            print(f"   drift conocido {nom}: {campo} ausente en "
                  f"{info.get('ausente_en')} — si aparece en FALTANTES abajo, es "
                  "esto y no un error del build")

    with open(a.esquema_v1, encoding="utf-8") as fh:
        esq = json.load(fh)
    ref_v1 = esq.get("_referencia_filas_v1", {})

    cat_hist = CAT_V1[a.tipo]["historico"]
    cat_vig = CAT_V1[a.tipo]["vigente"]
    if cat_hist not in esq:
        sys.exit(f"esquema_v1.json no trae la categoria {cat_hist!r}. "
                 f"Categorias presentes: {[k for k in esq if not k.startswith('_')]}")
    cols_v1 = [c["nombre"] for c in esq[cat_hist]["columnas"]]
    tipos_v1 = {c["nombre"]: c["tipo"] for c in esq[cat_hist]["columnas"]}
    print(f"\nEsquema de referencia: categoria={cat_hist}  ({len(cols_v1)} columnas)")

    # --- inventario ----------------------------------------------------------
    archivos = descubrir(a.nuevo)
    if not archivos:
        sys.exit(f"No se encontro ningun csv/zip bajo {a.nuevo!r}")
    print(f"\nINVENTARIO: {len(archivos)} archivo(s) bajo {a.nuevo!r}")

    seleccion = []   # (etiqueta, ruta, interno, anio, cab, enc, sep)
    por_anio_desc = Counter()
    meses_vistos = defaultdict(set)
    traza = []       # (etiqueta, anio, mes, como se resolvio)
    sin_anio, otro_tipo = [], []
    for etiqueta, ruta, interno in archivos:
        try:
            cab, enc, sep, _ = cabecera_de(ruta, interno)
        except Exception as e:                                  # noqa: BLE001
            print(f"   [aviso] ilegible {etiqueta}: {type(e).__name__}")
            continue
        t = tipo_de_archivo(cab)
        if t != a.tipo:
            otro_tipo.append((etiqueta, t))
            continue
        anio, mes, como = anio_mes_de_ruta(
            ruta if interno is None else f"{ruta}/{interno}")
        if anio is None:
            anio = anio_de_nombre(etiqueta) or anio_de_nombre(ruta)
            como = "ultimo 20NN de la ruta (fragil)"
        if anio is not None and mes is not None:
            meses_vistos[anio].add(mes)
        traza.append((etiqueta, anio, mes, como))
        if anio is None:
            sin_anio.append(etiqueta)
            continue
        por_anio_desc[anio] += 1
        if anio in a.anios:
            seleccion.append((etiqueta, ruta, interno, anio, cab, enc, sep))

    print(f"   archivos del tipo {a.tipo!r}: {sum(por_anio_desc.values())}")
    for y in sorted(por_anio_desc):
        marca = "  <- se procesa" if y in a.anios else "  (fuera de --anios)"
        print(f"      year={y}: {por_anio_desc[y]:>3} archivo(s){marca}")
    if sin_anio:
        print(f"   [PARAR] {len(sin_anio)} archivo(s) sin ano en el nombre: "
              f"{sin_anio[:5]}{' ...' if len(sin_anio) > 5 else ''}")
        print("           Con --particion archivo no se pueden asignar. Renombralos")
        print("           o usa --particion fecha DECLARANDOLO en el informe.")
    if otro_tipo:
        print(f"   {len(otro_tipo)} archivo(s) de otro tipo (se ignoran en esta pasada)")
    print("\n   ASIGNACION A PARTICION, ARCHIVO POR ARCHIVO")
    print("   (revisar esta tabla ANTES de escribir: un archivo mal asignado mete")
    print("    filas de un anio dentro de la particion de otro, y `_year` sale de la")
    print("    particion — F19 §2.5)")
    _modos = Counter(c.split(" ***")[0] for _e, _a, _m, c in traza)
    for modo, n in _modos.most_common():
        print(f"      {n:>4} archivo(s)  <-  {modo}")
    _disc = [t for t in traza if "DISCREPANCIA" in t[3]]
    for etiqueta, anio, mes, como in _disc:
        print(f"      {etiqueta[:52]:<52} year={anio} mes={mes}{como[como.index(' ***'):]}")
    _sinmes = [t for t in traza if t[1] in a.anios and t[2] is None]
    if _sinmes:
        print(f"      *** {len(_sinmes)} archivo(s) SIN MES resoluble — el control de meses")
        print("      *** de abajo no los cuenta. Ejemplos: "
              + ", ".join(t[0][:28] for t in _sinmes[:3]))
    if len(traza) <= 60:
        print("\n      detalle:")
        for etiqueta, anio, mes, como in traza:
            if anio in a.anios:
                print(f"        {etiqueta[:46]:<46} -> year={anio} "
                      f"mes={mes if mes else '-':<3} [{como}]")

    print("\n   CONTROL DE MESES (la descarga trae 12 por anio y categoria):")
    for y in sorted(set(a.anios) & set(por_anio_desc)):
        ms = sorted(meses_vistos.get(y, set()))
        faltan_m = [m for m in range(1, 13) if m not in ms]
        if not ms:
            print(f"      year={y}: no se pudo leer el mes de ningun archivo "
                  "(ni de la carpeta YYYY-MM ni del nombre). Control NO aplicado —")
            print("                 contar los archivos a mano antes de escribir.")
        elif faltan_m and y != max(a.anios):
            print(f"      year={y}: FALTAN los meses {faltan_m}  *** revisar antes de escribir")
        elif faltan_m:
            print(f"      year={y}: meses {ms} (anio en curso, faltan {faltan_m} — normal)")
        else:
            print(f"      year={y}: 12/12 meses OK")

    faltan_anios = [y for y in a.anios if por_anio_desc.get(y, 0) == 0]
    if faltan_anios:
        print(f"\n   *** ADVERTENCIA GRAVE: no hay NINGUN archivo para {faltan_anios}.")
        print("   *** La tabla V-2 no tiene fila 2026 y esto lo explicaria. Si vas a")
        print("   *** reemplazar esos anios, el corte v2 los dejaria VACIOS. Parar.")

    if not seleccion:
        sys.exit("\nNada que procesar con los --anios pedidos.")

    # --- diagnostico de esquema ---------------------------------------------
    cab_ref = seleccion[0][4]
    mapa, faltan, sobran, n_pp = diagnostico_por_capa(cols_v1, cab_ref, a.tipo, EC)
    print("\n" + "=" * 78)
    print("DIAGNOSTICO DE ESQUEMA  (contra %s)" % seleccion[0][0])
    print("=" * 78)
    print(f"   columnas de v1              : {len(cols_v1)}")
    print(f"   columnas del CSV nuevo      : {len(cab_ref)}")
    print(f"   emparejadas                 : {len(mapa)}")
    print(f"   de v1 SIN equivalente nuevo : {len(faltan)}")
    print(f"   nuevas SIN equivalente en v1: {len(sobran)}")
    print(f"   canonicas del esquema comun : {n_pp} "
          f"(el resto viaja como _sinclasificar_*)")

    renombradas = [(k, v) for k, v in mapa.items()
                   if k != v and k != "_sinclasificar_" + str(v)]
    if renombradas:
        print(f"\n   {len(renombradas)} columna(s) emparejadas por nombre NORMALIZADO "
              "(mayusculas/guiones distintos). Revisar una por una:")
        for k, v in renombradas[:25]:
            print(f"      v1 {k!r}  <-  nuevo {v!r}")
        if len(renombradas) > 25:
            print(f"      ... y {len(renombradas)-25} mas")
    if faltan:
        print(f"\n   FALTANTES (existen en v1, no en el CSV nuevo):")
        for c in faltan:
            print(f"      - {c}   [{tipos_v1.get(c)}]")
        print("      -> con --rellenar-faltantes se crean NULL. Si alguna de estas")
        print("         columnas la usa el notebook, la mitad nueva de la serie")
        print("         quedaria vacia EN SILENCIO. Revisar antes de aceptar.")
    if sobran:
        print(f"\n   SOBRANTES (existen en el CSV nuevo, no en v1): {len(sobran)}")
        for c in sobran[:20]:
            print(f"      + {c}")
        if len(sobran) > 20:
            print(f"      ... y {len(sobran)-20} mas")
        print("      -> con --descartar-sobrantes se descartan. Es lo correcto para")
        print("         que unionByName no cambie de forma, pero si aqui aparece algo")
        print("         valioso (p.ej. una columna de estado de licitacion que E2")
        print("         necesita), anotalo: es un hallazgo, no basura.")

    # cabeceras heterogeneas entre archivos
    firmas = defaultdict(list)
    for etiqueta, _r, _i, _y, cab, _e, _s in seleccion:
        firmas["|".join(sorted(norm(c) for c in cab))].append(etiqueta)
    if len(firmas) > 1:
        print(f"\n   *** {len(firmas)} CABECERAS DISTINTAS entre los archivos nuevos.")
        for i, (_f, ejs) in enumerate(firmas.items(), 1):
            print(f"       grupo {i}: {len(ejs)} archivo(s), p.ej. {ejs[0]}")
        print("   *** Cada grupo se alinea contra v1 por separado; las columnas que")
        print("   *** falten en un grupo quedan NULL solo para esos archivos. Mirar.")

    bloqueo = (faltan and not a.rellenar_faltantes) or (sobran and not a.descartar_sobrantes)
    if a.escribir and bloqueo:
        sys.exit("\nNO SE ESCRIBE NADA: hay faltantes o sobrantes sin decision explicita.\n"
                 "Volver a correr agregando --rellenar-faltantes y/o --descartar-sobrantes\n"
                 "DESPUES de leer la lista de arriba.")
    if not a.escribir:
        print("\n" + "=" * 78)
        print("MODO DIAGNOSTICO — no se escribio ningun archivo.")
        print("Si la lista de arriba te parece bien, repetir con --escribir")
        print("(y --rellenar-faltantes / --descartar-sobrantes segun corresponda).")
        print("=" * 78)
        return 0

    # --- escritura -----------------------------------------------------------
    os.makedirs(a.salida, exist_ok=True)
    esquema_pa = pa.schema([pa.field(c, a_pyarrow(tipos_v1[c])) for c in cols_v1])

    por_anio_arch = defaultdict(list)
    for item in seleccion:
        por_anio_arch[item[3]].append(item)

    manifiesto = {"generado": datetime.now().isoformat(timespec="seconds"),
                  "tipo": a.tipo, "particion": a.particion,
                  "origen": os.path.abspath(a.nuevo),
                  "esquema_v1": os.path.abspath(a.esquema_v1),
                  "n_columnas": len(cols_v1), "anios": {}}
    referencia_v2 = {}
    cruce_particion = Counter()   # (year_carpeta, year_fecha) -> n

    for anio in sorted(por_anio_arch):
        cat = CATEGORIA[(a.tipo, "vigente" if anio in ANIOS_VIGENTE else "historico")]
        destino = os.path.join(a.salida, f"categoria={cat}", f"year={anio}")
        os.makedirs(destino, exist_ok=True)
        ruta_pq = os.path.join(destino, "parte-00000.parquet")
        print("\n" + "-" * 78)
        print(f"year={anio}  ->  categoria={cat}")
        print(f"   destino: {ruta_pq}")

        writer = pq.ParquetWriter(ruta_pq, esquema_pa, compression="snappy")
        total = 0
        fallos_col = Counter()
        col_fecha_part = None
        try:
            for etiqueta, ruta, interno, _y, cab, enc, sep in por_anio_arch[anio]:
                if a.encoding != "auto":
                    enc = a.encoding
                if a.sep != "auto":
                    sep = a.sep
                mapa_i, faltan_i, _sobran_i, _npp = diagnostico_por_capa(cols_v1, cab, a.tipo, EC)
                if col_fecha_part is None:
                    for cand in FECHA_PARTICION:
                        real = {norm(c): c for c in cab}.get(norm(cand))
                        if real:
                            col_fecha_part = real
                            break
                fuente, tmp = _fuente_legible(ruta, interno, enc)
                try:
                    lector = pd.read_csv(
                        fuente, sep=sep, dtype=str, chunksize=CHUNK, quotechar='"',
                        encoding=enc, encoding_errors="replace",
                        on_bad_lines="warn",
                        keep_default_na=False, na_values=[""], low_memory=False)
                except TypeError:   # pandas < 1.3 no conoce on_bad_lines
                    lector = pd.read_csv(
                        fuente, sep=sep, dtype=str, chunksize=CHUNK, quotechar='"',
                        encoding=enc, error_bad_lines=False,
                        keep_default_na=False, na_values=[""], low_memory=False)
                n_arch = 0
                for trozo in lector:
                    trozo.columns = [str(c).strip().lstrip("\ufeff") for c in trozo.columns]
                    datos = {}
                    for c1 in cols_v1:
                        real = mapa_i.get(c1)
                        if real is not None and real in trozo.columns:
                            datos[c1], fal = convertir(trozo[real], tipos_v1[c1])
                            if fal:
                                fallos_col[c1] += fal
                        else:
                            datos[c1] = pd.Series([None] * len(trozo), index=trozo.index,
                                                  dtype="object")
                    if col_fecha_part and col_fecha_part in trozo.columns:
                        fp = pd.to_datetime(trozo[col_fecha_part].str.slice(0, 10),
                                            errors="coerce", format="%Y-%m-%d")
                        for yv, n in fp.dt.year.value_counts(dropna=False).items():
                            cruce_particion[(anio, None if pd.isna(yv) else int(yv))] += int(n)
                    tabla = pa.Table.from_pandas(pd.DataFrame(datos), schema=esquema_pa,
                                                 preserve_index=False)
                    writer.write_table(tabla)
                    n_arch += len(trozo)
                    total += len(trozo)
                print(f"      {etiqueta[:56]:<56} {n_arch:>12,} filas  [{enc}, '{sep}']")
                if tmp and os.path.exists(tmp):
                    os.remove(tmp)
                if faltan_i:
                    print(f"         ({len(faltan_i)} columna(s) de v1 ausentes en este "
                          "archivo -> NULL)")
        finally:
            writer.close()

        tam = os.path.getsize(ruta_pq)
        print(f"   TOTAL year={anio}: {total:,} filas · {tam/1e6:,.1f} MB · 1 archivo parquet")
        ref = ref_v1.get(str(anio), {}).get(CAT_V1[a.tipo]["vigente" if anio in ANIOS_VIGENTE
                                                           else "historico"])
        if ref:
            d = total - ref
            print(f"   corte viejo (REFERENCIA_FILAS): {ref:,} filas   delta = {d:+,} "
                  f"({100.0*d/ref:+.2f}%)")
            if d < 0:
                print("   *** EL CORTE NUEVO TIENE MENOS FILAS QUE EL VIEJO EN ESTE ANIO.")
                print("   *** Un corte posterior solo deberia agregar. Esto es un NO-GO")
                print("   *** hasta explicarlo. Ver criterio A4 del protocolo F23.")
        if fallos_col:
            print("   valores que NO convirtieron al tipo de v1 (quedaron NULL):")
            for c, n in fallos_col.most_common(12):
                print(f"      {c:<46} {n:>12,}")
            print("   ^ si esto no es cero en una columna que el notebook usa, mirar")
            print("     los valores crudos ANTES de subir el parquet.")

        manifiesto["anios"][str(anio)] = {
            "categoria": cat, "filas": total, "bytes": tam,
            "archivos_origen": [e for e, *_ in por_anio_arch[anio]],
            "filas_corte_viejo": ref,
            "conversiones_fallidas": dict(fallos_col),
        }
        referencia_v2[anio] = (cat, total)

    # --- auditoria de particion (la version v2 de F19 §2.5) ------------------
    if cruce_particion:
        print("\n" + "=" * 78)
        print("AUDITORIA DE PARTICION — year de la CARPETA vs. year de la FECHA")
        print("=" * 78)
        tot = sum(cruce_particion.values())
        desb = sum(n for (yc, yf), n in cruce_particion.items() if yf is not None and yc != yf)
        print(f"   filas con year(carpeta) != year(fecha): {desb:,} de {tot:,} "
              f"= {100.0*desb/tot if tot else 0:.4f}%")
        print("   (F19 §2.5 midio 0,5982% en el corte viejo. Si aqui sale muy distinto,")
        print("    la serie por ano del corte v2 NO es comparable con la del viejo y hay")
        print("    que decirlo antes de comparar cualquier cifra.)")
        for (yc, yf), n in sorted(cruce_particion.items(),
                                  key=lambda kv: (kv[0][0], -1 if kv[0][1] is None else kv[0][1])):
            if yf is not None and yc != yf:
                print(f"      carpeta {yc} -> fecha {yf}: {n:,}")

    # --- bloque para pegar en la Celda 2 -------------------------------------
    ruta_ref = os.path.join(a.salida, f"REFERENCIA_FILAS_V2_{a.tipo}.txt")
    with open(ruta_ref, "w", encoding="utf-8") as fh:
        fh.write("# Pegar en la Celda 2 del notebook (REFERENCIA_FILAS_V2).\n")
        fh.write("# Generado por preparar_corte_v2.py el %s\n"
                 % datetime.now().strftime("%Y-%m-%d %H:%M"))
        for anio in sorted(referencia_v2):
            cat, n = referencia_v2[anio]
            fh.write(f'    {anio}: {{"{cat}": {n}}},\n')
    ruta_man = os.path.join(a.salida, f"manifiesto_v2_{a.tipo}.json")
    with open(ruta_man, "w", encoding="utf-8") as fh:
        json.dump(manifiesto, fh, ensure_ascii=False, indent=2)

    print("\n" + "=" * 78)
    print(f"LISTO. parquet en {os.path.abspath(a.salida)}")
    print(f"   {ruta_ref}   <- pegar en la Celda 2")
    print(f"   {ruta_man}   <- manifiesto, va al repositorio como evidencia")
    print("\nSIGUIENTE: subir las carpetas categoria=*_v2 al Volumen, AL LADO de las")
    print("viejas (no encima), y correr CAP15_comparador_de_cortes.py.")
    print("NO tocar el notebook maestro hasta que CAP-15 pase el bloque A.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
