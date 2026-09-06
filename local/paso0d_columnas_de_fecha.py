# -*- coding: utf-8 -*-
"""
paso0d_columnas_de_fecha.py — Capstone Big Data · Mercado Publico
==================================================================
CORRE EN TU PC. Cero Databricks, cero cupo.

QUE CONTESTA (F24 §3)
---------------------
En el corte viejo, la columna `_sinclasificar_FechaAdjudicacion` esta llena en
el 100,0% de los meses desde 2024-08, incluidas las 59 licitaciones publicadas
en los dias previos al corte, y tiene valores en 2036 y 2050. Con un rezago
mediano publicacion -> adjudicacion de 23-35 dias, eso es imposible para
adjudicaciones efectivas. La sospecha es que el campo es la fecha ESTIMADA de
adjudicacion que el comprador declara en las bases.

Este script lo decide sin gastar cupo, mirando los CSV de la re-descarga:

  1) lista TODAS las columnas de fecha que publica ChileCompra. Si aparecen
     dos (`FechaAdjudicacion` y `FechaEstimadaAdjudicacion`), el problema es
     que el corte viejo engancho la equivocada, y se arregla cambiando de
     columna;
  2) perfila cada una: cuantas vienen llenas, cuantas parsean, minimo, maximo,
     y **que porcentaje cae en el futuro**. Una columna de hechos no tiene
     futuro; una de intenciones, si;
  3) mide el rezago publicacion -> esa fecha. Si el rezago mediano de un mes
     recien cerrado es igual al de un mes viejo, la columna se llena al
     publicar y no cuando pasa algo.

USO
---
    # un mes asentado y un mes fresco, para comparar
    python paso0d_columnas_de_fecha.py --nuevo "D:\\...\\new" --archivo 2024-3
    python paso0d_columnas_de_fecha.py --nuevo "D:\\...\\new" --archivo 2026-7

    # ademas, si ya bajaste esquema_v1.json del Volumen:
    python paso0d_columnas_de_fecha.py --nuevo "D:\\...\\new" --archivo 2024-3 \\
                                       --esquema-v1 esquema_v1.json

Requiere pandas.
"""

import argparse
import io
import json
import os
import re
import sys
import zipfile

try:
    import pandas as pd
except ImportError:
    sys.exit("Falta pandas.  pip install pandas")

CHUNK = 200_000
SNIFF = 4 * 1024 * 1024
CAND_COD = ["codigo_externo", "CodigoExterno", "Codigo Externo", "CodigoLicitacion"]
CAND_FPUB = ["FechaPublicacion", "fecha_publicacion", "FechaCreacion", "fecha_creacion"]


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def resolver(cols, cands):
    reales = {norm(c): c for c in cols}
    for c in cands:
        if norm(c) in reales:
            return reales[norm(c)]
    return None


def descubrir(raiz):
    out, rutas = [], []
    if os.path.isfile(raiz):
        rutas = [raiz]
    else:
        for dp, _dn, fns in os.walk(raiz):
            for fn in fns:
                if fn.lower().endswith((".csv", ".zip", ".txt")):
                    rutas.append(os.path.join(dp, fn))
    for r in sorted(rutas):
        if r.lower().endswith(".zip"):
            with zipfile.ZipFile(r) as z:
                for n in z.namelist():
                    if n.lower().endswith((".csv", ".txt")):
                        out.append((f"{os.path.basename(r)}::{n}", r, n))
        else:
            out.append((os.path.basename(r), r, None))
    return out


def abrir(ruta, interno, enc):
    if interno is None:
        return open(ruta, "r", encoding=enc, errors="replace", newline="")
    with zipfile.ZipFile(ruta) as z:
        return io.StringIO(z.read(interno).decode(enc, errors="replace"))


def cabecera(ruta, interno, enc, sep):
    if interno is None:
        with open(ruta, "rb") as fh:
            raw = fh.read(SNIFF)
    else:
        with zipfile.ZipFile(ruta) as z:
            with z.open(interno) as fh:
                raw = fh.read(SNIFF)
    lineas = raw.decode(enc, errors="replace").splitlines()
    linea = lineas[0].lstrip("\ufeff") if lineas else ""
    if sep == "auto":
        sep = ";" if linea.count(";") >= linea.count(",") and linea.count(";") else ","
    return [c.strip().strip('"') for c in linea.split(sep)], sep


def a_fecha(serie):
    fp = pd.to_datetime(serie.astype(str).str.slice(0, 10), errors="coerce",
                        format="%Y-%m-%d")
    malas = fp.isna() & serie.astype(str).str.strip().ne("")
    if malas.any():
        fp.loc[malas] = pd.to_datetime(serie[malas].astype(str).str.slice(0, 10),
                                       errors="coerce", dayfirst=True)
    return fp


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nuevo", required=True)
    p.add_argument("--archivo", default=None,
                   help="trozo del nombre del archivo a perfilar, p.ej. 2024-3")
    p.add_argument("--tipo", choices=["lic", "oc"], default="lic")
    p.add_argument("--esquema-v1", default=None)
    p.add_argument("--encoding", default="latin-1")
    p.add_argument("--sep", default=";")
    a = p.parse_args(argv)

    hoy = pd.Timestamp.today().normalize()
    print("=" * 78)
    print("PASO 0-D · COLUMNAS DE FECHA — ¿hecho o intencion?")
    print(f"hoy = {hoy.date()}")
    print("=" * 78)

    # ------------------------------------------------------------------ (1a)
    if a.esquema_v1:
        print("\n1a · COLUMNAS DE FECHA EN EL CORTE VIEJO (esquema_v1.json)")
        try:
            with open(a.esquema_v1, encoding="utf-8") as fh:
                esq = json.load(fh)
            for cat, d in esq.items():
                if not isinstance(d, dict) or "columnas" not in d:
                    continue
                hits = [c for c in d["columnas"]
                        if "fecha" in c["nombre"].lower() or "adjud" in c["nombre"].lower()]
                print(f"\n   {cat}  ({d['n_columnas']} columnas, {len(hits)} de fecha)")
                for c in hits:
                    print(f"      {c['nombre']:<50} {c['tipo']}")
        except Exception as e:                                    # noqa: BLE001
            print(f"   [aviso] no se pudo leer {a.esquema_v1}: {e}")
    else:
        print("\n1a · (sin --esquema-v1: se omite el lado del corte viejo)")

    # ------------------------------------------------------------------ (1b)
    archivos = descubrir(a.nuevo)
    pat = re.compile(r"lic" if a.tipo == "lic" else r"(oc|orden)", re.I)
    sel = [t for t in archivos if pat.search(t[0])] or archivos
    if a.archivo:
        sel = [t for t in sel if a.archivo in t[0]] or sel
    if not sel:
        sys.exit(f"No hay archivos bajo {a.nuevo}")
    etq, ruta, interno = sel[0]
    print(f"\n1b · COLUMNAS DE FECHA EN LA RE-DESCARGA — archivo {etq}")
    cols, sep = cabecera(ruta, interno, a.encoding, a.sep)
    c_cod = resolver(cols, CAND_COD)
    c_fp = resolver(cols, CAND_FPUB)
    fechas = [c for c in cols if "fecha" in norm(c) or "adjudic" in norm(c)]
    print(f"   {len(cols)} columnas en el CSV; {len(fechas)} de fecha:")
    for c in fechas:
        print(f"      {c}")
    if not c_cod or not c_fp:
        sys.exit("   [PARAR] no se resolvio codigo o fecha de publicacion.")
    adj = [c for c in fechas if "adjudic" in norm(c)]
    if len(adj) >= 2:
        print(f"\n   *** HAY {len(adj)} COLUMNAS DE ADJUDICACION: {adj}")
        print("   *** Entonces el corte viejo pudo enganchar la equivocada. Comparar")
        print("   *** los perfiles de abajo y quedarse con la que NO tenga futuro.")
    elif len(adj) == 1:
        print(f"\n   Hay UNA sola columna de adjudicacion: {adj[0]!r}. Si esa tiene")
        print("   futuro y se llena al publicar, entonces ChileCompra publica la")
        print("   estimada y NO publica la efectiva. Eso cambia lo que se puede")
        print("   afirmar del capitulo, no como se calcula.")
    else:
        print("\n   No hay ninguna columna con 'adjudic' en el nombre en este archivo.")

    # ------------------------------------------------------------------ (2,3)
    usar = list(dict.fromkeys([c_cod] + fechas))
    print(f"\n2 · PERFIL DE CADA COLUMNA DE FECHA  (leyendo {etq} completo)")
    fuente = abrir(ruta, interno, a.encoding)
    acum = []
    for trozo in pd.read_csv(fuente, sep=sep, usecols=usar, dtype=str, chunksize=CHUNK,
                             quotechar='"', on_bad_lines="skip",
                             keep_default_na=False, na_values=[""], low_memory=False):
        d = {"_cod": trozo[c_cod].astype(str).str.strip()}
        for c in fechas:
            d[c] = a_fecha(trozo[c].fillna(""))
        acum.append(pd.DataFrame(d).drop_duplicates("_cod"))
    fuente.close()
    tab = pd.concat(acum, ignore_index=True).drop_duplicates("_cod")
    n = len(tab)
    print(f"   {n:,} licitaciones distintas en el archivo\n")
    print(f"   {'columna':<40}{'% con dato':>11}{'minimo':>12}{'maximo':>12}{'% futuro':>10}")
    print("   " + "-" * 85)
    for c in fechas:
        s = tab[c]
        con = s.notna().sum()
        if con == 0:
            print(f"   {c[:40]:<40}{0:>10.1f}%{'-':>12}{'-':>12}{'-':>10}")
            continue
        fut = (s > hoy).sum()
        print(f"   {c[:40]:<40}{100.0*con/n:>10.1f}%{str(s.min().date()):>12}"
              f"{str(s.max().date()):>12}{100.0*fut/con:>9.2f}%")
    print("\n   LECTURA: una columna de HECHOS no puede tener '% futuro' > 0. Si lo")
    print("   tiene, o esta mal tipeada o es una fecha declarada, no observada.")

    if c_fp in fechas:
        print(f"\n3 · REZAGO desde {c_fp} (dias)")
        print(f"   {'columna':<40}{'mediana':>10}{'p90':>10}{'% negativo':>12}")
        print("   " + "-" * 72)
        base = tab[c_fp]
        for c in fechas:
            if c == c_fp:
                continue
            d = (tab[c] - base).dt.days.dropna()
            if d.empty:
                continue
            print(f"   {c[:40]:<40}{d.median():>10.0f}{d.quantile(0.9):>10.0f}"
                  f"{100.0*(d < 0).mean():>11.1f}%")
        print("\n   LECTURA: comparar este mismo cuadro entre un mes asentado y uno")
        print("   recien cerrado. Si la mediana no cambia, la fecha se escribe al")
        print("   publicar y no mide ningun evento posterior.")

    print("\nPASO 0-D termina. Pegar esta salida completa en el chat.")


if __name__ == "__main__":
    main()
