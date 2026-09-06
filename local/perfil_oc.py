# -*- coding: utf-8 -*-
"""
perfil_oc.py — la re-descarga de ORDENES DE COMPRA, medida contra el corte
             congelado, sin gastar cupo de Databricks
==============================================================================
Capstone Big Data · Mercado Publico · F27 §8 (29-ago-2026)

POR QUE ES LA CORRIDA LOCAL QUE MAS IMPORTA
-------------------------------------------
`F26` §2 sostiene que ninguna cifra publicada quedo contaminada por el x3, y su
primer argumento es: **el dinero no toca la particion afectada**, porque toda la
cascada en pesos, la reconciliacion 2024 (0,9825), la concentracion del gasto,
Q4, el G7 y la Pregunta 2 se calculan sobre lineas de ORDEN DE COMPRA, y
`oc_historico` no muestra la anomalia.

Ese argumento se apoyaba en una sola evidencia: bytes por fila del parquet
(258-269 en 2023-2025, sin outlier). Ahora hay una re-descarga de OC de las
mismas fechas, o sea una **segunda medicion independiente** de la misma cosa.
Si la re-descarga reproduce los conteos del corte congelado, la afirmacion mas
importante del proyecto pasa de "un indicio" a "dos mediciones independientes".
Si NO los reproduce, es mil veces mejor saberlo ahora que el 9 de septiembre.

QUE COMPARA
-----------
Los conteos de filas por anio de la re-descarga contra `REFERENCIA_FILAS_V1`
(`celdas_1a3_v19_parametrizadas.py` lineas 101-112), que son las filas que el
corte congelado declara por particion, con su run_id al lado:

    2023  oc_historico  5.167.807      2024  oc_historico  5.344.111
    2025  oc_historico  5.150.919      2026  oc_vigente    2.950.934

Es exactamente el test que delato el x3 del lado licitacion, aplicado al lado
que sostiene las cifras en pesos.

USO
---
    python perfil_oc.py --nuevo "C:\\...\\Mercado Publico\\new_oc"

    # si las OC viven mezcladas con las licitaciones en la misma carpeta,
    # el filtro por nombre de archivo (oc_* / orden*) las separa solo:
    python perfil_oc.py --nuevo "C:\\...\\Mercado Publico\\new"

SALVEDADES QUE HAY QUE LEER ANTES DE CITAR NADA
-----------------------------------------------
· Una OC que aparezca en dos archivos se cuenta dos veces en los montos. Es
  raro (cada OC pertenece a un mes) pero no imposible: el script lo avisa.
· 2026 esta incompleto por definicion y su delta no significa nada.
· El monto por OC se toma del primer valor no vacio de MontoTotalOC_PesosChilenos
  por codigo de OC dentro de cada archivo. NO es la cifra del informe ni la
  reemplaza: es un orden de magnitud para detectar un corrimiento grosero.
"""

import argparse
import csv
import io
import os
import re
import sys
import zipfile
from collections import defaultdict

csv.field_size_limit(min(2147483647, sys.maxsize))

ESQUEMA_OC = {
    "codigo_oc": ["Codigo", "CodigoOrdenCompra", "codigo_oc"],
    "codigo_licitacion": ["CodigoLicitacion", "codigo_licitacion"],
    "onu_oc": ["codigoProductoONU", "CodigoProductoONU", "onu_oc"],
    "cantidad_oc": ["cantidad", "Cantidad", "cantidad_oc"],
    "precio_neto_oc": ["precioNeto", "PrecioNeto", "precio_neto_oc"],
    "monto_total_oc_clp": ["MontoTotalOC_PesosChilenos", "monto_total_oc_clp"],
    "fecha_creacion": ["FechaCreacion", "fecha_creacion"],
}

# celdas_1a3_v19_parametrizadas.py lineas 101-112
REFERENCIA_FILAS_V1 = {
    2017: ("oc_historico", 6_609_983), 2018: ("oc_historico", 6_614_357),
    2019: ("oc_historico", 6_209_651), 2020: ("oc_historico", 4_408_740),
    2021: ("oc_historico", 4_266_941), 2022: ("oc_historico", 4_728_085),
    2023: ("oc_historico", 5_167_807), 2024: ("oc_historico", 5_344_111),
    2025: ("oc_historico", 5_150_919), 2026: ("oc_vigente",   2_950_934),
}

PAT_OC = re.compile(r"(^|[^a-z])oc[^a-z]|orden", re.I)


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def resolver(cols):
    reales = {norm(c): c for c in cols}
    return {k: next((reales[norm(c)] for c in cands if norm(c) in reales), None)
            for k, cands in ESQUEMA_OC.items()}


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
            try:
                with zipfile.ZipFile(r) as z:
                    for n in z.namelist():
                        if n.lower().endswith((".csv", ".txt")):
                            out.append(("%s::%s" % (os.path.basename(r), n), r, n))
            except (zipfile.BadZipFile, OSError) as e:
                print("   [aviso] zip ilegible %s: %s" % (r, e))
        else:
            out.append((os.path.basename(r), r, None))
    return out


def abrir(ruta, interno, enc):
    if interno is None:
        return open(ruta, "r", encoding=enc, errors="replace", newline="")
    with zipfile.ZipFile(ruta) as z:
        return io.StringIO(z.read(interno).decode(enc, errors="replace"))


def anio_de(etiqueta):
    c = re.findall(r"(20[0-9]{2})", etiqueta.replace("\\", "/"))
    return int(c[-1]) if c else None


def num(s):
    if not s:
        return None
    t = s.strip().replace(" ", "")
    if not t:
        return None
    if "," in t and "." in t:
        t = t.replace(".", "").replace(",", ".")
    elif "," in t:
        t = t.replace(",", ".")
    try:
        return float(t)
    except ValueError:
        return None


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nuevo", required=True, help="carpeta de la re-descarga de OC")
    p.add_argument("--encoding", default="latin-1")
    p.add_argument("--sep", default=";")
    p.add_argument("--salida-csv", default="csv")
    p.add_argument("--sin-filtro-nombre", action="store_true",
                   help="no filtrar por 'oc'/'orden' en el nombre del archivo")
    a = p.parse_args(argv)

    archivos = descubrir(a.nuevo)
    if not a.sin_filtro_nombre:
        sel = [t for t in archivos if PAT_OC.search(t[0]) or PAT_OC.search(t[1])]
        if not sel:
            print("   [aviso] ningun nombre de archivo dice 'oc' ni 'orden'.")
            print("   Se toman TODOS. Si eso mezcla licitaciones, la comparacion")
            print("   no vale: separar las carpetas o usar --sin-filtro-nombre a")
            print("   proposito.")
            sel = archivos
    else:
        sel = archivos
    if not sel:
        print("ABORTA: no hay archivos bajo %r" % a.nuevo)
        return 2

    print("=" * 78)
    print("PERFIL DE LA RE-DESCARGA DE ORDENES DE COMPRA")
    print("=" * 78)
    print("   %d archivo(s)\n" % len(sel))

    por_anio = defaultdict(lambda: {"filas": 0, "oc": 0, "dups": 0,
                                    "monto": 0.0, "linea": 0.0, "archivos": 0,
                                    "sin_onu": 0})
    resoluciones = {}
    for etiqueta, ruta, interno in sel:
        anio = anio_de(etiqueta) or anio_de(ruta)
        try:
            fh = abrir(ruta, interno, a.encoding)
        except Exception as e:                                   # noqa: BLE001
            print("   [aviso] no se pudo abrir %s: %s" % (etiqueta, e))
            continue
        with fh:
            lector = csv.DictReader(fh, delimiter=a.sep)
            cols = lector.fieldnames or []
            res = resolver(cols)
            if res["codigo_oc"] is None:
                print("   [salta] %-34s sin columna de codigo de OC" % etiqueta)
                continue
            resoluciones[etiqueta] = res
            vistos, dups, montos = set(), 0, {}
            n = 0
            linea_tot = 0.0
            sin_onu = 0
            for fila in lector:
                n += 1
                h = hash(tuple(fila.get(c) for c in cols))
                if h in vistos:
                    dups += 1
                else:
                    vistos.add(h)
                cod = (fila.get(res["codigo_oc"]) or "").strip().upper()
                if cod and cod not in montos:
                    m = num(fila.get(res["monto_total_oc_clp"])) if res["monto_total_oc_clp"] else None
                    montos[cod] = m or 0.0
                if res["precio_neto_oc"] and res["cantidad_oc"]:
                    pn = num(fila.get(res["precio_neto_oc"]))
                    ca = num(fila.get(res["cantidad_oc"]))
                    if pn is not None and ca is not None:
                        linea_tot += pn * ca
                if res["onu_oc"] and not (fila.get(res["onu_oc"]) or "").strip():
                    sin_onu += 1
        d = por_anio[anio]
        d["filas"] += n
        d["oc"] += len(montos)
        d["dups"] += dups
        d["monto"] += sum(montos.values())
        d["linea"] += linea_tot
        d["archivos"] += 1
        d["sin_onu"] += sin_onu
        print("   %-34s %10s filas · %9s OC · %7s dup · %5s lin/OC"
              % (etiqueta[:34], "{:,}".format(n), "{:,}".format(len(montos)),
                 "{:,}".format(dups),
                 ("%.1f" % (n / len(montos))) if montos else "-"))

    print("\n" + "=" * 78)
    print("CONTRA EL CORTE CONGELADO (REFERENCIA_FILAS_V1)")
    print("=" * 78)
    print("   %-6s %14s %14s %11s %10s %12s"
          % ("anio", "filas nuevo", "filas corte", "delta", "delta %", "dup exactas"))
    print("   " + "-" * 72)
    filas_csv = []
    for anio in sorted(k for k in por_anio if k):
        d = por_anio[anio]
        ref = REFERENCIA_FILAS_V1.get(anio)
        if ref:
            delta = d["filas"] - ref[1]
            pct = 100.0 * delta / ref[1]
            marca = "" if abs(pct) <= 2 else ("   <-- MIRAR" if abs(pct) <= 15
                                              else "   <-- ALERTA")
        else:
            delta, pct, marca = "", "", "   (sin referencia)"
        print("   %-6s %14s %14s %11s %10s %12s%s"
              % (anio, "{:,}".format(d["filas"]),
                 "{:,}".format(ref[1]) if ref else "-",
                 "{:+,}".format(delta) if ref else "-",
                 ("%+.2f%%" % pct) if ref else "-",
                 "{:,}".format(d["dups"]), marca))
        filas_csv.append({
            "anio": anio, "archivos": d["archivos"], "filas_nuevo": d["filas"],
            "filas_corte_congelado": ref[1] if ref else "",
            "delta": delta, "delta_pct": round(pct, 4) if ref else "",
            "oc_distintas": d["oc"], "filas_duplicadas_exactas": d["dups"],
            "lineas_por_oc": round(d["filas"] / d["oc"], 2) if d["oc"] else "",
            "monto_total_oc_clp": round(d["monto"], 0),
            "suma_precio_neto_x_cantidad": round(d["linea"], 0),
            "lineas_sin_onu": d["sin_onu"],
        })

    print("\n   LECTURA")
    print("   · |delta| <= 2 % en 2023-2025: la re-descarga REPRODUCE el corte")
    print("     congelado y `F26` §2.1 pasa a tener dos mediciones independientes.")
    print("   · un anio con +150 % o mas es el mismo x3 del lado licitacion, y")
    print("     entonces `F26` §2 hay que reabrirlo ENTERO antes de la defensa.")
    print("   · 2026 esta incompleto por definicion: su delta no significa nada.")
    print("   · 'dup exactas' > 0 en un archivo dice que la duplicacion viaja en")
    print("     el CSV descargado, no en la conversion a parquet.")
    print("   · 'lineas por OC' tiene que ser estable entre anios. Si un anio se")
    print("     dispara, ahi esta el problema, con nombre y apellido.")

    if filas_csv:
        os.makedirs(a.salida_csv, exist_ok=True)
        ruta = os.path.join(a.salida_csv, "perfil_oc_vs_corte.csv")
        cab = list(filas_csv[0].keys())
        with open(ruta, "w", encoding="utf-8") as fh:
            fh.write(";".join(cab) + "\n")
            for f in filas_csv:
                fh.write(";".join(str(f[c]) for c in cab) + "\n")
        print("\n   [OK] escrito %s" % ruta)
    print("\nPegar esta salida completa en el chat.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
