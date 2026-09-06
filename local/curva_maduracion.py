# -*- coding: utf-8 -*-
"""
curva_maduracion.py — la figura 7 del informe, medida en vez de supuesta
==============================================================================
Capstone Big Data · Mercado Publico · F27 (29-ago-2026)

QUE REEMPLAZA
-------------
La figura 7 iba a salir de `v2_censura_por_anio.csv`. Esa tabla no es citable:
se corrio con `--fecha-descarga-viejo 2026-08-01`, una fecha POSTERIOR al
ultimo mes del corte, asi que por definicion casi nada podia quedar censurado
(0,02 %). El numero medía el parametro, no el dato.

QUE MIDE ESTA
-------------
El paso 0-D encontro que en `lic_2026-7` —un mes recien cerrado— la columna
`FechaAdjudicacion` viene poblada al 100 % y el 38,34 % de sus valores estan
en el FUTURO, con maximo 2029. En `lic_2023-1` la misma columna tiene 0,00 %
en el futuro. O sea: **la misma columna es una intencion en un mes fresco y un
hecho en un mes viejo.**

Esto lo convierte en una curva: para cada mes del corte, que porcentaje de sus
licitaciones declara una adjudicacion futura, contra la antiguedad del mes al
momento de la descarga. Sale monotona, se mide con un solo corte (no hace falta
la re-descarga), no cuesta cupo de Databricks, y dice en una imagen por que
ninguna serie que dependa de adjudicaciones es comparable en su ultimo tramo.

USO
---
    python curva_maduracion.py --nuevo "...\\Mercado Publico\\new" \\
        --salida-csv csv --salida-fig figuras

    # si el mtime no sirve como fecha de descarga, declararla:
    python curva_maduracion.py --nuevo "..." --fecha-descarga 2026-08-12

Escribe `curva_maduracion.csv` (que es el insumo de la figura y una tabla
citable por si sola) y `fig7_maduracion_del_archivo.png/.pdf`.
"""

import argparse
import os
import sys
from collections import defaultdict
from datetime import date, datetime

_AQUI = os.path.dirname(os.path.abspath(__file__))
if _AQUI not in sys.path:
    sys.path.insert(0, _AQUI)

try:
    from auditoria_vintage_v2 import (descubrir_archivos, leer_filas, parse_fecha,
                                      norm_codigo, norm_txt, fmt_n, fmt_pct)
except ImportError:
    print("ABORTA: no encuentro auditoria_vintage_v2.py en la misma carpeta que")
    print("        este script. Copialos juntos (los dos viven en local/).")
    raise

CLAVES_MIN = ["codigo_externo", "fecha_publicacion", "fecha_adjudicacion", "estado"]


def mes_del_archivo(etiqueta):
    """'...\\lic_2026-7.csv' -> (2026, 7). None si no calza el patron."""
    base = os.path.basename(etiqueta)
    cuerpo = os.path.splitext(base)[0]
    trozo = cuerpo.split("_")[-1]
    if "-" not in trozo:
        return None
    a, m = trozo.split("-", 1)
    try:
        a, m = int(a), int(m)
    except ValueError:
        return None
    if not (2000 <= a <= 2100 and 1 <= m <= 12):
        return None
    return a, m


def antiguedad_meses(anio, mes, ref):
    return (ref.year - anio) * 12 + (ref.month - mes)


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Curva de maduracion del archivo mensual de ChileCompra.")
    p.add_argument("--nuevo", required=True, help="carpeta de la re-descarga")
    p.add_argument("--fecha-descarga", default=None,
                   help="YYYY-MM-DD. Si no se pasa, se usa el mtime maximo (PROXY)")
    p.add_argument("--salida-csv", default="csv")
    p.add_argument("--salida-fig", default="figuras")
    p.add_argument("--sin-figura", action="store_true")
    a = p.parse_args(argv)

    archivos = descubrir_archivos(a.nuevo)
    if not archivos:
        print("ABORTA: no hay CSV bajo %r" % a.nuevo)
        return 2

    ref = None
    if a.fecha_descarga:
        ref = parse_fecha(a.fecha_descarga, "fecha_descarga")
        etiqueta_ref = "DECLARADA"
    if ref is None:
        mt = []
        for _, _, ruta in archivos:
            try:
                mt.append(datetime.fromtimestamp(os.path.getmtime(ruta)).date())
            except OSError:
                pass
        ref = max(mt) if mt else date.today()
        etiqueta_ref = "PROXY por mtime — declararlo en el informe"

    print("=" * 78)
    print("CURVA DE MADURACION DEL ARCHIVO MENSUAL")
    print("corrida: %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("fecha de referencia: %s   [%s]" % (ref, etiqueta_ref))
    print("=" * 78)

    filas = []
    for etiqueta, abridor, _ruta in sorted(archivos, key=lambda t: t[0]):
        mm = mes_del_archivo(etiqueta)
        if mm is None:
            print("   [salta] %s — el nombre no dice de que mes es"
                  % os.path.basename(etiqueta))
            continue
        anio, mes = mm
        try:
            res, gen, _cab, _enc, _sep = leer_filas(abridor, CLAVES_MIN, verbose=False)
        except Exception as e:                                   # noqa: BLE001
            print("   [aviso] no se pudo leer %s: %s" % (etiqueta, e))
            continue
        if res.get("codigo_externo") is None or res.get("fecha_adjudicacion") is None:
            print("   [salta] %s — sin codigo o sin FechaAdjudicacion"
                  % os.path.basename(etiqueta))
            continue

        lic = {}
        n_filas = 0
        for d in gen:
            n_filas += 1
            cod = norm_codigo(d.get("codigo_externo"))
            if not cod:
                continue
            reg = lic.get(cod)
            if reg is None:
                reg = {"pub": None, "adj": None, "estado": None}
                lic[cod] = reg
            if reg["pub"] is None:
                reg["pub"] = parse_fecha(d.get("fecha_publicacion"), "fecha_publicacion")
            if reg["adj"] is None:
                reg["adj"] = parse_fecha(d.get("fecha_adjudicacion"), "fecha_adjudicacion")
            if not reg["estado"]:
                reg["estado"] = d.get("estado")

        n_lic = len(lic)
        if not n_lic:
            continue
        n_con_adj = n_futuro = n_adjudicada = 0
        rezagos = []
        for reg in lic.values():
            if norm_txt(reg["estado"]).startswith("adjudicada"):
                n_adjudicada += 1
            if reg["adj"] is None:
                continue
            n_con_adj += 1
            if reg["adj"] > ref:
                n_futuro += 1
            elif reg["pub"] is not None:
                rezagos.append((reg["adj"] - reg["pub"]).days)
        rezagos.sort()
        edad = antiguedad_meses(anio, mes, ref)
        fila = {
            "archivo": os.path.basename(etiqueta),
            "anio": anio, "mes": mes,
            "antiguedad_meses": edad,
            "filas": n_filas,
            "licitaciones": n_lic,
            "con_fecha_adjudicacion": n_con_adj,
            "pct_con_fecha_adjudicacion": round(100.0 * n_con_adj / n_lic, 4),
            "adjudicacion_en_futuro": n_futuro,
            "pct_adjudicacion_en_futuro": round(100.0 * n_futuro / n_con_adj, 4)
                                          if n_con_adj else "",
            "pct_estado_adjudicada": round(100.0 * n_adjudicada / n_lic, 4),
            "rezago_mediano_dias": rezagos[len(rezagos) // 2] if rezagos else "",
            "rezago_p90_dias": rezagos[int(len(rezagos) * 0.9)] if rezagos else "",
        }
        filas.append(fila)
        print("   %-18s edad %3d m · %8s lic · futuro %7s · adjudicada %7s"
              % (fila["archivo"], edad, fmt_n(n_lic),
                 fmt_pct(n_futuro, n_con_adj), fmt_pct(n_adjudicada, n_lic)))

    if not filas:
        print("\nABORTA: ningun archivo aporto datos.")
        return 2

    filas.sort(key=lambda f: (f["anio"], f["mes"]))
    os.makedirs(a.salida_csv, exist_ok=True)
    ruta = os.path.join(a.salida_csv, "curva_maduracion.csv")
    cab = list(filas[0].keys())
    with open(ruta, "w", encoding="utf-8") as fh:
        fh.write(";".join(cab) + "\n")
        for f in filas:
            fh.write(";".join(str(f[c]) for c in cab) + "\n")
    print("\n   [OK] escrito %s  (%d filas)" % (ruta, len(filas)))

    print("\n" + "-" * 78)
    print("LECTURA")
    print("-" * 78)
    frescos = [f for f in filas if f["antiguedad_meses"] <= 2
               and f["pct_adjudicacion_en_futuro"] != ""]
    viejos = [f for f in filas if f["antiguedad_meses"] >= 24
              and f["pct_adjudicacion_en_futuro"] != ""]
    if frescos and viejos:
        pf = sum(f["pct_adjudicacion_en_futuro"] for f in frescos) / len(frescos)
        pv = sum(f["pct_adjudicacion_en_futuro"] for f in viejos) / len(viejos)
        print("   meses de 0-2 de antiguedad : %.2f %% con adjudicacion en el futuro"
              % pf)
        print("   meses de 24+ de antiguedad : %.2f %%" % pv)
        print("   La misma columna, el mismo corte: intencion en un extremo y")
        print("   hecho en el otro. Es la razon por la que el ultimo tramo de")
        print("   cualquier serie que dependa de adjudicaciones no es comparable")
        print("   con el resto, y aqui esta medido en vez de supuesto.")

    if not a.sin_figura:
        try:
            dibujar(filas, a.salida_fig, ref, etiqueta_ref)
        except Exception as e:                                    # noqa: BLE001
            print("\n   [aviso] no se pudo dibujar la figura: %s" % e)
            print("   El CSV ya esta escrito; la figura se puede hacer aparte.")
    return 0


def dibujar(filas, salida, ref, etiqueta_ref):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    try:
        from graficos_capstone import (_guardar, _limpiar, _pie, cl_pct,
                                       NARANJA, TINTA, REJILLA)
        azul = globals().get("AZUL")
    except ImportError:
        NARANJA, TINTA, REJILLA = "#C1512A", "#1F1F1F", "#DCDCDC"
        cl_pct = lambda v, d=1: ("%.*f" % (d, v)).replace(".", ",") + " %"   # noqa: E731

        def _limpiar(ax, ejes=("top", "right")):
            for e in ejes:
                ax.spines[e].set_visible(False)

        def _pie(ax, texto, dy=-0.16):
            ax.annotate(texto, xy=(0, dy), xycoords="axes fraction",
                        fontsize=7.6, color="#555555", va="top", ha="left")

        def _guardar(fig, salida, nombre):
            os.makedirs(salida, exist_ok=True)
            for ext in ("png", "pdf"):
                fig.savefig(os.path.join(salida, "%s.%s" % (nombre, ext)),
                            bbox_inches="tight", pad_inches=0.28)
            plt.close(fig)
            print("   [OK] %s.png / .pdf" % nombre)

    d = [f for f in filas if f["pct_adjudicacion_en_futuro"] != ""]
    x = [f["antiguedad_meses"] for f in d]
    y = [f["pct_adjudicacion_en_futuro"] for f in d]
    y2 = [f["pct_estado_adjudicada"] for f in d]

    fig, ax = plt.subplots(figsize=(9.4, 4.2))
    ax.plot(x, y, "o-", color=NARANJA, lw=1.9, ms=5.2,
            label="% de FechaAdjudicacion en el futuro")
    ax.plot(x, y2, "s--", color="#5B7FA6", lw=1.5, ms=4.2,
            label="% en estado Adjudicada")
    ax.invert_xaxis()
    ax.set_xlabel("antiguedad del mes al momento de la descarga (meses)",
                  fontsize=9.3, labelpad=8)
    ax.set_ylabel("% de las licitaciones del mes", fontsize=9.3)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: cl_pct(v, 0)))
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    ax.legend(frameon=False, fontsize=8.6, loc="upper left")
    _limpiar(ax)
    ax.set_title("La misma columna es una intencion en un mes fresco y un hecho "
                 "en uno viejo", loc="left", pad=16)
    _pie(ax, "[MEDIDO] curva_maduracion.py sobre la re-descarga, fecha de "
             "referencia %s (%s).\nEn el mes mas reciente FechaAdjudicacion viene "
             "poblada al 100 %% pero una parte apunta a anios futuros: es la fecha "
             "estimada, no la observada.\nA medida que el mes envejece el valor "
             "converge al hecho. Por eso el ultimo tramo de cualquier serie que "
             "dependa de adjudicaciones no es comparable con el resto."
             % (ref, etiqueta_ref))
    _guardar(fig, salida, "fig7_maduracion_del_archivo")


if __name__ == "__main__":
    sys.exit(main())
