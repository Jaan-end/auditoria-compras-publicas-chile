# -*- coding: utf-8 -*-
"""
==============================================================================
QUE HACER CON ESTE SCRIPT
==============================================================================
QUE ES      Cierra F28 3.2: contesta con que se rellena FechaAdjudicacion
            cuando la licitacion NO esta adjudicada, Y —lo importante— trae
            el CONTROL que falta: la misma medicion sobre las que SI estan
            adjudicadas. Sin ese control, el 59,88 % no prueba nada.

DONDE       Guardarlo en  ...\\Insumos\\Codigos\\  (junto a los otros).

COMO CORRER Un archivo:
              python chequeo_relleno_fecha.py "...\\new\\Lic\\2025\\lic_2025-6.csv"
            Varios meses de una vez (recomendado, 3 o 4 basta):
              python chequeo_relleno_fecha.py "...\\new\\Lic\\2025" --max 4

QUE DEVUELVE  Una tabla por archivo en pantalla y, si se pasa --csv,
              un CSV con una fila por archivo para el informe.
              NO escribe nada mas. NO toca ningun dato.

QUE MIRAR   La linea "DIFERENCIAL". Es el veredicto:
              · si es > 30 pp  -> la igualdad es diagnostica: la columna se
                rellena con la fecha estimada. F28 3.2 CIERRA con mecanismo.
              · si es < 10 pp  -> la igualdad NO distingue: las adjudicadas
                tambien coinciden con su estimacion, y el 59,88 % era solo
                "ChileCompra estima bien". Hay que buscar otra explicacion.

DEPENDE DE  pandas. Nada mas.
==============================================================================
"""
import argparse, os, sys

try:
    import pandas as pd
except ImportError:
    print("Falta pandas:  pip install pandas")
    sys.exit(2)

COL_ADJ = "FechaAdjudicacion"
COL_EST = "FechaEstimadaAdjudicacion"
COL_EST_ALT = ["FechaEstimadaAdjudicacion", "Fecha estimada de Adjudicacion",
               "FechaEstimadaadjudicacion", "FechaAdjudicacionEstimada"]
COL_COD = "CodigoExterno"
COL_EDO = "Estado"
CENTINELAS = ("1900-01-01", "1899-12-30", "1970-01-01", "0", "", "nan")


def _col(df, principal, alternativas=()):
    if principal in df.columns:
        return principal
    baja = {c.lower().replace(" ", ""): c for c in df.columns}
    for cand in (principal,) + tuple(alternativas):
        k = cand.lower().replace(" ", "")
        if k in baja:
            return baja[k]
    return None


def _norm(s):
    return (s.astype(str).str.strip()
             .str.replace(r"\s+0:00:00$", "", regex=True)
             .str.replace(r"\s+00:00:00$", "", regex=True)
             .str.slice(0, 10))


def revisar(ruta, por_licitacion=True):
    print("=" * 78)
    print(os.path.basename(ruta))
    print("=" * 78)
    df = pd.read_csv(ruta, sep=";", encoding="latin-1",
                     low_memory=False, on_bad_lines="skip")

    c_adj = _col(df, COL_ADJ)
    c_est = _col(df, COL_EST, COL_EST_ALT)
    c_edo = _col(df, COL_EDO)
    c_cod = _col(df, COL_COD)
    faltan = [n for n, c in (("FechaAdjudicacion", c_adj),
                             ("FechaEstimadaAdjudicacion", c_est),
                             ("Estado", c_edo)) if c is None]
    if faltan:
        print("   [ABORTA] faltan columnas: %s" % ", ".join(faltan))
        print("   columnas disponibles: %s" % ", ".join(map(str, df.columns[:25])))
        return None

    if por_licitacion and c_cod is not None:
        antes = len(df)
        df = df.drop_duplicates(subset=[c_cod])
        print("   %s filas -> %s licitaciones (deduplicado por %s)"
              % (format(antes, ","), format(len(df), ","), c_cod))
    else:
        print("   %s filas (sin deduplicar)" % format(len(df), ","))

    adj = _norm(df[c_adj])
    est = _norm(df[c_est])
    edo = df[c_edo].astype(str).str.strip()

    es_adj = edo.eq("Adjudicada")
    igual = adj.eq(est)

    n_si, n_no = int(es_adj.sum()), int((~es_adj).sum())
    p_no = float(igual[~es_adj].mean()) * 100 if n_no else float("nan")
    p_si = float(igual[es_adj].mean()) * 100 if n_si else float("nan")
    dif = p_no - p_si

    print()
    print("   %-34s %10s %14s" % ("grupo", "n", "adj == estimada"))
    print("   " + "-" * 60)
    print("   %-34s %10s %13.2f %%" % ("NO adjudicadas", format(n_no, ","), p_no))
    print("   %-34s %10s %13.2f %%" % ("SI adjudicadas  (CONTROL)",
                                       format(n_si, ","), p_si))
    print("   " + "-" * 60)
    print("   %-34s %26.2f pp" % ("DIFERENCIAL", dif))
    print()

    cent = adj[~es_adj].str.slice(0, 10).isin(CENTINELAS).mean() * 100 \
        if n_no else float("nan")
    print("   centinelas (1900-01-01 y similares) en las NO adjudicadas: %.4f %%"
          % cent)

    vac = (adj.eq("") | adj.str.lower().eq("nan")).mean() * 100
    print("   FechaAdjudicacion vacia o nan, todo el archivo:            %.4f %%"
          % vac)

    dist = adj[~es_adj].value_counts().head(3)
    print("   valores mas repetidos en las NO adjudicadas: %s"
          % ", ".join("%s (x%s)" % (i, format(v, ","))
                      for i, v in dist.items()))

    print("   " + "-" * 74)
    if pd.isna(dif):
        print("   VEREDICTO: no calculable.")
    elif dif > 30:
        print("   VEREDICTO: la igualdad ES diagnostica (diferencial %.1f pp)." % dif)
        print("   La columna se rellena con la fecha estimada cuando no hay")
        print("   adjudicacion. F28 3.2 cierra CON MECANISMO, y la brecha de")
        print("   17 pp deja de ser una magnitud sin explicacion.")
    elif dif < 10:
        print("   VEREDICTO: la igualdad NO distingue (diferencial %.1f pp)." % dif)
        print("   Las adjudicadas coinciden con su estimacion casi tanto como")
        print("   las no adjudicadas: el 60 %% solo dice que ChileCompra estima")
        print("   bien. NO escribir el mecanismo. Buscar otra via.")
    else:
        print("   VEREDICTO: zona gris (diferencial %.1f pp). Correr mas meses" % dif)
        print("   antes de escribir nada.")
    print()
    return dict(archivo=os.path.basename(ruta), n_no_adjudicadas=n_no,
                n_adjudicadas=n_si, pct_igual_no_adj=round(p_no, 4),
                pct_igual_si_adj=round(p_si, 4), diferencial_pp=round(dif, 4),
                pct_centinelas=round(cent, 4))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("destino", help="un CSV o una carpeta con CSV")
    ap.add_argument("--max", type=int, default=6,
                    help="cuantos archivos revisar si es carpeta (def. 6)")
    ap.add_argument("--csv", default=None,
                    help="ruta de un CSV resumen a escribir (opcional)")
    ap.add_argument("--por-fila", action="store_true",
                    help="no deduplicar por CodigoExterno")
    a = ap.parse_args()

    if os.path.isfile(a.destino):
        rutas = [a.destino]
    else:
        rutas = sorted(os.path.join(dp, fn)
                       for dp, _dn, fns in os.walk(a.destino)
                       for fn in fns if fn.lower().endswith(".csv"))[:a.max]
    if not rutas:
        print("no encontre CSV en %s" % a.destino)
        return 2

    filas = []
    for r in rutas:
        try:
            out = revisar(r, por_licitacion=not a.por_fila)
            if out:
                filas.append(out)
        except Exception as e:
            print("   [error] %s: %s\n" % (os.path.basename(r), e))

    if filas:
        print("=" * 78)
        print("RESUMEN")
        print("=" * 78)
        print("   %-24s %12s %12s %12s"
              % ("archivo", "no-adj", "si-adj", "difer. pp"))
        for f in filas:
            print("   %-24s %11.2f%% %11.2f%% %12.2f"
                  % (f["archivo"][:24], f["pct_igual_no_adj"],
                     f["pct_igual_si_adj"], f["diferencial_pp"]))
        m = sum(f["diferencial_pp"] for f in filas) / len(filas)
        print("   diferencial medio: %.2f pp sobre %d archivo(s)" % (m, len(filas)))
        if a.csv:
            pd.DataFrame(filas).to_csv(a.csv, sep=";", index=False)
            print("   [OK] escrito %s" % a.csv)
    print()
    print("Pegar esta salida completa en el chat.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
