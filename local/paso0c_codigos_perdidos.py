# -*- coding: utf-8 -*-
"""
paso0c_codigos_perdidos.py — Capstone Big Data · Mercado Publico
==================================================================
CORRE EN TU PC. Cero Databricks, cero cupo.

QUE CONTESTA
------------
El PASO 0-B dejo un numero exacto: **11.402 licitaciones** que estan en el
corte viejo y no estan en el nuevo, todas dentro de la ventana contigua
2024-11 .. 2025-10, a razon de 9,8% de cada mes (desviacion 1,1 puntos),
con borde duro a los dos lados. Fuera de esa ventana los dos cortes calzan.

Un numero no es un diagnostico. Hay dos historias que producen exactamente
esa tabla y llevan a decisiones OPUESTAS:

  A) NO MIGRARON. Esas 11.402 nunca pasaron de la carpeta `Vigente` a
     `Historico` — siguen abiertas, o cerraron sin que el archivo mensual de
     `Historico` se regenerara. Estan disponibles HOY, en otra carpeta.
     -> Se bajan de `Vigente`, se unen, y el corte nuevo pasa a ser
        superconjunto. GO.

  B) DESAPARECIERON. ChileCompra ya no las publica en ningun archivo. El
     corte viejo conserva registros que la fuente borro.
     -> El corte nuevo NUNCA va a ser superconjunto. Hay que ir a corte
        `mixto`, y las 11.402 son un hallazgo del informe, no un estorbo:
        "los archivos mensuales de Mercado Publico no son append-only".

Este script las identifica una por una y mira QUE SON. Necesita dos carpetas
de CSV. Sirve para dos comparaciones distintas:

  1) descarga ORIGINAL (la que armo el corte viejo) vs re-descarga:
     el caso completo, si todavia tienes los CSV originales en disco.

        python paso0c_codigos_perdidos.py --viejo "D:/descarga_original" \\
                                          --nuevo "D:/.../new" --tipo lic

  2) UN SOLO MES bajado hoy de `Vigente` vs ese mismo mes en la re-descarga:
     la prueba barata de A) contra B) sin necesitar los CSV originales.
     Bajar `Licitaciones_Nacional_Vigente` (o como se llame hoy) del mes
     2025-03, dejarlo solo en una carpeta, y:

        python paso0c_codigos_perdidos.py --viejo "D:/prueba_vigente_2025-03" \\
                                          --nuevo "D:/.../new" \\
                                          --tipo lic --anios 2025 --meses 3

     El mes 2025-03 tiene 9.302 licitaciones en el corte viejo y 8.484 en el
     nuevo. Si el archivo de `Vigente` de hoy trae las ~818 que faltan, la
     historia es A) y el arreglo es bajar una carpeta mas. Si no las trae,
     es B).

SALIDA
------
  · tabla mes a mes con los tres conjuntos: solo-viejo, solo-nuevo, comun
  · distribucion de ESTADO de las perdidas contra las que si estan
    (la pregunta que decide todo: ¿son procesos abiertos, o son de todo?)
  · `codigos_perdidos.csv` con una fila por licitacion perdida y las
    columnas que pediste, para pegar en CAP-15 o mirar a mano

Requiere pandas.
"""

import argparse
import io
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict

try:
    import pandas as pd
except ImportError:
    sys.exit("Falta pandas.  pip install pandas")

CHUNK = 300_000
SNIFF = 4 * 1024 * 1024

CAND_COD = ["codigo_externo", "CodigoExterno", "Codigo Externo", "CodigoLicitacion"]
CAND_FPUB = ["FechaPublicacion", "fecha_publicacion", "Fecha Publicacion",
             "FechaPublicacionLicitacion", "fecha_creacion", "FechaCreacion"]
CAND_EST = ["Estado", "estado", "CodigoEstado", "EstadoLicitacion", "Estado Licitacion"]
# columnas extra que se arrastran al csv de perdidas si existen
CAND_EXTRA = ["FechaCierre", "FechaAdjudicacion", "FechaEstimadaAdjudicacion",
              "NombreOrganismo", "Tipo", "NumeroOferentes"]


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def resolver(cols, candidatos):
    reales = {norm(c): c for c in cols}
    for cand in candidatos:
        if norm(cand) in reales:
            return reales[norm(cand)]
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


def abrir(ruta, interno, enc):
    if interno is None:
        return open(ruta, "r", encoding=enc, errors="replace", newline="")
    with zipfile.ZipFile(ruta) as z:
        raw = z.read(interno)
    return io.StringIO(raw.decode(enc, errors="replace"))


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


def fechas(serie):
    fp = pd.to_datetime(serie.astype(str).str.slice(0, 10), errors="coerce",
                        format="%Y-%m-%d")
    malas = fp.isna()
    if malas.any():
        fp.loc[malas] = pd.to_datetime(serie[malas].astype(str).str.slice(0, 10),
                                       errors="coerce", dayfirst=True)
    return fp


def recorrer(raiz, tipo, enc, sep, guardar_campos, etiqueta_lado):
    """Devuelve un DataFrame con una fila por licitacion distinta.

    Columnas: _cod, _yy, _mm y, si guardar_campos, _estado + las de CAND_EXTRA
    que existan. Todo vectorizado: 8 millones de filas no se recorren a mano.
    """
    archivos = descubrir(raiz)
    if not archivos:
        sys.exit(f"\nNo hay csv/zip bajo {raiz}")
    pat = re.compile(r"lic" if tipo == "lic" else r"(oc|orden)", re.I)
    sel = [t for t in archivos if pat.search(t[0]) or pat.search(t[1])] or archivos
    print(f"\n  [{etiqueta_lado}] {raiz}")
    print(f"  archivos: {len(sel)}")

    partes, leidas = [], 0
    for etq, ruta, interno in sel:
        try:
            cols, sep_f = cabecera(ruta, interno, enc, sep)
        except Exception as e:                                   # noqa: BLE001
            print(f"     [aviso] cabecera ilegible {etq}: {e}")
            continue
        c_cod = resolver(cols, CAND_COD)
        c_fp = resolver(cols, CAND_FPUB)
        if not c_cod or not c_fp:
            print(f"     [PARAR] {etq}: falta codigo o fecha. cabecera: {cols[:6]}")
            continue
        c_est = resolver(cols, CAND_EST) if guardar_campos else None
        extras = []
        if guardar_campos:
            for cand in CAND_EXTRA:
                real = resolver(cols, [cand])
                if real and real not in extras and real not in (c_cod, c_fp, c_est):
                    extras.append(real)
        usar = list(dict.fromkeys([c for c in [c_cod, c_fp, c_est] + extras if c]))

        fuente = abrir(ruta, interno, enc)
        n = 0
        try:
            for trozo in pd.read_csv(fuente, sep=sep_f, usecols=usar, dtype=str,
                                     chunksize=CHUNK, quotechar='"',
                                     on_bad_lines="skip", keep_default_na=False,
                                     na_values=[""], low_memory=False):
                n += len(trozo)
                cod = trozo[c_cod].astype(str).str.strip()
                fp = fechas(trozo[c_fp])
                ok = fp.notna() & (cod != "") & (cod.str.lower() != "nan")
                if not ok.any():
                    continue
                sub = pd.DataFrame({"_cod": cod[ok],
                                    "_yy": fp[ok].dt.year.astype("int32"),
                                    "_mm": fp[ok].dt.month.astype("int8")})
                if c_est:
                    sub["_estado"] = trozo.loc[ok, c_est].astype(str)
                for e in extras:
                    sub[e] = trozo.loc[ok, e].astype(str)
                partes.append(sub.drop_duplicates("_cod"))
        except Exception as e:                                   # noqa: BLE001
            print(f"     [aviso] {etq}: {type(e).__name__}: {e}")
        finally:
            try:
                fuente.close()
            except Exception:                                    # noqa: BLE001
                pass
        leidas += n
        print(f"     {etq[:52]:<52} {n:>12,} filas")

    if not partes:
        sys.exit(f"\nNo se pudo leer nada util de {raiz}")
    tabla = pd.concat(partes, ignore_index=True).drop_duplicates("_cod")
    print(f"  total: {leidas:,} filas · {len(tabla):,} licitaciones distintas")
    return tabla


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--viejo", required=True, help="carpeta CSV del corte de referencia")
    p.add_argument("--nuevo", required=True, help="carpeta CSV de la re-descarga")
    p.add_argument("--tipo", choices=["lic", "oc"], default="lic")
    p.add_argument("--anios", nargs="*", type=int, default=[])
    p.add_argument("--meses", nargs="*", type=int, default=[])
    p.add_argument("--encoding", default="latin-1")
    p.add_argument("--sep", default=";")
    p.add_argument("--salida", default="codigos_perdidos.csv")
    p.add_argument("--max-csv", type=int, default=50000,
                   help="tope de filas del csv de perdidas")
    a = p.parse_args(argv)

    print("=" * 78)
    print("PASO 0-C · QUE SON LAS LICITACIONES QUE EL CORTE NUEVO NO TRAE")
    print("=" * 78)

    viejo = recorrer(a.viejo, a.tipo, a.encoding, a.sep, True, "VIEJO")
    nuevo = recorrer(a.nuevo, a.tipo, a.encoding, a.sep, False, "NUEVO")

    if a.anios:
        viejo = viejo[viejo["_yy"].isin(a.anios)]
        nuevo = nuevo[nuevo["_yy"].isin(a.anios)]
    if a.meses:
        viejo = viejo[viejo["_mm"].isin(a.meses)]
        nuevo = nuevo[nuevo["_mm"].isin(a.meses)]

    en_nuevo = viejo["_cod"].isin(set(nuevo["_cod"]))
    perdidas = viejo[~en_nuevo]
    comunes = viejo[en_nuevo]
    solo_nuevo = nuevo[~nuevo["_cod"].isin(set(viejo["_cod"]))]

    print("\n" + "=" * 78)
    print("CONJUNTOS, MES A MES")
    print("=" * 78)
    print(f"  {'mes':<10}{'viejo':>10}{'nuevo':>10}{'comun':>10}"
          f"{'solo viejo':>12}{'solo nuevo':>12}")
    print("  " + "-" * 64)
    cv = viejo.groupby(["_yy", "_mm"]).size()
    cn = nuevo.groupby(["_yy", "_mm"]).size()
    cc = comunes.groupby(["_yy", "_mm"]).size()
    cp = perdidas.groupby(["_yy", "_mm"]).size()
    cs = solo_nuevo.groupby(["_yy", "_mm"]).size()
    for k in sorted(set(cv.index) | set(cn.index)):
        yy, mm = k
        print(f"  {yy}-{mm:02d}  {cv.get(k,0):>10,}{cn.get(k,0):>10,}"
              f"{cc.get(k,0):>10,}{cp.get(k,0):>12,}{cs.get(k,0):>12,}")
    print("  " + "-" * 64)
    print(f"  {'TOTAL':<10}{len(viejo):>10,}{len(nuevo):>10,}"
          f"{len(comunes):>10,}{len(perdidas):>12,}{len(solo_nuevo):>12,}")

    if perdidas.empty:
        print("\n  No hay ninguna licitacion del corte viejo ausente del nuevo en el")
        print("  rango pedido. El corte nuevo es superconjunto POR CONTEO Y POR")
        print("  CODIGO aqui. (Por CONTENIDO — campos que cambian de valor — sigue")
        print("  siendo CAP-15 el que decide.)")
        return

    # -------------------------------------------------------------------------
    # LA PREGUNTA QUE DECIDE: ¿que estado tienen las perdidas?
    # -------------------------------------------------------------------------
    if "_estado" not in viejo.columns:
        print("\n  [aviso] no se resolvio la columna de ESTADO en los CSV del lado")
        print("  viejo, asi que no se puede separar A) de B) por estado. Agregar el")
        print("  nombre real a CAND_EST y volver a correr.")
    else:
        est_perd = perdidas["_estado"].replace("", "(vacio)").value_counts()
        est_com = comunes["_estado"].replace("", "(vacio)").value_counts()
        n_p, n_c = int(est_perd.sum()), int(est_com.sum())
        print("\n" + "=" * 78)
        print("ESTADO DE LAS PERDIDAS vs ESTADO DE LAS QUE SI ESTAN")
        print("=" * 78)
        print("  Si las perdidas son abrumadoramente un estado 'abierto' (Publicada,")
        print("  En Proceso...), la historia es A): viven en Vigente y se bajan.")
        print("  Si se reparten como las demas, es B): la fuente las borro.\n")
        print(f"  {'estado':<34}{'perdidas':>10}{'% perd':>9}{'comunes':>11}{'% com':>9}")
        print("  " + "-" * 73)
        for est, n in est_perd.head(14).items():
            nc = int(est_com.get(est, 0))
            print(f"  {str(est)[:34]:<34}{n:>10,}{100.0*n/n_p:>8.1f}%"
                  f"{nc:>11,}{(100.0*nc/n_c if n_c else 0):>8.1f}%")
        faltan = [e for e in est_com.index if e not in set(est_perd.index)]
        if faltan:
            print(f"  ... y {len(faltan)} estado(s) que NO aparecen en las perdidas")

        top_e = est_perd.index[0]
        top_n = int(est_perd.iloc[0])
        pc_com = (100.0 * int(est_com.get(top_e, 0)) / n_c) if n_c else 0.0
        print("\n  LECTURA:")
        if top_n / n_p > 0.6 and pc_com < 30.0:
            print(f"    El {100.0*top_n/n_p:.0f}% de las perdidas esta en estado {str(top_e)!r},")
            print(f"    contra {pc_com:.0f}% de las que si estan. Las perdidas son UNA")
            print("    POBLACION, no una muestra al azar. Historia A): buscar esa")
            print("    poblacion en la carpeta que la re-descarga no incluyo, antes de")
            print("    declarar que la fuente borro nada.")
        else:
            print("    Las perdidas se reparten entre estados de forma parecida a las")
            print("    que si estan. No son 'los procesos abiertos': es un corte")
            print("    transversal del mes. Eso apunta a B) — el archivo mensual de")
            print("    origen cambio de contenido entre una descarga y la otra — y")
            print("    convierte esto en un hallazgo del informe, no en un tramite.")

    # -------------------------------------------------------------------------
    # CSV de perdidas
    # -------------------------------------------------------------------------
    salida = perdidas.sort_values(["_yy", "_mm", "_cod"]).head(a.max_csv)
    salida = salida.rename(columns={"_cod": "codigo_externo", "_yy": "anio_pub",
                                    "_mm": "mes_pub", "_estado": "estado"})
    salida.to_csv(a.salida, index=False, sep=";", encoding="utf-8-sig")
    print(f"\n  Escrito: {a.salida}  ({len(salida):,} fila(s)"
          f"{' — TRUNCADO por --max-csv' if len(salida) >= a.max_csv else ''})")
    print("  Ese archivo es la lista que CAP-15 tendria que reencontrar. Guardalo")
    print("  aunque el veredicto sea A): es la evidencia de que se reviso.")

    print("\nPASO 0-C termina. Pegar esta salida completa en el chat.")


if __name__ == "__main__":
    main()
