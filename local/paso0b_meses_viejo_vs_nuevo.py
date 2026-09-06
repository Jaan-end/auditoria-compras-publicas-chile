# -*- coding: utf-8 -*-
"""
paso0b_meses_viejo_vs_nuevo.py — Capstone Big Data · Mercado Publico
=====================================================================
CORRE EN TU PC. Cero Databricks, cero cupo.

QUE CONTESTA
------------
El PASO 0 (RUN_ID_P0=770618aa84a4) dio NO-GO por conteo anual:

    anio   lic viejo   lic nuevo      delta
    2023     149.411     149.584       +173   (+0,12%)
    2024     156.470     154.142     -2.328   (-1,49%)
    2025     109.398     100.532     -8.866   (-8,10%)
    2026      52.603       (s/d)         s/d

Un delta anual no dice POR QUE. Hay dos explicaciones rivales, y las dos
predicen el mismo total anual pero una tabla MENSUAL distinta:

  H1 · FALTA UN MES EN LA DESCARGA.
       El deficit se concentra en uno o dos meses que valen ~0 o muy poco, y
       el resto de los meses calza casi exacto con el corte viejo.
       -> El diagnostico es trivial: volver a bajar ese mes. El corte nuevo
          sirve. (8.866 es sospechosamente parecido a un mes de 2025, que
          promedia 9.116 licitaciones.)

  H2 · SE BAJO Historico DONDE ANTES HABIA Vigente (o al reves).
       El deficit CRECE hacia el presente: 2023 calza, 2024 empieza a faltar
       hacia fin de anio, 2025 falta parejo y mas fuerte en los ultimos meses.
       Ningun mes vale cero; todos valen un poco menos.
       -> El corte nuevo NO es comparable con el viejo y hay que rearmar la
          descarga con las cuatro carpetas, no con una.

Una sola tabla mes a mes las separa. Este script la construye contando el
corte NUEVO con la MISMA definicion que uso el PASO 0 sobre el viejo:
licitaciones distintas (codigo_externo) por MES DE PUBLICACION.

USO
---
    python paso0b_meses_viejo_vs_nuevo.py --nuevo "D:/descarga_2026" --tipo lic

    # si la re-descarga vive en otra carpeta por tipo:
    python paso0b_meses_viejo_vs_nuevo.py --nuevo "D:/descarga_2026/Licitaciones" --tipo lic

Requiere pandas. Lee en latin-1 y separador ';' por defecto, igual que
`preparar_corte_v2.py`, porque eso declaran los manifiestos del build original.
Lee SOLO dos columnas (codigo externo y fecha de publicacion), asi que es
mucho mas rapido que la conversion a parquet.

LO QUE ESTE SCRIPT NO HACE
--------------------------
No prueba que el corte nuevo sea un superconjunto por CONTENIDO: dos meses
pueden tener el mismo conteo y distintas licitaciones adentro. Eso lo prueba
CAP-15 en Databricks. Esto es el paso barato de antes.
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

# -----------------------------------------------------------------------------
# CORTE VIEJO — licitaciones distintas por mes de publicacion.
# Fuente: PASO 0, RUN_ID_P0=770618aa84a4, seccion 0.C (28-ago-2026).
# NO editar a mano: si se vuelve a correr el PASO 0, pegar la tabla nueva.
# -----------------------------------------------------------------------------
VIEJO = {
    2023: {1: 10232, 2: 10789, 3: 12872, 4: 11610, 5: 13531, 6: 12810,
           7: 12881, 8: 14002, 9: 9088, 10: 14563, 11: 16556, 12: 10477},
    2024: {1: 11685, 2: 12439, 3: 12166, 4: 14848, 5: 14096, 6: 12596,
           7: 13939, 8: 14497, 9: 11496, 10: 15410, 11: 15068, 12: 8230},
    2025: {1: 7775, 2: 8475, 3: 9302, 4: 10321, 5: 9575, 6: 9516,
           7: 10320, 8: 9611, 9: 8765, 10: 10041, 11: 8627, 12: 7070},
    2026: {1: 7198, 2: 7526, 3: 7895, 4: 7782, 5: 7458, 6: 8368,
           7: 6317, 8: 59},
}

CAND_COD = ["codigo_externo", "CodigoExterno", "Codigo Externo",
            "codigoexterno", "CodigoLicitacion"]
CAND_FPUB = ["FechaPublicacion", "fecha_publicacion", "Fecha Publicacion",
             "FechaPublicacionLicitacion", "fecha_creacion", "FechaCreacion"]


def norm(s):
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def resolver(cols, candidatos):
    reales = {norm(c): c for c in cols}
    for cand in candidatos:
        if norm(cand) in reales:
            return reales[norm(cand)]
    return None


def descubrir(raiz):
    """[(etiqueta, ruta_fisica, interno_o_None)] para cada csv suelto o en zip."""
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


def anio_de_ruta(ruta):
    """Ultimo 20xx de la ruta: sirve solo para filtrar por --anios y para el
    aviso de archivo cuyo contenido no es del anio de su nombre."""
    cands = re.findall(r"(20[0-9]{2})", str(ruta).replace("\\", "/"))
    return int(cands[-1]) if cands else None


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
    linea = raw.decode(enc, errors="replace").splitlines()[0] if raw else ""
    linea = linea.lstrip("\ufeff")
    if sep == "auto":
        sep = ";" if linea.count(";") >= linea.count(",") and linea.count(";") else ","
    return [c.strip().strip('"') for c in linea.split(sep)], sep


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--nuevo", required=True, help="carpeta de la re-descarga 2023-2026")
    p.add_argument("--tipo", choices=["lic", "oc"], default="lic")
    p.add_argument("--anios", nargs="*", type=int, default=[2023, 2024, 2025, 2026])
    p.add_argument("--encoding", default="latin-1")
    p.add_argument("--sep", default=";")
    a = p.parse_args(argv)

    print("=" * 78)
    print("PASO 0-B · LICITACIONES POR MES DE PUBLICACION — VIEJO vs NUEVO")
    print("=" * 78)
    print(f"  carpeta nueva : {a.nuevo}")
    print(f"  tipo          : {a.tipo}   encoding: {a.encoding}   sep: {a.sep!r}")

    archivos = descubrir(a.nuevo)
    if not archivos:
        sys.exit(f"\nNo hay csv/zip bajo {a.nuevo}")

    pat_tipo = re.compile(r"lic" if a.tipo == "lic" else r"(oc|orden)", re.I)
    sel = [t for t in archivos if pat_tipo.search(t[0]) or pat_tipo.search(t[1])]
    if not sel:
        print(f"\n  [aviso] ningun nombre de archivo dice {a.tipo!r}; se toman los "
              f"{len(archivos)} encontrados y se resuelve por cabecera.")
        sel = archivos
    print(f"  archivos a leer: {len(sel)}")

    codigos = defaultdict(set)      # (anio, mes) de PUBLICACION -> set de codigos
    filas_mes = Counter()
    sin_fecha = Counter()           # archivo -> filas sin fecha parseable
    aporte = defaultdict(Counter)   # archivo -> Counter de (anio,mes) publicados
    leidos = 0

    for etiqueta, ruta, interno in sel:
        y_arch = anio_de_ruta(interno or ruta)
        if a.anios and y_arch is not None and y_arch not in a.anios:
            continue
        try:
            cols, sep = cabecera(ruta, interno, a.encoding, a.sep)
        except Exception as e:                                  # noqa: BLE001
            print(f"   [aviso] no se pudo leer la cabecera de {etiqueta}: {e}")
            continue
        c_cod = resolver(cols, CAND_COD)
        c_fp = resolver(cols, CAND_FPUB)
        if not c_cod or not c_fp:
            print(f"   [PARAR] {etiqueta}: no se resolvio "
                  f"{'codigo_externo' if not c_cod else 'fecha_publicacion'}. "
                  f"Cabecera: {cols[:8]} ...")
            continue

        n_arch = 0
        fuente = abrir(ruta, interno, a.encoding)
        try:
            lector = pd.read_csv(fuente, sep=sep, usecols=[c_cod, c_fp], dtype=str,
                                 chunksize=CHUNK, quotechar='"', on_bad_lines="skip",
                                 keep_default_na=False, na_values=[""],
                                 low_memory=False)
            for trozo in lector:
                cod = trozo[c_cod].astype(str).str.strip()
                fp = pd.to_datetime(trozo[c_fp].astype(str).str.slice(0, 10),
                                    errors="coerce", format="%Y-%m-%d")
                malas = fp.isna()
                if malas.any():
                    fp2 = pd.to_datetime(trozo.loc[malas, c_fp].astype(str).str.slice(0, 10),
                                         errors="coerce", dayfirst=True)
                    fp.loc[malas] = fp2
                sin_fecha[etiqueta] += int(fp.isna().sum())
                ok = fp.notna() & (cod != "") & (cod.str.lower() != "nan")
                for (yy, mm), grupo in cod[ok].groupby([fp[ok].dt.year, fp[ok].dt.month]):
                    clave = (int(yy), int(mm))
                    codigos[clave].update(grupo.tolist())
                    filas_mes[clave] += len(grupo)
                    aporte[etiqueta][clave] += len(grupo)
                n_arch += len(trozo)
        except Exception as e:                                  # noqa: BLE001
            print(f"   [aviso] {etiqueta}: {type(e).__name__}: {e}")
        finally:
            try:
                fuente.close()
            except Exception:                                   # noqa: BLE001
                pass
        leidos += n_arch
        print(f"      {etiqueta[:56]:<56} {n_arch:>12,} filas")

    print(f"\n  total leido: {leidos:,} filas")

    # -------------------------------------------------------------------------
    # TABLA MES A MES
    # -------------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("TABLA MES A MES  (licitaciones distintas por mes de PUBLICACION)")
    print("=" * 78)
    print(f"  {'mes pub':<10}{'viejo':>11}{'nuevo':>11}{'delta':>11}{'delta %':>10}   marca")
    print("  " + "-" * 66)

    huecos, caidas, deficit_total = [], [], 0
    for y in sorted(VIEJO):
        if a.anios and y not in a.anios:
            continue
        for m in sorted(VIEJO[y]):
            v = VIEJO[y][m]
            n = len(codigos.get((y, m), ()))
            d = n - v
            deficit_total += min(d, 0)
            if n == 0:
                marca = "*** MES AUSENTE EN EL CORTE NUEVO"
                huecos.append((y, m, v))
            elif v and d < 0 and abs(d) > 0.5 * v:
                marca = "*** el nuevo trae menos de la mitad"
                huecos.append((y, m, v))
            elif v and d < 0 and abs(d) > 0.02 * v:
                marca = "caida > 2%"
                caidas.append((y, m, d, 100.0 * d / v))
            elif d < 0:
                marca = "-"
                caidas.append((y, m, d, 100.0 * d / v if v else 0.0))
            else:
                marca = "ok"
            print(f"  {y}-{m:02d}   {v:>11,}{n:>11,}{d:>+11,}"
                  f"{(100.0*d/v if v else 0):>+9.2f}%   {marca}")

    # meses que el nuevo trae y el viejo no
    extras = [(y, m) for (y, m) in sorted(codigos)
              if m not in VIEJO.get(y, {}) and (not a.anios or y in a.anios)]
    if extras:
        print("\n  MESES QUE SOLO EXISTEN EN EL CORTE NUEVO (el viejo no llegaba):")
        for y, m in extras:
            print(f"    {y}-{m:02d}   {len(codigos[(y, m)]):>11,} licitaciones")

    fuera = [(y, m) for (y, m) in sorted(codigos) if y not in VIEJO]
    if fuera:
        print("\n  PUBLICACIONES FUERA DEL RANGO 2023-2026 (revisar: el filtro de la")
        print("  descarga no es por mes de publicacion, o hay fechas corruptas):")
        for y, m in fuera[:24]:
            print(f"    {y}-{m:02d}   {len(codigos[(y, m)]):>11,}")
        if len(fuera) > 24:
            print(f"    ... y {len(fuera)-24} mes(es) mas")

    malas = sum(sin_fecha.values())
    if malas:
        print(f"\n  filas sin fecha de publicacion parseable: {malas:,} "
              f"({100.0*malas/leidos if leidos else 0:.3f}% de lo leido)")
        for e, n in sin_fecha.most_common(5):
            if n:
                print(f"    {e[:56]:<56} {n:>12,}")

    # -------------------------------------------------------------------------
    # VEREDICTO
    # -------------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("VEREDICTO — que hipotesis sobrevive")
    print("=" * 78)
    print(f"  deficit total (suma de los deltas negativos): {deficit_total:+,}")
    if huecos:
        print(f"\n  H1 GANA (hueco de descarga). {len(huecos)} mes(es) faltan enteros o")
        print("  casi enteros:")
        for y, m, v in huecos:
            print(f"      {y}-{m:02d}   el viejo tiene {v:,} licitaciones, el nuevo casi nada")
        print("\n  ACCION: volver a bajar ESOS meses de ChileCompra y repetir este")
        print("  script. No se convierte nada a parquet hasta que la tabla cierre.")
        print("  El resto del corte nuevo no queda invalidado por esto.")
    elif caidas:
        peor = sorted(caidas, key=lambda t: t[3])[:6]
        recientes = [t for t in caidas if (t[0], t[1]) >= (2025, 1)]
        print("\n  NINGUN mes falta entero: el deficit esta REPARTIDO. Eso descarta H1.")
        print(f"  meses con caida: {len(caidas)}   peores:")
        for y, m, d, pc in peor:
            print(f"      {y}-{m:02d}   {d:>+9,}  ({pc:+.2f}%)")
        if len(recientes) >= 0.6 * len(caidas):
            print("\n  H2 GANA (carpeta equivocada). La caida se concentra hacia el")
            print("  presente, que es justo donde viven los procesos todavia abiertos.")
            print("  Un proceso abierto esta en Vigente, no en Historico: bajar solo")
            print("  Historico de un anio reciente deja fuera los procesos largos.")
            print("  ACCION: revisar de QUE carpeta de ChileCompra salio cada archivo")
            print("  (Licitaciones_Nacional_Historico vs _Vigente) antes de convertir.")
        else:
            print("\n  La caida NO se concentra en el presente. No es ni un mes")
            print("  faltante ni el corte Historico/Vigente: hay un tercer motivo")
            print("  (filtro de region, de tipo de licitacion, o truncamiento de")
            print("  archivo). Mirar el conteo de FILAS por archivo de arriba: un")
            print("  archivo truncado se ve como filas redondas o muy por debajo de")
            print("  sus vecinos.")
    else:
        print("\n  El corte nuevo NO pierde licitaciones en ningun mes. El NO-GO del")
        print("  PASO 0 venia de la cifra anual de v2_censura_por_anio.csv, que se")
        print("  calculo con otra definicion. Recontar 0.D con ESTA tabla antes de")
        print("  decidir. Superconjunto por conteo sigue sin ser superconjunto por")
        print("  contenido: eso lo prueba CAP-15.")

    print("\nPASO 0-B termina. Pegar esta salida completa en el chat.")


if __name__ == "__main__":
    main()
