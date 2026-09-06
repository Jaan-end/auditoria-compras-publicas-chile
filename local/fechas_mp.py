# -*- coding: utf-8 -*-
"""
fechas_mp.py — parseo de fechas de Mercado Publico, con el centinela declarado
==============================================================================
Capstone Big Data · Mercado Publico · F26 §3.4 (28-ago-2026)

POR QUE EXISTE
--------------
El paso 0-D descubrio que ChileCompra usa **1900-01-01 como valor centinela**
de "sin fecha". Aparece como minimo en `FechaEstimadaAdjudicacion`,
`FechaSoporteFisico`, `FechaEstimadaFirma`, `FechaVisitaTerreno` y
`FechaEntregaAntecedentes`, y en `FechaPublicacion` explica los 31 codigos que
el paso 0-B reporto publicados en 1900-01.

Hoy todos los parsers del proyecto lo convierten en una fecha valida de 1900.
Eso NO es un error de parseo: es un error de interpretacion, y contamina
exactamente tres cosas:

  · cualquier `min()` de una columna de fecha -> da 1900 y no significa nada;
  · cualquier rezago publicacion -> otra fecha, que sale de -45.000 dias;
  · cualquier serie por anio, que gana un anio 1900 con volumen ridiculo.

La regla del proyecto es no inventar: un centinela NO es una fecha, es un NULL
declarado. Esta funcion lo trata como tal, y CUENTA cuantos encontro, porque
descartar en silencio es el otro error.

Se usa igual en pandas (PC) y en Spark (Databricks). Las dos versiones aplican
el MISMO criterio, a proposito: si divergen, las cifras del PC y las del
notebook dejan de ser comparables.

CENTINELAS
----------
`1900-01-01` es el unico confirmado [MEDIDO, paso 0-D, 28-ago-2026].
`1899-12-30` y `1970-01-01` se incluyen como candidatos habituales de Excel y
de epoch, y se cuentan por separado: si aparecen, hay que declararlo, no
asumirlo.
"""

import datetime as _dt

CENTINELAS = (_dt.date(1900, 1, 1), _dt.date(1899, 12, 30), _dt.date(1970, 1, 1))
ANIO_MIN_PLAUSIBLE = 2000     # Mercado Publico opera desde 2003


# -----------------------------------------------------------------------------
# PANDAS (PC)
# -----------------------------------------------------------------------------
def fecha_pandas(serie, contador=None, etiqueta=""):
    """Serie de texto -> serie de fechas, con centinelas convertidos a NaT.

    `contador` es un dict opcional donde se acumula cuantos centinelas se
    encontraron por columna. Si se pasa, el llamador DEBE imprimirlo.
    """
    import pandas as pd

    s = serie.astype(str).str.strip()
    fp = pd.to_datetime(s.str.slice(0, 10), errors="coerce", format="%Y-%m-%d")
    malas = fp.isna() & s.ne("") & s.str.lower().ne("nan")
    if malas.any():
        fp.loc[malas] = pd.to_datetime(s[malas].str.slice(0, 10),
                                       errors="coerce", dayfirst=True)
    es_cent = fp.dt.date.isin(CENTINELAS) | (fp.dt.year < ANIO_MIN_PLAUSIBLE)
    n_cent = int(es_cent.sum())
    if contador is not None and n_cent:
        contador[etiqueta or serie.name or "?"] = n_cent
    return fp.mask(es_cent)


def informe_centinelas(contador, total_filas=None):
    """Imprime lo que `fecha_pandas` acumulo. Nunca se descarta en silencio."""
    if not contador:
        print("   centinelas de fecha: ninguno.")
        return
    print("   CENTINELAS DE FECHA CONVERTIDOS A NULL (1900-01-01 y similares):")
    for col, n in sorted(contador.items(), key=lambda kv: -kv[1]):
        extra = f"  ({100.0 * n / total_filas:.3f}% de las filas)" if total_filas else ""
        print(f"      {col:<44} {n:>12,}{extra}")
    print("   No son fechas: son 'sin dato' escrito con una fecha. Declararlo en")
    print("   el informe junto a cualquier minimo, rezago o serie por anio.")


# -----------------------------------------------------------------------------
# SPARK (Databricks)
# -----------------------------------------------------------------------------
def fecha_spark(nombre_col, F):
    """Nombre de columna -> Column de fecha, con centinelas a NULL.

    `F` es `pyspark.sql.functions`. Se pasa el NOMBRE, no la Column, porque la
    expresion se arma en SQL para poder usar `try_to_date`.

    try_to_date y no to_date: `to_date` LANZA excepcion con texto invalido y
    mata el job entero. Eso ya paso el 28-ago (corrida c234b182170b, un literal
    '0' en una columna de fecha).
    """
    c = f"CAST(`{nombre_col}` AS STRING)"
    formatos = ("yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy", "yyyy/MM/dd")
    partes = []
    for f in formatos:
        partes.append(f"try_to_date({c}, '{f}')")
        partes.append(f"try_to_date(substring({c}, 1, 10), '{f}')")
    parseada = F.expr("COALESCE(" + ", ".join(partes) + ")")
    # Un centinela no es una fecha: es 'sin dato' escrito como fecha. Se corta
    # por anio para atrapar de una vez 1900-01-01, 1899-12-30 y cualquier otro
    # centinela viejo que aparezca despues.
    return F.when(F.year(parseada) < ANIO_MIN_PLAUSIBLE, None).otherwise(parseada)


def conteo_centinelas_spark(df, columnas, F):
    """Devuelve una expresion de agregacion por columna con el nº de centinelas.

    Se pensó para ir DENTRO de una agregacion que ya se va a correr: contar
    centinelas no vale una accion de Spark propia.
    """
    aggs = []
    for c in columnas:
        parseada = F.expr(
            "COALESCE(" + ", ".join(
                [f"try_to_date(CAST(`{c}` AS STRING), '{f}')" for f in
                 ("yyyy-MM-dd", "dd-MM-yyyy", "dd/MM/yyyy")]
                + [f"try_to_date(substring(CAST(`{c}` AS STRING), 1, 10), 'yyyy-MM-dd')"]
            ) + ")"
        )
        aggs.append(F.sum(
            (parseada.isNotNull() & (F.year(parseada) < ANIO_MIN_PLAUSIBLE)).cast("int")
        ).alias(f"cent_{c[:28]}"))
    return aggs
