"""
enriquecer_muestra_30_casos.py — sesión 31 (31-ago-2026)

QUÉ HACE. Cruza, FUERA de Databricks, dos archivos que hasta hoy nunca se habían
juntado:

  1. resultados/cap11c_muestra_reponderada.csv
     Las 300 líneas de CAP-11 ya reponderadas (pesos de Horvitz-Thompson: pi,
     w_lineas, w_clp). Trae onu8, texto, tipo_oc, anio — pero NO trae
     codigo_licitacion, codigo_oc, ni el par de códigos ONU lic/OC.

  2. resultados/cap11d_enriquecido_30_casos.csv
     Salida de la celda NUEVA "CAP-11-D" (agregada a
     notebooks/etapa2_ANEXO_PROFESOR_v19.py, sesión 31). Trae, para las MISMAS
     300 líneas (mismo hash `_id`, verificado dentro de esa misma celda de
     Databricks): codigo_licitacion, codigo_oc, si el ONU declarado estaba o no
     en la licitación de origen, cuántas líneas/ONU distintos tiene esa
     licitación, y el CLP real de la línea (no el del estrato).

POR QUÉ DOS ARCHIVOS Y NO UNO. cap11c ya está calculado (reponderación hecha
una vez, cara de rehacer). cap11d es la reparación mínima: agrega SOLO lo que
faltaba, sobre la MISMA selección determinista de 300 líneas. Este script
verifica que son, de verdad, la misma selección (mismos 300 `_id`) antes de
confiar en el cruce — es la comprobación de reproducibilidad que pide la
regla §3.14 del proyecto, y NO es opcional: si los `_id` no calzan 300 de 300,
el script se detiene y no escribe nada, porque eso significaría que algo
cambió entre las dos corridas de Databricks (CAP-11 original vs. CAP-11-D) y
la selección de 300 ya no es "la misma con más columnas".

QUÉ NO HACE. No decide la URL de la ficha por sí solo: `mercadopublico.cl` no
tiene un esquema de URL confirmado en fuente primaria dentro de este proyecto
(los parámetros de consulta del sitio suelen ir cifrados, no ser el código
plano). Este script deja la columna `url_ficha` marcada
"[POR VERIFICAR: confirmar patrón de URL en mercadopublico.cl antes de usar]"
y expone URL_LICITACION_TEMPLATE / URL_OC_TEMPLATE al principio del archivo
para que, UNA VEZ verificado el patrón real abriendo una ficha en el
navegador, se complete aquí y se vuelva a correr — no antes.

CÓMO SE USA.
    python enriquecer_muestra_30_casos.py \
        --reponderada resultados/cap11c_muestra_reponderada.csv \
        --enriquecida resultados/cap11d_enriquecido_30_casos.csv \
        --salida resultados/tabla_30_casos.csv

Requiere solo pandas. No toca Spark ni Databricks.
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd

# =============================================================================
# CONFIGURACIÓN — completar SOLO después de verificar en fuente primaria
# (abrir una ficha real de licitación y una de OC en mercadopublico.cl y
# copiar el patrón exacto de la URL). Mientras estén en None, el script deja
# la columna `url_ficha` marcada como pendiente de verificar, y NO inventa nada.
# =============================================================================
URL_LICITACION_TEMPLATE: str | None = None  # p.ej. "https://www.mercadopublico.cl/.../{codigo}"
URL_OC_TEMPLATE: str | None = None          # p.ej. "https://www.mercadopublico.cl/.../{codigo}"

# Cuotas del protocolo F10 (docs/00-PLAN-19-DIAS-Y-FICHAS-DE-TAREA.md, sección "F10")
CUOTA_SI_ESTABA = 10
CUOTA_NO_ESTABA = 10
CUOTA_TESTIMONIAL = 5
CUOTA_EXTREMOS_PRECIO = 5


def cargar_y_verificar(ruta_reponderada: str, ruta_enriquecida: str) -> pd.DataFrame:
    reponderada = pd.read_csv(ruta_reponderada, dtype={"_id": str})
    enriquecida = pd.read_csv(ruta_enriquecida, dtype={"_id": str})

    ids_reponderada = set(reponderada["_id"])
    ids_enriquecida = set(enriquecida["_id"])
    solo_reponderada = ids_reponderada - ids_enriquecida
    solo_enriquecida = ids_enriquecida - ids_reponderada

    print(f"_id en cap11c_muestra_reponderada.csv ... {len(ids_reponderada):,}")
    print(f"_id en cap11d_enriquecido_30_casos.csv ... {len(ids_enriquecida):,}")
    print(f"presentes en ambos ....................... {len(ids_reponderada & ids_enriquecida):,}")

    if solo_reponderada or solo_enriquecida:
        print(f"\n❌ DIFERENCIA: {len(solo_reponderada)} solo en la reponderada, "
              f"{len(solo_enriquecida)} solo en la enriquecida.")
        print("NO se escribe ningún archivo de salida. Esto significa que la corrida de")
        print("CAP-11-D en Databricks NO reprodujo exactamente la misma selección de 300")
        print("líneas que produjo CAP-11 cuando se generó cap11c_muestra_reponderada.csv.")
        print("Antes de seguir: revisar si cambió `N_MUESTRA`, el filtro `_f_eval` de CAP-10,")
        print("o el corte de datos (df_oc_all) entre esa corrida y esta.")
        sys.exit(1)

    print("\n✅ Los 300 `_id` coinciden exactamente entre los dos archivos. Cruce válido.")

    fusion = reponderada.merge(enriquecida, on="_id", how="inner", suffixes=("_reponderada", ""))
    assert len(fusion) == len(reponderada), "el merge no debería cambiar el número de filas"
    return fusion


def clasificar_grupo(fusion: pd.DataFrame) -> pd.DataFrame:
    """Asigna el `grupo` del protocolo F10 a cada línea, cuando la evidencia alcanza."""
    df = fusion.copy()

    es_testimonial = (df["n_lineas_lic"].fillna(0) >= 2) & (df["n_onu_distintos_lic"].fillna(99) <= 1)

    # Los extremos de dispersión se leen DENTRO de cada _estrato (rubro x mecanismo x
    # tramo de monto), porque comparar precio de un CM de bajo monto contra una LR de
    # alto monto no dice nada — el protocolo original habla de "extremos de dispersión
    # de precio" y el criterio comparable disponible en esta muestra es el que ya usa
    # la reponderación: el `_estrato` de CAP-11.
    rango_precio = df.groupby("_estrato")["clp"].rank(pct=True)
    es_extremo_precio = (rango_precio <= 0.10) | (rango_precio >= 0.90)

    condiciones = [
        df["onu_declarado_en_licitacion"] == True,   # noqa: E712
        df["onu_declarado_en_licitacion"] == False,  # noqa: E712
        es_testimonial,
        es_extremo_precio,
    ]
    etiquetas = [
        "1_SI_estaba_en_licitacion",
        "2_NO_estaba_en_licitacion",
        "3_testimonial",
        "4_extremo_dispersion_precio",
    ]
    # Un caso puede calificar para más de un grupo (p.ej. testimonial Y extremo de
    # precio). Se prioriza en el orden del protocolo: 1, 2, 3, 4. Queda declarado en
    # `grupos_posibles` para no esconder el solapamiento.
    df["grupos_posibles"] = [
        "|".join(et for et, cond in zip(etiquetas, [c1, c2, c3, c4]) if cond)
        for c1, c2, c3, c4 in zip(*condiciones)
    ]
    df["grupo_prioritario"] = None
    for etiqueta, cond in zip(etiquetas, condiciones):
        sin_grupo = df["grupo_prioritario"].isna()
        df.loc[sin_grupo & cond, "grupo_prioritario"] = etiqueta

    return df


def _diversificar(df_grupo: pd.DataFrame, cuota: int) -> pd.DataFrame:
    """Toma hasta `cuota` filas del grupo, repartiendo por año y por mecanismo
    (protocolo F10: 'diversificar cada grupo por año y por mecanismo de compra').
    Si el grupo tiene menos filas que la cuota, se declara — no se rellena con otra
    cosa."""
    if len(df_grupo) <= cuota:
        return df_grupo
    df_grupo = df_grupo.sort_values(["anio", "tipo_oc"])
    paso = max(1, len(df_grupo) // cuota)
    elegidos = df_grupo.iloc[::paso].head(cuota)
    if len(elegidos) < cuota:
        resto = df_grupo.drop(elegidos.index)
        elegidos = pd.concat([elegidos, resto.head(cuota - len(elegidos))])
    return elegidos


def armar_tabla_30(df: pd.DataFrame) -> pd.DataFrame:
    cuotas = {
        "1_SI_estaba_en_licitacion": CUOTA_SI_ESTABA,
        "2_NO_estaba_en_licitacion": CUOTA_NO_ESTABA,
        "3_testimonial": CUOTA_TESTIMONIAL,
        "4_extremo_dispersion_precio": CUOTA_EXTREMOS_PRECIO,
    }
    piezas = []
    print("\nCobertura real de cada grupo, ANTES de aplicar la cuota:")
    for etiqueta, cuota in cuotas.items():
        sub = df[df["grupo_prioritario"] == etiqueta]
        print(f"   {etiqueta:<30} disponibles={len(sub):>4}  cuota={cuota}")
        if len(sub) < cuota:
            print(f"      ⚠ el grupo no llena la cuota — el protocolo pide {cuota}, "
                  f"hay {len(sub)}. Declarar esta salvedad, no forzar el número.")
        piezas.append(_diversificar(sub, cuota))

    tabla = pd.concat(piezas, ignore_index=True)
    tabla = tabla.sort_values(["grupo_prioritario", "anio"]).reset_index(drop=True)
    tabla.insert(0, "n", range(1, len(tabla) + 1))

    tabla["url_ficha_licitacion"] = (
        tabla["codigo_licitacion"].map(URL_LICITACION_TEMPLATE.format)
        if URL_LICITACION_TEMPLATE else
        "[POR VERIFICAR: confirmar patrón de URL en mercadopublico.cl antes de usar]"
    )
    tabla["url_ficha_oc"] = (
        tabla["codigo_oc"].map(URL_OC_TEMPLATE.format)
        if URL_OC_TEMPLATE else
        "[POR VERIFICAR: confirmar patrón de URL en mercadopublico.cl antes de usar]"
    )

    salida = tabla.rename(columns={
        "grupo_prioritario": "grupo",
        "codigo_licitacion": "codigo_licitacion",
        "codigo_oc": "codigo_oc",
        "onu8": "onu_oc",
        "anio": "anio",
        "tipo_oc": "mecanismo",
    })[[
        "n", "grupo", "codigo_licitacion", "codigo_oc", "onu_oc",
        "onu_declarado_en_licitacion", "anio", "mecanismo",
        "url_ficha_licitacion", "url_ficha_oc", "grupos_posibles", "clp",
    ]]
    return salida


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--reponderada", default="resultados/cap11c_muestra_reponderada.csv")
    ap.add_argument("--enriquecida", default="resultados/cap11d_enriquecido_30_casos.csv")
    ap.add_argument("--salida", default="resultados/tabla_30_casos.csv")
    args = ap.parse_args()

    print("=" * 78)
    print("enriquecer_muestra_30_casos.py — cruce y armado de la tabla de 30 casos")
    print("=" * 78)

    fusion = cargar_y_verificar(args.reponderada, args.enriquecida)
    clasificada = clasificar_grupo(fusion)
    tabla = armar_tabla_30(clasificada)

    tabla.to_csv(args.salida, index=False)
    print(f"\nEscrita: {args.salida}  ({len(tabla)} filas)")
    print("\nRecordatorios antes de usar esta tabla:")
    print("  1. Las columnas url_ficha_* siguen [POR VERIFICAR] hasta confirmar en el")
    print("     navegador el esquema real de URL de mercadopublico.cl (una ficha de")
    print("     licitación y una de OC), y completar las plantillas al inicio de este")
    print("     archivo.")
    print("  2. La revisión de las 30 fichas la hace el usuario, a mano — es lo que")
    print("     convierte un porcentaje en una afirmación defendible (00-estado.md).")
    print("  3. Si algún grupo salió con menos casos que su cuota, está declarado arriba —")
    print("     no rellenar con otro grupo sin decirlo en el informe.")


if __name__ == "__main__":
    main()
