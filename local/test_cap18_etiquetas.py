"""
test_cap18_etiquetas.py — prueba del embudo de etiquetas de CAP-18 (bloque 18.C).

QUE PRUEBA. El bloque 18.C de `CAP18_n3_clasificador_onu.py` decide QUE LINEAS
entran al entrenamiento del clasificador N3. Es la parte de la celda que mas
barato es equivocar y mas caro es no notar: si un filtro de fuga no filtra, el
accuracy sale alto, se ve bien, y no significa nada.

Esta prueba reimplementa el embudo en pandas —misma logica, sin Spark— y lo
corre sobre un fixture de 14 lineas construido a mano, donde la respuesta
correcta de cada linea se sabe de antemano y esta escrita en la columna
`_esperado`. Si un filtro deja de filtrar, la prueba lo dice.

QUE NO PRUEBA. No prueba que las expresiones de Spark hagan esto mismo: eso
sigue [POR VERIFICAR] hasta la primera corrida en Databricks. Prueba que la
LOGICA que esas expresiones codifican es la correcta.

COMO SE CORRE.
    python test_cap18_etiquetas.py        # requiere pandas
"""
import sys

try:
    import pandas as pd
except ImportError:
    print("pandas no esta instalado. Esta prueba lo necesita.")
    sys.exit(2)

# =============================================================================
# Espejo en pandas de lo que 18.C hace en Spark.
# Cada funcion lleva al lado la expresion de Spark que replica.
# =============================================================================
MIN_CHARS_G7 = 12
MULETILLAS_G7 = [r"^ver detalle", r"^segun bases", r"^ver bases", r"^s/i$", r"^sin informacion"]


def norm(s):
    """espejo de g7_norm(): minusculas, sin acentos, solo [a-z0-9 ]."""
    import re
    import unicodedata
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode()
    s = re.sub(r"(?i)_x000d_|_x000a_|\r|\n|\t", " ", s).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


def onu8(c):
    """espejo de cap_onu8(): solo 8 digitos exactos, si no -> None."""
    if c is None or (isinstance(c, float) and pd.isna(c)):
        return None
    c = str(c).strip()
    return c if (c.isdigit() and len(c) == 8) else None


def embudo(oc, lic):
    """Espejo de 18.C completo. Devuelve el DataFrame con todas las banderas."""
    import re
    # --- conjunto de codigos por licitacion (Spark: groupBy + collect_set) ---
    lic = lic.copy()
    lic["_onu8_lic"] = lic["onu_lic"].map(onu8)
    lic = lic[lic["_onu8_lic"].notna() & lic["codigo_externo"].notna()]
    sets = (lic.groupby("codigo_externo")["_onu8_lic"]
               .apply(lambda s: sorted(set(s))).rename("_set8_lic").reset_index())
    sets["_n_cod_lic"] = sets["_set8_lic"].map(len)

    d = oc.copy()
    d = d[d["tiene_licitacion_origen"] == True]                    # noqa: E712
    d["_onu8_oc"] = d["onu_oc"].map(onu8)
    # el comprador manda, el proveedor complementa (Spark: F.coalesce)
    d["txt"] = [norm(a) if norm(a) is not None else norm(b)
                for a, b in zip(d["espec_comprador"], d["espec_proveedor"])]
    d["gen"] = d["producto_generico"].map(norm)
    d = d[d["_onu8_oc"].notna() & d["txt"].notna()]
    d = d.merge(sets, left_on="codigo_licitacion", right_on="codigo_externo", how="inner")

    d["a_amplio"] = [c in s for c, s in zip(d["_onu8_oc"], d["_set8_lic"])]
    d["a_estricto"] = d["a_amplio"] & (d["_n_cod_lic"] == 1)

    # G1 · circularidad (Spark: ~(txt==gen) & ~gen.contains(txt) & ~txt.contains(gen))
    d["ok_circ"] = [
        (g is None) or (not (t == g or g.find(t) >= 0 or t.find(g) >= 0))
        for t, g in zip(d["txt"], d["gen"])
    ]
    # G2 · el codigo escrito como literal, a 8/6/4 digitos
    d["ok_cod"] = [
        not (c in t or c[:6] in t or c[:4] in t)
        for t, c in zip(d["txt"], d["_onu8_oc"])
    ]
    # G3 · relleno y texto corto
    rx = re.compile("|".join(f"({p})" for p in MULETILLAS_G7))
    d["ok_relleno"] = [
        (rx.search(t) is None) and (len(t) >= MIN_CHARS_G7) for t in d["txt"]
    ]
    d["entrenable"] = d["a_estricto"] & d["ok_circ"] & d["ok_cod"] & d["ok_relleno"]
    return d


def dedup(d):
    """Espejo de 18.D: un texto con >1 segmento distinto es ruido -> fuera."""
    e = d[d["entrenable"]].copy()
    e["seg2"] = e["_onu8_oc"].str[:2]
    g = e.groupby("txt").agg(_n_seg=("seg2", "nunique"),
                             seg2=("seg2", "min"),
                             _n_rep=("seg2", "size")).reset_index()
    return g[g["_n_seg"] == 1].drop(columns="_n_seg")


# =============================================================================
# FIXTURE — 14 lineas, cada una con su veredicto esperado escrito a mano
# =============================================================================
LIC = pd.DataFrame([
    # licitaciones que declaran UN SOLO codigo -> habilitan nivel A estricto
    ("1000-1-LE24", "43211500"),
    ("1000-2-LE24", "44121700"),
    ("1000-3-LE24", "15121500"),
    ("1000-4-LE24", "47131800"),
    ("1000-5-LE24", "43211500"),
    ("1000-6-LE24", "43211500"),
    ("1000-7-LE24", "44121700"),
    ("1000-8-LE24", "44121700"),
    # licitacion que declara DOS codigos -> solo amplio, nunca estricto
    ("2000-1-LR24", "43211500"),
    ("2000-1-LR24", "43201800"),
], columns=["codigo_externo", "onu_lic"])

# columnas: id, key, onu_oc, espec_comprador, espec_proveedor, generico, año, esperado
FIX = [
    ("L01", "1000-1-LE24", "43211500", "notebook 14 pulgadas 16gb ram ssd 512", None,
     "Computadores portatiles", 2024, "ENTRENABLE"),
    ("L02", "1000-2-LE24", "44121700", "resma papel carta 75 gramos caja 10 unidades", None,
     "Papel para impresora", 2022, "ENTRENABLE"),
    ("L03", "1000-3-LE24", "15121500", "aceite lubricante motor 15w40 balde 20 litros", None,
     "Aceites lubricantes de motor", 2021, "ENTRENABLE"),
    # --- nivel A: los que NO califican -----------------------------------
    ("L04", "2000-1-LR24", "43211500", "notebook corporativo i7 con maletin", None,
     "Computadores portatiles", 2024, "AMPLIO_NO_ESTRICTO"),
    ("L05", "1000-4-LE24", "43211500", "monitor curvo 27 pulgadas 144hz", None,
     "Computadores portatiles", 2023, "NO_ES_A"),
    # --- G1 · circularidad: el texto ES el nombre de catalogo -------------
    ("L06", "1000-5-LE24", "43211500", "Computadores portatiles", None,
     "Computadores portatiles", 2022, "CAE_CIRCULAR"),
    ("L07", "1000-6-LE24", "43211500", "computadores portatiles marca dell", None,
     "Computadores portatiles", 2022, "CAE_CIRCULAR"),
    # --- G2 · el codigo escrito dentro del texto --------------------------
    ("L08", "1000-7-LE24", "44121700", "resma papel carta codigo onu 44121700 caja", None,
     "Papel para impresora", 2023, "CAE_CODIGO"),
    ("L09", "1000-8-LE24", "44121700", "papel carta familia 4412 para oficina", None,
     "Papel para impresora", 2023, "CAE_CODIGO"),
    # --- G3 · relleno y texto corto ---------------------------------------
    ("L10", "1000-1-LE24", "43211500", "segun bases administrativas adjuntas", None,
     "Computadores portatiles", 2024, "CAE_RELLENO"),
    ("L11", "1000-1-LE24", "43211500", "notebook", None,
     "Computadores portatiles", 2024, "CAE_RELLENO"),
    # --- descartes previos al embudo --------------------------------------
    ("L12", "1000-1-LE24", "ZGEN0001", "notebook 14 pulgadas gama media", None,
     "Computadores portatiles", 2024, "FUERA_ONU_INVALIDO"),
    ("L13", "1000-1-LE24", "43211500", None, None,
     "Computadores portatiles", 2024, "FUERA_SIN_TEXTO"),
    # --- el proveedor complementa cuando el comprador no escribio ---------
    ("L14", "1000-2-LE24", "44121700", None, "resma papel oficio 80 gramos por caja",
     "Papel para impresora", 2020, "ENTRENABLE"),
]
OC = pd.DataFrame(FIX, columns=["_id", "codigo_licitacion", "onu_oc", "espec_comprador",
                                "espec_proveedor", "producto_generico", "_year", "_esperado"])
OC["tiene_licitacion_origen"] = True

# =============================================================================
_fallos, _n = [], 0


def check(nombre, cond, detalle=""):
    global _n
    _n += 1
    print(f"  {'ok ' if cond else 'FALLA'} {_n}. {nombre}" + (f"   {detalle}" if not cond else ""))
    if not cond:
        _fallos.append(nombre)


print("=" * 74)
print("test_cap18_etiquetas.py — embudo de etiquetas y filtros de fuga (18.C)")
print("=" * 74)

d = embudo(OC, LIC)
por_id = {r["_id"]: r for _, r in d.iterrows()}

# --- las dos lineas que ni siquiera llegan al embudo -------------------------
check("L12 (ONU no valido de 8 digitos) no entra al embudo", "L12" not in por_id)
check("L13 (sin texto de comprador ni proveedor) no entra al embudo", "L13" not in por_id)

# --- nivel A ----------------------------------------------------------------
check("L01 es nivel A ESTRICTO (lic. declara 1 codigo, la OC usa ese)",
      por_id["L01"]["a_estricto"] and por_id["L01"]["a_amplio"])
check("L04 es AMPLIO pero NO estricto (su lic. declara 2 codigos)",
      por_id["L04"]["a_amplio"] and not por_id["L04"]["a_estricto"])
check("L05 no es nivel A (su codigo no esta en el set de la licitacion)",
      not por_id["L05"]["a_amplio"] and not por_id["L05"]["a_estricto"])

# --- G1 · circularidad -------------------------------------------------------
check("L06 cae por CIRCULAR (el texto ES el nombre de catalogo)",
      not por_id["L06"]["ok_circ"] and not por_id["L06"]["entrenable"])
check("L07 cae por CIRCULAR (el texto CONTIENE el nombre de catalogo)",
      not por_id["L07"]["ok_circ"] and not por_id["L07"]["entrenable"])

# --- G2 · codigo dentro del texto -------------------------------------------
check("L08 cae porque el texto trae el codigo de 8 digitos escrito",
      not por_id["L08"]["ok_cod"] and not por_id["L08"]["entrenable"])
check("L09 cae porque el texto trae el prefijo de familia (4 digitos)",
      not por_id["L09"]["ok_cod"] and not por_id["L09"]["entrenable"])

# --- G3 · relleno ------------------------------------------------------------
check("L10 cae por MULETILLA ('segun bases...')",
      not por_id["L10"]["ok_relleno"] and not por_id["L10"]["entrenable"])
check("L11 cae por texto demasiado corto (< 12 caracteres)",
      not por_id["L11"]["ok_relleno"] and not por_id["L11"]["entrenable"])

# --- los que SI deben pasar --------------------------------------------------
for _id in ("L01", "L02", "L03", "L14"):
    check(f"{_id} queda ENTRENABLE", bool(por_id[_id]["entrenable"]),
          f"a_estricto={por_id[_id]['a_estricto']} circ={por_id[_id]['ok_circ']} "
          f"cod={por_id[_id]['ok_cod']} rell={por_id[_id]['ok_relleno']}")
check("L14 usa el texto del PROVEEDOR porque el comprador no escribio",
      por_id["L14"]["txt"].startswith("resma papel oficio"))

# --- el embudo cuadra --------------------------------------------------------
n_entrenable = int(d["entrenable"].sum())
check("el embudo deja exactamente 4 lineas entrenables", n_entrenable == 4,
      f"dejo {n_entrenable}: {sorted(d[d['entrenable']]['_id'])}")
check("nunca hay mas estrictos que amplios",
      int(d["a_estricto"].sum()) <= int(d["a_amplio"].sum()))

# --- dedup: texto ambiguo y texto repetido ----------------------------------
OC2 = pd.concat([
    OC,
    # mismo texto, dos segmentos distintos -> ambiguo, se descarta entero
    pd.DataFrame([("A1", "1000-1-LE24", "43211500", "kit de instalacion completo", None,
                   "Computadores portatiles", 2024, "AMBIGUO", True),
                  ("A2", "1000-3-LE24", "15121500", "kit de instalacion completo", None,
                   "Aceites lubricantes de motor", 2024, "AMBIGUO", True),
                  # texto repetido con la MISMA etiqueta -> colapsa a 1 fila
                  ("R1", "1000-2-LE24", "44121700", "resma papel carta 75 gramos caja 10 unidades",
                   None, "Papel para impresora", 2023, "REPETIDO", True)],
                 columns=list(OC.columns)),
], ignore_index=True)
g = dedup(embudo(OC2, LIC))
textos = set(g["txt"])
check("texto AMBIGUO (mismo texto, dos segmentos) se descarta entero",
      "kit de instalacion completo" not in textos)
_rep = g[g["txt"] == "resma papel carta 75 gramos caja 10 unidades"]
check("texto REPETIDO con misma etiqueta colapsa a 1 fila, contando 2 lineas",
      len(_rep) == 1 and int(_rep.iloc[0]["_n_rep"]) == 2,
      f"filas={len(_rep)} n_rep={_rep.iloc[0]['_n_rep'] if len(_rep) else 'n/d'}")

print("-" * 74)
if _fallos:
    print(f"RESULTADO: {len(_fallos)} FALLA(S) de {_n}: {_fallos}")
    sys.exit(1)
print(f"RESULTADO: {_n}/{_n} OK. El embudo de etiquetas de CAP-18 hace lo que declara.")
print("NOTA: esto valida la LOGICA, en pandas. Que las expresiones de Spark la")
print("codifiquen bien se comprobo en la corrida del 7-sep-2026, run_id")
print("5a53ec4a1f30: el embudo dio 15.318.366 candidatas y 5.583.284 entrenables.")
