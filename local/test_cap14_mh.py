# =============================================================================
#  test_cap14_mh.py — validacion de la matematica del driver de CAP-14
#  Capstone Big Data · Mercado Publico nacional 2017-2026
#
#  POR QUE EXISTE ESTE ARCHIVO (y por que se reescribio el 3-sep-2026)
#  ------------------------------------------------------------------
#  La cabecera de `notebooks/CAP14_oc030_vs_oferentes.py` declara:
#      "-> `test_cap14_mh.py`, incluido en el paquete. Corre en 1 segundo, sin Spark."
#  Ese archivo NO estaba en el paquete de traspaso (solo `test_cap15_verificacion.py`).
#  Es el patron del error #24 / regla §3.14: una afirmacion de verificacion que
#  no se puede señalar con un archivo y una ruta NO es una verificacion.
#  Este archivo repone esa prueba ANTES de que CAP-14 se corra y sus cifras se citen.
#
#  QUE VALIDA — Y QUE NO
#  ---------------------
#  VALIDA: la matematica del DRIVER (Mantel-Haenszel, varianza de
#  Robins-Breslow-Greenland, descarte de estratos, direccion del efecto,
#  robustez a celdas en cero y a listas vacias). Corre sin Spark, en ~1 segundo.
#
#  NO VALIDA: las expresiones de SPARK de 14.B (el join, los `collect_set`, los
#  prefijos UNSPSC, el `groupBy`). Siguen `[POR VERIFICAR]`, tal como declara la
#  cabecera de CAP-14. **La primera corrida de CAP-14 es una prueba, no un
#  resultado**: leer 14.C.1 antes que nada y comprobar que el numero de
#  licitaciones pareadas se parece a las 964.171 de la Pregunta 1. Si no se
#  parece, parar y revisar el join antes de gastar mas cupo.
#
#  COMO SE USA (pararse en la raiz del paquete)
#  --------------------------------------------
#      python local\test_cap14_mh.py
#  Sale con codigo 0 si pasan los 9 escenarios, 1 si falla alguno.
#
#  REQUISITOS DE ENTORNO
#  ---------------------
#  Python 3.8+, solo biblioteca estandar (math, sys). Sin pytest, sin Spark,
#  sin pandas. Determinista.
#
#  QUE PRODUCE
#  -----------
#  Solo salida por pantalla. No promueve ninguna cifra a [MEDIDO]: valida una
#  implementacion, no mide el mundo. Lo que habilita es CITAR el OR_MH que
#  CAP-14 imprima, sabiendo que el estimador esta probado.
#
#  ADVERTENCIA DE MANTENIMIENTO
#  ----------------------------
#  `_mh_copia()` es una COPIA VERBATIM de la funcion `_mh()` de
#  `notebooks/CAP14_oc030_vs_oferentes.py` (bloque 14.C, ~linea 300). Python no
#  puede importarla desde ahi: la celda ejecuta `cap_global(...)` en el momento
#  de importarse y solo existe dentro del kernel de Databricks. **Si se toca
#  `_mh()` en CAP-14, hay que copiar el cambio aqui, o esta prueba valida una
#  version que ya no corre.**
# =============================================================================

import math
import sys

MIN_LIC_POR_ESTRATO = 30      # mismo valor predeclarado que CAP-14

_fallos = []


# -----------------------------------------------------------------------------
# COPIA VERBATIM de _mh() de CAP-14 (bloque 14.C). Ver advertencia arriba.
# -----------------------------------------------------------------------------
def _mh_copia(celdas, clave_pred):
    estr = {}
    for c in celdas:
        k = (c["e_sector"], c["e_anio"], c["e_monto"])
        t = estr.setdefault(k, {(0, 0): 0, (0, 1): 0, (1, 0): 0, (1, 1): 0})
        t[(c[clave_pred], c["oferente_unico"])] += c["n"]

    num = den = 0.0
    usados = descartados = 0
    detalle = []
    for k, t in sorted(estr.items()):
        a = t[(1, 1)]
        b = t[(1, 0)]
        c_ = t[(0, 1)]
        d = t[(0, 0)]
        N = a + b + c_ + d
        if N < MIN_LIC_POR_ESTRATO or (a + b) == 0 or (c_ + d) == 0:
            descartados += 1
            continue
        usados += 1
        num += (a * d) / float(N)
        den += (b * c_) / float(N)
        P = (a + d) / float(N)
        Q = (b + c_) / float(N)
        R = (a * d) / float(N)
        S = (b * c_) / float(N)
        detalle.append((k, a, b, c_, d, N, R, S, P, Q))
    if den <= 0 or num <= 0:
        return None, None, None, detalle, usados, descartados
    OR = num / den
    sR = sum(x[6] for x in detalle)
    sS = sum(x[7] for x in detalle)
    t1 = sum(x[8] * x[6] for x in detalle) / (2 * sR * sR) if sR else 0.0
    t2 = sum(x[8] * x[7] + x[9] * x[6] for x in detalle) / (2 * sR * sS) if sR and sS else 0.0
    t3 = sum(x[9] * x[7] for x in detalle) / (2 * sS * sS) if sS else 0.0
    var = t1 + t2 + t3
    if var <= 0:
        return OR, None, None, detalle, usados, descartados
    se = math.sqrt(var)
    return (OR, math.exp(math.log(OR) - 1.96 * se),
            math.exp(math.log(OR) + 1.96 * se), detalle, usados, descartados)


# -----------------------------------------------------------------------------
# Utilidades del test
# -----------------------------------------------------------------------------
def estrato(sector, anio, monto, a, b, c, d):
    """Devuelve las 4 celdas de contingencia de un estrato.
       a = discordante & oferente unico   b = discordante & >=2 oferentes
       c = concordante & oferente unico   d = concordante & >=2 oferentes"""
    base = {"e_sector": sector, "e_anio": anio, "e_monto": monto}
    return [
        dict(base, discordante=1, oferente_unico=1, n=a),
        dict(base, discordante=1, oferente_unico=0, n=b),
        dict(base, discordante=0, oferente_unico=1, n=c),
        dict(base, discordante=0, oferente_unico=0, n=d),
    ]


def woolf(a, b, c, d):
    """OR e IC95 de Woolf (log-OR con EE = sqrt(1/a+1/b+1/c+1/d)).
       Estimador INDEPENDIENTE: con un solo estrato, MH debe coincidir."""
    orr = (a * d) / float(b * c)
    se = math.sqrt(1.0 / a + 1.0 / b + 1.0 / c + 1.0 / d)
    return orr, math.exp(math.log(orr) - 1.96 * se), math.exp(math.log(orr) + 1.96 * se)


def check(nombre, condicion, detalle=""):
    if condicion:
        print(f"  [OK ] {nombre}")
    else:
        print(f"  [FALLA] {nombre}   {detalle}")
        _fallos.append(nombre)


print("=" * 78)
print("test_cap14_mh · validacion del estimador Mantel-Haenszel de CAP-14")
print("=" * 78)

# --- 1 · un solo estrato: MH debe reproducir el OR crudo exacto --------------
print("\n1 · UN SOLO ESTRATO — MH debe dar el OR crudo (ad/bc)")
cel = estrato("salud", "2023", "2 100-1k", a=40, b=60, c=30, d=120)
OR, lo, hi, det, usados, desc = _mh_copia(cel, "discordante")
esperado = (40 * 120) / float(60 * 30)
check("OR_MH == ad/bc", abs(OR - esperado) < 1e-12, f"OR={OR} esperado={esperado}")
check("1 estrato usado, 0 descartados", (usados, desc) == (1, 0), f"{usados}/{desc}")

# --- 2 · IC de un solo estrato debe coincidir con Woolf a 4 decimales --------
print("\n2 · IC95 — con un solo estrato, RBG debe coincidir con Woolf (4 decimales)")
w_or, w_lo, w_hi = woolf(40, 60, 30, 120)
check("OR coincide con Woolf", abs(OR - w_or) < 1e-9, f"{OR} vs {w_or}")
check("limite inferior a 4 decimales", abs(lo - w_lo) < 1e-4, f"{lo:.6f} vs {w_lo:.6f}")
check("limite superior a 4 decimales", abs(hi - w_hi) < 1e-4, f"{hi:.6f} vs {w_hi:.6f}")

# --- 3 · lista vacia ---------------------------------------------------------
print("\n3 · LISTA VACIA — no debe reventar, debe devolver None")
r = _mh_copia([], "discordante")
check("devuelve OR=None", r[0] is None, str(r[0]))
check("0 usados y 0 descartados", (r[4], r[5]) == (0, 0), f"{r[4]}/{r[5]}")

# --- 4 · estrato chico (N < MIN_LIC_POR_ESTRATO) -----------------------------
print(f"\n4 · ESTRATO CHICO (N < {MIN_LIC_POR_ESTRATO}) — se descarta, no se promedia")
cel = estrato("A", "2023", "1 <100 UTM", 2, 3, 2, 3)          # N=10, se descarta
cel += estrato("B", "2023", "2 100-1k", 40, 60, 30, 120)      # N=250, se usa
OR2, lo2, hi2, _, usados, desc = _mh_copia(cel, "discordante")
check("1 usado / 1 descartado", (usados, desc) == (1, 1), f"{usados}/{desc}")
check("el estrato chico NO mueve el OR", abs(OR2 - esperado) < 1e-12, f"{OR2}")

# --- 5 · estrato sin variacion en el predictor -------------------------------
print("\n5 · ESTRATO SIN VARIACION (a+b == 0 o c+d == 0) — se descarta")
cel = estrato("A", "2023", "1 <100 UTM", 0, 0, 100, 100)      # ningun discordante
cel += estrato("B", "2023", "2 100-1k", 40, 60, 30, 120)
OR3, _, _, _, usados, desc = _mh_copia(cel, "discordante")
check("1 usado / 1 descartado", (usados, desc) == (1, 1), f"{usados}/{desc}")
check("no contamina el OR", abs(OR3 - esperado) < 1e-12, f"{OR3}")

# --- 6 · celda en cero -------------------------------------------------------
print("\n6 · CELDA EN CERO — no debe dividir por cero ni devolver NaN")
cel = estrato("A", "2023", "2 100-1k", 0, 50, 50, 100)        # a=0 -> num aporta 0
r = _mh_copia(cel, "discordante")
check("devuelve None (num<=0), no NaN ni excepcion", r[0] is None, str(r[0]))
cel = estrato("A", "2023", "2 100-1k", 50, 0, 50, 100)        # b=0 -> den aporta 0
r = _mh_copia(cel, "discordante")
check("den==0 devuelve None, no infinito", r[0] is None, str(r[0]))

# --- 7 · direccion inversa (OR < 1) ------------------------------------------
print("\n7 · DIRECCION INVERSA — un OR < 1 se estima igual de bien")
cel = estrato("A", "2023", "2 100-1k", a=20, b=130, c=60, d=90)
OR4, lo4, hi4, _, _, _ = _mh_copia(cel, "discordante")
esp4 = (20 * 90) / float(130 * 60)
check("OR_MH < 1", OR4 < 1.0, f"{OR4}")
check("OR_MH == ad/bc", abs(OR4 - esp4) < 1e-12, f"{OR4} vs {esp4}")
w = woolf(20, 130, 60, 90)
check("IC coincide con Woolf", abs(lo4 - w[1]) < 1e-4 and abs(hi4 - w[2]) < 1e-4,
      f"[{lo4:.6f},{hi4:.6f}] vs [{w[1]:.6f},{w[2]:.6f}]")

# --- 8 · paradoja de Simpson -------------------------------------------------
print("\n8 · PARADOJA DE SIMPSON — el crudo dice una cosa y el ajustado la contraria")
# Dos estratos, en cada uno OR < 1 (0,667 y 0,800); agregados sin estratificar,
# el OR crudo da 1,3125 > 1. Es el caso que hace que 14.C.2 lleve escrito
# "NO citar sola" al lado de la tabla cruda.
cel = estrato("A", "2023", "1 <100 UTM", a=10, b=10, c=30, d=20)     # N=70,  OR=0,667
cel += estrato("B", "2023", "4 >=5k UTM", a=200, b=50, c=50, d=10)   # N=310, OR=0,800
A = sum(c["n"] for c in cel if c["discordante"] == 1 and c["oferente_unico"] == 1)
B = sum(c["n"] for c in cel if c["discordante"] == 1 and c["oferente_unico"] == 0)
C = sum(c["n"] for c in cel if c["discordante"] == 0 and c["oferente_unico"] == 1)
D = sum(c["n"] for c in cel if c["discordante"] == 0 and c["oferente_unico"] == 0)
or_crudo = (A * D) / float(B * C)
OR5, _, _, _, usados, _ = _mh_copia(cel, "discordante")
print(f"     OR crudo = {or_crudo:.4f}   OR_MH = {OR5:.4f}   (estratos usados: {usados})")
check("el crudo y el ajustado CAMBIAN de signo (Simpson detectable)",
      (or_crudo > 1.0) != (OR5 > 1.0), f"crudo={or_crudo:.4f} MH={OR5:.4f}")
check("los 2 estratos se usan", usados == 2, str(usados))

# --- 9 · dos estratos identicos ---------------------------------------------
print("\n9 · DOS ESTRATOS IDENTICOS — el OR_MH no se mueve; el IC se estrecha")
uno = estrato("A", "2023", "2 100-1k", 40, 60, 30, 120)
dos = uno + estrato("B", "2023", "2 100-1k", 40, 60, 30, 120)
OR_a, lo_a, hi_a, _, _, _ = _mh_copia(uno, "discordante")
OR_b, lo_b, hi_b, _, _, _ = _mh_copia(dos, "discordante")
check("OR_MH identico con el doble de datos", abs(OR_a - OR_b) < 1e-12,
      f"{OR_a} vs {OR_b}")
check("IC mas estrecho con el doble de datos", (hi_b - lo_b) < (hi_a - lo_a),
      f"ancho {hi_b-lo_b:.4f} vs {hi_a-lo_a:.4f}")

# --- cierre ------------------------------------------------------------------
print("\n" + "=" * 78)
if _fallos:
    print(f"RESULTADO: {len(_fallos)} FALLA(S) -> {_fallos}")
    print("NO correr CAP-14 para citar hasta arreglar esto.")
    sys.exit(1)
print("RESULTADO: los 9 escenarios pasan.")
print("El ESTIMADOR esta validado. Las expresiones de Spark de 14.B siguen")
print("[POR VERIFICAR]: la primera corrida de CAP-14 es una prueba, no un")
print("resultado. Leer 14.C.1 y comparar contra las 964.171 licitaciones de P1.")
print("=" * 78)
sys.exit(0)
