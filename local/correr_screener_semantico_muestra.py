# -*- coding: utf-8 -*-
"""
Tamiz SEMÁNTICO sobre la misma muestra de 300 líneas de CAP-11 (RUN_ID_G7=fb7c3b125bf7).

[PROPUESTA] — escrito el 26-ago-2026, NO CORRIDO. Espejo exacto de
correr_screener_muestra.py (el tamiz léxico), pero usando ScreenerONUSemantico
(screener_onu_semantico.py) en vez de ScreenerONU — mismos archivos de entrada,
misma muestra de 300, mismas tablas A-F, para que el resultado sea DIRECTAMENTE
comparable, línea por línea, contra cap11b_tamiz_salida.txt.

Corre exactamente como el original:
    python correr_screener_semantico_muestra.py

Requiere, en el mismo directorio relativo que usa el proyecto:
    datos/catalogo_onu_18881.csv
    datos/cap11_muestra_validez_onu.csv   (o cap11c_muestra_reponderada.csv, ver abajo)

Termina con una sección §G que compara, con el criterio declarado ANTES de
correr (±10 pp, mismo criterio de CAP-13 en la guía / Parte 7 §7.3), el nivel de
acuerdo acumulado semántico contra el 21,00% ya [MEDIDO] del tamiz léxico
(hoja+N3+N2+N1, de cap11b_tamiz_salida.txt) — y declara CONSISTENTE o
DIVERGENTE, sin mezclar las dos cifras.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from screener_onu_semantico import ScreenerONUSemantico, norm  # noqa: E402

CAT = "datos/catalogo_onu_18881.csv"
MUE = "datos/cap11_muestra_validez_onu.csv"
OUT = "resultados/cap11b_tamiz_semantico_resultado.csv"

# Cifra congelada del tamiz LÉXICO (cap11b_tamiz_salida.txt, RUN_ID_G7=fb7c3b125bf7),
# para la comparación declarada en §G. NO se recalcula aquí — se copia tal cual del
# archivo de salida cruda ya [MEDIDO], siguiendo la Regla 1 del proyecto.
ACUMULADO_LEXICO_HASTA_N1_PCT = 21.00
UMBRAL_CONSISTENCIA_PP = 10.0

sc = ScreenerONUSemantico(CAT)
print(f"\nMétodo usado por este tamiz semántico: {sc.metodo}  ({sc.modelo_nombre})")
print(f"catálogo: {len(sc.cat):,} códigos")

m = pd.read_csv(MUE, dtype=str).fillna("")
m["onu8"] = m["onu8"].str.zfill(8)
print(f"muestra : {len(m):,} líneas · {m.onu8.nunique()} códigos distintos · {m.rubro_n1.nunique()} rubros N1")

filas = []
for _, r in m.iterrows():
    txt = norm(f"{r.texto_comprador} {r.texto_proveedor}")
    out = sc.screen(txt, r.onu8, k=3)
    top = out["top"]
    filas.append(dict(
        _id=r._id, onu8=r.onu8, en_catalogo=(out["sim_declarado"] is not None),
        sim_declarado=out["sim_declarado"], rank_declarado=out["rank_declarado"],
        nivel_acuerdo_top1=out["nivel_acuerdo_top1"],
        top1_cod=top[0][0], top1_nom=top[0][1], top1_sim=top[0][2],
        top2_cod=top[1][0], top2_sim=top[1][2],
        top3_cod=top[2][0], top3_sim=top[2][2],
        rubro_n1=r.rubro_n1, tipo_oc=r.tipo_oc, tramo=r.tramo, anio=r.anio,
        texto=txt[:200], producto_generico=r.producto_generico))
res = pd.DataFrame(filas)
os.makedirs(os.path.dirname(OUT), exist_ok=True)
res.to_csv(OUT, index=False)

print("\n" + "=" * 78)
print(f"RESULTADO DEL TAMIZ SEMÁNTICO SOBRE n=300 (misma muestra de RUN_ID_G7=fb7c3b125bf7)")
print(f"método: {sc.metodo}")
print("=" * 78)
n = len(res)
print(f"\n### A · ¿el código declarado existe en el catálogo de 18.881?")
print(f"   sí: {res.en_catalogo.sum():,}/{n}  ({100*res.en_catalogo.mean():.2f}%)")
print(f"   no: {(~res.en_catalogo).sum():,}/{n}  ({100*(~res.en_catalogo).mean():.2f}%)")

print(f"\n### B · nivel en que el TOP-1 del tamiz SEMÁNTICO coincide con el declarado")
vc = res.nivel_acuerdo_top1.value_counts()
for k in ["hoja", "N3", "N2", "N1", "ninguno"]:
    v = int(vc.get(k, 0))
    print(f"   {k:<8} {v:>4}  {100*v/n:6.2f}%")
acum = 0
print("\n   acumulado (el tamiz acierta AL MENOS hasta...):")
acum_pct_final = 0.0
for k in ["hoja", "N3", "N2", "N1"]:
    acum += int(vc.get(k, 0))
    acum_pct_final = 100 * acum / n
    print(f"   {k:<8} {acum:>4}  {acum_pct_final:6.2f}%")

r2 = res[res.en_catalogo]
print(f"\n### C · rank del código declarado entre los 18.881 (n={len(r2)} con código en catálogo)")
rk = r2.rank_declarado.astype(float)
for lab, cond in [("top-1", rk == 1), ("top-3", rk <= 3), ("top-10", rk <= 10),
                  ("top-100", rk <= 100), ("top-1000", rk <= 1000), ("peor que 1000", rk > 1000)]:
    print(f"   {lab:<14} {int(cond.sum()):>4}  {100*cond.mean():6.2f}%")
print(f"   mediana del rank: {rk.median():,.0f} de {len(sc.cat):,}")
print(f"   similitud media del declarado: {r2.sim_declarado.astype(float).mean():.3f}")

print(f"\n### D · por tramo de monto")
print(res.groupby("tramo").apply(lambda d: pd.Series({
    "n": len(d), "acierta_hoja_%": round(100 * (d.nivel_acuerdo_top1 == "hoja").mean(), 2),
    "acierta_N1+_%": round(100 * (d.nivel_acuerdo_top1 != "ninguno").mean(), 2)}),
    include_groups=False).to_string())

print(f"\n### E · por mecanismo")
print(res.groupby("tipo_oc").apply(lambda d: pd.Series({
    "n": len(d), "acierta_hoja_%": round(100 * (d.nivel_acuerdo_top1 == "hoja").mean(), 2),
    "acierta_N1+_%": round(100 * (d.nivel_acuerdo_top1 != "ninguno").mean(), 2)}),
    include_groups=False).to_string())

print(f"\n### F · 12 casos donde el tamiz SEMÁNTICO discrepa MÁS (declarado fuera del top-1000)")
peor = r2[rk > 1000].sort_values("rank_declarado", ascending=False).head(12)
for _, x in peor.iterrows():
    print(f"\n   texto     : {x.texto[:95]}")
    print(f"   declarado : {x.onu8} · {x.producto_generico}  "
          f"(rank {int(x.rank_declarado):,}/{len(sc.cat):,}, sim {x.sim_declarado})")
    print(f"   top1 tamiz: {x.top1_cod} · {x.top1_nom}  (sim {x.top1_sim})")

print("\n" + "=" * 78)
print("### G · VEREDICTO — comparación DECLARADA contra el tamiz léxico (criterio ±10 pp)")
print("=" * 78)
print(f"   Tamiz LÉXICO   (screener_onu.py, TF-IDF char n-gramas) : "
      f"{ACUMULADO_LEXICO_HASTA_N1_PCT:.2f}%  (cap11b_tamiz_salida.txt, [MEDIDO])")
print(f"   Tamiz SEMÁNTICO ({sc.metodo})                          : {acum_pct_final:.2f}%")
delta_g = acum_pct_final - ACUMULADO_LEXICO_HASTA_N1_PCT
print(f"   delta                                                  : {delta_g:+.2f} pp")
if abs(delta_g) <= UMBRAL_CONSISTENCIA_PP:
    print(f"   → CONSISTENTE (dentro de ±{UMBRAL_CONSISTENCIA_PP:.0f} pp): dos algoritmos distintos por")
    print("     diseño miden aproximadamente lo mismo. Refuerza la confianza en el hallazgo de")
    print("     que la mayoría de las líneas no calza con el top-1 del catálogo.")
else:
    print(f"   → DIVERGENTE (más de ±{UMBRAL_CONSISTENCIA_PP:.0f} pp): los métodos capturan señales")
    print("     distintas. Citar las dos cifras por separado en el informe, nunca mezcladas ni")
    print("     promediadas — misma disciplina que ya descartó CAP-12B por no replicar su propio")
    print("     criterio de validación.")
print("\nTamiz semántico termina.")
