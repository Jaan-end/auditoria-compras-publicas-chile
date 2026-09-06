# -*- coding: utf-8 -*-
"""
CAP-11-B · Tamiz léxico sobre la muestra de 300 líneas de CAP-11 (RUN_ID_G7=fb7c3b125bf7).
Corre `ScreenerONU` (screener_onu.py) contra el catálogo ONU de 18.881 códigos y mide
el TECHO del método de coste cero: hasta qué nivel de la jerarquía acierta.
NO emite veredictos de correccion. Salida: cap11b_tamiz_resultado.csv + resumen impreso.
"""
import os, re, unicodedata, sys
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

CAT = "datos/catalogo_onu_18881.csv"
MUE = "datos/cap11_muestra_validez_onu.csv"
OUT = "resultados/cap11b_tamiz_resultado.csv"   # se crea el directorio si no existe

BOILER = re.compile(r"(ver detalle en resolucion.*|segun bases.*|segun adjudicacion.*|"
                    r"ver bases.*|de acuerdo a bases.*|norma mobiliario.*|"
                    r"y posteriores modificaciones.*|_x000d_|\s+)", re.I)

def norm(s):
    s = "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = BOILER.sub(" ", s.lower())
    return re.sub(r"[^a-z0-9 ]", " ", s).strip()

c = pd.read_csv(CAT, dtype=str)
c["CodigoProducto"] = c["CodigoProducto"].astype(str).str.zfill(8)
c["_txt"] = (c.NombreProducto.map(norm) + " " + c.Nivel3.map(norm) + " " + c.Nivel2.map(norm))
c = c.reset_index(drop=True)
vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1, sublinear_tf=True)
M = vec.fit_transform(c._txt)
pos = {k: i for i, k in enumerate(c.CodigoProducto)}
print(f"catálogo: {len(c):,} códigos · matriz {M.shape}")

m = pd.read_csv(MUE, dtype=str).fillna("")
m["onu8"] = m["onu8"].str.zfill(8)
print(f"muestra : {len(m):,} líneas · {m.onu8.nunique()} códigos distintos · {m.rubro_n1.nunique()} rubros N1")

filas = []
for _, r in m.iterrows():
    txt = norm(f"{r.texto_comprador} {r.texto_proveedor}")
    q = vec.transform([txt])
    s = linear_kernel(q, M).ravel()
    cod = r.onu8
    i = pos.get(cod)
    rank = int((s > s[i]).sum()) + 1 if i is not None else None
    orden = s.argsort()[::-1][:3]
    top = [(c.CodigoProducto[j], c.NombreProducto[j], round(float(s[j]), 3)) for j in orden]
    t1 = top[0][0]
    niv = ("hoja" if t1 == cod else "N3" if t1[:6] == cod[:6] else
           "N2" if t1[:4] == cod[:4] else "N1" if t1[:2] == cod[:2] else "ninguno")
    filas.append(dict(
        _id=r._id, onu8=cod, en_catalogo=(i is not None),
        sim_declarado=round(float(s[i]), 3) if i is not None else None,
        rank_declarado=rank, nivel_acuerdo_top1=niv,
        top1_cod=top[0][0], top1_nom=top[0][1], top1_sim=top[0][2],
        top2_cod=top[1][0], top2_sim=top[1][2],
        top3_cod=top[2][0], top3_sim=top[2][2],
        rubro_n1=r.rubro_n1, tipo_oc=r.tipo_oc, tramo=r.tramo, anio=r.anio,
        texto=txt[:200], producto_generico=r.producto_generico))
res = pd.DataFrame(filas)
os.makedirs(os.path.dirname(OUT), exist_ok=True)
res.to_csv(OUT, index=False)

print("\n" + "="*78)
print("RESULTADO DEL TAMIZ LÉXICO SOBRE n=300 (RUN_ID_G7=fb7c3b125bf7)")
print("="*78)
n = len(res)
print(f"\n### A · ¿el código declarado existe en el catálogo de 18.881?")
print(f"   sí: {res.en_catalogo.sum():,}/{n}  ({100*res.en_catalogo.mean():.2f}%)")
print(f"   no: {(~res.en_catalogo).sum():,}/{n}  ({100*(~res.en_catalogo).mean():.2f}%)")

print(f"\n### B · nivel en que el TOP-1 del tamiz coincide con el declarado")
vc = res.nivel_acuerdo_top1.value_counts()
for k in ["hoja","N3","N2","N1","ninguno"]:
    v = int(vc.get(k,0)); print(f"   {k:<8} {v:>4}  {100*v/n:6.2f}%")
acum = 0
print("\n   acumulado (el tamiz acierta AL MENOS hasta...):")
for k in ["hoja","N3","N2","N1"]:
    acum += int(vc.get(k,0)); print(f"   {k:<8} {acum:>4}  {100*acum/n:6.2f}%")

r2 = res[res.en_catalogo]
print(f"\n### C · rank del código declarado entre los 18.881 (n={len(r2)} con código en catálogo)")
rk = r2.rank_declarado.astype(float)
for lab, cond in [("top-1", rk==1), ("top-3", rk<=3), ("top-10", rk<=10),
                  ("top-100", rk<=100), ("top-1000", rk<=1000), ("peor que 1000", rk>1000)]:
    print(f"   {lab:<14} {int(cond.sum()):>4}  {100*cond.mean():6.2f}%")
print(f"   mediana del rank: {rk.median():,.0f} de 18.881")
print(f"   similitud media del declarado: {r2.sim_declarado.astype(float).mean():.3f}")
print(f"   sim con el DECLARADO == 0: {int((r2.sim_declarado.astype(float)==0).sum())}  "
      f"({100*(r2.sim_declarado.astype(float)==0).mean():.2f}%)")
print(f"   top1_sim == 0 (el tamiz no opinó): {int((res.top1_sim.astype(float)==0).sum())}"
      f"   <- distinto de lo anterior: sim 0 con el declarado no es abstencion")

print(f"\n### D · por tramo de monto")
print(res.groupby("tramo").apply(lambda d: pd.Series({
    "n": len(d), "acierta_hoja_%": round(100*(d.nivel_acuerdo_top1=="hoja").mean(),2),
    "acierta_N1+_%": round(100*(d.nivel_acuerdo_top1!="ninguno").mean(),2)}), include_groups=False).to_string())

print(f"\n### E · por mecanismo")
print(res.groupby("tipo_oc").apply(lambda d: pd.Series({
    "n": len(d), "acierta_hoja_%": round(100*(d.nivel_acuerdo_top1=="hoja").mean(),2),
    "acierta_N1+_%": round(100*(d.nivel_acuerdo_top1!="ninguno").mean(),2)}), include_groups=False).to_string())

print(f"\n### F · 12 casos donde el tamiz discrepa MÁS (declarado fuera del top-1000)")
peor = r2[rk>1000].sort_values("rank_declarado", ascending=False).head(12)
for _, x in peor.iterrows():
    print(f"\n   texto     : {x.texto[:95]}")
    print(f"   declarado : {x.onu8} · {x.producto_generico}  (rank {int(x.rank_declarado):,}/18881, sim {x.sim_declarado})")
    print(f"   top1 tamiz: {x.top1_cod} · {x.top1_nom}  (sim {x.top1_sim})")
