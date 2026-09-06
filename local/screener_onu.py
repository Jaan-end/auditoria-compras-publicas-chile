"""
Screener léxico ONU/UNSPSC — coste $0, corre sobre pandas o Spark (pandas_udf).
Dado el texto libre de una línea de OC y su código ONU declarado, devuelve:
  - sim_declarado : similitud del texto con el NOMBRE OFICIAL del código declarado
  - rank_declarado: posición del código declarado en el ranking de 18.881 candidatos
  - top1 / top3   : mejores candidatos del catálogo
  - nivel_de_acuerdo: N1 / N2 / N3 / hoja en que coinciden declarado y top1
NO emite veredictos. Es un TAMIZ: prioriza qué líneas mira un juez (humano o LLM).
"""
import re, unicodedata, pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

BOILER = re.compile(r"(ver detalle en resolucion.*|segun bases.*|segun adjudicacion.*|"
                    r"ver bases.*|de acuerdo a bases.*|norma mobiliario.*|"
                    r"y posteriores modificaciones.*|_x000d_|\s+)", re.I)

def norm(s):
    s = "" if s is None else str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = BOILER.sub(" ", s.lower())
    return re.sub(r"[^a-z0-9 ]", " ", s).strip()

class ScreenerONU:
    def __init__(self, path_catalogo):
        c = pd.read_excel(path_catalogo)
        c["CodigoProducto"] = c["CodigoProducto"].astype(str)
        # el texto candidato incluye la jerarquía: desambigua hojas homónimas
        c["_txt"] = (c.NombreProducto.map(norm) + " " + c.Nivel3.map(norm) + " " + c.Nivel2.map(norm))
        self.cat = c.reset_index(drop=True)
        self.vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1, sublinear_tf=True)
        self.M = self.vec.fit_transform(self.cat._txt)
        self.pos = {k: i for i, k in enumerate(self.cat.CodigoProducto)}

    def screen(self, texto, codigo_declarado, k=3):
        q = self.vec.transform([norm(texto)])
        s = linear_kernel(q, self.M).ravel()
        orden = s.argsort()[::-1]
        cod = str(codigo_declarado)
        i = self.pos.get(cod)
        rank = int((s > s[i]).sum()) + 1 if i is not None else None
        top = [(self.cat.CodigoProducto[j], self.cat.NombreProducto[j], round(float(s[j]), 3)) for j in orden[:k]]
        t1 = top[0][0]
        niv = ("hoja" if t1 == cod else "N3" if t1[:6] == cod[:6] else
               "N2" if t1[:4] == cod[:4] else "N1" if t1[:2] == cod[:2] else "ninguno")
        return dict(sim_declarado=round(float(s[i]), 3) if i is not None else None,
                    rank_declarado=rank, nivel_acuerdo_top1=niv, top=top)

if __name__ == "__main__":
    sc = ScreenerONU("4e00b053-Listado_rubros_ONU.xlsx")
    oc = pd.read_excel("c2048371-muestra_oc.xlsx")
    for _, r in oc.iterrows():
        txt = f"{r.EspecificacionComprador} {r.EspecificacionProveedor}"
        out = sc.screen(txt, r.codigoProductoONU)
        print("=" * 78)
        print("TEXTO   :", norm(txt)[:110])
        print(f"DECLARA : {r.codigoProductoONU} · {r.NombreroductoGenerico}")
        print(f"  sim={out['sim_declarado']}  rank={out['rank_declarado']}/18881  acuerdo con top1: {out['nivel_acuerdo_top1']}")
        for c, n, s in out["top"]:
            print(f"  top → {c}  sim={s}  {n}")
