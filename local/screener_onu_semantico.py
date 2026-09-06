# -*- coding: utf-8 -*-
"""
Screener SEMÁNTICO ONU/UNSPSC — segundo algoritmo para el tamiz de coherencia
código↔texto (complementa, no reemplaza, a screener_onu.py).

[PROPUESTA] — escrito el 26-ago-2026, NO CORRIDO. No hay forma de confirmar desde
este chat si la máquina donde corre esto tiene salida a internet para descargar el
modelo de sentence-transformers. Por eso este archivo intenta dos rutas, en orden,
y DECLARA cuál usó — nunca finge que usó la que no pudo:

  1. `sentence-transformers` (modelo multilingüe preentrenado). Es la opción de
     mejor calidad semántica: entrenado sobre miles de millones de oraciones,
     capta sinónimos y paráfrasis que el TF-IDF de screener_onu.py no puede.
     Requiere `pip install sentence-transformers` y descargar el modelo
     (~470 MB) la primera vez — necesita internet esa primera vez.
  2. Si (1) falla (sin paquete, sin internet, sin espacio), cae automáticamente
     a LSA (`TruncatedSVD` de scikit-learn sobre la MISMA matriz TF-IDF de
     caracteres que ya usa screener_onu.py) — cero dependencias nuevas, cero
     descargas, corre en minutos. Es menos potente semánticamente (sigue
     basado en co-ocurrencia de n-gramas, no en significado aprendido de
     textos generales) pero es la opción de respaldo con riesgo cero.

Misma interfaz que ScreenerONU (screener_onu.py) a propósito: `screen(texto,
codigo_declarado, k)` devuelve el mismo diccionario (`sim_declarado`,
`rank_declarado`, `nivel_acuerdo_top1`, `top`) — así el runner
(`correr_screener_semantico_muestra.py`) y cualquier comparación con el tamiz
léxico quedan directas, sin traducir formatos.
"""
import re
import unicodedata

import numpy as np
import pandas as pd

BOILER = re.compile(r"(ver detalle en resolucion.*|segun bases.*|segun adjudicacion.*|"
                    r"ver bases.*|de acuerdo a bases.*|norma mobiliario.*|"
                    r"y posteriores modificaciones.*|_x000d_|\s+)", re.I)


def norm(s):
    s = "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = BOILER.sub(" ", s.lower())
    return re.sub(r"[^a-z0-9 ]", " ", s).strip()


class ScreenerONUSemantico:
    """Mismo contrato que ScreenerONU (screener_onu.py), método distinto por dentro.

    Atributos nuevos respecto al léxico, para poder declarar qué corrió:
      .metodo         : "sentence-transformers" o "lsa_tfidf" (cuál se usó de verdad)
      .modelo_nombre  : nombre del modelo/params usado, para citar en el informe
    """

    MODELO_ST = "paraphrase-multilingual-MiniLM-L12-v2"  # multilingüe, liviano (~470 MB), rápido
    LSA_COMPONENTES = 150

    def __init__(self, path_catalogo):
        c = pd.read_excel(path_catalogo) if str(path_catalogo).endswith(("xlsx", "xls")) \
            else pd.read_csv(path_catalogo, dtype=str)
        c["CodigoProducto"] = c["CodigoProducto"].astype(str).str.zfill(8)
        c["_txt"] = (c.NombreProducto.map(norm) + " " + c.Nivel3.map(norm) + " " + c.Nivel2.map(norm))
        self.cat = c.reset_index(drop=True)
        self.pos = {k: i for i, k in enumerate(self.cat.CodigoProducto)}

        self.metodo = None
        self.modelo_nombre = None
        self._st_model = None
        self._M = None          # matriz de vectores del catálogo (embeddings o LSA), ya normalizada
        self._vec = None        # TfidfVectorizer, solo si se cayó a LSA
        self._svd = None        # TruncatedSVD, solo si se cayó a LSA

        self._construir_indice()

    def _construir_indice(self):
        try:
            from sentence_transformers import SentenceTransformer
            print(f"   [screener semántico] intentando cargar '{self.MODELO_ST}' "
                  f"(sentence-transformers)...")
            self._st_model = SentenceTransformer(self.MODELO_ST)
            self.metodo = "sentence-transformers"
            self.modelo_nombre = self.MODELO_ST
            emb = self._st_model.encode(self.cat._txt.tolist(), show_progress_bar=True,
                                         normalize_embeddings=True)
            self._M = np.asarray(emb, dtype=np.float32)
            print(f"   [screener semántico] OK — modelo cargado, catálogo vectorizado "
                  f"({self._M.shape[0]:,} x {self._M.shape[1]}).")
        except Exception as e:
            print(f"   [screener semántico] sentence-transformers NO disponible "
                  f"({type(e).__name__}: {e}). Cayendo a LSA (TruncatedSVD sobre TF-IDF) —")
            print("   cero dependencias nuevas, sin descargas, sin necesitar internet.")
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.decomposition import TruncatedSVD
            from sklearn.preprocessing import normalize as sk_normalize

            self._vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1,
                                         sublinear_tf=True)
            Mtfidf = self._vec.fit_transform(self.cat._txt)
            n_comp = min(self.LSA_COMPONENTES, min(Mtfidf.shape) - 1)
            self._svd = TruncatedSVD(n_components=n_comp, random_state=20260826)
            Mlsa = self._svd.fit_transform(Mtfidf)
            self._M = sk_normalize(Mlsa).astype(np.float32)
            self.metodo = "lsa_tfidf"
            self.modelo_nombre = f"TruncatedSVD(n_components={n_comp}) sobre TF-IDF char_wb(3,5)"
            var_exp = self._svd.explained_variance_ratio_.sum()
            print(f"   [screener semántico] LSA listo — {n_comp} componentes, "
                  f"{100*var_exp:.1f}% de varianza explicada.")

    def _vectorizar_texto(self, texto):
        t = norm(texto)
        if self.metodo == "sentence-transformers":
            v = self._st_model.encode([t], normalize_embeddings=True)
            return np.asarray(v[0], dtype=np.float32)
        else:
            from sklearn.preprocessing import normalize as sk_normalize
            q_tfidf = self._vec.transform([t])
            q_lsa = self._svd.transform(q_tfidf)
            return sk_normalize(q_lsa).astype(np.float32)[0]

    def screen(self, texto, codigo_declarado, k=3):
        q = self._vectorizar_texto(texto)
        s = self._M @ q          # coseno directo: ambos lados ya están L2-normalizados
        orden = np.argsort(s)[::-1]
        cod = str(codigo_declarado).zfill(8)
        i = self.pos.get(cod)
        rank = int((s > s[i]).sum()) + 1 if i is not None else None
        top = [(self.cat.CodigoProducto[j], self.cat.NombreProducto[j], round(float(s[j]), 3))
               for j in orden[:k]]
        t1 = top[0][0]
        niv = ("hoja" if t1 == cod else "N3" if t1[:6] == cod[:6] else
               "N2" if t1[:4] == cod[:4] else "N1" if t1[:2] == cod[:2] else "ninguno")
        return dict(sim_declarado=round(float(s[i]), 3) if i is not None else None,
                    rank_declarado=rank, nivel_acuerdo_top1=niv, top=top)


if __name__ == "__main__":
    import sys
    cat_path = sys.argv[1] if len(sys.argv) > 1 else "datos/catalogo_onu_18881.csv"
    sc = ScreenerONUSemantico(cat_path)
    print(f"\nMétodo usado: {sc.metodo}  ({sc.modelo_nombre})")
    print("Prueba rápida con un caso de la tabla F de la guía (discrepancia léxica fuerte):")
    out = sc.screen(
        "prima de seguros colectivo de vida mes de noviembre 2016 segun factura 11185",
        "94101806")  # declarado: "sindicatos de personal medico" — sim léxica 0.005, rank 16.057/18.881
    print(f"  declarado=94101806 (sindicatos de personal medico)")
    print(f"  sim={out['sim_declarado']}  rank={out['rank_declarado']}/{len(sc.cat)}  "
          f"acuerdo top1: {out['nivel_acuerdo_top1']}")
    for c, n, s in out["top"]:
        print(f"  top → {c}  sim={s}  {n}")
