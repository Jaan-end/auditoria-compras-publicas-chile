# =============================================================================
#  CAP18B_familia4.py — la segunda granularidad de N3, barata
#  Capstone Big Data · Mercado Publico nacional 2017-2026
#
#  QUE HACE. Entrena el mismo pipeline de CAP-18 sobre la MISMA tabla ya
#  materializada, pero prediciendo FAMILIA (4 digitos) en vez de SEGMENTO (2).
#  NO recomputa el embudo de 15,3 millones de filas: lee la tabla de Unity
#  Catalog que CAP-18 ya dejo escrita. Por eso es barata.
#
#  CORRER DESPUES DE: CAP-18 (corrida 5a53ec4a1f30 o la que corresponda).
#  La tabla tiene que existir. Si no existe, esta celda aborta y te lo dice.
#
#  ---------------------------------------------------------------------------
#  UN DEFECTO CONOCIDO DE LA TABLA, Y COMO SE CONTROLA — LEER ANTES DE CITAR
#  ---------------------------------------------------------------------------
#  CAP-18 deduplico por texto con el criterio de SEGMENTO: se quedo con los
#  textos que mapean a UN SOLO segmento de 2 digitos, y para las demas columnas
#  tomo el minimo. Consecuencia: un texto que dentro de un mismo segmento
#  apuntaba a DOS familias distintas quedo con la familia mas baja, arbitraria.
#  Eso es RUIDO DE ETIQUETA a nivel de familia, y no existia a nivel de segmento.
#
#  No se puede reparar leyendo esta tabla —la informacion se perdio al escribir—
#  pero SI se puede acotar, y gratis: los textos con `_n_rep = 1` vienen de UNA
#  sola linea de origen y por construccion NO PUEDEN ser ambiguos. Asi que esta
#  celda evalua DOS VECES:
#
#      (a) sobre el test completo          -> puede traer etiquetas contaminadas
#      (b) sobre el subconjunto _n_rep = 1 -> limpio por construccion
#
#  SI LAS DOS CIFRAS SE PARECEN, la contaminacion es inmaterial y el resultado de
#  familia se cita normal. SI DIFIEREN, se cita (b) y se declara por que.
#  Sin ese control, el numero de familia no deberia entrar a ninguna parte.
#
#  ADVERTENCIA: esperar un macro-F1 MAS BAJO que en segmento. Son 342 clases en
#  vez de 55 y la cola larga es mucho mas larga. Eso NO es un fracaso: es el
#  resultado que la referencia externa tambien describe, y decirlo con su cifra
#  vale mas que esconderlo.
#
#  ---------------------------------------------------------------------------
#  YA CORRIO — 7-sep-2026. Salida en resultados/cap18b_familia4_salida_cruda.txt
#  ---------------------------------------------------------------------------
#  LogisticRegression: accuracy 56,58% · baseline 7,61% · macro-F1 0,2739
#  NaiveBayes        : accuracy 48,66% · baseline 7,61% · macro-F1 0,2481
#
#  Y EL CONTROL DE CONTAMINACION SALIO LIMPIO: la macro-F1 sobre el test
#  completo y sobre el subconjunto sin ambiguedad posible (86,3% del test)
#  difieren en 0,001. Contaminacion INMATERIAL -> se cita el test completo.
#
#  SALVEDAD DE TRAZABILIDAD, declarada: esta celda NO acuña identificador de
#  corrida propio y se cita por el de la tabla que consume (5a53ec4a1f30). Es
#  un descuido de diseño de la celda, no un problema de calculo. Si se vuelve a
#  correr, conviene arreglarlo antes.
# =============================================================================

# %% ==========================================================================
# CAP-18-B · N3 a nivel de FAMILIA (4 digitos)
# =============================================================================

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (RegexTokenizer, StopWordsRemover,
                                CountVectorizer, IDF, StringIndexer)
from pyspark.ml.classification import LogisticRegression, NaiveBayes

# --- COMPLETAR con el run_id de la corrida de CAP-18 que dejo la tabla --------
RUN_ID_CAP18 = "5a53ec4a1f30"

ANIO_CORTE_TRAIN = 2023
VOCAB_SIZE, MIN_DF = 20_000, 5
LR_MAX_ITER, LR_REG_PARAM = 15, 0.01

try:
    _cat = spark.catalog.currentCatalog()
    _sch = spark.catalog.currentDatabase()
except Exception:
    _cat, _sch = "workspace", "default"
TABLA = f"{_cat}.{_sch}.cap18_train_{RUN_ID_CAP18}"

print("=" * 78)
print(f"CAP-18-B · N3 familia (4 digitos) · lee {TABLA}")
print("=" * 78)

try:
    _tab = spark.read.table(TABLA)
except Exception as _e:
    _tab = None
    print(f"\nABORTA: no se pudo leer {TABLA}")
    print(f"   {type(_e).__name__}: {str(_e)[:300]}")
    print("\n   Revisa RUN_ID_CAP18 arriba: tiene que ser el de la corrida de")
    print("   CAP-18 que imprimio '[materializado] tabla UC: ...'.")

if _tab is not None:
    _faltan = [c for c in ("txt", "anio", "fam4", "_n_rep") if c not in _tab.columns]
    if _faltan:
        print(f"\nABORTA: a la tabla le faltan columnas: {_faltan}")
        print(f"   columnas presentes: {_tab.columns}")
    else:
        _train = _tab.filter(F.col("anio") <= ANIO_CORTE_TRAIN)
        _test = _tab.filter(F.col("anio") > ANIO_CORTE_TRAIN)
        _test_limpio = _test.filter(F.col("_n_rep") == 1)

        _r = _tab.agg(
            F.count(F.lit(1)).alias("n"),
            F.sum(F.when(F.col("anio") <= ANIO_CORTE_TRAIN, 1).otherwise(0)).alias("n_tr"),
            F.sum(F.when(F.col("anio") > ANIO_CORTE_TRAIN, 1).otherwise(0)).alias("n_te"),
            F.sum(F.when((F.col("anio") > ANIO_CORTE_TRAIN) & (F.col("_n_rep") == 1),
                         1).otherwise(0)).alias("n_te_limpio"),
            F.countDistinct("fam4").alias("k"),
        ).collect()[0]

        print(f"\n  textos            : {_r['n']:>10,}")
        print(f"  train (<= {ANIO_CORTE_TRAIN})   : {_r['n_tr']:>10,}")
        print(f"  test  (>= {ANIO_CORTE_TRAIN + 1})   : {_r['n_te']:>10,}")
        print(f"  · de ellos LIMPIOS (_n_rep = 1, no pueden ser ambiguos):"
              f" {_r['n_te_limpio']:>10,}  "
              f"({100.0 * _r['n_te_limpio'] / max(_r['n_te'], 1):.1f}%)")
        print(f"  clases familia    : {_r['k']:>10,}")

        if _r["n_te_limpio"] < 5000:
            print("\n  AVISO: el subconjunto limpio es chico. El control de contaminacion")
            print("  pierde potencia; leer la comparacion con cuidado.")

        def _metricas(pred_df):
            """Identica a la de CAP-18: macro a mano, porque Spark llama 'f1' al ponderado."""
            filas = (pred_df.groupBy("label", "prediction")
                     .agg(F.count(F.lit(1)).alias("n")).collect())
            tp, fp, fn, sop, clases, total = {}, {}, {}, {}, set(), 0
            for r in filas:
                y, p, n = r["label"], r["prediction"], r["n"]
                total += n
                sop[y] = sop.get(y, 0) + n
                clases.add(y)
                clases.add(p)
                if y == p:
                    tp[y] = tp.get(y, 0) + n
                else:
                    fp[p] = fp.get(p, 0) + n
                    fn[y] = fn.get(y, 0) + n
            acc = 100.0 * sum(tp.values()) / total if total else 0.0
            f1s = {}
            for y in clases:
                a, b, c = tp.get(y, 0), fp.get(y, 0), fn.get(y, 0)
                pr = a / (a + b) if (a + b) else 0.0
                rc = a / (a + c) if (a + c) else 0.0
                f1s[y] = (2 * pr * rc / (pr + rc)) if (pr + rc) else 0.0
            macro = sum(f1s.values()) / len(f1s) if f1s else 0.0
            wtd = (sum(f1s[y] * sop.get(y, 0) for y in f1s) / total) if total else 0.0
            return acc, macro, wtd, len(sop), total

        def _baseline(train_df, test_df):
            top = (train_df.groupBy("fam4").agg(F.count(F.lit(1)).alias("n"))
                   .orderBy(F.col("n").desc()).limit(1).collect())
            if not top:
                return None, 0.0
            cl = top[0]["fam4"]
            r = test_df.agg(F.count(F.lit(1)).alias("n"),
                            F.sum(F.when(F.col("fam4") == cl, 1).otherwise(0)).alias("ok")
                            ).collect()[0]
            return cl, (100.0 * r["ok"] / r["n"] if r["n"] else 0.0)

        _stop = StopWordsRemover.loadDefaultStopWords("spanish")
        _resultados = []

        def _corre(nombre, modelo):
            print("\n" + "-" * 78)
            print(f"  entrenando {nombre} · objetivo = familia (4 dig)")
            print("-" * 78)
            pipe = Pipeline(stages=[
                RegexTokenizer(inputCol="txt", outputCol="_tok", pattern=r"\W+", minTokenLength=2),
                StopWordsRemover(inputCol="_tok", outputCol="_tok2", stopWords=_stop),
                CountVectorizer(inputCol="_tok2", outputCol="_tf",
                                vocabSize=VOCAB_SIZE, minDF=MIN_DF),
                IDF(inputCol="_tf", outputCol="features"),
                StringIndexer(inputCol="fam4", outputCol="label", handleInvalid="keep"),
                modelo])
            m = pipe.fit(_train)

            a1, m1, w1, k1, n1 = _metricas(m.transform(_test))
            a2, m2, w2, k2, n2 = _metricas(m.transform(_test_limpio))
            cl, ab = _baseline(_train, _test)

            print(f"    BASELINE mayoritaria (familia {cl}) : {ab:6.2f}%")
            print(f"\n    (a) TEST COMPLETO        n={n1:>9,} · {k1} clases")
            print(f"        accuracy {a1:6.2f}%  ·  macro-F1 {m1:6.4f}  ·  ponderado {w1:6.4f}")
            print(f"    (b) TEST LIMPIO (_n_rep=1) n={n2:>9,} · {k2} clases")
            print(f"        accuracy {a2:6.2f}%  ·  macro-F1 {m2:6.4f}  ·  ponderado {w2:6.4f}")
            _d_acc, _d_mac = a2 - a1, m2 - m1
            print(f"\n    DIFERENCIA (b - a)  accuracy {_d_acc:+6.2f} pp  ·  macro-F1 {_d_mac:+7.4f}")
            if abs(_d_mac) < 0.02:
                print("    -> Las dos cifras coinciden. La contaminacion de etiqueta de")
                print("       familia es INMATERIAL. Se cita (a), el test completo.")
            else:
                print("    -> Las dos cifras DIFIEREN. La contaminacion SI muerde.")
                print("       Se cita (b) —el subconjunto limpio— y se declara por que,")
                print("       o no se cita familia. NO promediar las dos.")
            _resultados.append((nombre, cl, ab, a1, m1, w1, a2, m2, w2))
            return m

        _corre("NaiveBayes", NaiveBayes(smoothing=1.0, modelType="multinomial"))
        _corre("LogisticRegression",
               LogisticRegression(maxIter=LR_MAX_ITER, regParam=LR_REG_PARAM,
                                  family="multinomial"))

        print("\n" + "=" * 78)
        print(f"### RESUMEN · familia (4 dig) · tabla de RUN_ID_CAP18 = {RUN_ID_CAP18}")
        print("=" * 78)
        print(f"  {'modelo':<20} {'base':>7} {'acc(a)':>8} {'macro(a)':>9} "
              f"{'acc(b)':>8} {'macro(b)':>9}")
        for nm, cl, ab, a1, m1, w1, a2, m2, w2 in _resultados:
            print(f"  {nm:<20} {ab:>6.2f}% {a1:>7.2f}% {m1:>9.4f} {a2:>7.2f}% {m2:>9.4f}")
        print("\n  (a) test completo · (b) test limpio, textos de una sola linea de origen")
        print(f"\n  COMO SE CITA: [MEDIDO], tabla de la corrida {RUN_ID_CAP18}, esta celda,")
        print("  SIEMPRE con el baseline al lado y SIEMPRE declarando cual de las dos")
        print("  columnas se esta citando y por que.")
        print("\nCAP-18-B termina.")
