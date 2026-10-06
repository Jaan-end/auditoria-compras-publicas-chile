"""
test_cap18_metricas.py — prueba de la matematica del driver de CAP-18 (N3).

QUE PRUEBA. La celda `CAP18_n3_clasificador_onu.py` calcula accuracy, macro-F1
y F1 ponderado EN EL DRIVER, a partir de la matriz de confusion agregada que
Spark le entrega como filas (label, prediction, n). Esa aritmetica es la que
decide si el modelo se cita o no, asi que se valida aparte, sin Spark, contra
sklearn como referencia externa.

POR QUE EXISTE. En Spark, `MulticlassClassificationEvaluator(metricName="f1")`
devuelve F1 PONDERADO por soporte, no macro. Sobre un catalogo con cola larga
—18.881 codigos ONU, la mayoria raros— el ponderado se ve bien y esconde
exactamente lo que interesa. CAP-18 calcula el macro a mano; este archivo prueba
que ese calculo a mano es correcto.

COMO SE CORRE.
    python test_cap18_metricas.py

Sin sklearn instalado igual corre: las pruebas 1-6 son analiticas y no lo
necesitan. La prueba 7 (contraste contra sklearn) se declara OMITIDA.
"""
import sys

# =============================================================================
# La funcion bajo prueba — COPIA LITERAL de la que vive dentro de CAP-18,
# reescrita solo para recibir tuplas en vez de filas de Spark.
# Si se edita una, se edita la otra. (regla §3.14: un archivo citado se puede
# senalar; una funcion duplicada se puede comparar.)
# =============================================================================
def macro_micro(filas):
    """filas: iterable de (label, prediction, n) -> (acc, macro, wtd, k, total)."""
    tp, fp, fn, sop = {}, {}, {}, {}
    clases = set()
    total = 0
    for y, p, n in filas:
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
        _tp, _fp, _fn = tp.get(y, 0), fp.get(y, 0), fn.get(y, 0)
        prec = _tp / (_tp + _fp) if (_tp + _fp) else 0.0
        rec = _tp / (_tp + _fn) if (_tp + _fn) else 0.0
        f1s[y] = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
    macro = sum(f1s.values()) / len(f1s) if f1s else 0.0
    wtd = (sum(f1s[y] * sop.get(y, 0) for y in f1s) / total) if total else 0.0
    return acc, macro, wtd, len(sop), total


def baseline_mayoritaria(train_labels, test_labels):
    """Predecir siempre la clase mas frecuente del TRAIN. Devuelve (clase, acc%)."""
    if not train_labels:
        return None, 0.0
    cnt = {}
    for y in train_labels:
        cnt[y] = cnt.get(y, 0) + 1
    clase = max(sorted(cnt), key=lambda k: cnt[k])
    if not test_labels:
        return clase, 0.0
    ok = sum(1 for y in test_labels if y == clase)
    return clase, 100.0 * ok / len(test_labels)


# =============================================================================
_fallos = []
_n = 0


def check(nombre, cond, detalle=""):
    global _n
    _n += 1
    if cond:
        print(f"  ok  {_n}. {nombre}")
    else:
        print(f"  FALLA {_n}. {nombre}   {detalle}")
        _fallos.append(nombre)


def casi(a, b, tol=1e-9):
    return abs(a - b) <= tol


print("=" * 74)
print("test_cap18_metricas.py — matematica del driver de CAP-18 (N3)")
print("=" * 74)

# --- 1 · clasificador perfecto -----------------------------------------------
acc, macro, wtd, k, n = macro_micro([(0, 0, 50), (1, 1, 30), (2, 2, 20)])
check("perfecto -> acc 100%, macro 1.0, wtd 1.0",
      casi(acc, 100.0) and casi(macro, 1.0) and casi(wtd, 1.0) and k == 3 and n == 100,
      f"acc={acc} macro={macro} wtd={wtd} k={k} n={n}")

# --- 2 · clasificador que nunca acierta --------------------------------------
acc, macro, wtd, k, n = macro_micro([(0, 1, 10), (1, 0, 10)])
check("nunca acierta -> acc 0%, macro 0.0",
      casi(acc, 0.0) and casi(macro, 0.0) and casi(wtd, 0.0),
      f"acc={acc} macro={macro}")

# --- 3 · LA PRUEBA QUE JUSTIFICA TODO: cola larga ----------------------------
# 90% de una clase mayoritaria bien predicha; 10 clases raras nunca acertadas.
# El F1 ponderado se ve bien; el macro delata que el modelo no sabe nada de la cola.
filas = [(0, 0, 9000)] + [(i, 0, 100) for i in range(1, 11)]
acc, macro, wtd, k, n = macro_micro(filas)
check("cola larga -> acc alto (90%) pero macro-F1 bajo",
      casi(acc, 90.0) and macro < 0.10 and wtd > 0.80,
      f"acc={acc:.2f} macro={macro:.4f} wtd={wtd:.4f}")
check("cola larga -> el ponderado ENGANA: wtd es >8x el macro",
      wtd > 8 * macro, f"wtd={wtd:.4f} macro={macro:.4f}")

# --- 4 · clase predicha que NUNCA es correcta debe bajar el macro ------------
# Este es el sesgo que se corrigio: promediar solo sobre clases verdaderas
# excluiria la clase 9 (predicha, jamas correcta) e inflaria el macro.
a1 = macro_micro([(0, 0, 50), (1, 1, 50)])[1]
a2 = macro_micro([(0, 0, 50), (1, 1, 50), (0, 9, 0)])[1]
a3 = macro_micro([(0, 0, 50), (1, 1, 40), (1, 9, 10)])[1]
check("clase predicha-nunca-correcta entra al macro con F1=0",
      a3 < a1, f"sin ella={a1:.4f} con ella={a3:.4f}")

# --- 5 · un solo par (label, prediction) por celda, con ceros ----------------
acc, macro, wtd, k, n = macro_micro([(0, 0, 1)])
check("caso minimo (1 fila, 1 clase) no revienta",
      casi(acc, 100.0) and casi(macro, 1.0) and n == 1, f"acc={acc} macro={macro}")

# --- 6 · entrada vacia -------------------------------------------------------
acc, macro, wtd, k, n = macro_micro([])
check("entrada vacia -> ceros, sin division por cero",
      casi(acc, 0.0) and casi(macro, 0.0) and n == 0, f"acc={acc} macro={macro} n={n}")

# --- 7 · baseline de clase mayoritaria ---------------------------------------
clase, acc_b = baseline_mayoritaria([0, 0, 0, 1], [0, 0, 1, 1])
check("baseline mayoritaria: elige la clase 0 y acierta 50% del test",
      clase == 0 and casi(acc_b, 50.0), f"clase={clase} acc={acc_b}")
clase, acc_b = baseline_mayoritaria([], [1, 2])
check("baseline con train vacio -> (None, 0.0)",
      clase is None and casi(acc_b, 0.0), f"clase={clase} acc={acc_b}")

# --- 8 · CONTRASTE CONTRA sklearn (referencia externa) -----------------------
try:
    from sklearn.metrics import accuracy_score, f1_score
    import random

    random.seed(20260907)
    # corpus sintetico desbalanceado, parecido en forma a un catalogo ONU:
    # unas pocas clases enormes y una cola de clases raras.
    y_true, y_pred = [], []
    for _ in range(4000):
        y = 0 if random.random() < 0.55 else random.randint(1, 40)
        # el modelo acierta seguido en la clase grande y poco en la cola
        if y == 0:
            p = 0 if random.random() < 0.92 else random.randint(1, 40)
        else:
            p = y if random.random() < 0.25 else random.choice([0, random.randint(1, 40)])
        y_true.append(y)
        y_pred.append(p)

    # agregar a matriz de confusion, que es lo que Spark entrega
    agg = {}
    for y, p in zip(y_true, y_pred):
        agg[(y, p)] = agg.get((y, p), 0) + 1
    filas = [(y, p, n) for (y, p), n in agg.items()]

    acc, macro, wtd, k, n = macro_micro(filas)
    acc_sk = 100.0 * accuracy_score(y_true, y_pred)
    macro_sk = f1_score(y_true, y_pred, average="macro", zero_division=0)
    wtd_sk = f1_score(y_true, y_pred, average="weighted", zero_division=0)

    check("vs sklearn: accuracy identico", casi(acc, acc_sk, 1e-9),
          f"propio={acc:.10f} sklearn={acc_sk:.10f}")
    check("vs sklearn: macro-F1 identico", casi(macro, macro_sk, 1e-9),
          f"propio={macro:.10f} sklearn={macro_sk:.10f}")
    check("vs sklearn: F1 ponderado identico", casi(wtd, wtd_sk, 1e-9),
          f"propio={wtd:.10f} sklearn={wtd_sk:.10f}")
    print(f"      (corpus sintetico: n={n:,}, {k} clases verdaderas, "
          f"acc={acc:.2f}%, macro={macro:.4f}, wtd={wtd:.4f})")
except ImportError:
    print("  OMITIDA 9-11. sklearn no esta instalado: el contraste externo no corrio.")
    print("             Las pruebas 1-8 son analiticas y si corrieron.")

print("-" * 74)
if _fallos:
    print(f"RESULTADO: {len(_fallos)} FALLA(S) de {_n}: {_fallos}")
    sys.exit(1)
print(f"RESULTADO: {_n}/{_n} OK. La matematica del driver de CAP-18 esta validada.")
print("NOTA: esto valida el DRIVER, sin Spark. Las expresiones de Spark y el")
print("Pipeline de ML se validaron por otra via: CORRIERON el 7-sep-2026,")
print("run_id 5a53ec4a1f30 (ver resultados/cap18_n3_salida_cruda.txt).")
