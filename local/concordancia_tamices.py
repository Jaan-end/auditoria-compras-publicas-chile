# =============================================================================
#  concordancia_tamices.py — concordancia entre dos tamices sobre las MISMAS 300 lineas
#  Capstone Big Data · Mercado Publico nacional 2017-2026
#
#  QUE HACE
#  --------
#  Cruza fila a fila (por `_id`) el veredicto de dos tamices de coherencia
#  codigo<->texto corridos sobre la misma muestra de 300 lineas, y mide si
#  coinciden EN LOS MISMOS CASOS, no solo en la tasa agregada. Imprime:
#  matriz 2x2, kappa de Cohen con IC95, McNemar exacto (¿la discordancia es
#  sistematica o es ruido?), matriz ordinal de 5 niveles con kappa ponderado,
#  y el % de casos en que ambos proponen el MISMO codigo de reemplazo.
#
#  POR QUE HACE FALTA
#  ------------------
#  Dos metodos pueden marcar 21% cada uno y estar marcando lineas DISTINTAS.
#  Comparar tasas marginales no prueba concordancia; kappa si. Es el mismo
#  principio de §3.10: el titular de una metrica es una lectura auditable.
#
#  COMO SE USA (Windows, VS Code, pararse en la raiz del paquete del proyecto)
#  --------------------------------------------------------------------------
#     python Codigos\concordancia_tamices.py ^
#            resultados\cap11b_tamiz_resultado.csv ^
#            resultados\cap11b_tamiz_semantico_resultado.csv ^
#            --nombre-a lexico --nombre-b LSA
#
#  Para el par que de verdad importa (lexico vs sentence-transformers), pasar
#  el CSV por fila de la corrida de ST en lugar del semantico:
#     python Codigos\concordancia_tamices.py ^
#            resultados\cap11b_tamiz_resultado.csv ^
#            resultados\<salida_por_fila_sentence_transformers>.csv ^
#            --nombre-a lexico --nombre-b ST
#
#  REQUISITOS DE ENTORNO
#  ---------------------
#  Python 3.8+. Solo biblioteca estandar (csv, math, argparse, collections).
#  Sin pandas, sin scipy, sin Spark. Determinista: no hay azar, dos corridas
#  sobre el mismo insumo dan el mismo numero.
#
#  QUE PRODUCE
#  -----------
#  Solo salida por pantalla (para pegar cruda en la bitacora). Promueve a
#  [MEDIDO] la concordancia entre tamices, que hasta ahora no estaba medida:
#  el proyecto tenia tres TASAS (21,00% / 10,67% / 21,33%) y ninguna medicion
#  de si coincidian caso a caso.
#
#  COLUMNAS QUE EXIGE EN AMBOS CSV
#  -------------------------------
#  `_id`, `nivel_acuerdo_top1` (valores: ninguno / N1 / N2 / N3 / hoja),
#  `top1_cod`, y opcionalmente `tipo_oc` y `rubro_n1` para el desglose.
#  Si falta alguna, ABORTA imprimiendo las columnas que si hay. No inventa
#  sustitutos.
# =============================================================================

import argparse
import csv
import math
import sys
from collections import Counter, defaultdict

ORDEN = ["ninguno", "N1", "N2", "N3", "hoja"]
REQUERIDAS = ["_id", "nivel_acuerdo_top1", "top1_cod"]


def cargar(ruta, etiqueta):
    try:
        with open(ruta, encoding="utf-8") as fh:
            filas = list(csv.DictReader(fh))
    except OSError as exc:
        sys.exit(f"ABORTA: no se pudo leer {etiqueta} ({ruta}): {exc}")
    if not filas:
        sys.exit(f"ABORTA: {etiqueta} ({ruta}) no tiene filas.")
    faltan = [c for c in REQUERIDAS if c not in filas[0]]
    if faltan:
        sys.exit(
            f"ABORTA: a {etiqueta} ({ruta}) le faltan columnas {faltan}.\n"
            f"        COLUMNAS DISPONIBLES: {list(filas[0].keys())}"
        )
    d = {}
    for r in filas:
        d[r["_id"]] = r
    if len(d) != len(filas):
        print(f"[aviso] {etiqueta}: {len(filas)} filas pero {len(d)} _id distintos "
              f"-> hay _id repetidos, se conserva el ultimo de cada uno.")
    return d


def kappa_binario(a, b):
    """kappa de Cohen y su EE (Fleiss) para dos vectores binarios pareados."""
    n = len(a)
    n11 = sum(1 for x, y in zip(a, b) if x == 1 and y == 1)
    n10 = sum(1 for x, y in zip(a, b) if x == 1 and y == 0)
    n01 = sum(1 for x, y in zip(a, b) if x == 0 and y == 1)
    n00 = n - n11 - n10 - n01
    po = (n11 + n00) / n
    pa, pb = sum(a) / n, sum(b) / n
    pe = pa * pb + (1 - pa) * (1 - pb)
    if abs(1 - pe) < 1e-12:
        return None, None, (n11, n10, n01, n00), po, pe
    k = (po - pe) / (1 - pe)
    ee = math.sqrt(po * (1 - po) / (n * (1 - pe) ** 2))
    return k, ee, (n11, n10, n01, n00), po, pe


def mcnemar_exacto(n10, n01):
    """p exacto de dos colas (binomial 0.5) sobre los pares discordantes."""
    nd = n10 + n01
    if nd == 0:
        return None
    k = min(n10, n01)
    p = 2.0 * sum(math.comb(nd, i) for i in range(k + 1)) / (2 ** nd)
    return min(p, 1.0)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv_a")
    ap.add_argument("csv_b")
    ap.add_argument("--nombre-a", default="A")
    ap.add_argument("--nombre-b", default="B")
    args = ap.parse_args()

    A = cargar(args.csv_a, args.nombre_a)
    B = cargar(args.csv_b, args.nombre_b)
    ids = sorted(set(A) & set(B))
    if not ids:
        sys.exit("ABORTA: los dos CSV no comparten ningun `_id`. No son la misma muestra.")

    print("=" * 78)
    print(f"CONCORDANCIA ENTRE TAMICES · {args.nombre_a} vs {args.nombre_b}")
    print("=" * 78)
    print(f"  {args.nombre_a}: {len(A)} filas · {args.nombre_b}: {len(B)} filas "
          f"· pareadas por _id: {len(ids)}")
    if len(ids) < len(A) or len(ids) < len(B):
        print(f"  [!] NO son exactamente la misma muestra. Se analiza la interseccion "
              f"({len(ids)}); declararlo si se cita.")

    desconocidos = {r["nivel_acuerdo_top1"] for r in list(A.values()) + list(B.values())} - set(ORDEN)
    if desconocidos:
        sys.exit(f"ABORTA: valores de `nivel_acuerdo_top1` fuera de {ORDEN}: {sorted(desconocidos)}")

    binario = lambda r: 0 if r["nivel_acuerdo_top1"] == "ninguno" else 1
    a = [binario(A[i]) for i in ids]
    b = [binario(B[i]) for i in ids]
    n = len(ids)

    k, ee, (n11, n10, n01, n00), po, pe = kappa_binario(a, b)
    print(f"\n### 1 · BINARIO — ¿el codigo declarado aparece en el top-1, a cualquier nivel?")
    print(f"  {args.nombre_a:>10} acuerda: {sum(a):>4}/{n} = {100*sum(a)/n:6.2f}%")
    print(f"  {args.nombre_b:>10} acuerda: {sum(b):>4}/{n} = {100*sum(b)/n:6.2f}%")
    print(f"  matriz 2x2   ambos-si={n11}  solo-{args.nombre_a}={n10}  "
          f"solo-{args.nombre_b}={n01}  ambos-no={n00}")
    print(f"  acuerdo observado = {100*po:.2f}%   esperado por azar = {100*pe:.2f}%")
    if k is None:
        print("  kappa NO CALCULABLE (un metodo no varia). Decirlo asi, no reportar 0.")
    else:
        print(f"  kappa de Cohen = {k:.4f}   IC95 = [{k-1.96*ee:.4f}, {k+1.96*ee:.4f}]")
        print("  escala Landis-Koch: <0 nula · 0-0,20 muy debil · 0,21-0,40 debil ·")
        print("                      0,41-0,60 moderada · 0,61-0,80 buena · >0,80 muy buena")
    p = mcnemar_exacto(n10, n01)
    if p is not None:
        print(f"  McNemar exacto sobre los {n10+n01} pares discordantes: p = {p:.6f}")
        print("  (p chico = la discordancia tiene DIRECCION, no es ruido simetrico)")

    idx = {v: i for i, v in enumerate(ORDEN)}
    K = len(ORDEN)
    M = [[0] * K for _ in range(K)]
    for i in ids:
        M[idx[A[i]["nivel_acuerdo_top1"]]][idx[B[i]["nivel_acuerdo_top1"]]] += 1
    po5 = sum(M[i][i] for i in range(K)) / n
    fa = [sum(M[i]) for i in range(K)]
    co = [sum(M[i][j] for i in range(K)) for j in range(K)]
    pe5 = sum(fa[i] * co[i] for i in range(K)) / n ** 2
    w = [[1 - ((i - j) ** 2) / ((K - 1) ** 2) for j in range(K)] for i in range(K)]
    pow_ = sum(w[i][j] * M[i][j] for i in range(K) for j in range(K)) / n
    pew = sum(w[i][j] * fa[i] * co[j] for i in range(K) for j in range(K)) / n ** 2
    print(f"\n### 2 · ORDINAL — nivel de acuerdo (filas={args.nombre_a}, cols={args.nombre_b})")
    print("           " + "".join(f"{o:>9}" for o in ORDEN))
    for i, o in enumerate(ORDEN):
        print(f"  {o:>8} " + "".join(f"{M[i][j]:>9}" for j in range(K)))
    if abs(1 - pe5) > 1e-12:
        print(f"  kappa simple                 = {(po5-pe5)/(1-pe5):.4f}")
    if abs(1 - pew) > 1e-12:
        print(f"  kappa ponderado (cuadratico) = {(pow_-pew)/(1-pew):.4f}")

    igual = sum(1 for i in ids if A[i]["top1_cod"] == B[i]["top1_cod"])
    print(f"\n### 3 · ¿PROPONEN EL MISMO CODIGO DE REEMPLAZO (top-1)?")
    print(f"  {igual}/{n} = {100*igual/n:.2f}%")
    print("  LECTURA: esto separa el DIAGNOSTICO (¿el declarado esta mal?) del")
    print("  REMEDIO (¿cual deberia ser?). Los dos numeros pueden ser muy distintos,")
    print("  y si lo son, el remedio no se cita como si fuera el diagnostico.")

    for col in ("tipo_oc", "rubro_n1"):
        if col not in next(iter(A.values())):
            continue
        d = defaultdict(lambda: [0, 0])
        for i in ids:
            g = A[i][col]
            d[g][0] += 1
            if binario(A[i]) != binario(B[i]):
                d[g][1] += 1
        print(f"\n### 4 · discordancia binaria por `{col}` (solo grupos con n>=10)")
        chicos_n = chicos_d = chicos_g = 0
        for g, (tot, dis) in sorted(d.items(), key=lambda x: -x[1][0]):
            if tot >= 10:
                print(f"  {g:>8}: n={tot:>4}  discordantes={dis:>3}  ({100*dis/tot:5.1f}%)")
            else:
                chicos_g += 1
                chicos_n += tot
                chicos_d += dis
        if chicos_g:
            print(f"  [{chicos_g} grupos con n<10, agregados: n={chicos_n}, "
                  f"discordantes={chicos_d} ({100*chicos_d/chicos_n:.1f}%) — no se abren, "
                  f"no sostienen lectura]")

    print("\n" + "=" * 78)
    print("NINGUN NUMERO DE AQUI ES UNA CONCLUSION (§3.10). Releerlo contra su tabla.")
    print("=" * 78)


if __name__ == "__main__":
    main()
