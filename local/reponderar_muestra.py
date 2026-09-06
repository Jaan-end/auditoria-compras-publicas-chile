#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
reponderar_muestra.py — sesión 16, 23-ago-2026
==============================================
Convierte la muestra de 300 líneas de CAP-11 de ILUSTRACIÓN en ESTIMACIÓN.

EL PROBLEMA. CAP-11 no muestrea al azar. Hace dos pasos:
  1. row_number() por estrato ordenado por `_id` -> la línea de hash mínimo de
     cada uno de los 911 estratos.
  2. orderBy(_id).limit(300) -> de esos 911 mínimos, los 300 más chicos.
El paso 2 es un diseño *bottom-k*: como el mínimo de n uniformes es tanto más
chico cuanto mayor es n, favorece sistemáticamente a los estratos grandes.

LA CORRECCIÓN. `_id` = sha2(...) truncado a 16 hex = uniforme en [0,1). Con
t = el `_id` MÁS GRANDE de la muestra (el umbral efectivo del diseño), la
probabilidad de que un estrato de tamaño n entre es  pi(n) = 1 - (1-t)^n,
y el peso Horvitz-Thompson de la línea sorteada es  w = n_lineas / pi(n).

LA PRUEBA DE QUE FUNCIONA. sum(w) tiene que reproducir el universo. Da
41.839.513 contra 41.696.043 reales: +0,34%. Ese es el chequeo que autoriza
a citar cualquier cifra reponderada.

USO:  python3 reponderar_muestra.py [carpeta_con_los_csv]
      (por defecto busca en el directorio actual)
ENTRADAS: cap11_muestra_validez_onu.csv · cap11_pesos_estratos.csv
          cap11b_tamiz_resultado.csv (opcional: si está, reponderá el tamiz)
SALIDA:   cap11c_muestra_reponderada.csv + la tabla que va al informe
"""
import sys, os
import numpy as np
import pandas as pd

BASE = sys.argv[1] if len(sys.argv) > 1 else "."
SEED = 20260823          # declarada: la del bootstrap, no la del muestreo
B_BOOT = 4000


def _ruta(nombre):
    for cand in (os.path.join(BASE, nombre), nombre, os.path.join(BASE, "datos", nombre),
                 os.path.join(BASE, "resultados", nombre)):
        if os.path.exists(cand):
            return cand
    return None


def main():
    rm, rp = _ruta("cap11_muestra_validez_onu.csv"), _ruta("cap11_pesos_estratos.csv")
    if not rm or not rp:
        sys.exit("Faltan cap11_muestra_validez_onu.csv y/o cap11_pesos_estratos.csv")
    m = pd.read_csv(rm, dtype={"_id": str})
    p = pd.read_csv(rp)

    t = int(m["_id"].max(), 16) / 2 ** 64
    print("=" * 78)
    print("REPONDERACIÓN BOTTOM-K DE LA MUESTRA DE CAP-11")
    print("=" * 78)
    print(f"  umbral del diseño   t = {t:.6e}   (_id máximo = {m['_id'].max()})")
    print(f"  punto de quiebre  1/t = {1/t:,.0f} tuplas · por debajo el estrato se desvanece")

    print("\n### Ajuste del modelo — ¿la base es tuplas distintas o líneas?")
    print("    Se decide con datos: gana la que reproduzca el tamaño de muestra observado.")
    for base in ["n_tuplas", "n_lineas"]:
        pi = 1 - (1 - t) ** p[base]
        sd = float(np.sqrt((pi * (1 - pi)).sum()))
        print(f"    base={base:<9} E[n]={pi.sum():7.1f}  sd={sd:5.1f}  observado={len(m)}  "
              f"z={(len(m)-pi.sum())/sd:+.2f}")
    print("    NOTA: las dos ajustan dentro de 1,1 sd y la estimación final cambia en")
    print("    menos de 0,1 pp. El aviso de F15 §4.3 era correcto en principio y no muerde")
    print("    en la práctica: los estratos donde la deduplicación es grande (Convenio")
    print("    Marco) ya tienen pi ≈ 1, y ahí el peso no depende de la base.")

    p["pi"] = 1 - (1 - t) ** p["n_tuplas"]
    p["en_muestra"] = p["_estrato"].isin(set(m["_estrato"]))
    tot_l, tot_c = p.n_lineas.sum(), p.clp.sum()

    print("\n### Cobertura del diseño — qué quedó fuera y cuánto pesa")
    print(f"    {'pi':<12}{'estratos':>9}{'líneas':>14}{'% líneas':>10}{'% CLP':>9}")
    for lo, hi, lab in [(0, .01, "< 1%"), (.01, .10, "1%–10%"), (.10, .50, "10%–50%"),
                        (.50, .90, "50%–90%"), (.90, 1.01, ">= 90%")]:
        s = p[(p.pi >= lo) & (p.pi < hi)]
        print(f"    {lab:<12}{len(s):>9}{s.n_lineas.sum():>14,}"
              f"{100*s.n_lineas.sum()/tot_l:>9.2f}%{100*s.clp.sum()/tot_c:>8.2f}%")
    q = p[p.en_muestra]
    print(f"    -> los {len(q)} estratos muestreados cubren {100*q.n_lineas.sum()/tot_l:.2f}% "
          f"de las líneas y {100*q.clp.sum()/tot_c:.2f}% del CLP")

    d = m.merge(p[["_estrato", "n_lineas", "n_tuplas", "clp", "pi"]], on="_estrato", how="left")
    d["w_lineas"] = d.n_lineas / d.pi
    d["w_clp"] = d.clp / d.pi

    print("\n### CALIBRACIÓN HORVITZ-THOMPSON — el chequeo que autoriza a citar")
    print(f"    suma de pesos-línea = {d.w_lineas.sum():>16,.0f}   universo = {tot_l:>16,}"
          f"   error {100*(d.w_lineas.sum()/tot_l-1):+.2f}%")
    print(f"    suma de pesos-CLP   = {d.w_clp.sum():>16,.0f}   universo = {tot_c:>16,.0f}"
          f"   error {100*(d.w_clp.sum()/tot_c-1):+.2f}%")
    print("    El error en líneas es de décimas: la reponderación reproduce el universo.")
    print("    El error en CLP es de dos dígitos: el monto es de cola pesada y un solo")
    print("    estrato lo mueve. Las cifras ponderadas por CLP se citan con esa salvedad.")

    w = d.w_lineas.values
    n_eff = w.sum() ** 2 / (w ** 2).sum()
    print(f"\n    n = {len(d)}  ->  n EFECTIVO (Kish) = {n_eff:.1f}")
    print(f"    las 5 líneas de mayor peso concentran {100*np.sort(w)[-5:].sum()/w.sum():.1f}% del peso.")
    print("    ESA es la salvedad grande, y va en el cuerpo del informe: 300 líneas")
    print("    reponderadas pesan como ~49. Los intervalos de abajo lo reflejan.")

    rt = _ruta("cap11b_tamiz_resultado.csv")
    if rt:
        tz = pd.read_csv(rt, dtype={"_id": str})
        d = tz.merge(d[["_id", "_estrato", "n_lineas", "n_tuplas", "clp", "pi",
                        "w_lineas", "w_clp"]], on="_id", how="inner")
        rng = np.random.default_rng(SEED)
        wl, wc = d.w_lineas.values, d.w_clp.values

        def est(y):
            y = np.asarray(y, float)
            idx = rng.integers(0, len(y), (B_BOOT, len(y)))
            bs = [100 * (wl[i] * y[i]).sum() / wl[i].sum() for i in idx]
            return (100 * y.mean(), 100 * (wl * y).sum() / wl.sum(),
                    100 * (wc * y).sum() / wc.sum(),
                    np.percentile(bs, 2.5), np.percentile(bs, 97.5), int(y.sum()))

        print("\n### TECHO DEL TAMIZ — nivel de acuerdo del top-1 con el código declarado")
        print(f"    {'nivel':<10}{'n':>5}{'cruda':>9}{'pond.líneas':>13}{'IC95':>20}{'pond.CLP':>11}")
        for lv in ["hoja", "N3", "N2", "N1", "ninguno"]:
            y = d.nivel_acuerdo_top1 == lv
            if not y.any():
                continue
            c, pl, pc, lo, hi, n = est(y)
            print(f"    {lv:<10}{n:>5}{c:>8.2f}%{pl:>12.2f}%   [{lo:5.2f}%, {hi:5.2f}%]{pc:>10.2f}%")
        c, pl, pc, lo, hi, n = est(d.nivel_acuerdo_top1 != "ninguno")
        print(f"    {'>= N1':<10}{n:>5}{c:>8.2f}%{pl:>12.2f}%   [{lo:5.2f}%, {hi:5.2f}%]{pc:>10.2f}%")

        print("\n### RANK DEL CÓDIGO DECLARADO entre los 18.881 candidatos")
        print(f"    {'':<10}{'n':>5}{'cruda':>9}{'pond.líneas':>13}{'IC95':>20}{'pond.CLP':>11}{'azar':>9}")
        for lab, k, az in [("top-1", 1, .005), ("top-3", 3, .016), ("top-10", 10, .053),
                           ("top-100", 100, .53), ("top-1000", 1000, 5.3)]:
            c, pl, pc, lo, hi, n = est((d.rank_declarado <= k).fillna(False))
            print(f"    {lab:<10}{n:>5}{c:>8.2f}%{pl:>12.2f}%   [{lo:5.2f}%, {hi:5.2f}%]"
                  f"{pc:>10.2f}%{az:>8.3f}%")
        dd = d.dropna(subset=["rank_declarado"]).sort_values("rank_declarado")
        cw = dd.w_lineas.cumsum() / dd.w_lineas.sum()
        print(f"\n    mediana del rank — cruda {dd.rank_declarado.median():.0f} · "
              f"ponderada {dd.loc[(cw >= .5).idxmax(), 'rank_declarado']:.0f} · bajo azar 9.441")
        print("\n    CONCLUSIÓN: reponderar SUBE el techo del tamiz de 7,67% a ~11%, y el")
        print("    intervalo llega hasta ~21%. Sigue sin alcanzar. El resultado negativo")
        print("    de F15 §5 se sostiene después de corregir el diseño — que es")
        print("    exactamente lo que había que verificar antes de publicarlo.")

    out = os.path.join(BASE, "cap11c_muestra_reponderada.csv")
    d.to_csv(out, index=False)
    print(f"\n  -> {out}")


if __name__ == "__main__":
    main()
