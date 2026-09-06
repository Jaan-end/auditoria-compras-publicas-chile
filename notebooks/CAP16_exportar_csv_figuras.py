# -*- coding: utf-8 -*-
# =============================================================================
# CAP-16 · EXPORTADOR DE LOS CSV QUE LES FALTAN A LAS FIGURAS
# Capstone Big Data · Mercado Publico · sesion 25 (28-ago-2026)
#
# CELDA DE DATABRICKS. Pegar al final del notebook maestro v18, DESPUES de que
# hayan corrido la Celda 1-3 (carga) y CAP-0/CAP-0-BIS (utilidades).
#
# COSTO: DOS acciones de Spark. Es la corrida del dia mejor gastada que queda:
#        desbloquea TRES de las siete figuras del informe.
#
# -----------------------------------------------------------------------------
# POR QUE ESTA CELDA Y NO OTRA
# -----------------------------------------------------------------------------
# `graficos_capstone.py` corre fuera de Databricks y no cuesta cupo, pero tres
# de sus siete figuras se saltan solas porque les falta su CSV:
#
#   figura 4 · Pregunta 2, serie interrumpida   <- cap2_serie_onu_por_anio.csv
#   figura 6 · auditoria _year                  <- cap9bis_auditoria_year.csv
#   figura 2 · embudo de trazabilidad           <- cap9_cascada.csv  (YA RESUELTO:
#              va escrito a mano en csv/, copiado de la salida cruda de
#              01-RESULTADOS seccion 2. No cuesta corrida.)
#
# Las dos primeras se calculan aca. Las cifras NO son nuevas: son exactamente
# las que ya imprime la Celda 9-BIS del v18. Lo unico que agrega esta celda es
# escribirlas en CSV con los nombres de columna que `graficos_capstone.py`
# espera, para que la figura salga sin que nadie transcriba numeros a mano
# (que es como se producen las regresiones de la regla §1.20).
#
# UNA DIFERENCIA DELIBERADA CON LA CELDA 9-BIS
# La 9-BIS restringe a 2024 y 2025 porque su pregunta era el confundidor del
# 12-dic-2024. La figura 4 dibuja la serie COMPLETA con la restringida encima,
# asi que aca el semi-join de organismos (los presentes en 2024 Y en 2025) se
# aplica a TODOS los anios. Es el mismo conjunto de organismos; cambia el rango
# sobre el que se lo evalua. El veredicto de 2024 vs 2025 tiene que reproducir
# el +0,30 pp de `70680deaec66`: si no lo reproduce, hay algo mal y NO se
# escribe el CSV.
# =============================================================================

import os

MEDIR_DUPLICACION_LIC = False   # ver el bloque 16.C antes de ponerlo en True

RUN_ID_16 = globals().get("RUN_ID_CAP") or "sin_run_id"
DIR_SALIDA = f"/Volumes/workspace/default/mercado_publico/capstone_salidas/{RUN_ID_16}"

print("=" * 78)
print(f"CAP-16 · exportador de CSV para las figuras · RUN_ID_CAP={RUN_ID_16}")
print("=" * 78)

_falta = [n for n in ("cap_resolver", "cap_onu_clase", "cap_global")
          if n not in globals()]
if _falta:
    print(f"ABORTA: faltan utilidades en el kernel: {_falta}.")
    print("        Correr CAP-0 y CAP-0-BIS antes de esta celda.")
else:
    def _escribir_csv(nombre, cabecera, filas):
        """Escribe un CSV chico desde el driver. No usa Spark: son 10-20 filas."""
        os.makedirs(DIR_SALIDA, exist_ok=True)
        ruta = os.path.join(DIR_SALIDA, nombre)
        with open(ruta, "w", encoding="utf-8") as fh:
            fh.write(";".join(cabecera) + "\n")
            for f in filas:
                fh.write(";".join("" if v is None else str(v) for v in f) + "\n")
        print(f"   [OK] escrito {ruta}  ({len(filas)} filas)")
        print("        Descargarlo a la carpeta csv/ del repositorio "
              "(Catalog -> Volumes) y correr graficos_capstone.py.")
        return ruta

    # =========================================================================
    # 16.A · cap2_serie_onu_por_anio.csv  ->  FIGURA 4
    # =========================================================================
    print("\n" + "-" * 78)
    print("16.A · Serie de cobertura ONU por anio: completa y restringida")
    print("-" * 78)

    _oc = cap_global("df_oc_all") or cap_global("df_oc_linked")   # noqa: F821
    if _oc is None:
        print("ABORTA 16.A: falta df_oc_all / df_oc_linked en el kernel.")
    else:
        c_onu = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")                    # noqa: F821
        c_anio = cap_resolver(_oc, ["_year", "anio", "año"], "año (oc)")     # noqa: F821
        c_org = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"],  # noqa: F821
                             "organismo")
        c_uni = cap_resolver(_oc, ["_sinclasificar_CodigoUnidadCompra"],      # noqa: F821
                             "unidad de compra")

        if not c_onu or not c_anio or not (c_org or c_uni):
            print(f"ABORTA 16.A: ONU={c_onu} anio={c_anio} org={c_org} uni={c_uni}")
        else:
            ent = F.col(c_org) if c_org else F.col(c_uni)
            print(f"   Entidad del semi-join: {c_org or c_uni}")

            base = _oc.select(
                F.col(c_anio).cast("int").alias("anio"),
                F.trim(ent.cast("string")).alias("entidad"),
                cap_onu_clase(F.col(c_onu)).alias("clase_onu"),   # noqa: F821
            ).where(F.col("anio").isNotNull())

            _e = (F.col("entidad").isNotNull() & (F.col("entidad") != ""))
            orgs_ambos = (base.where(_e & (F.col("anio") == 2024)).select("entidad").distinct()
                          .join(
                              base.where(_e & (F.col("anio") == 2025)).select("entidad").distinct(),
                              "entidad", "inner"))

            def _serie(df, etiqueta):
                return (df.groupBy("anio")
                          .agg(F.lit(etiqueta).alias("serie"),
                               F.count(F.lit(1)).alias("lineas"),
                               (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))
                                ).alias("pct_onu_valido"),
                               F.countDistinct("entidad").alias("entidades")))

            # >>> ACCION DE SPARK 1 de 2 <<<
            filas = (_serie(base, "1_completa")
                     .unionByName(_serie(base.join(orgs_ambos, "entidad", "inner"),
                                         "2_restringida"))
                     .orderBy("serie", "anio").collect())

            d = {(r["serie"], r["anio"]): r for r in filas}
            anios = sorted({r["anio"] for r in filas})

            print(f"\n   {'anio':<7}{'lineas':>14}{'completa':>11}"
                  f"{'restringida':>13}{'organismos':>12}")
            print("   " + "-" * 57)
            salida = []
            for a in anios:
                rc = d.get(("1_completa", a))
                rr = d.get(("2_restringida", a))
                pc = round(rc["pct_onu_valido"], 4) if rc else None
                pr = round(rr["pct_onu_valido"], 4) if rr else None
                print(f"   {a:<7}{(rc['lineas'] if rc else 0):>14,}"
                      f"{(f'{pc:.2f}%' if pc is not None else '-'):>11}"
                      f"{(f'{pr:.2f}%' if pr is not None else '-'):>13}"
                      f"{(rr['entidades'] if rr else 0):>12,}")
                salida.append((a, pc, pr))

            # --- CONTROL: tiene que reproducir el +0,30 pp de 70680deaec66 ---
            r24 = d.get(("2_restringida", 2024))
            r25 = d.get(("2_restringida", 2025))
            print("\n   CONTROL (§1.16) — el delta restringido 2024->2025 tiene que dar")
            print("   +0,30 pp, que es la cifra congelada de RUN_ID_CAP=70680deaec66.")
            if r24 and r25:
                delta = r25["pct_onu_valido"] - r24["pct_onu_valido"]
                print(f"      medido ahora = {delta:+.2f} pp   "
                      f"(2024 {r24['pct_onu_valido']:.2f}% -> 2025 {r25['pct_onu_valido']:.2f}%)")
                ok = abs(delta - 0.30) <= 0.02
                print(f"      organismos en ambos anios = {r25['entidades']:,} "
                      f"(congelado: 1.079)")
                if ok:
                    print("      -> REPRODUCE. Se escribe el CSV.")
                    _escribir_csv("cap2_serie_onu_por_anio.csv",
                                  ("anio", "pct_completa", "pct_restringida"), salida)
                else:
                    print("      -> NO REPRODUCE. NO se escribe el CSV.")
                    print("      No es un problema del grafico: o cambio el universo cargado,")
                    print("      o la entidad del semi-join no es la misma. Mirar eso ANTES")
                    print("      de dibujar nada. Una figura que contradice una cifra")
                    print("      congelada sin explicacion es exactamente lo que la regla")
                    print("      §1.20 prohibe.")
            else:
                print("      NO SE PUEDE COMPARAR: falta 2024 o 2025 en la serie restringida.")

    # =========================================================================
    # 16.B · cap9bis_auditoria_year.csv  ->  FIGURA 6
    # =========================================================================
    print("\n" + "-" * 78)
    print("16.B · Auditoria _year vs anio(fecha_creacion)  (F19 §2.5)")
    print("-" * 78)

    _oc3 = cap_global("df_oc_all")            # noqa: F821
    if _oc3 is None:
        print("ABORTA 16.B: falta df_oc_all en el kernel.")
    elif "fecha_creacion_norm" not in _oc3.columns:
        print("ABORTA 16.B: falta 'fecha_creacion_norm'. Correr la Celda 4 (v18,")
        print("             corregida) antes de esta celda.")
    else:
        # >>> ACCION DE SPARK 2 de 2 <<<
        aud = (_oc3
               .where(F.col("fecha_creacion_norm").isNotNull())
               .withColumn("anio_creacion", F.year(F.col("fecha_creacion_norm")))
               .withColumn("desborde", (F.col("anio_creacion") != F.col("_year")).cast("int"))
               .groupBy("_year")
               .agg(F.count(F.lit(1)).alias("n_total"),
                    F.sum("desborde").alias("n_desborde"))
               .withColumn("pct_desborde",
                           F.round(100.0 * F.col("n_desborde") / F.col("n_total"), 4))
               .orderBy("_year")
               .collect())

        print(f"\n   {'_year':<8}{'n_total':>15}{'n_desborde':>13}{'pct':>10}")
        print("   " + "-" * 46)
        salida6 = []
        for r in aud:
            print(f"   {r['_year']:<8}{r['n_total']:>15,}{r['n_desborde']:>13,}"
                  f"{r['pct_desborde']:>9.4f}%")
            salida6.append((r["_year"], r["pct_desborde"]))

        n_tot = sum((r["n_total"] or 0) for r in aud)
        n_des = sum((r["n_desborde"] or 0) for r in aud)
        pct_glob = 100.0 * n_des / n_tot if n_tot else 0.0
        print(f"\n   GLOBAL: {n_des:,} de {n_tot:,} = {pct_glob:.4f}%")
        print("   CONTROL (§1.16): la cifra congelada es 0,5982% "
              "(307.775 / 51.451.528, RUN_ID_CAP=70680deaec66).")
        if abs(pct_glob - 0.5982) <= 0.01:
            print("   -> REPRODUCE. Se escribe el CSV.")
            _escribir_csv("cap9bis_auditoria_year.csv",
                          ("anio", "pct_discrepante"), salida6)
        else:
            print("   -> NO REPRODUCE. NO se escribe el CSV. Averiguar por que antes")
            print("      de dibujar la figura 6.")

    # =========================================================================
    # 16.C · OPCIONAL — medir la duplicacion de lic_historico/2025
    # =========================================================================
    # NO se corre por defecto. La duplicacion ya esta establecida por tres
    # metodos que no cuestan cupo (F26 §1.3), y ninguna cifra publicada depende
    # de ella (F26 §2). Esto solo agrega el numero exacto para el informe.
    #
    # Cuesta UN escaneo completo de licitaciones, que es la tabla grande. Si se
    # activa, hacerlo en una corrida propia, no encima de las dos de arriba.
    if MEDIR_DUPLICACION_LIC:
        print("\n" + "-" * 78)
        print("16.C · Duplicacion en el lado licitacion  (F26 §1.3)")
        print("-" * 78)
        _lic = cap_global("df_lic_all")      # noqa: F821
        if _lic is None:
            print("ABORTA 16.C: falta df_lic_all.")
        else:
            c_k = cap_resolver(_lic, ["codigo_externo"], "clave lic")     # noqa: F821
            c_i = cap_resolver(_lic, ["_sinclasificar_Codigoitem"], "item")  # noqa: F821
            c_of = cap_resolver(_lic, ["_sinclasificar_Nombre de la Oferta",  # noqa: F821
                                       "_sinclasificar_RutProveedor"], "oferta")
            if not c_k or not c_i:
                print(f"ABORTA 16.C: clave={c_k} item={c_i}")
            else:
                # La clave natural de una fila de licitacion es
                # (licitacion, item, oferta). Si count(*) supera al distinct de
                # esa terna, las filas sobrantes son copias literales.
                llaves = [F.col(c_k), F.col(c_i)] + ([F.col(c_of)] if c_of else [])
                dup = (_lic.groupBy("_year")
                       .agg(F.count(F.lit(1)).alias("filas"),
                            F.countDistinct(*llaves).alias("filas_distintas"),
                            F.countDistinct(F.col(c_k)).alias("licitaciones"))
                       .withColumn("factor", F.round(F.col("filas") /
                                                     F.col("filas_distintas"), 3))
                       .orderBy("_year").collect())
                print(f"\n   {'_year':<8}{'filas':>14}{'distintas':>14}"
                      f"{'licitac.':>12}{'factor':>9}")
                print("   " + "-" * 57)
                for r in dup:
                    print(f"   {r['_year']:<8}{r['filas']:>14,}{r['filas_distintas']:>14,}"
                          f"{r['licitaciones']:>12,}{r['factor']:>9.3f}")
                print("\n   LECTURA: factor ~1,0 = sin copias. factor ~3,0 en 2025 (y ~1,2")
                print("   en 2024, que solo tiene dos meses de la ventana) confirma F26 §1.3")
                print("   con el numero exacto. Si el factor da 1,0 en 2025, la duplicacion")
                print("   NO es de filas identicas: son vintages distintos del mismo mes, y")
                print("   entonces la terna (lic, item, oferta) no alcanza para deduplicar.")

print(f"\nCAP-16 termina. RUN_ID_CAP={RUN_ID_16}")
print("Pegar esta salida completa en el chat.")
