# Databricks notebook source
# MAGIC %md
# MAGIC # v18 · sesión 17 (24-ago-2026) — diagnóstico del fallo de CAP-12B
# MAGIC
# MAGIC **NO CORRER ANTES DE QUE EXISTA EL REPOSITORIO.** Esta celda no es requisito
# MAGIC de aprobación y consume tope diario. Está escrita para que el diagnóstico no
# MAGIC se pierda, no para hacerlo hoy.
# MAGIC
# MAGIC ## Qué pasó
# MAGIC CAP-12B corrió el 24-ago con `TOPE=200.000` y **no replicó** su criterio
# MAGIC predeclarado:
# MAGIC
# MAGIC | métrica | CAP-12 (válida) | CAP-12B | razón |
# MAGIC |---|---:|---:|---:|
# MAGIC | declarado ES el mejor (línea) | 30,57% | 15,24% | 0,4985 |
# MAGIC | declarado ES el mejor (CLP) | 28,84% | 15,64% | 0,5423 |
# MAGIC | similitud 0 con todos los hermanos | 62,28% | **62,28%** | **replica** |
# MAGIC | lectura condicional | 81,04% | 40,40% | 0,4985 |
# MAGIC
# MAGIC **Por protocolo (`F16` §5) CAP-12B está descartada y ninguna cifra suya se
# MAGIC cita.** Esta celda no la resucita: sólo dice *por qué* falló.
# MAGIC
# MAGIC ## Qué se sabe ya, sin correr nada
# MAGIC 1. El conjunto «no opinó» replica **exacto** → la vectorización y el join por
# MAGIC    `fam6` están bien.
# MAGIC 2. El export de 200 desacuerdos salió **idéntico fila por fila** al de CAP-12
# MAGIC    (200/200 pares, mismo `mejor_cod`, `mejor_sim` coincidiendo a ~1e-15 en 198
# MAGIC    de 200) → **el motor de similitud reproduce a la UDF de Python.**
# MAGIC 3. El fallo está confinado a la bandera `declarado_es_mejor`, y cae casi
# MAGIC    exactamente a la mitad.
# MAGIC
# MAGIC ## La hipótesis
# MAGIC ```python
# MAGIC _w12b = W.partitionBy("par_id").orderBy(F.col("sim").desc(), F.col("codigo"))
# MAGIC _top1 = _sim.withColumn("_rk", F.row_number().over(_w12b)).where(F.col("_rk") == 1)
# MAGIC ...
# MAGIC .withColumn("declarado_es_mejor", F.col("onu8") == F.col("mejor_cod"))
# MAGIC ```
# MAGIC `row_number()` fuerza **un** ganador y desempata por **código ascendente**.
# MAGIC Donde el código declarado **empata en la similitud máxima** con un hermano de
# MAGIC código menor, CAP-12B lo declara perdedor. CAP-12 (Spark ML) puede resolver el
# MAGIC empate al revés.
# MAGIC
# MAGIC **La definición correcta de «es el mejor de su familia» es empatar en el
# MAGIC máximo, no ganar el desempate.** Si la hipótesis es cierta, la corrección es
# MAGIC de una línea — y entonces el coseno nativo sin UDF queda disponible y la
# MAGIC corrida de 2.000.000 deja de costar cinco horas.
# MAGIC
# MAGIC ## Costo
# MAGIC Una agregación sobre 200.000 pares × ~9 hermanos. **No hay una sola llamada a
# MAGIC Python por fila.** Si `_tmp_cap12b_usa` sigue en el Volume, no hay que
# MAGIC recalcular los pares.
# MAGIC
# MAGIC **Requiere:** `_catB`, `_idf`, `_vectorizar`, `_tokenizar`, `_usaB` — todo lo
# MAGIC deja la celda de CAP-12B del `v17`.

# COMMAND ----------

# --- DIAG-12B · ¿el fallo es el desempate? ----------------------------------
_falta = [n for n in ("_catVec", "_usaB", "_catB") if g7_global(n) is None]
if _falta:
    print(f"ABORTA DIAG-12B: faltan en memoria {_falta}. Correr la celda CAP-12B del v17 primero.")
else:
    print("=" * 78)
    print(f"DIAG-12B · anatomía del desempate · RUN_ID_G7={RUN_ID_G7}")
    print("=" * 78)

    EPS_12B = 1e-9   # tolerancia de empate en punto flotante

    # 1 · recomputar la matriz de similitudes y MATERIALIZARLA
    #     (barato: sin UDF. Se guarda para que cualquier diagnóstico posterior
    #      sea gratis y no gaste tope diario otra vez.)
    _linVecD = _vectorizar(_tokenizar(_usaB.select("par_id", "fam6", "doc"), ["par_id", "fam6"]),
                           ["par_id", "fam6"]).withColumnRenamed("val", "val_l")
    _simD = (_linVecD.join(_catVec, on=["fam6", "term"], how="inner")
                     .groupBy("par_id", "codigo")
                     .agg(F.sum(F.col("val_l") * F.col("val_c")).alias("sim")))
    _rs = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}/_tmp_cap12b_sim"
    _simD.write.mode("overwrite").parquet(_rs)
    _simD = spark.read.parquet(_rs)                                          # noqa: F821
    print(f"   matriz de similitudes materializada en {_rs}")

    # 2 · máximo por par, y la similitud del código DECLARADO en ese mismo par
    _maxD = _simD.groupBy("par_id").agg(F.max("sim").alias("max_sim"),
                                        F.count(F.lit(1)).alias("n_hermanos"))
    _empD = (_simD.join(_maxD, on="par_id", how="inner")
                  .where(F.col("sim") >= F.col("max_sim") - F.lit(EPS_12B))
                  .groupBy("par_id").agg(F.count(F.lit(1)).alias("n_en_el_maximo"),
                                         F.min("codigo").alias("cod_desempate")))

    _decD = (_usaB.select("par_id", "onu8", "n_lineas", "clp")
                  .join(_simD.select("par_id", F.col("codigo").alias("onu8"),
                                     F.col("sim").alias("sim_declarado")),
                        on=["par_id", "onu8"], how="left")
                  .join(_maxD, on="par_id", how="left")
                  .join(_empD, on="par_id", how="left")
                  .withColumn("sim_declarado", F.coalesce("sim_declarado", F.lit(0.0)))
                  .withColumn("max_sim", F.coalesce("max_sim", F.lit(0.0)))
                  .withColumn("n_en_el_maximo", F.coalesce("n_en_el_maximo", F.lit(0)))
                  # LA definición correcta: empatar en el máximo ES ser el mejor
                  .withColumn("es_mejor_EMPATE",
                              (F.col("max_sim") > 0) &
                              (F.col("sim_declarado") >= F.col("max_sim") - F.lit(EPS_12B)))
                  # la definición que usó 12B: ganar el desempate por código ascendente
                  .withColumn("es_mejor_DESEMPATE", F.col("onu8") == F.col("cod_desempate")))

    _rd = f"{RUTA_SALIDA_G7}/{RUN_ID_G7}/_tmp_cap12b_diag"
    _decD.write.mode("overwrite").parquet(_rd)
    _decD = spark.read.parquet(_rd)                                          # noqa: F821

    _r = _decD.agg(
        F.sum("n_lineas").alias("lineas"),
        F.sum("clp").alias("clp"),
        F.sum(F.when(F.col("max_sim") <= 0.0, F.col("n_lineas")).otherwise(0)).alias("sim0"),
        F.sum(F.when(F.col("es_mejor_EMPATE"), F.col("n_lineas")).otherwise(0)).alias("ok_emp"),
        F.sum(F.when(F.col("es_mejor_DESEMPATE"), F.col("n_lineas")).otherwise(0)).alias("ok_des"),
        F.sum(F.when(F.col("es_mejor_EMPATE"), F.col("clp")).otherwise(0.0)).alias("clp_emp"),
        F.sum(F.when(F.col("es_mejor_EMPATE") & ~F.col("es_mejor_DESEMPATE"),
                     F.col("n_lineas")).otherwise(0)).alias("perdidas_por_desempate"),
        F.sum(F.when(F.col("n_en_el_maximo") >= 2, F.col("n_lineas")).otherwise(0)).alias("lin_con_empate"),
    ).collect()[0]

    _lin = _r["lineas"] or 1
    _opino = _lin - (_r["sim0"] or 0)

    print("\n### DIAG.1 · ¿cuántos empates hay?")
    print(f"    líneas con 2 o más códigos empatados en el máximo : {g7_pc(_r['lin_con_empate'], _lin)}")
    print(f"    líneas que 12B perdió SOLO por el desempate       : {g7_pc(_r['perdidas_por_desempate'], _lin)}")

    print("\n### DIAG.2 · la métrica con las dos definiciones")
    print(f"    similitud 0 con todos los hermanos     : {g7_pc(_r['sim0'], _lin)}   [12: 62,28%]")
    print(f"    declarado es el mejor · DESEMPATE (12B): {g7_pc(_r['ok_des'], _lin)}   [12B imprimió 15,24%]")
    print(f"    declarado es el mejor · EMPATE  (corr.): {g7_pc(_r['ok_emp'], _lin)}   [12  imprimió 30,57%]")
    print(f"    por CLP · EMPATE                       : {g7_pc(_r['clp_emp'], _r['clp'])}   [12  imprimió 28,84%]")
    print(f"    condicional · EMPATE                   : {g7_pc(_r['ok_emp'], _opino)}   [12  imprimió 81,04%]")

    print("\n### DIAG.3 · VEREDICTO")
    print("    Si la columna EMPATE reproduce 30,57% / 28,84% / 81,04%, la hipótesis")
    print("    del desempate queda CONFIRMADA y la corrección de CAP-12B es de una")
    print("    línea: definir 'es el mejor' como empatar en el máximo.")
    print("    Si NO reproduce, la hipótesis se descarta y CAP-12B se abandona.")
    print("    En cualquier caso: nada de 12B entra al informe hasta que replique")
    print("    su criterio predeclarado completo (§1.19).")

    print("\n### DIAG.4 · los 20 pares con más líneas perdidas por el desempate")
    for r in (_decD.where(F.col("es_mejor_EMPATE") & ~F.col("es_mejor_DESEMPATE"))
                   .orderBy(F.col("n_lineas").desc()).limit(20).collect()):
        print(f"    {r['onu8']}  gana {r['cod_desempate']}  "
              f"empatados={r['n_en_el_maximo']}  sim={r['max_sim']:.4f}  "
              f"líneas={r['n_lineas']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Si la hipótesis se confirma — la corrección, y sólo entonces
# MAGIC En la celda CAP-12B del `v17`, reemplazar:
# MAGIC ```python
# MAGIC .withColumn("declarado_es_mejor",
# MAGIC             F.coalesce(F.col("onu8") == F.col("mejor_cod"), F.lit(False)))
# MAGIC ```
# MAGIC por una comparación contra la similitud del propio código declarado:
# MAGIC ```python
# MAGIC # sim_declarado viene de un left join de _sim sobre (par_id, onu8)
# MAGIC .withColumn("declarado_es_mejor",
# MAGIC             (F.col("max_sim") > 0) &
# MAGIC             (F.col("sim_declarado") >= F.col("max_sim") - F.lit(1e-9)))
# MAGIC ```
# MAGIC y **volver a correr la validación completa a 200.000 antes de citar nada.**
# MAGIC Recién si replica 30,57% · 28,84% · 62,28% · 81,04% se puede pensar en subir
# MAGIC el tope — y aun así, sólo cuando el repositorio, el informe, el dashboard y
# MAGIC los tres ensayos estén hechos (regla de sacrificio, `S17` §4.2).
