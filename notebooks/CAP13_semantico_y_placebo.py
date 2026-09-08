# Databricks notebook source
# =============================================================================
# CAP-13 · Segunda mirada algorítmica (Word2Vec) + placebo del semi-join
# ESTADO: [PROPUESTA] — escrito el 26-ago-2026, NO CORRIDO EN DATABRICKS.
#
#
# CÓMO USARLO:
#   Bloque 1 (CAP-13)      — pegar como celda nueva DESPUÉS de CAP-12
#                            (celdas_capstone_v16_CAP12_PARCHE.py / v12/v14),
#                            en el MISMO kernel/sesión — reutiliza _cat,
#                            _pares_usa y STOPWORDS_ES ya calculados por CAP-12
#                            vía g7_global(), sin recalcular ni gastar cupo.
#   Bloque 2 (CAP-13-BIS)  — pegar como celda nueva DESPUÉS de la Celda 9-BIS
#                            de etapa2_databricks_v18_COMPLETO.py, en el mismo
#                            kernel — reutiliza RUN_ID_CAP, cap_resolver,
#                            cap_global, cap_onu_clase, cap_print_tabla.
#   Bloque 3 (CAP-13-TER)  — igual que el Bloque 2, pensado para correr justo
#                            después.
#
# Los tres bloques son baratos (una pasada de Spark cada uno) y están
# pensados para correr JUNTOS, en la misma sesión, respetando la regla de
# una corrida de Databricks por día (S17 §1.18).
# =============================================================================


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-13 · ¿El código declarado es razonable, visto por un segundo algoritmo?
# MAGIC
# MAGIC **Por qué existe esta celda.** El tamiz léxico de CAP-11 (`screener_onu.py`,
# MAGIC TF-IDF de n-gramas de caracteres) y CAP-12 (Spark ML, TF-IDF de palabras +
# MAGIC coseno) son dos variantes del MISMO tipo de algoritmo: ambos miden
# MAGIC superposición de texto, ninguno entiende significado. `Word2Vec` (ya
# MAGIC incluido en `pyspark.ml.feature`, sin dependencias nuevas) entrena vectores
# MAGIC DENSOS donde palabras que aparecen en contextos parecidos quedan cerca en el
# MAGIC espacio, aunque no compartan ni una letra — es la forma más barata de
# MAGIC preguntar "¿el desacuerdo léxico es de verdad conceptual, o solo de
# MAGIC vocabulario?" sin salir de Spark ni de Databricks Free Edition.
# MAGIC
# MAGIC **Criterio de éxito declarado ANTES de correr (disciplina `[PRUEBA]` del
# MAGIC proyecto):** si la tasa "declarado ES el mejor de su familia" de CAP-13
# MAGIC queda dentro de ±10 pp de la de CAP-12 (30,57%, o 81,04% en su lectura
# MAGIC condicional), se declara **CONSISTENTE** — dos algoritmos distintos por
# MAGIC diseño miden aproximadamente lo mismo, lo que refuerza la confianza en el
# MAGIC hallazgo. Si diverge más de eso, se declara que **los métodos capturan
# MAGIC señales distintas**, y las dos cifras se citan por separado, nunca
# MAGIC mezcladas ni promediadas — la misma disciplina que ya se aplicó al
# MAGIC descartar CAP-12B por no replicar su propio criterio.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, Word2Vec, Normalizer
from pyspark.sql import functions as F, Window as W

print("\n" + "=" * 78)
print(f"CAP-13 · Segunda mirada algorítmica (Word2Vec) · RUN_ID_G7={RUN_ID_G7}")  # noqa: F821
print("=" * 78)

_cat13 = g7_global("_cat")            # noqa: F821 — catálogo ya vectorizado por CAP-12 (12.A)
_pares13 = g7_global("_pares_usa")    # noqa: F821 — la MISMA muestra de pares que evaluó CAP-12

if _cat13 is None or _pares13 is None:
    print("ABORTA: correr CAP-10, CAP-11 y CAP-12 (12.A/12.B) primero en este kernel.")
    print("        CAP-13 reutiliza _cat y _pares_usa a propósito — no vuelve a leer")
    print("        ni el catálogo ni la muestra de pares desde disco.")
else:
    _n_pares13 = _pares13.count()
    print(f"   reutilizando {_n_pares13:,} pares ya evaluados por CAP-12 (misma muestra,")
    print("   comparación directa, sin gastar más cupo de Spark del que ya se gastó).")

    # --- 13.A · Pipeline Word2Vec (en vez de CountVectorizer+IDF de CAP-12) ---
    _pipe13 = Pipeline(stages=[
        RegexTokenizer(inputCol="doc", outputCol="tok", pattern=r"\s+", minTokenLength=3),
        StopWordsRemover(inputCol="tok", outputCol="tok2", stopWords=STOPWORDS_ES),  # noqa: F821
        Word2Vec(inputCol="tok2", outputCol="w2v", vectorSize=100, minCount=2, seed=20260826),
        Normalizer(inputCol="w2v", outputCol="vec", p=2.0),
    ])
    # Se entrena sobre la UNIÓN de catálogo + líneas evaluadas — Word2Vec necesita
    # ver ambos vocabularios en el mismo espacio para que la comparación tenga sentido.
    _corpus13 = _cat13.select("doc").unionByName(_pares13.select("doc")).distinct()
    _model13 = _pipe13.fit(_corpus13)
    print(f"   Word2Vec entrenado · vectorSize=100 · vocabulario del corpus conjunto")

    _cat13_v = _model13.transform(_cat13).select(
        F.col("codigo").alias("cand_cod"), F.col("NombreProducto").alias("cand_nom"),
        F.col("fam6"), F.col("vec").alias("cand_vec"))
    _lin13_v = _model13.transform(_pares13).select(
        "doc", "onu8", "fam6", "n_lineas", "clp", F.col("vec").alias("linea_vec"))

    # --- 13.B · similitud coseno contra los hermanos (misma UDF que CAP-12) ---
    def _cos13(a, b):
        if a is None or b is None:
            return 0.0
        from pyspark.ml.linalg import DenseVector
        return float(DenseVector(a).dot(DenseVector(b)))

    from pyspark.sql.types import DoubleType
    _udf_cos13 = F.udf(_cos13, DoubleType())

    _scored13 = (_lin13_v.join(F.broadcast(_cat13_v), on="fam6", how="inner")
                         .withColumn("sim", _udf_cos13(F.col("linea_vec"), F.col("cand_vec"))))

    _w13 = W.partitionBy("doc", "onu8").orderBy(F.col("sim").desc(), F.col("cand_cod"))
    _best13 = (_scored13.withColumn("_rk", F.row_number().over(_w13))
                        .where(F.col("_rk") == 1)
                        .select("doc", "onu8", "n_lineas", "clp",
                                F.col("cand_cod").alias("mejor_cod"),
                                F.col("sim").alias("mejor_sim"))
                        .withColumn("declarado_es_mejor", F.col("onu8") == F.col("mejor_cod")))

    # --- 13.C · resultado + comparación DECLARADA contra CAP-12 --------------
    _r13 = _best13.agg(
        F.count(F.lit(1)).alias("pares"),
        F.sum("n_lineas").alias("lineas"),
        F.sum(F.when(F.col("declarado_es_mejor"), F.col("n_lineas")).otherwise(0)).alias("lineas_ok"),
        F.sum(F.when(F.col("mejor_sim") <= 0.0, F.col("n_lineas")).otherwise(0)).alias("lineas_sim0"),
    ).collect()[0]

    _tasa13 = cap_pct(_r13["lineas_ok"], _r13["lineas"]) or 0.0     # noqa: F821

    print("\n### 13.C · ¿El código declarado es el mejor de su familia, según Word2Vec?")
    print(f"    pares evaluados       : {_r13['pares']:,}")
    print(f"    líneas que representan: {_r13['lineas']:,}")
    print(f"    declarado ES el mejor : {_tasa13:.2f}%  (por línea)")
    print(f"    similitud 0 con TODOS los hermanos: "
          f"{cap_pct(_r13['lineas_sim0'], _r13['lineas']) or 0:.2f}%")  # noqa: F821

    _tasa12 = g7_global("_tasa_cap12_lineas_ok")   # si CAP-12 la dejó guardada; si no, declarar a mano
    print("\n### 13.D · Veredicto — comparación DECLARADA contra CAP-12 (criterio ±10 pp)")
    if _tasa12 is not None:
        _delta13 = _tasa13 - _tasa12
        print(f"    CAP-12  (léxico, TF-IDF palabras) : {_tasa12:.2f}%")
        print(f"    CAP-13  (semántico, Word2Vec)     : {_tasa13:.2f}%")
        print(f"    delta                             : {_delta13:+.2f} pp")
        if abs(_delta13) <= 10.0:
            print("    → CONSISTENTE: los dos algoritmos, distintos por diseño, miden")
            print("      aproximadamente lo mismo. Refuerza la confianza en el hallazgo.")
        else:
            print("    → DIVERGENTE: los métodos capturan señales distintas. Citar las dos")
            print("      cifras por separado, nunca mezcladas ni promediadas (misma regla")
            print("      que descartó CAP-12B).")
    else:
        print("    [aviso] no se encontró la tasa de CAP-12 en el kernel (g7_global).")
        print(f"    Comparar a mano contra el 30,57% (bruto) / 81,04% (condicional) ya citados.")

    print(f"\nCAP-13 termina. RUN_ID_G7={RUN_ID_G7}")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-13-BIS · Placebo del semi-join (Mejora nº1) sobre pares de años sin
# MAGIC # intervención conocida
# MAGIC
# MAGIC **Por qué existe esta celda.** La Mejora nº1 (Celda 9-BIS) mostró que el
# MAGIC +0,28 pp de 2024-2025 sobrevive restringido a los mismos organismos
# MAGIC (+0,30 pp). Para saber si +0,30 pp es una señal real o simplemente lo que
# MAGIC se mueve un indicador de un año a otro por pura rotación de organismos, se
# MAGIC repite EXACTAMENTE el mismo código sobre pares de años donde no hay ningún
# MAGIC cambio normativo conocido el mismo día: 2022-2023 y 2023-2024.

# COMMAND ----------

print("\n" + "=" * 78)
print(f"CAP-13-BIS · Placebo del semi-join · RUN_ID_CAP={RUN_ID_CAP}")  # noqa: F821
print("=" * 78)

_oc_bis = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")  # noqa: F821

if _oc_bis is None:
    print("ABORTA: falta df_oc_all/df_oc_linked en el kernel.")
else:
    c_onu_bis = cap_resolver(_oc_bis, ["onu_oc"], "ONU (oc)")                                    # noqa: F821
    c_anio_bis = cap_resolver(_oc_bis, ["_year", "anio", "año"], "año (oc)")                      # noqa: F821
    c_org_bis = cap_resolver(_oc_bis, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")     # noqa: F821

    PARES_PLACEBO = [(2022, 2023), (2023, 2024)]   # sin cambio normativo conocido el mismo día

    if not c_onu_bis or not c_anio_bis or not c_org_bis:
        print("ABORTA: faltan columnas mínimas (ONU, año, organismo).")
    else:
        resumen_placebo = []
        for anio_a, anio_b in PARES_PLACEBO:
            base_bis = _oc_bis.select(
                F.col(c_anio_bis).cast("int").alias("anio"),
                F.trim(F.col(c_org_bis).cast("string")).alias("entidad"),
                cap_onu_clase(F.col(c_onu_bis)).alias("clase_onu"),                                # noqa: F821
            ).where(F.col("anio").isin(anio_a, anio_b))

            orgs_a = (base_bis.where(F.col("anio") == anio_a).select("entidad")
                              .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                              .distinct())
            orgs_b = (base_bis.where(F.col("anio") == anio_b).select("entidad")
                              .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                              .distinct())
            orgs_comunes = orgs_a.join(orgs_b, "entidad", "inner")
            restringido_bis = base_bis.join(orgs_comunes, "entidad", "inner")

            fila_a = (restringido_bis.where(F.col("anio") == anio_a)
                      .agg((100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct"))
                      .collect()[0]["pct"])
            fila_b = (restringido_bis.where(F.col("anio") == anio_b)
                      .agg((100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct"))
                      .collect()[0]["pct"])
            n_comunes = orgs_comunes.count()
            delta_bis = (fila_b or 0) - (fila_a or 0)
            resumen_placebo.append((anio_a, anio_b, n_comunes, fila_a, fila_b, delta_bis))
            print(f"   {anio_a}→{anio_b}  ·  organismos comunes: {n_comunes:,}  ·  "
                  f"{fila_a:.2f}% → {fila_b:.2f}%  ·  delta = {delta_bis:+.2f} pp")

        print("\nVEREDICTO — placebo vs. Mejora nº1")
        print("   Mejora nº1 (2024→2025, CON intervención el 12-dic-2024): delta restringido = +0.30 pp")
        for anio_a, anio_b, n_comunes, fa, fb, d in resumen_placebo:
            print(f"   Placebo    ({anio_a}→{anio_b}, SIN intervención conocida)   : delta restringido = {d:+.2f} pp")
        print("   Si los deltas placebo son cercanos a 0 (o de signo variable), refuerza que el")
        print("   +0,30 pp de 2024-2025 no es ruido normal de año a año — declarar así en el informe.")
        print("   Si algún placebo también da ~+0,3 pp sin razón normativa, es una deriva de fondo")
        print("   del indicador que hay que declarar junto al resultado de la Mejora nº1.")

    print(f"\nCAP-13-BIS termina. RUN_ID_CAP={RUN_ID_CAP}")  # noqa: F821


# COMMAND ----------

# MAGIC %md
# MAGIC # CAP-13-TER · Semi-join a nivel de unidad de compra (sensibilidad de
# MAGIC # granularidad)
# MAGIC
# MAGIC **Por qué existe esta celda.** La Mejora nº1 usó `CodigoOrganismoPublico`
# MAGIC porque fue la primera columna que el resolvedor encontró. Un organismo
# MAGIC puede tener varias unidades de compra que entran o salen del sistema por
# MAGIC separado — repetir el mismo semi-join a nivel de `CodigoUnidadCompra` (más
# MAGIC fino) es una segunda vuelta de tuerca barata para blindar la conclusión.

# COMMAND ----------

print("\n" + "=" * 78)
print(f"CAP-13-TER · Semi-join a nivel de unidad de compra · RUN_ID_CAP={RUN_ID_CAP}")  # noqa: F821
print("=" * 78)

_oc_ter = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")  # noqa: F821

if _oc_ter is None:
    print("ABORTA: falta df_oc_all/df_oc_linked en el kernel.")
else:
    c_onu_ter = cap_resolver(_oc_ter, ["onu_oc"], "ONU (oc)")                                     # noqa: F821
    c_anio_ter = cap_resolver(_oc_ter, ["_year", "anio", "año"], "año (oc)")                       # noqa: F821
    c_uni_ter = cap_resolver(_oc_ter, ["_sinclasificar_CodigoUnidadCompra"], "unidad de compra")   # noqa: F821

    if not c_onu_ter or not c_anio_ter or not c_uni_ter:
        print("ABORTA: faltan columnas mínimas (ONU, año, unidad de compra).")
    else:
        base_ter = _oc_ter.select(
            F.col(c_anio_ter).cast("int").alias("anio"),
            F.trim(F.col(c_uni_ter).cast("string")).alias("entidad"),
            cap_onu_clase(F.col(c_onu_ter)).alias("clase_onu"),                                    # noqa: F821
        ).where(F.col("anio").isin(2024, 2025))

        unid_2024 = (base_ter.where(F.col("anio") == 2024).select("entidad")
                             .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                             .distinct())
        unid_2025 = (base_ter.where(F.col("anio") == 2025).select("entidad")
                             .where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                             .distinct())
        unid_ambos = unid_2024.join(unid_2025, "entidad", "inner")

        print(f"   Unidades de compra en 2024 .................. {unid_2024.count():,}")
        print(f"   Unidades de compra en 2025 .................. {unid_2025.count():,}")
        print(f"   Presentes en AMBOS años (semi-join, fino) ... {unid_ambos.count():,}")

        restringido_ter = base_ter.join(unid_ambos, "entidad", "inner")
        r24_ter = (restringido_ter.where(F.col("anio") == 2024)
                   .agg((100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct"))
                   .collect()[0]["pct"])
        r25_ter = (restringido_ter.where(F.col("anio") == 2025)
                   .agg((100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct"))
                   .collect()[0]["pct"])
        delta_ter = (r25_ter or 0) - (r24_ter or 0)

        print("\nVEREDICTO — semi-join a nivel de unidad de compra (más fino que organismo)")
        print(f"   2024 (restringido, unidad de compra) : {r24_ter:.2f}%")
        print(f"   2025 (restringido, unidad de compra) : {r25_ter:.2f}%")
        print(f"   delta restringido (unidad de compra)  = {delta_ter:+.2f} pp")
        print("   (para comparar: +0,30 pp a nivel de organismo, +0,28 pp sin restringir)")
        print("   Si el delta a nivel de unidad de compra sigue cerca de +0,3 pp, la Mejora nº1")
        print("   queda blindada también a la granularidad más fina posible con este esquema.")

    print(f"\nCAP-13-TER termina. RUN_ID_CAP={RUN_ID_CAP}")  # noqa: F821
