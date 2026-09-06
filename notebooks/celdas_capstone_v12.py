# =============================================================================
#  celdas_capstone_v12.py — PARCHE DE LA CORRIDA DEL 21-ago-2026
#  Capstone Big Data · Mercado Público nacional 2017–2026
#  Corrige v11 contra el ESQUEMA REAL de los Parquet y contra los dos fallos
#  observados en la corrida RUN_ID_CAP=1314c4f6d481.
#
#  ORDEN DE PEGADO (importante):
#    1. CAP-0      (el de v11, sin cambios — define RUN_ID_CAP y las utilidades)
#    2. CAP-0-BIS  (NUEVO — parseo de números en formato chileno)
#    3. CAP-1 v12  ·  4. CAP-2 v12  ·  5. CAP-3 v12  ·  6. CAP-4 v12
#    7. CAP-5      (el de v11, sin cambios — es seguro, solo castea a string)
#    8. CAP-6      (NUEVO — exporta a CSV lo ya calculado, sin cómputo extra)
#
#  LOS DOS FALLOS DE v11 Y SU CAUSA
#  · CAP-1 abortó: la clave de licitación en df_lic_all se llama `codigo_externo`,
#    y no estaba entre los candidatos del resolvedor.
#  · CAP-3 abortó con CAST_INVALID_INPUT sobre '1112,72975': TODAS las columnas
#    numéricas del Parquet son `string` y traen coma decimal chilena. Apagar ANSI
#    NO es la solución: convertiría esas filas en NULL y desinflaría toda cifra en
#    pesos sin que nada lo avise. Hay que parsear, y hay que CONTAR lo no parseado.
#
#  LO QUE HAY QUE VOLVER A CORRER AUNQUE NO HAYA ABORTADO
#  · CAP-2: los placebos dieron 100,00% en los diez años porque bajo ANSI Spark
#    reescribe `cast(x AS DOUBLE) IS NOT NULL` como `x IS NOT NULL`. Midieron
#    "el string no está vacío". No son un resultado.
#
#  TRES HALLAZGOS DE ESQUEMA QUE CAMBIAN EL ALCANCE DEL PROYECTO
#  · `fecha_creacion` existe en df_oc_all → hay granularidad temporal fina; el
#    diseño de la Pregunta 2 no está condenado al pre-post anual.
#  · `_sinclasificar_UnidadMedida` existe → la Pregunta 3 puede estratificar por
#    unidad en vez de amputar el universo con `cantidad_oc = 1`.
#  · Hay RUT de unidad y de proveedor, y `numero_oferentes` en licitaciones.
#
#  ADVERTENCIA DE LECTURA (§1.16)
#  Ningún veredicto impreso aquí es una conclusión. Cada uno viene con su tabla de
#  magnitudes al lado y hay que releerlo contra ella antes de copiarlo a ninguna parte.
# =============================================================================


# %% ==========================================================================
# CAP-0-BIS · PARCHE DE NUMÉRICOS Y DE VISIBILIDAD
# Correr DESPUÉS de CAP-0 y ANTES de volver a correr CAP-1, CAP-3 o CAP-4.
#
# QUÉ HACE: (1) define cap_num(), que parsea números escritos en formato chileno
# ('1.112,72975' o '1112,72975') sin abortar y sin convertirlos en NULL en
# silencio; (2) define cap_num_falla(), que MARCA lo que no se pudo parsear para
# poder contarlo en la misma agregación; (3) apaga ANSI como red de seguridad,
# NO como solución; (4) hace parlante al resolvedor de columnas.
#
# POR QUÉ IMPORTA: con ANSI apagado y sin cap_num(), '1112,72975' se convierte en
# NULL y desaparece de toda suma de pesos SIN QUE NADA LO AVISE. Eso no es un
# error de ejecución: es una cifra falsa en el informe. La regla §1.15.2 del
# proyecto (el bug de la rama vacía) es exactamente esto.
# =============================================================================
print("=" * 78)
print(f"CAP-0-BIS · parche de numéricos · RUN_ID_CAP = {RUN_ID_CAP}")
print("=" * 78)

try:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    print("   ANSI SQL desactivado (red de seguridad; el parseo real lo hace cap_num).")
except Exception as _e:
    print(f"   [aviso] no se pudo desactivar ANSI ({type(_e).__name__}). cap_num() igual sirve.")

_HAS_TRY_CAST = hasattr(F.lit(1), "try_cast")
print(f"   Column.try_cast disponible : {_HAS_TRY_CAST}")


def _cap_limpia_num(col):
    """Normaliza el TEXTO de un número antes de castear. No castea todavía."""
    s = F.trim(col.cast("string"))
    s = F.regexp_replace(s, r"[\s $]", "")          # espacios, NBSP, signo $
    # Si hay coma, la coma es el separador DECIMAL (formato chileno):
    # entonces los puntos que haya son separadores de miles y se eliminan.
    s = F.when(s.contains(","),
               F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", ".")).otherwise(s)
    return F.when((s == "") | (s.rlike(r"(?i)^(null|na|n/a|s/i|-)$")), F.lit(None)).otherwise(s)


def cap_num(col):
    """Texto o número → double. Formato chileno soportado. Nunca aborta."""
    s = _cap_limpia_num(col)
    return s.try_cast("double") if _HAS_TRY_CAST else s.cast("double")


def cap_num_falla(col):
    """1 si el valor venía con contenido pero NO se pudo parsear a número.

    Es la contrapartida obligatoria de cap_num(): permite contar, dentro de la
    MISMA agregación y sin acciones extra, cuánto universo se está perdiendo.
    """
    s = _cap_limpia_num(col)
    d = s.try_cast("double") if _HAS_TRY_CAST else s.cast("double")
    return F.when(s.isNotNull() & d.isNull(), 1).otherwise(0)


def cap_resolver(df, candidatos, etiqueta=""):
    """Igual que en CAP-0, pero AVISA cuando resuelve por coincidencia parcial.

    La versión original resolvía en silencio: 'ano' puede resolver a
    'OrganoComprador', 'tipo' a 'TipoMoneda'. Un resolvedor mudo convierte un
    error de esquema en una cifra plausible y falsa.
    """
    if df is None:
        return None
    reales = {c.lower(): c for c in df.columns}
    for c in candidatos:
        if c.lower() in reales:
            return reales[c.lower()]
    for c in candidatos:
        for low, real in reales.items():
            if c.lower() in low:
                print(f"   [resolver] '{etiqueta or candidatos[0]}' → '{real}' por coincidencia "
                      f"PARCIAL con '{c}'. VERIFICAR que sea la columna correcta.")
                return real
    if etiqueta:
        print(f"   [aviso] no se resolvió la columna '{etiqueta}'. "
              f"Candidatos probados: {candidatos}")
        print(f"          columnas disponibles: {', '.join(df.columns)}")
    return None


print("   cap_num / cap_num_falla / cap_resolver(parlante) definidos.")
print("   Ahora sí: volver a correr CAP-3 (versión v12) y CAP-4 (versión v12).")
# %% ==========================================================================
# CAP-1 v12 · PREGUNTA 1 — consistencia ONU lic↔OC CON BASELINE NULO
# Requiere CAP-0 y CAP-0-BIS. Reemplaza por completo al CAP-1 de v11.
#
# QUÉ CAMBIÓ RESPECTO DE v11 (todo verificado contra el esquema real):
#  1. La clave de licitación en df_lic_all es `codigo_externo`. v11 no la
#     resolvía y abortaba.
#  2. df_lic_all trae OFERTAS: hay varias filas por línea de adquisición. Contar
#     filas sobreestima el número de líneas y rompe la definición de
#     "testimonial = >=2 líneas y <=1 código". Ahora se cuenta
#     `_sinclasificar_Codigoitem` DISTINTOS.
#  3. La permutación del baseline nulo se ordena por xxhash64(clave), no por
#     rand(): rand() dentro de una ventana NO es reproducible si cambia el
#     particionamiento, y en serverless el particionamiento no lo controlas.
#  4. Las filas con año NULL ya no se pierden en el join de permutación
#     (coalesce a -1): antes real y nulo comparaban universos distintos.
#  5. Los pares donde NINGÚN lado tiene código ONU válido ya no puntúan
#     "consistencia perfecta": se aíslan en el universo D.
#  6. Los montos se parsean con cap_num() y la cifra en pesos se restringe a
#     moneda CLP nativa (v11 sumaba todas las monedas a valor facial).
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-1 v12 · Pregunta 1 — consistencia ONU con baseline nulo · {RUN_ID_CAP}")
print("=" * 78)

_lic = cap_global("df_lic_all")
_ocl = cap_global("df_oc_linked")

if _lic is None or _ocl is None:
    print("ABORTA: falta df_lic_all y/o df_oc_linked en el kernel.")
else:
    c_lic_key_l = cap_resolver(_lic, ["codigo_externo", "_lic_key", "lic_key",
                                      "codigo_licitacion"], "clave licitación (lic)")
    c_onu_l  = cap_resolver(_lic, ["onu_lic"], "ONU (lic)")
    c_anio_l = cap_resolver(_lic, ["_year", "anio", "año"], "año (lic)")
    c_item_l = cap_resolver(_lic, ["_sinclasificar_Codigoitem", "codigoitem"], "id de línea (lic)")

    c_lic_key_o = cap_resolver(_ocl, ["_lic_key", "codigo_licitacion"], "clave licitación (oc)")
    c_onu_o = cap_resolver(_ocl, ["onu_oc"], "ONU (oc)")
    c_flag  = cap_resolver(_ocl, ["tiene_licitacion_origen"], "flag de enlace")
    c_cant  = cap_resolver(_ocl, ["cantidad_oc"], "cantidad")
    c_prec  = cap_resolver(_ocl, ["precio_neto_oc"], "precio")
    c_mon   = cap_resolver(_ocl, ["moneda_item", "moneda"], "moneda")

    if not all([c_lic_key_l, c_onu_l, c_lic_key_o, c_onu_o]):
        print("ABORTA: no se resolvieron las columnas mínimas.")
        print(f"        lic: clave={c_lic_key_l} onu={c_onu_l}")
        print(f"        oc : clave={c_lic_key_o} onu={c_onu_o}")
    else:
        if not c_item_l:
            print("   [ADVERTENCIA] no hay id de línea en df_lic_all: n_lineas_lic contará FILAS,")
            print("   que incluyen una por oferta. La definición de 'testimonial' NO es citable así.")

        # ---- Lado licitación --------------------------------------------------
        lic = _lic.select(
            F.trim(F.col(c_lic_key_l).cast("string")).alias("k"),
            cap_onu8(F.col(c_onu_l)).alias("onu8"),
            (F.col(c_anio_l).cast("int") if c_anio_l else F.lit(None).cast("int")).alias("anio"),
            (F.col(c_item_l).cast("string") if c_item_l else F.lit(None).cast("string")).alias("item"),
        ).where(F.col("k").isNotNull() & (F.col("k") != ""))

        agg_lic = lic.groupBy("k").agg(
            F.max("anio").alias("anio"),
            F.count(F.lit(1)).alias("n_filas_lic"),
            (F.countDistinct("item") if c_item_l else F.count(F.lit(1))).alias("n_lineas_lic"),
            F.collect_set("onu8").alias("set8_lic"),
        )

        # ---- Lado OC ----------------------------------------------------------
        oc_src = _ocl
        if c_flag:
            oc_src = oc_src.where(F.col(c_flag) == True)  # noqa: E712
        es_clp = (F.upper(F.trim(F.col(c_mon).cast("string"))) == "CLP") if c_mon else F.lit(True)
        monto = ((cap_num(F.col(c_prec)) * cap_num(F.col(c_cant)))
                 if (c_prec and c_cant) else F.lit(None).cast("double"))
        oc = oc_src.select(
            F.trim(F.col(c_lic_key_o).cast("string")).alias("k"),
            cap_onu8(F.col(c_onu_o)).alias("onu8"),
            F.when(es_clp, monto).alias("clp_linea"),          # solo CLP nativo
        ).where(F.col("k").isNotNull() & (F.col("k") != ""))

        agg_oc = oc.groupBy("k").agg(
            F.count(F.lit(1)).alias("n_lineas_oc"),
            F.collect_set("onu8").alias("set8_oc"),
            F.sum("clp_linea").alias("clp_oc"),
        )

        par = agg_lic.join(agg_oc, "k", "inner")

        def _trunc(col, n):
            return F.array_distinct(F.transform(col, lambda x: F.substring(x, 1, n)))

        par = (par
               .withColumn("anio", F.coalesce(F.col("anio"), F.lit(-1)))   # no perder NULL en el join
               .withColumn("set8_lic", F.array_sort(F.array_compact(F.col("set8_lic"))))
               .withColumn("set8_oc",  F.array_sort(F.array_compact(F.col("set8_oc"))))
               .withColumn("card_lic8", F.size("set8_lic"))
               .withColumn("card_oc8",  F.size("set8_oc"))
               .withColumn("bucket", cap_bucket_card(F.col("card_lic8")))
               .withColumn("universo",
                           F.when((F.col("card_lic8") == 0) & (F.col("card_oc8") == 0),
                                  F.lit("D_ninguno_tiene_onu"))
                            .when(F.col("card_lic8") == 0, F.lit("E_lic_sin_onu"))
                            .when(F.col("card_lic8") <= 1,
                                  F.when(F.col("n_lineas_lic") >= 2,
                                         F.lit("B_testimonial_multilinea"))
                                   .otherwise(F.lit("A_monolinea_legitima")))
                            .otherwise(F.lit("C_informativo"))))

        def _metricas(df, sufijo):
            out = df
            for n in (2, 4, 8):
                sl = _trunc(F.col("set8_lic"), n) if n != 8 else F.col("set8_lic")
                so = (_trunc(F.col(f"set8_oc{sufijo}"), n) if n != 8
                      else F.col(f"set8_oc{sufijo}"))
                inter = F.size(F.array_intersect(sl, so))
                union = F.size(F.array_union(sl, so))
                fuera = F.size(F.array_except(so, sl))
                falta = F.size(F.array_except(sl, so))
                out = (out
                       .withColumn(f"overlap_{n}", (inter > 0).cast("int"))
                       .withColumn(f"subset_{n}", (fuera == 0).cast("int"))
                       .withColumn(f"iguales_{n}", ((fuera == 0) & (falta == 0)).cast("int"))
                       .withColumn(f"jacc_{n}",
                                   F.when(union > 0, inter / union).otherwise(F.lit(None))))
            return out

        # ---- Baseline nulo: permutación DETERMINISTA dentro de (año × bucket) --
        w_a = W.partitionBy("anio", "bucket").orderBy(F.xxhash64("k", F.lit(SEED_NULO)))
        w_b = W.partitionBy("anio", "bucket").orderBy(F.xxhash64("k", F.lit(SEED_NULO + 7)))
        izq = par.withColumn("_i", F.row_number().over(w_a))
        der = (par.select("anio", "bucket", "k", F.col("set8_oc").alias("set8_oc_x"))
                  .withColumn("_i", F.row_number().over(w_b)).drop("k"))
        par_perm = izq.drop("set8_oc").join(der, ["anio", "bucket", "_i"], "inner")

        real = _metricas(par, "").withColumn("serie", F.lit("1_real"))
        nulo = _metricas(par_perm, "_x").withColumn("serie", F.lit("2_nulo_permutado"))

        cols_m = [f"{m}_{n}" for n in (2, 4, 8)
                  for m in ("overlap", "subset", "iguales", "jacc")]
        sel = ["serie", "universo", "bucket"] + cols_m
        juntos = real.select(*sel).unionByName(nulo.select(*sel))

        aggs = [F.count(F.lit(1)).alias("n")]
        for c in cols_m:
            aggs.append((100.0 * F.avg(F.col(c))).alias(c) if not c.startswith("jacc")
                        else F.avg(F.col(c)).alias(c))

        # ---- ACCIÓN 1 de 2 ----------------------------------------------------
        res = (juntos.groupBy("serie", "universo", "bucket").agg(*aggs)
                     .orderBy("universo", "bucket", "serie").collect())

        cap_print_tabla(
            res,
            "CAP-1.A · MÉTRICA PRIMARIA (truncamiento 4 dígitos) — REAL vs. NULO PERMUTADO\n"
            "          universos: A_monolinea_legitima | B_testimonial_multilinea |\n"
            "          C_informativo | D_ninguno_tiene_onu | E_lic_sin_onu\n"
            "          CONTROL DE INTEGRIDAD: la columna n debe ser IGUAL entre 1_real y\n"
            "          2_nulo_permutado en cada fila. Si no lo es, la permutación perdió filas\n"
            "          y la brecha NO es comparable.",
            cols=["serie", "universo", "bucket", "n",
                  "overlap_4", "subset_4", "iguales_4", "jacc_4"])
        cap_print_tabla(
            res,
            "CAP-1.A-bis · RELECTURA CONTRA OTRA VARA (§1.16): 2 y 8 dígitos.\n"
            "          Si el resultado cambia de signo al cambiar el truncamiento, la cifra\n"
            "          de 4 dígitos NO es citable sola.",
            cols=["serie", "universo", "bucket", "n",
                  "subset_2", "iguales_2", "subset_8", "iguales_8"])

        d = {}
        for r in res:
            d.setdefault((r["serie"], r["universo"]), []).append(r)
        print("\nCAP-1.B · VEREDICTO — brecha real − nulo en la métrica PRIMARIA "
              f"(subset_4), universo informativo. Umbral declarado: {UMBRAL_BRECHA_PP} pp")
        rr = d.get(("1_real", "C_informativo"), [])
        nn = d.get(("2_nulo_permutado", "C_informativo"), [])
        if not rr or not nn:
            print("   NO SE PUEDE COMPARAR: falta uno de los dos lados.")
            print("   Esto NO significa 'no hay diferencia'. No citar nada de aquí.")
        else:
            n_r = sum(x["n"] for x in rr)
            n_n = sum(x["n"] for x in nn)
            s_r = sum(x["subset_4"] * x["n"] for x in rr) / n_r
            s_n = sum(x["subset_4"] * x["n"] for x in nn) / n_n
            brecha = s_r - s_n
            print(f"   real = {s_r:6.2f}%  (n={n_r:,})")
            print(f"   nulo = {s_n:6.2f}%  (n={n_n:,})")
            if n_r != n_n:
                print(f"   [ALERTA] n real ({n_r:,}) != n nulo ({n_n:,}): universos distintos.")
            print(f"   brecha = {brecha:+6.2f} pp")
            if brecha >= UMBRAL_BRECHA_PP:
                print("   → HAY SEÑAL sobre el azar. Releer contra la tabla por bucket: si la")
                print("     brecha vive en un solo tramo, la cifra agregada no es citable sola.")
            else:
                print("   → NO HAY SEÑAL sobre el azar con este umbral. Si esto pasa,")
                print("     `oc_subconjunto_de_lic` NO sirve como cifra citable y hay que")
                print("     liderar con la cobertura ponderada de CAP-1.C. Decirlo, no esconderlo.")

        # ---- ACCIÓN 2 de 2: cobertura ponderada -------------------------------
        set_lic4 = agg_lic.select(
            "k", _trunc(F.array_compact(F.col("set8_lic")), 4).alias("set4_lic"))
        cob = (oc.join(set_lic4, "k", "inner")
                 .withColumn("onu4", F.substring(F.col("onu8"), 1, 4))
                 .withColumn("declarada",
                             F.when(F.col("onu4").isNull(), F.lit(None))
                              .otherwise(F.array_contains(F.col("set4_lic"),
                                                          F.col("onu4")).cast("int"))))
        cr = cob.agg(
            F.count(F.lit(1)).alias("lineas_oc_enlazadas"),
            F.sum(F.when(F.col("onu4").isNotNull(), 1).otherwise(0)).alias("lineas_con_onu_valido"),
            F.sum(F.col("declarada")).alias("lineas_declaradas"),
            F.sum(F.when(F.col("onu4").isNotNull(), F.col("clp_linea"))).alias("clp_con_onu"),
            F.sum(F.when(F.col("declarada") == 1, F.col("clp_linea"))).alias("clp_declarado"),
        ).collect()

        if cr:
            r = cr[0]
            n_enl = r["lineas_oc_enlazadas"] or 0
            n_val = r["lineas_con_onu_valido"] or 0
            n_dec = r["lineas_declaradas"] or 0
            c_val = r["clp_con_onu"] or 0
            c_dec = r["clp_declarado"] or 0
            print("\n" + "-" * 78)
            print("CAP-1.C · COBERTURA PONDERADA (sin sesgo de cardinalidad)")
            print("-" * 78)
            print("   Denominador declarado: líneas de OC enlazadas CUYA LICITACIÓN ESTÁ")
            print("   PRESENTE en df_lic_all (el borde del universo ya está descontado aquí).")
            print(f"   Líneas de OC enlazadas y con licitación presente  {n_enl:,}")
            print(f"   ... con código ONU válido de 8 dígitos ........... {n_val:,}")
            p_l = cap_pct(n_dec, n_val)
            p_c = cap_pct(c_dec, c_val)
            print("   ... cuya familia (4 díg.) SÍ estaba declarada en la licitación:")
            print(f"       por LÍNEA ..................... "
                  f"{f'{p_l:.2f}%' if p_l is not None else 'NO CALCULABLE (denominador 0)'}")
            print(f"       por PESOS (solo CLP nativo) ... "
                  f"{f'{p_c:.2f}%' if p_c is not None else 'NO CALCULABLE (denominador 0)'}")
            print(f"       CLP con ONU válido: {int(c_val):,}")
            print(f"       CLP declarado     : {int(c_dec):,}")
            print("\n   NOTA: cifra recomendada como TITULAR de la Pregunta 1.")
        print(f"\nCAP-1 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")
# %% ==========================================================================
# CAP-2 v12 · PREGUNTA 2 — Cobertura y validez del código ONU por año
# Requiere CAP-0 y CAP-0-BIS. Reemplaza al CAP-2 de v11.
#
# POR QUÉ HAY QUE VOLVER A CORRERLO aunque v11 no abortó:
# En v11 los placebos dieron 100,00% en los diez años, y eso NO era un resultado.
# Todas las columnas numéricas del Parquet son `string`; bajo ANSI, Spark
# reescribe `cast(x AS DOUBLE) IS NOT NULL` como `x IS NOT NULL`, así que
# placebo_pct_cant y placebo_pct_prec midieron "el string no está vacío", no
# "es un número válido". Un placebo pegado al techo no puede refutar nada: si no
# se mueve nunca, no informa sobre si el salto de 2025 es de norma o de plataforma.
#
# QUÉ CAMBIÓ:
#  1. Los placebos usan cap_num(): miden validez NUMÉRICA real.
#  2. Se agrega un tercer placebo: cobertura de UNIDAD DE MEDIDA, campo que el
#     art. 20 bis no manda y que existe en los datos.
#  3. El panel se puede balancear por ORGANISMO (`CodigoOrganismoPublico`), no
#     por el prefijo del código de proceso. Elimina el riesgo 5 del documento de
#     segunda opinión (unidades que se fusionan y renombran).
#  4. Imprime qué años están realmente presentes antes de exigir presencia en todos.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-2 v12 · Pregunta 2 — cobertura del código ONU por año · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")

if _oc is None:
    print("ABORTA: falta df_oc_all / df_oc_linked en el kernel.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year", "anio", "año"], "año (oc)")
    c_tipo = cap_resolver(_oc, ["tipo_oc"], "tipo_oc")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_um   = cap_resolver(_oc, ["_sinclasificar_UnidadMedida"], "unidad de medida")
    c_org  = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")
    c_uni  = cap_resolver(_oc, ["_sinclasificar_CodigoUnidadCompra"], "unidad de compra")

    if not c_onu or not c_anio:
        print(f"ABORTA: no se resolvieron ONU ({c_onu}) o año ({c_anio}).")
    else:
        # ENTIDAD del panel: organismo si existe; si no, unidad de compra.
        if c_org:
            ent, ent_nombre = F.col(c_org), f"organismo ({c_org})"
        elif c_uni:
            ent, ent_nombre = F.col(c_uni), f"unidad de compra ({c_uni})"
        else:
            ent, ent_nombre = F.lit(None).cast("string"), "NINGUNA (panel no calculable)"
        print(f"   Entidad del panel balanceado: {ent_nombre}")

        base = _oc.select(
            F.col(c_anio).cast("int").alias("anio"),
            cap_onu_clase(F.col(c_onu)).alias("clase_onu"),
            (F.col(c_tipo) if c_tipo else F.lit("NA")).alias("tipo_oc"),
            F.trim(ent.cast("string")).alias("entidad"),
            (cap_num(F.col(c_cant)).isNotNull().cast("int")
             if c_cant else F.lit(None).cast("int")).alias("placebo_cant_num"),
            (cap_num(F.col(c_prec)).isNotNull().cast("int")
             if c_prec else F.lit(None).cast("int")).alias("placebo_prec_num"),
            ((F.trim(F.col(c_um).cast("string")).isNotNull() &
              (F.trim(F.col(c_um).cast("string")) != "")).cast("int")
             if c_um else F.lit(None).cast("int")).alias("placebo_um"),
        ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

        _anios = sorted(r["anio"] for r in base.select("anio").distinct().collect()
                        if r["anio"] is not None)
        print(f"   Años presentes en los datos: {_anios}")
        print(f"   El panel exigirá presencia en los {len(_anios)} años listados.")
        print("   OJO: 2026 es un año PARCIAL. Exigir presencia en 2026 sesga el panel hacia")
        print("   entidades grandes y activas. Considerar además un panel 2017-2025.")

        panel = (base.where(F.col("entidad").isNotNull() & (F.col("entidad") != ""))
                     .groupBy("entidad").agg(F.countDistinct("anio").alias("na"))
                     .where(F.col("na") == len(_anios)).select("entidad"))

        def _serie(df, etiqueta):
            return (df.groupBy("anio")
                      .agg(F.lit(etiqueta).alias("serie"),
                           F.count(F.lit(1)).alias("lineas"),
                           (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct_onu_valido"),
                           (100.0 * F.avg((F.col("clase_onu") == "CERO").cast("int"))).alias("pct_onu_cero"),
                           (100.0 * F.avg((F.col("clase_onu") == "AUSENTE").cast("int"))).alias("pct_ausente"),
                           (100.0 * F.avg((F.col("clase_onu") == "NO_NUMERICO").cast("int"))).alias("pct_no_num"),
                           (100.0 * F.avg(F.col("placebo_cant_num"))).alias("plac_cant"),
                           (100.0 * F.avg(F.col("placebo_prec_num"))).alias("plac_prec"),
                           (100.0 * F.avg(F.col("placebo_um"))).alias("plac_unidad_medida"),
                           F.countDistinct("entidad").alias("entidades")))

        filas = (_serie(base, "1_universo_completo")
                 .unionByName(_serie(base.join(panel, "entidad", "inner"), "2_panel_balanceado"))
                 .orderBy("serie", "anio").collect())
        cap_print_tabla(
            filas,
            "CAP-2.A v12 · Cobertura y validez del código ONU por año\n"
            "          plac_cant / plac_prec / plac_unidad_medida: campos que el art. 20 bis\n"
            "          NO manda. Ahora miden validez NUMÉRICA real, no 'string no vacío'.\n"
            "          Si estos placebos se mueven igual que pct_onu_valido, el cambio es de\n"
            "          plataforma o de composición, NO de la norma.\n"
            "          Un placebo pegado a 100,00 en los diez años NO refuta nada: dilo así.")

        print("\nCAP-2.B · LECTURA OBLIGADA (§1.16)")
        print("   El corte legal es el 12-dic-2024. Con granularidad ANUAL 2024 tiene 19 días")
        print("   tratados y 347 no tratados, por eso 2024 se lee como PRE y 2025 es el primer")
        print("   año post. 2026 es PARCIAL. Con solo el año NO se puede afirmar 'efecto'.")
        print("   PERO: `fecha_creacion` EXISTE en estos datos. Si parsea, este bloque puede")
        print("   rehacerse con granularidad mensual y el diseño deja de ser un pre-post ciego.")

        comp = (base.groupBy("anio", "tipo_oc")
                    .agg(F.count(F.lit(1)).alias("lineas"),
                         (100.0 * F.avg((F.col("clase_onu") == "VALIDO_8").cast("int"))).alias("pct_onu_valido"))
                    .orderBy("anio", F.col("lineas").desc()).limit(200).collect())
        cap_print_tabla(comp,
                        "CAP-2.C · Cobertura ONU por año y mecanismo de compra\n"
                        "          Si la cobertura global se mueve porque cambió la MEZCLA de\n"
                        "          mecanismos, se ve aquí y no en la serie agregada.")
        print(f"\nCAP-2 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")
# %% ==========================================================================
# CAP-3 v12 · CASCADA EN PESOS + G6 RESTRINGIDO-Y-AJUSTADO + RECONCILIACIÓN
# Requiere CAP-0 y CAP-0-BIS. Reemplaza por completo al CAP-3 de v11.
#
# QUÉ CAMBIÓ RESPECTO DE v11:
#  1. Los montos se parsean con cap_num(): TODAS las columnas numéricas del
#     Parquet son `string` y al menos una trae coma decimal chilena
#     ('1112,72975'). v11 abortó con CAST_INVALID_INPUT. Apagar ANSI sin parsear
#     habría convertido esas filas en NULL y desinflado toda cifra en pesos SIN
#     AVISO — que es peor que abortar.
#  2. Se cuenta e imprime cuánto NO se pudo parsear, ANTES de cualquier cifra.
#  3. Usa df_oc_linked como universo si existe, porque el flag de enlace
#     (`tiene_licitacion_origen`) SOLO vive ahí. Imprime los conteos de ambos
#     DataFrames para que quede probado que es el mismo universo y no un
#     subconjunto: si no coinciden, la cascada NO es citable.
#  4. La cascada es ANIDADA de verdad y el total facial multi-moneda se declara
#     como tal (v11 lo llamaba "CLP" sumando USD, EUR y CLF a valor facial).
#  5. tipo_oc NULL ya no cae mudo a "no licitatorio": se cuenta y se imprime.
#  6. Imprime la tabla de mecanismos con su tasa de clave de licitación, para
#     que la lista de exclusión del denominador sea auditable.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-3 v12 · Cascada, G6 restringido-ajustado y reconciliación · {RUN_ID_CAP}")
print("=" * 78)

_all = cap_global("df_oc_all")
_lnk = cap_global("df_oc_linked")
_oc = _lnk if _lnk is not None else _all
_fuente = "df_oc_linked" if _lnk is not None else "df_oc_all (SIN flag de enlace: G6 no calculable)"
print(f"   Universo usado: {_fuente}")

if _oc is None:
    print("ABORTA: no hay df_oc_all ni df_oc_linked en el kernel.")
else:
    if _all is not None and _lnk is not None:
        n_all, n_lnk = _all.count(), _lnk.count()
        print(f"   Filas df_oc_all = {n_all:,} · df_oc_linked = {n_lnk:,}")
        if n_all != n_lnk:
            print("   [ALERTA] NO son el mismo universo. df_oc_linked es un SUBCONJUNTO:")
            print("   la cascada calculada sobre él NO representa el gasto total y NO es citable")
            print("   como tal. Usar df_oc_all para la cascada y df_oc_linked solo para el G6.")
        else:
            print("   Mismo número de filas: df_oc_linked es df_oc_all + flag. Cascada válida.")

    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year", "anio", "año"], "año (oc)")
    c_tipo = cap_resolver(_oc, ["tipo_oc"], "tipo_oc")
    c_klic = cap_resolver(_oc, ["codigo_licitacion"], "código licitación")
    c_flag = cap_resolver(_oc, ["tiene_licitacion_origen"], "flag de enlace")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_doc  = cap_resolver(_oc, ["codigo_oc"], "código OC")

    # Mecanismos que por diseño normativo NO llevan código de licitación.
    # SE MANTIENE la lista original para que la cifra sea comparable con el 66,97%
    # ya medido. ADVERTENCIA [FUENTE, 21-ago-2026, https://www.chilecompra.cl/api/]:
    # en la tabla de dominio oficial figuran CM, AG, MC y CA, pero NO figuran TD
    # ni CB; y sí figura R1 ("Orden de compra menor a 3UTM"), que hoy no se excluye.
    # La tabla CAP-3.B-bis existe para decidir con datos si se recalcula.
    SIN_LICITACION = ["CM", "AG", "MC", "TD", "CA", "CB"]

    if not c_anio or not (c_prec and c_cant):
        print(f"ABORTA: faltan año ({c_anio}) o precio/cantidad ({c_prec}/{c_cant}).")
    else:
        b = _oc.select(
            F.col(c_anio).cast("int").alias("anio"),
            (F.col(c_tipo) if c_tipo else F.lit(None)).alias("tipo_oc"),
            (F.col(c_klic) if c_klic else F.lit(None)).alias("k_lic"),
            (F.col(c_flag).cast("boolean") if c_flag else F.lit(None).cast("boolean")).alias("enlazada"),
            (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
            (F.col(c_doc) if c_doc else F.lit(None)).alias("doc"),
            (cap_onu_clase(F.col(c_onu)) if c_onu else F.lit("NA")).alias("clase_onu"),
            cap_num(F.col(c_prec)).alias("precio"),
            cap_num(F.col(c_cant)).alias("cant"),
            cap_num_falla(F.col(c_prec)).alias("falla_prec"),
            cap_num_falla(F.col(c_cant)).alias("falla_cant"),
        ).where(F.col("anio").between(ANIO_MIN, ANIO_MAX))

        b = (b.withColumn("clp_linea", F.col("precio") * F.col("cant"))
              .withColumn("es_clp", F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP")
              .withColumn("tipo_norm", F.upper(F.trim(F.col("tipo_oc").cast("string"))))
              .withColumn("tipo_nulo", F.col("tipo_norm").isNull() | (F.col("tipo_norm") == ""))
              .withColumn("mecanismo_licitatorio",
                          F.when(F.col("tipo_nulo"), F.lit(False))
                           .otherwise(~F.col("tipo_norm").isin(SIN_LICITACION)))
              .withColumn("tiene_clave", F.col("k_lic").isNotNull() &
                          (F.trim(F.col("k_lic").cast("string")) != ""))
              .withColumn("anio_ref", cap_anio_sufijo(F.col("k_lic")))
              .withColumn("dentro_universo",
                          F.coalesce(F.col("anio_ref") >= ANIO_MIN, F.lit(False))))

        # Peldaños ANIDADOS: cada uno incluye a todos los anteriores.
        p1 = F.col("es_clp")
        p2 = p1 & F.col("mecanismo_licitatorio")
        p3 = p2 & F.col("tiene_clave")
        p4 = p3 & F.col("dentro_universo")
        p5 = p4 & F.coalesce(F.col("enlazada"), F.lit(False))
        p6 = p5 & (F.col("clase_onu") == "VALIDO_8")
        p7 = p6 & (F.col("cant") == 1.0)
        # G6-RA en su definición original, SIN filtro de moneda, para ser
        # comparable con las corridas previas del proyecto.
        d_g6 = F.col("mecanismo_licitatorio") & F.col("tiene_clave") & F.col("dentro_universo")
        n_g6 = d_g6 & F.coalesce(F.col("enlazada"), F.lit(False))

        def _cn(cond, alias):
            return F.sum(F.when(cond, 1).otherwise(0)).alias(alias)

        def _cc(cond, alias):
            return F.coalesce(F.sum(F.when(cond, F.col("clp_linea"))), F.lit(0.0)).alias(alias)

        # ---- ACCIÓN 1 de 3 : una sola pasada -----------------------------------
        casc = b.agg(
            F.count(F.lit(1)).alias("n_total"),
            F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("facial_todas_monedas"),
            F.sum("falla_prec").alias("n_falla_prec"),
            F.sum("falla_cant").alias("n_falla_cant"),
            _cn(F.col("tipo_nulo"), "n_tipo_nulo"),
            _cn(F.col("precio").isNull(), "n_precio_null"),
            _cn(F.col("cant").isNull(), "n_cant_null"),
            _cn(p1, "n_1"), _cc(p1, "c_1"),
            _cn(p2, "n_2"), _cc(p2, "c_2"),
            _cn(p3, "n_3"), _cc(p3, "c_3"),
            _cn(p4, "n_4"), _cc(p4, "c_4"),
            _cn(p5, "n_5"), _cc(p5, "c_5"),
            _cn(p6, "n_6"), _cc(p6, "c_6"),
            _cn(p7, "n_7"), _cc(p7, "c_7"),
            _cn(d_g6, "n_g6_den"), _cc(d_g6, "c_g6_den"),
            _cn(n_g6, "n_g6_num"), _cc(n_g6, "c_g6_num"),
        ).collect()[0]

        nt = casc["n_total"] or 0
        print("\n" + "-" * 78)
        print("CAP-3.0 · SALUD DEL PARSEO NUMÉRICO (leer ANTES que cualquier cifra en pesos)")
        print("-" * 78)
        for et, k in [("PRECIO con contenido que NO se pudo parsear a número", "n_falla_prec"),
                      ("CANTIDAD con contenido que NO se pudo parsear a número", "n_falla_cant"),
                      ("precio NULL tras parsear", "n_precio_null"),
                      ("cantidad NULL tras parsear", "n_cant_null"),
                      ("tipo_oc nulo o vacío (NO cuenta como licitatorio)", "n_tipo_nulo")]:
            v = casc[k] or 0
            print(f"   {et:<56} {v:>12,}  ({cap_pct(v, nt) or 0:.4f}%)")
        print("   REGLA: si el % de no parseables no es ~0, NINGUNA cifra en pesos de este")
        print("   bloque es citable todavía. Arreglar el parseo, no reportar igual.")

        base_c = casc["c_1"] or 0
        print("\n" + "-" * 78)
        print("CAP-3.A · CASCADA ANIDADA — del gasto en CLP a lo efectivamente auditable")
        print("   Denominador de la columna %: el peldaño 1 (CLP nativo).")
        print("-" * 78)
        print(f"{'PELDAÑO':<54}{'LÍNEAS':>13}{'% de p1':>11}")
        print(f"{'0 · Universo total de líneas de OC ' + str(ANIO_MIN) + '-' + str(ANIO_MAX):<54}{nt:>13,}{'—':>11}")
        for et, kn, kc in [
            ("1 · ... en pesos chilenos nativos (CLP)", "n_1", "c_1"),
            ("2 · ... y de mecanismo que pasa por licitación", "n_2", "c_2"),
            ("3 · ... y que trae código de licitación", "n_3", "c_3"),
            ("4 · ... y cuya licitación cae dentro del universo", "n_4", "c_4"),
            ("5 · ... y que efectivamente enlaza", "n_5", "c_5"),
            ("6 · ... y con código ONU válido de 8 dígitos", "n_6", "c_6"),
            ("7 · ... y con cantidad = 1  -> AUDITABLE", "n_7", "c_7"),
        ]:
            print(f"{et:<54}{casc[kn] or 0:>13,}{cap_pct(casc[kc] or 0, base_c) or 0:>10.2f}%")
        print(f"\n   Suma FACIAL de todas las monedas (NO es CLP): "
              f"{int(casc['facial_todas_monedas'] or 0):,}")
        print(f"   CLP nativo (peldaño 1) ....................: {int(base_c):,}")
        print(f"   CLP auditable (peldaño 7) .................: {int(casc['c_7'] or 0):,}")

        print("\n" + "-" * 78)
        print("CAP-3.B · G6 RESTRINGIDO **Y** AJUSTADO")
        print("-" * 78)
        if not c_flag:
            print("   NO CALCULABLE: el flag de enlace no existe en este DataFrame.")
            print("   (NO es 0%.)")
        else:
            print("   Denominador: líneas de mecanismo licitatorio, CON código de licitación,")
            print("   cuya licitación de origen cae dentro del universo. SIN filtro de moneda,")
            print("   para ser comparable con las cifras previas del proyecto.")
            g_n = cap_pct(casc["n_g6_num"] or 0, casc["n_g6_den"] or 0)
            g_c = cap_pct(casc["c_g6_num"] or 0, casc["c_g6_den"] or 0)
            print(f"   Denominador: {casc['n_g6_den'] or 0:,} líneas · numerador: {casc['n_g6_num'] or 0:,}")
            print(f"   por CONTEO : {f'{g_n:.2f}%' if g_n is not None else 'NO CALCULABLE'}")
            print(f"   por MONTO  : {f'{g_c:.2f}%' if g_c is not None else 'NO CALCULABLE'}")

        # ---- ACCIÓN 2 de 3: auditoría de la lista de exclusión -----------------
        mec = (b.groupBy("tipo_norm")
                .agg(F.count(F.lit(1)).alias("lineas"),
                     F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("monto_facial"),
                     (100.0 * F.avg(F.col("tiene_clave").cast("int"))).alias("pct_con_clave"),
                     (100.0 * F.avg(F.coalesce(F.col("enlazada"), F.lit(False)).cast("int"))
                      ).alias("pct_enlazada"))
                .orderBy(F.col("lineas").desc()).limit(40).collect())
        cap_print_tabla(
            mec,
            "CAP-3.B-bis · MECANISMOS PRESENTES Y SU TASA DE CLAVE DE LICITACIÓN\n"
            f"          Excluidos hoy del denominador: {SIN_LICITACION}\n"
            "          LECTURA: un mecanismo con pct_con_clave ALTO no debería estar excluido,\n"
            "          y uno con pct_con_clave ~0 debería estarlo. Contrastar contra la tabla\n"
            "          de dominio oficial: TD y CB NO figuran en ella; R1 sí, y hoy no se excluye.",
            cols=["tipo_norm", "lineas", "monto_facial", "pct_con_clave", "pct_enlazada"])

        # ---- ACCIÓN 3 de 3: reconciliación externa por año ---------------------
        rec = (b.groupBy("anio")
                 .agg(F.coalesce(F.sum(F.when(F.col("es_clp"), F.col("clp_linea"))),
                                 F.lit(0.0)).alias("clp_pipeline"),
                      (F.countDistinct("doc") if c_doc else F.lit(None)).alias("n_oc_distintas"),
                      F.count(F.lit(1)).alias("n_lineas"))
                 .orderBy("anio").collect())
        cap_print_tabla(rec, "CAP-3.C · Gasto por año según el pipeline (solo líneas CLP nativas)")

        r24 = [r for r in rec if r["anio"] == 2024]
        print("\nCAP-3.D · RECONCILIACIÓN EXTERNA CONTRA CIFRA OFICIAL 2024  [FUENTE]")
        print("   ChileCompra, 09-jun-2025: 'US$17.643 millones, equivalentes a $16.653.131")
        print("   millones de pesos' y '2.031.670 órdenes de compra'.")
        if not r24:
            print("   NO SE PUEDE COMPARAR: no hay filas de 2024.")
        else:
            p = r24[0]["clp_pipeline"] or 0
            print(f"   Pipeline 2024 (CLP)      : {int(p):,}")
            print(f"   Oficial  2024 (CLP)      : {OFICIAL_2024_CLP:,}")
            print(f"   Razón pipeline/oficial   : {p / OFICIAL_2024_CLP:.4f}")
            print(f"   Pipeline x 1,19 (con IVA): {int(p * 1.19):,}")
            print(f"   Razón con IVA            : {p * 1.19 / OFICIAL_2024_CLP:.4f}")
            if r24[0]["n_oc_distintas"]:
                print(f"   Pipeline 2024 (N OC)     : {r24[0]['n_oc_distintas']:,}")
                print(f"   Oficial  2024 (N OC)     : {OFICIAL_2024_NUM_OC:,}")
                print(f"   Razon de N de OC         : {r24[0]['n_oc_distintas'] / OFICIAL_2024_NUM_OC:.4f}")
            print("   LECTURA: el pipeline suma precio_NETO x cantidad y solo CLP nativo. Una")
            print("   razon cercana a 0,84 es compatible con el solo efecto del IVA; una cercana")
            print("   a 1,00 podria ser tambien dos errores que se compensan (neto sin IVA +")
            print("   doble conteo Historico/Vigente). Explicar la brecha ANTES de citar.")
            print("   NOTA: existe la columna `monto_total_oc_clp` a nivel de OC. Reconciliar")
            print("   ademas con ella cierra la pregunta del IVA de forma directa.")
        print(f"\nCAP-3 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")
# %% ==========================================================================
# CAP-4 v12 · CONCENTRACIÓN DEL CATÁLOGO + DISPERSIÓN DE PRECIO UNITARIO
# Requiere CAP-0 y CAP-0-BIS. Reemplaza al CAP-4 de v11.
#
# EL CAMBIO IMPORTANTE: existe `_sinclasificar_UnidadMedida`. El proyecto había
# declarado que NO había campo de unidad de medida y por eso restringía a
# `cantidad_oc = 1` — un control tosco que bota casi todo el universo y aun así
# no separa "caja de 10" de "caja de 100". Con unidad de medida se puede
# estratificar de verdad: mismo código ONU **y** misma unidad. Esa es la versión
# defendible de la Pregunta 3. Se reportan las dos, para poder decir en la
# defensa cuánto cambia el resultado al cambiar el control.
#
# También: los precios se parsean con cap_num() y `organismos` ya no es el
# prefijo del código de proceso sino `CodigoOrganismoPublico` cuando existe.
# =============================================================================
print("\n" + "=" * 78)
print(f"CAP-4 v12 · Concentración del catálogo y dispersión de precio · {RUN_ID_CAP}")
print("=" * 78)

_oc = cap_global("df_oc_all") if cap_global("df_oc_all") is not None else cap_global("df_oc_linked")

if _oc is None:
    print("ABORTA: falta df_oc_all / df_oc_linked en el kernel.")
else:
    c_onu  = cap_resolver(_oc, ["onu_oc"], "ONU (oc)")
    c_anio = cap_resolver(_oc, ["_year", "anio"], "año (oc)")
    c_cant = cap_resolver(_oc, ["cantidad_oc"], "cantidad")
    c_prec = cap_resolver(_oc, ["precio_neto_oc"], "precio")
    c_mon  = cap_resolver(_oc, ["moneda_item"], "moneda")
    c_um   = cap_resolver(_oc, ["_sinclasificar_UnidadMedida"], "unidad de medida")
    c_org  = cap_resolver(_oc, ["_sinclasificar_CodigoOrganismoPublico"], "organismo")

    if not (c_onu and c_prec and c_cant):
        print(f"ABORTA: faltan ONU ({c_onu}), precio ({c_prec}) o cantidad ({c_cant}).")
    else:
        b = _oc.select(
            (F.col(c_anio).cast("int") if c_anio else F.lit(None).cast("int")).alias("anio"),
            cap_onu8(F.col(c_onu)).alias("onu8"),
            cap_num(F.col(c_prec)).alias("precio"),
            cap_num(F.col(c_cant)).alias("cant"),
            (F.col(c_mon) if c_mon else F.lit("CLP")).alias("moneda"),
            (F.upper(F.trim(F.col(c_um).cast("string"))) if c_um else F.lit(None).cast("string")).alias("um"),
            (F.trim(F.col(c_org).cast("string")) if c_org else F.lit(None).cast("string")).alias("org"),
        ).withColumn("clp_linea", F.col("precio") * F.col("cant"))

        # ---- ACCIÓN 1 de 3: concentración del catálogo -------------------------
        conc = (b.where(F.col("onu8").isNotNull())
                 .groupBy("onu8")
                 .agg(F.count(F.lit(1)).alias("lineas"),
                      F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp"))
                 .orderBy(F.col("lineas").desc()).limit(300).collect())

        # ---- ACCIÓN 2 de 3: totales del universo -------------------------------
        rt = b.where(F.col("onu8").isNotNull()).agg(
            F.count(F.lit(1)).alias("n"),
            F.countDistinct("onu8").alias("codigos"),
            F.coalesce(F.sum("clp_linea"), F.lit(0.0)).alias("clp"),
            F.countDistinct("um").alias("unidades_medida")).collect()[0]
        n_tot, k_tot, clp_tot = rt["n"] or 0, rt["codigos"] or 0, rt["clp"] or 0

        def _p(num, den):
            v = cap_pct(num, den)
            return f"{v:6.2f}%" if v is not None else "    n/c"

        print("\n" + "-" * 78)
        print("CAP-4.A · CONCENTRACIÓN DEL CATÁLOGO EFECTIVAMENTE USADO")
        print("-" * 78)
        print(f"   Líneas con código ONU de 8 dígitos ........... {n_tot:,}")
        print(f"   Códigos distintos EN FORMA de 8 dígitos ...... {k_tot:,}")
        print(f"   Valores distintos de unidad de medida ........ {rt['unidades_medida'] or 0:,}")
        print("   Catálogo de referencia ....................... 18.881 códigos")
        print("     [DATO INTERNO, no [FUENTE]: proviene de Listado_rubros_ONU_3.xlsx aportado")
        print("      por el autor. ChileCompra NO publica el tamaño de su catálogo ni la")
        print("      versión de UNSPSC que aplica — verificado el 21-ago-2026.]")
        print(f"   Razón contra ese catálogo .................... {_p(k_tot, 18881)}")
        print("     OJO: puede superar 100%. Cuenta códigos con FORMA válida, no membresía")
        print("     en el catálogo; los códigos retirados desde 2017 siguen contando.")
        if n_tot == 0:
            print("   (universo vacío — no se puede calcular concentración. NO es 'no hay")
            print("    concentración': es que no hay datos.)")
        for n in (10, 50, 100):
            sub = conc[:n]
            print(f"   Top-{n:<4} (por LÍNEAS) concentra ........... "
                  f"{_p(sum(r['lineas'] for r in sub), n_tot)} de las líneas · "
                  f"{_p(sum(r['clp'] or 0 for r in sub), clp_tot)} del gasto")
        print("   NOTA: el top-N está ordenado por LÍNEAS. El % de gasto es el de esos mismos")
        print("   códigos, no el top-N por gasto. No confundir las dos frases.")
        cap_print_tabla(conc[:15],
                        "CAP-4.B · Los 15 códigos ONU más usados (candidatos a 'cajón de sastre')")

        # ---- ACCIÓN 3 de 3: dispersión, con los DOS controles ------------------
        top_codes = [r["onu8"] for r in conc[:TOP_N_PRECIO]]
        filtro = (F.col("onu8").isin(top_codes) &
                  (F.upper(F.trim(F.col("moneda").cast("string"))) == "CLP") &
                  (F.col("precio") > 0))

        # Control A (nuevo, defendible): mismo código ONU Y misma unidad de medida.
        dispA = (b.where(filtro & F.col("um").isNotNull() & (F.col("um") != ""))
                  .groupBy("onu8", "um", "anio")
                  .agg(F.count(F.lit(1)).alias("n"),
                       F.countDistinct("org").alias("organismos"),
                       F.expr("percentile_approx(precio, 0.10, 1000)").alias("p10"),
                       F.expr("percentile_approx(precio, 0.50, 1000)").alias("p50"),
                       F.expr("percentile_approx(precio, 0.90, 1000)").alias("p90"))
                  .where(F.col("n") >= 100)
                  .withColumn("ratio_p90_p10",
                              F.when(F.col("p10") > 0, F.col("p90") / F.col("p10")))
                  .orderBy(F.col("ratio_p90_p10").desc_nulls_last()).limit(40).collect())
        cap_print_tabla(
            dispA,
            "CAP-4.C · DISPERSIÓN DE PRECIO UNITARIO — control por UNIDAD DE MEDIDA\n"
            f"          Filtros DECLARADOS: top-{TOP_N_PRECIO} códigos por volumen, moneda CLP\n"
            "          nativa, precio > 0, n >= 100 por (código × unidad × año).\n"
            "          SALVEDAD 1: la unidad de medida es texto libre del comprador; 'UN',\n"
            "          'Unidad' y 'c/u' pueden ser lo mismo escrito distinto. Antes de citar,\n"
            "          mirar los valores reales de `um` en las filas que uses.\n"
            "          SALVEDAD 2: `organismos` es CodigoOrganismoPublico. Si sale 1, la fila\n"
            "          NO sirve para comparar precios ENTRE organismos.\n"
            "          Antes de citar cualquier ratio: revisar a mano los 10 casos extremos.")
        print("\n   NOTA: los montos NO están deflactados. Antes de comparar entre años hay que")
        print("   llevarlos a CLP real (IPC) o a UTM del mes.")
        print("   COMPARACIÓN OBLIGATORIA: correr también la versión con `cant == 1` y sin")
        print("   unidad de medida (el control de v11) y reportar cuánto cambia el ratio. Si")
        print("   cambia mucho, el hallazgo depende del control y hay que decirlo.")
        print(f"\nCAP-4 v12 termina. RUN_ID_CAP={RUN_ID_CAP}")
# %% ==========================================================================
# CAP-6 · EXPORTAR A CSV LAS TABLAS CITADAS  —  CERO cómputo de Spark
#
# QUÉ HACE. Escribe a CSV las tablas que los bloques CAP-1 a CAP-5 ya trajeron al
# driver con .collect(). No lanza ninguna acción de Spark: lee las variables de
# Python que quedaron vivas en el kernel. Por eso no consume cupo y por eso hay
# que correrla DESPUÉS de los bloques, en la misma sesión.
#
# CÓMO SE USA. Editar VOL_BASE con un Volumen de Unity Catalog que YA EXISTA y
# correr la celda. Cada archivo lleva el RUN_ID_CAP en el nombre.
#
# REQUISITOS DE ENTORNO. Un Volumen existente con permiso de escritura. NO se
# construyen rutas con el segmento "..": en Unity Catalog el segmento que sigue
# al esquema es el NOMBRE del volumen, no una carpeta, y ".." resuelve a un
# volumen hermano que tendría que existir. Se usa una subcarpeta DENTRO del volumen.
#
# QUÉ PRODUCE. Un CSV por tabla citable, más un manifiesto con el run_id, la
# fecha y el inventario de lo exportado. Esto es el seguro contra el riesgo 9
# (si Databricks no responde el día de la defensa, las cifras siguen existiendo)
# y es además requisito de nota del curso.
# =============================================================================
import csv as _csv
import os as _os
import datetime as _dt

# ⟨EDITAR⟩ Volumen que YA existe. Formato: /Volumes/<catalogo>/<esquema>/<volumen>
VOL_BASE = "/Volumes/workspace/default/capstone"

_DEST = f"{VOL_BASE}/salidas_cap/{RUN_ID_CAP}"
_os.makedirs(_DEST, exist_ok=True)
print("=" * 78)
print(f"CAP-6 · exportación a CSV · RUN_ID_CAP = {RUN_ID_CAP}")
print(f"   Destino: {_DEST}")
print("=" * 78)

_manifiesto = []


def _escribir(filas, nombre, descripcion):
    """Escribe una lista de Row (o de dict) como CSV. Devuelve n filas escritas."""
    if not filas:
        print(f"   [salta] {nombre}: sin filas en el kernel.")
        return 0
    try:
        datos = [r.asDict() for r in filas]
    except AttributeError:
        datos = list(filas)
    ruta = f"{_DEST}/{nombre}__{RUN_ID_CAP}.csv"
    with open(ruta, "w", newline="", encoding="utf-8") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(datos[0].keys()))
        w.writeheader()
        w.writerows(datos)
    print(f"   [ok] {nombre:<28} {len(datos):>6,} filas → {ruta}")
    _manifiesto.append({"archivo": _os.path.basename(ruta), "tabla": nombre,
                        "filas": len(datos), "descripcion": descripcion,
                        "run_id_cap": RUN_ID_CAP})
    return len(datos)


def _tomar(nombre_var):
    """Lee una variable global del kernel sin romper si no existe."""
    try:
        return eval(nombre_var)
    except (NameError, SyntaxError):
        return None


# Nombre de variable en el kernel → (nombre de archivo, descripción)
_TABLAS = [
    ("res",   "cap1_real_vs_nulo",      "CAP-1.A · consistencia ONU real vs. nulo permutado, por universo y bucket"),
    ("filas", "cap2_cobertura_anual",   "CAP-2.A · cobertura y validez del código ONU por año, con placebos"),
    ("comp",  "cap2_cobertura_mecanismo", "CAP-2.C · cobertura ONU por año y mecanismo de compra"),
    ("mec",   "cap3_mecanismos",        "CAP-3.B-bis · mecanismos, tasa de clave de licitación y de enlace"),
    ("rec",   "cap3_gasto_anual",       "CAP-3.C · gasto por año del pipeline, solo líneas CLP nativas"),
    ("conc",  "cap4_concentracion",     "CAP-4.A/B · concentración del catálogo ONU efectivamente usado"),
    ("dispA", "cap4_dispersion_precio", "CAP-4.C · dispersión de precio unitario por código × unidad de medida × año"),
]

for _v, _n, _d in _TABLAS:
    _escribir(_tomar(_v), _n, _d)

# La cascada de CAP-3 es una sola Row: se exporta como tabla de una fila.
_casc = _tomar("casc")
if _casc is not None:
    _escribir([_casc], "cap3_cascada", "CAP-3.A · cascada del gasto, contadores y montos por peldaño")

if _manifiesto:
    _ruta_m = f"{_DEST}/00_MANIFIESTO__{RUN_ID_CAP}.csv"
    for _r in _manifiesto:
        _r["fecha_corrida"] = _dt.date.today().isoformat()
    with open(_ruta_m, "w", newline="", encoding="utf-8") as fh:
        w = _csv.DictWriter(fh, fieldnames=list(_manifiesto[0].keys()))
        w.writeheader()
        w.writerows(_manifiesto)
    print(f"\n   [ok] manifiesto → {_ruta_m}")
    print(f"   {len(_manifiesto)} tablas exportadas.")
else:
    print("\n   NADA EXPORTADO: ninguna de las variables esperadas está viva en el kernel.")
    print("   Correr CAP-1 a CAP-5 en ESTA misma sesión antes de CAP-6.")

print("\n   Descargar los CSV desde Catalog → Volumes en la interfaz de Databricks,")
print("   y subirlos al repositorio de GitHub junto con el manifiesto.")
print(f"\nCAP-6 termina. RUN_ID_CAP={RUN_ID_CAP}")