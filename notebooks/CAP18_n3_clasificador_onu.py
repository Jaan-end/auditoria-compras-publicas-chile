# =============================================================================
#  CAP18_n3_clasificador_onu.py — N3, el componente de ML del capstone
#  Capstone Big Data · Mercado Publico nacional 2017-2026
#
#  PREGUNTA: a partir del texto libre que escribe el comprador, ¿se puede
#  predecir el codigo ONU/UNSPSC que le corresponde a la linea? Y si se puede,
#  ¿con que exactitud, y sobre que parte del catalogo falla?
#
#  ---------------------------------------------------------------------------
#  QUE ES ESTO Y QUE NO ES
#  ---------------------------------------------------------------------------
#  ES el reclasificador supervisado N3 que el documento de evaluacion del 5-ago
#  describe como "el componente de ML del proyecto", con el stack que ahi se
#  propone y que es literalmente el contenido de la Clase 10:
#
#      Tokenizer -> CountVectorizer -> IDF -> LogisticRegression | NaiveBayes
#
#  NO ES un detector de irregularidades. Un desacuerdo entre lo que el modelo
#  predice y lo que la linea declara es una ALERTA ESTADISTICA (doctrina del
#  proyecto, regla 4), y ademas puede ser error del modelo: el macro-F1 que
#  esta celda imprime dice exactamente cuanto hay que desconfiar de el.
#
#  ---------------------------------------------------------------------------
#  LAS TRES TRAMPAS QUE ESTA CELDA EVITA A PROPOSITO
#  ---------------------------------------------------------------------------
#  1. FUGA DE ETIQUETA POR TEXTO CIRCULAR. La columna NombreProductoGenerico es
#     el nombre de catalogo del codigo declarado: entrenar con ella es entrenar
#     al modelo a leer la respuesta dentro de la pregunta, y el accuracy sale
#     altisimo y no significa nada. Esta celda (a) NUNCA usa esa columna como
#     feature, y (b) reutiliza el filtro de circularidad de CAP-10/G7 —el mismo
#     que ya produjo el 81,87% de lineas evaluables— para descartar las lineas
#     cuyo texto ES el nombre de catalogo. Ademas descarta el texto que contiene
#     el codigo escrito como literal.
#
#  2. FUGA POR REPETICION. El corpus tiene ~19,96 millones de pares distintos
#     sobre 41,7 millones de lineas: la misma descripcion aparece muchas veces.
#     Un split aleatorio pondria copias de la MISMA linea a los dos lados y el
#     accuracy seria memoria, no generalizacion. Esta celda deduplica por texto
#     normalizado ANTES de partir, y parte por AÑO, no al azar.
#
#  3. MACRO-F1 QUE NO ES MACRO. En Spark, metricName="f1" del
#     MulticlassClassificationEvaluator devuelve F1 PONDERADO por soporte, no
#     macro. Sobre un catalogo con cola larga, el ponderado se ve bien y esconde
#     justo lo que interesa. Esta celda calcula el macro-F1 a mano, en el
#     driver, desde la matriz de confusion agregada, e imprime los dos.
#
#  ---------------------------------------------------------------------------
#  ETIQUETAS: DE DONDE SALEN Y POR QUE SE PUEDE CONFIAR EN ELLAS
#  ---------------------------------------------------------------------------
#  Nivel A del proyecto: lineas donde la licitacion de origen y la orden de
#  compra coinciden EXACTAMENTE en el codigo de 8 digitos. Dos partes que
#  escribieron por separado dijeron lo mismo. Se calculan dos definiciones:
#
#     A_ESTRICTO : la licitacion declaro UN SOLO codigo distinto en toda su
#                  adjudicacion, y la linea de OC usa exactamente ese. No hay
#                  ambiguedad posible de a que linea de licitacion corresponde.
#     A_AMPLIO   : el codigo de la linea de OC pertenece al conjunto de codigos
#                  que declaro su licitacion. Mas datos, correspondencia no
#                  garantizada linea a linea.
#
#  Se ENTRENA con A_ESTRICTO. A_AMPLIO se cuenta y se imprime para poder decir
#  cuanto se dejo fuera. Sesgo declarado, no escondido: A_ESTRICTO sobre-
#  representa compras simples de un solo producto. Va en el informe como
#  limitacion, no se descubre en la defensa.
#
#  ---------------------------------------------------------------------------
#  GRANULARIDAD: 2 Y 4 DIGITOS, NUNCA 8
#  ---------------------------------------------------------------------------
#  UNSPSC es jerarquico por truncado: 2/4/6/8 = segmento/familia/clase/producto.
#  A 8 digitos hay 18.881 clases y la tarea no es tratable con este volumen de
#  etiquetas limpias. A 2 (segmento) y 4 (familia) el problema es tratable y la
#  metrica es defendible. Predecir a 8 y reportar un accuracy bajo no seria un
#  hallazgo, seria un error de diseño.
#
#  ---------------------------------------------------------------------------
#  COSTO Y SEGURIDAD DE EJECUCION (Databricks Free Edition, cupo diario)
#  ---------------------------------------------------------------------------
#  · SIN .persist() / .cache() (regla §3.8) — no estan soportados en serverless.
#    En su lugar la tabla de entrenamiento se MATERIALIZA una vez a Parquet y se
#    vuelve a leer. Sin eso, cada iteracion de LogisticRegression recomputaria
#    el join de 16,9M de filas desde cero: es la diferencia entre minutos y
#    horas, y es la razon por la que esta celda escribe a disco.
#  · La tabla materializada esta TOPEADA (TOPE_ENTRENAMIENTO). Si el freno
#    18.D.1 dice que hay mas filas que el tope, se muestrea con semilla fija y
#    se declara.
#  · NaiveBayes va PRIMERO: es de una sola pasada y da un numero en minutos. Si
#    ese numero es absurdo, se para ahi y no se gasta el cupo en LogisticRegression.
#  · CORRE_FAMILIA4 arranca en False. Primero segmento (2 digitos), se mira el
#    resultado, y recien despues se decide gastar cupo en familia (4 digitos).
#  · SIN .show() sobre DataFrames grandes.
#  · Guarda de dependencias: si falta un DataFrame o una columna, ABORTA
#    imprimiendo lo que hay. No inventa sustitutos (leccion de la sesion 7).
#
#  ---------------------------------------------------------------------------
#  ESTADO DE VERIFICACION — LEER ANTES DE CORRER (regla §3.11)
#  ---------------------------------------------------------------------------
#  · La matematica del driver (macro-F1, micro-F1, F1 ponderado, baseline de
#    clase mayoritaria, lectura de la matriz de confusion) SI se valido: ver
#    `test_cap18_metricas.py`, incluido en el paquete. Corre en 1 segundo, sin
#    Spark, y compara contra sklearn cuando sklearn esta disponible.
#  · Las expresiones de SPARK y el Pipeline de ML no se pudieron validar en
#    PySpark local: el entorno donde se escribio esta celda no logro instalar
#    pyspark (mismo caso que CAP-14). Se declararon [POR VERIFICAR] y se corrio
#    la celda tratando la primera corrida como una prueba.
#
#    >>> ACTUALIZADO EL 7-sep-2026: YA CORRIO. RUN_ID_N3 = 5a53ec4a1f30. <<<
#    Salida archivada en resultados/cap18_n3_salida_cruda.txt. El embudo de
#    etiquetas reproduce SUS SEIS CIFRAS EXACTAS en la corrida previa del mismo
#    dia (fa2bca7b426b), que aborto al materializar. Las expresiones de Spark
#    dejan de estar [POR VERIFICAR]: corrieron.
#
#    LO QUE SI FALLO en fa2bca7b426b, y esta corregido en esta version: escribir
#    a /tmp/... falla en Databricks Free Edition con DBFS_DISABLED / SQLSTATE
#    56038, porque el DBFS root publico esta deshabilitado. Ahora materializa en
#    una tabla de Unity Catalog.
#
#    Igual, si se vuelve a correr: mirar 18.C.1 antes que nada. Si el numero de
#    lineas candidatas no esta en el orden de los millones (fueron 15.318.366),
#    el join esta mal y hay que parar antes de gastar mas cupo.
#
#  CORRER DESPUES DE: CAP-0, CAP-0-BIS (definen RUN_ID_CAP, cap_resolver,
#  cap_global, cap_onu8), CAP-0-G7 (define g7_norm, MULETILLAS_G7, MIN_CHARS_G7)
#  y las celdas 1-8 de la Etapa 2 (definen df_oc_linked y df_lic_all).
#
#  ADVERTENCIA DE LECTURA (§3.12): ningun veredicto impreso aqui es una
#  conclusion. Cada uno viene con su baseline al lado y hay que releerlo contra
#  ese baseline antes de copiarlo a ninguna parte. Un accuracy sin su clase
#  mayoritaria al lado no es una cifra, es una decoracion.
# =============================================================================

# %% ==========================================================================
# CAP-18 · N3 — RECLASIFICADOR SUPERVISADO DE CODIGO ONU SOBRE TF-IDF
# =============================================================================

import uuid as _uuid_n3

from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (RegexTokenizer, StopWordsRemover,
                                CountVectorizer, IDF, StringIndexer)
from pyspark.ml.classification import LogisticRegression, NaiveBayes

# -----------------------------------------------------------------------------
# 18.0 · PARAMETROS — declarados ANTES de correr, para no mover la frontera
#        despues de ver el resultado (misma disciplina que los umbrales de 6D-4)
# -----------------------------------------------------------------------------
RUN_ID_N3 = _uuid_n3.uuid4().hex[:12]

ANIO_CORTE_TRAIN   = 2023      # train <= 2023 · test >= 2024. Split POR AÑO, no aleatorio.
TOPE_ENTRENAMIENTO = 600_000   # filas maximas de la tabla materializada
SEED_N3            = 20260907

VOCAB_SIZE   = 20_000          # terminos del CountVectorizer
MIN_DF       = 5               # un termino debe aparecer en >=5 documentos
MIN_TOKENS   = 3               # descartar textos de menos de 3 palabras utiles
LR_MAX_ITER  = 15              # iteraciones de LogisticRegression (cupo!)
LR_REG_PARAM = 0.01

MIN_FILAS_PARA_ENTRENAR = 100_000   # freno 18.D.1: por debajo de esto NO se entrena
MIN_CLASES              = 10        # freno: menos clases que esto = algo salio mal

CORRE_FAMILIA4 = False         # arranca en False A PROPOSITO. Ver 18.H.
CORRE_LOGREG   = True          # ponerlo en False para correr solo NaiveBayes

# La tabla materializada (reemplazo de .cache()) se nombra mas abajo, en 18.D:
# va a Unity Catalog, NO a una ruta de DBFS. Free Edition tiene el DBFS root
# publico deshabilitado y cualquier escritura a /tmp/... falla con DBFS_DISABLED.

# Referencia externa para comparar. NO es un resultado propio.
REF_CLICIT_ACC   = 77.7        # [DOC] CLiC-it 2023, misma tarea sobre CPV
REF_CLICIT_MACRO = 0.40        # [DOC] idem — a re-verificar en fuente primaria antes de citar

print("=" * 78)
print(f"CAP-18 · N3 — clasificador supervisado ONU · RUN_ID_N3 = {RUN_ID_N3}")
print("=" * 78)
print(f"  split          : train <= {ANIO_CORTE_TRAIN} · test >= {ANIO_CORTE_TRAIN + 1} (POR AÑO, no aleatorio)")
print(f"  tope train     : {TOPE_ENTRENAMIENTO:,} filas · seed {SEED_N3}")
print(f"  vocabulario    : {VOCAB_SIZE:,} terminos · minDF {MIN_DF} · minTokens {MIN_TOKENS}")
print(f"  modelos        : NaiveBayes{' + LogisticRegression' if CORRE_LOGREG else ''}")
print(f"  granularidad   : segmento (2 dig){' + familia (4 dig)' if CORRE_FAMILIA4 else ''} — NUNCA 8")
print("  materializa en : tabla de Unity Catalog (se nombra en 18.D)")


# -----------------------------------------------------------------------------
# 18.A · GUARDAS DE DEPENDENCIA — abortar limpio antes que inventar sustitutos
# -----------------------------------------------------------------------------
def _n3_global(nombre):
    return globals().get(nombre)


_ABORTA = []
for _dep in ["df_oc_linked", "df_lic_all"]:
    if _n3_global(_dep) is None:
        _ABORTA.append(f"falta el DataFrame `{_dep}`")
for _fn in ["cap_resolver", "cap_onu8", "g7_norm"]:
    if _n3_global(_fn) is None:
        _ABORTA.append(f"falta la funcion `{_fn}()`")
if _n3_global("MULETILLAS_G7") is None:
    _ABORTA.append("falta MULETILLAS_G7 (correr CAP-0-G7)")
if _n3_global("MIN_CHARS_G7") is None:
    _ABORTA.append("falta MIN_CHARS_G7 (correr CAP-0-G7)")

if _ABORTA:
    print("\n" + "!" * 78)
    print("CAP-18 ABORTA — no se corre nada. Motivos:")
    for _m in _ABORTA:
        print(f"   · {_m}")
    print("\n  Correr antes: CAP-0, CAP-0-BIS, CAP-0-G7 y las celdas 1-8 de la Etapa 2.")
    print("!" * 78)
else:
    _oc  = df_oc_linked
    _lic = df_lic_all

    # --- 18.B · resolucion de columnas ---------------------------------------
    print("\n### 18.B · columnas resueltas")
    c_onu_oc  = cap_resolver(_oc,  ["onu_oc", "codigo_producto_onu",
                                    "CodigoProductoONU"], "ONU (oc)")
    c_onu_lic = cap_resolver(_lic, ["onu_lic", "codigo_producto_onu",
                                    "CodigoProductoONU"], "ONU (lic)")
    c_key_oc  = cap_resolver(_oc,  ["codigo_licitacion"], "clave licitacion (oc)")
    c_key_lic = cap_resolver(_lic, ["codigo_externo"], "clave licitacion (lic)")
    c_esp_c   = cap_resolver(_oc,  ["_sinclasificar_EspecificacionComprador",
                                    "EspecificacionComprador",
                                    "especificacion_comprador"], "espec. comprador")
    c_esp_p   = cap_resolver(_oc,  ["_sinclasificar_EspecificacionProveedor",
                                    "EspecificacionProveedor",
                                    "especificacion_proveedor"], "espec. proveedor")
    c_gen     = cap_resolver(_oc,  ["_sinclasificar_NombreroductoGenerico",
                                    "NombreroductoGenerico",
                                    "NombreProductoGenerico"], "producto generico")
    c_anio    = cap_resolver(_oc,  ["_year", "anio", "year"], "año")

    for _e, _v in [("ONU oc", c_onu_oc), ("ONU lic", c_onu_lic),
                   ("key oc", c_key_oc), ("key lic", c_key_lic),
                   ("espec. comprador", c_esp_c), ("espec. proveedor", c_esp_p),
                   ("generico (SOLO filtro)", c_gen), ("año", c_anio)]:
        print(f"   {_e:<24}: {_v}")

    _FALTA = [n for n, v in [("ONU oc", c_onu_oc), ("ONU lic", c_onu_lic),
                             ("key oc", c_key_oc), ("key lic", c_key_lic),
                             ("año", c_anio)] if v is None]
    if _FALTA or (c_esp_c is None and c_esp_p is None):
        print("\n   CAP-18 ABORTA: faltan columnas imprescindibles: "
              f"{_FALTA or 'ninguna de especificacion'}")
        print(f"   COLUMNAS DISPONIBLES EN OC (copiar al chat):\n   {_oc.columns}")
    else:
        # =====================================================================
        # 18.C · ETIQUETAS DE NIVEL A + GUARDAS DE FUGA
        # =====================================================================
        # --- conjunto de codigos declarados por cada licitacion ---------------
        _lic_sets = (
            _lic
            .select(F.col(c_key_lic).cast("string").alias("_k"),
                    cap_onu8(F.col(c_onu_lic)).alias("_onu8_lic"))
            .filter(F.col("_k").isNotNull() & (F.trim(F.col("_k")) != "")
                    & F.col("_onu8_lic").isNotNull())
            .groupBy("_k")
            .agg(F.collect_set("_onu8_lic").alias("_set8_lic"))
            .withColumn("_n_cod_lic", F.size("_set8_lic"))
        )

        # --- texto: el comprador manda, el proveedor complementa --------------
        _t_c = g7_norm(F.col(c_esp_c)) if c_esp_c else F.lit(None).cast("string")
        _t_p = g7_norm(F.col(c_esp_p)) if c_esp_p else F.lit(None).cast("string")
        _txt = F.coalesce(_t_c, _t_p)
        _gen = g7_norm(F.col(c_gen)) if c_gen else F.lit(None).cast("string")

        _rx_mule = "|".join(f"({p})" for p in MULETILLAS_G7)

        _cand = (
            _oc
            .filter(F.col("tiene_licitacion_origen"))
            .select(
                F.col(c_key_oc).cast("string").alias("_k"),
                cap_onu8(F.col(c_onu_oc)).alias("_onu8_oc"),
                _txt.alias("txt"),
                _gen.alias("gen"),
                F.col(c_anio).cast("int").alias("anio"),
            )
            .filter(F.col("_onu8_oc").isNotNull() & F.col("txt").isNotNull())
            .join(_lic_sets, on="_k", how="inner")
        )

        # --- las dos definiciones de nivel A ----------------------------------
        _es_amplio   = F.array_contains(F.col("_set8_lic"), F.col("_onu8_oc"))
        _es_estricto = _es_amplio & (F.col("_n_cod_lic") == 1)

        # --- GUARDAS DE FUGA (las tres trampas del encabezado) ----------------
        # G1 · circularidad: el texto ES (o contiene, o esta contenido en) el
        #      nombre de catalogo del codigo declarado. Mismo criterio que CAP-10.
        _no_circular = (F.col("gen").isNull()
                        | (~(F.col("txt") == F.col("gen"))
                           & ~F.col("gen").contains(F.col("txt"))
                           & ~F.col("txt").contains(F.col("gen"))))
        # G2 · el codigo escrito como literal dentro del texto, a 8/6/4/2 digitos
        _sin_codigo = (~F.col("txt").contains(F.col("_onu8_oc"))
                       & ~F.col("txt").contains(F.substring(F.col("_onu8_oc"), 1, 6))
                       & ~F.col("txt").contains(F.substring(F.col("_onu8_oc"), 1, 4)))
        # G3 · relleno: muletillas y textos demasiado cortos (criterio de CAP-10)
        _no_relleno = (~F.col("txt").rlike(_rx_mule)) & (F.length("txt") >= MIN_CHARS_G7)

        _etq = (
            _cand
            .withColumn("a_amplio",   _es_amplio)
            .withColumn("a_estricto", _es_estricto)
            .withColumn("ok_circ",    _no_circular)
            .withColumn("ok_cod",     _sin_codigo)
            .withColumn("ok_relleno", _no_relleno)
        )

        # --- 18.C.1 · el embudo, en UNA sola accion de Spark -------------------
        _emb = _etq.agg(
            F.count(F.lit(1)).alias("n_cand"),
            F.sum(F.col("a_amplio").cast("int")).alias("n_amplio"),
            F.sum(F.col("a_estricto").cast("int")).alias("n_estricto"),
            F.sum((F.col("a_estricto") & F.col("ok_circ")).cast("int")).alias("n_e_circ"),
            F.sum((F.col("a_estricto") & F.col("ok_circ") & F.col("ok_cod")).cast("int")).alias("n_e_cod"),
            F.sum((F.col("a_estricto") & F.col("ok_circ") & F.col("ok_cod")
                   & F.col("ok_relleno")).cast("int")).alias("n_e_final"),
        ).collect()[0]

        def _pc(a, b):
            return f"{(100.0 * a / b):.2f}%" if b else "n/d"

        print("\n" + "=" * 78)
        print(f"### 18.C.1 · EMBUDO DE ETIQUETAS · RUN_ID_N3 = {RUN_ID_N3}")
        print("=" * 78)
        print(f"  lineas de OC enlazadas, con ONU valido y con texto : {_emb['n_cand']:>14,}")
        print(f"  · nivel A AMPLIO   (codigo en el set de su lic.)   : {_emb['n_amplio']:>14,}  {_pc(_emb['n_amplio'], _emb['n_cand'])}")
        print(f"  · nivel A ESTRICTO (lic. declara 1 solo codigo)    : {_emb['n_estricto']:>14,}  {_pc(_emb['n_estricto'], _emb['n_cand'])}")
        print(f"    - tras quitar texto CIRCULAR                     : {_emb['n_e_circ']:>14,}  {_pc(_emb['n_e_circ'], _emb['n_estricto'])} del estricto")
        print(f"    - tras quitar texto con el CODIGO dentro         : {_emb['n_e_cod']:>14,}  {_pc(_emb['n_e_cod'], _emb['n_estricto'])}")
        print(f"    - tras quitar RELLENO y texto corto  = ENTRENABLE: {_emb['n_e_final']:>14,}  {_pc(_emb['n_e_final'], _emb['n_estricto'])}")
        print()
        print("  LEER ESTO ANTES DE SEGUIR: si `n_cand` no esta en el orden de los")
        print("  millones, el join esta mal y NADA de lo que sigue sirve. Parar aqui.")

        n_entrenable = _emb["n_e_final"]

        if n_entrenable < MIN_FILAS_PARA_ENTRENAR:
            print("\n" + "!" * 78)
            print(f"FRENO 18.C.1: solo {n_entrenable:,} filas entrenables "
                  f"(< {MIN_FILAS_PARA_ENTRENAR:,}). NO se entrena.")
            print("Eso NO es un fracaso: es un hallazgo citable — la fraccion del")
            print("universo con etiqueta limpia y texto no circular es demasiado")
            print("chica para sostener un clasificador. Se declara y se para.")
            print("!" * 78)
        else:
            # =================================================================
            # 18.D · DEDUPLICAR POR TEXTO Y MATERIALIZAR (reemplazo de .cache())
            # =================================================================
            # Un texto que aparece con DOS etiquetas distintas es ruido
            # irreducible: no se resuelve con un modelo, se descarta y se cuenta.
            _limpio = (
                _etq
                .filter(F.col("a_estricto") & F.col("ok_circ")
                        & F.col("ok_cod") & F.col("ok_relleno"))
                .select("txt", "anio", F.col("_onu8_oc").alias("onu8"))
                .withColumn("seg2", F.substring("onu8", 1, 2))
                .withColumn("fam4", F.substring("onu8", 1, 4))
            )

            _dedup = (
                _limpio
                .groupBy("txt")
                .agg(F.countDistinct("seg2").alias("_n_seg"),
                     F.min("seg2").alias("seg2"),
                     F.min("fam4").alias("fam4"),
                     F.min("anio").alias("anio"),
                     F.count(F.lit(1)).alias("_n_rep"))
                .filter(F.col("_n_seg") == 1)          # texto ambiguo -> fuera
                .drop("_n_seg")
            )

            _frac = min(1.0, float(TOPE_ENTRENAMIENTO) * 3.0 / max(n_entrenable, 1))
            if _frac < 1.0:
                _dedup = _dedup.sample(withReplacement=False, fraction=_frac, seed=SEED_N3)
                print(f"\n  [tope] se muestrea fraction={_frac:.4f} con seed={SEED_N3} "
                      f"para no pasar {TOPE_ENTRENAMIENTO:,} filas de entrenamiento.")

            # ACCION: materializar. Reemplaza a .cache(), que no existe en serverless.
            #
            # ⚠ CORREGIDO tras la corrida fa2bca7b426b (7-sep-2026). La version
            # anterior escribia a `/tmp/...`, que en Databricks se resuelve a
            # `dbfs:/tmp/...`, y Free Edition tiene el DBFS root PUBLICO
            # DESHABILITADO: falla con DBFS_DISABLED / SQLSTATE 56038.
            # Se materializa en una TABLA de Unity Catalog, que no necesita ruta.
            # Tres intentos en orden, del mas robusto al ultimo recurso.
            try:
                _cat = spark.catalog.currentCatalog()
                _sch = spark.catalog.currentDatabase()
            except Exception:
                _cat, _sch = "workspace", "default"
            TABLA_N3 = f"{_cat}.{_sch}.cap18_train_{RUN_ID_N3}"

            _tab = None
            try:
                _dedup.write.mode("overwrite").saveAsTable(TABLA_N3)
                _tab = spark.read.table(TABLA_N3)
                print(f"\n  [materializado] tabla UC: {TABLA_N3}")
            except Exception as _e_tab:
                print(f"\n  [aviso] no se pudo crear la tabla: "
                      f"{type(_e_tab).__name__}: {str(_e_tab)[:200]}")
                try:
                    spark.sql(f"CREATE VOLUME IF NOT EXISTS {_cat}.{_sch}.capstone_n3")
                    _ruta_vol = f"/Volumes/{_cat}/{_sch}/capstone_n3/{RUN_ID_N3}"
                    _dedup.write.mode("overwrite").parquet(_ruta_vol)
                    _tab = spark.read.parquet(_ruta_vol)
                    print(f"  [materializado] volumen UC: {_ruta_vol}")
                except Exception as _e_vol:
                    print(f"  [aviso] tampoco el volumen: "
                          f"{type(_e_vol).__name__}: {str(_e_vol)[:200]}")
                    print("\n  SIN MATERIALIZAR — se sigue con el DataFrame en vivo.")
                    print("  CONSECUENCIA REAL, no cosmetica: LogisticRegression")
                    print("  recomputa el join de 15,3M de filas en CADA iteracion.")
                    print("  Se fuerza LR_MAX_ITER=5 y se RECOMIENDA correr solo")
                    print("  NaiveBayes (CORRE_LOGREG=False) para no quemar el cupo.")
                    _tab = _dedup
                    LR_MAX_ITER = min(LR_MAX_ITER, 5)

            _res = _tab.agg(
                F.count(F.lit(1)).alias("n"),
                F.sum(F.when(F.col("anio") <= ANIO_CORTE_TRAIN, 1).otherwise(0)).alias("n_train"),
                F.sum(F.when(F.col("anio") >  ANIO_CORTE_TRAIN, 1).otherwise(0)).alias("n_test"),
                F.countDistinct("seg2").alias("k_seg2"),
                F.countDistinct("fam4").alias("k_fam4"),
                F.sum("_n_rep").alias("n_lineas_representadas"),
            ).collect()[0]

            print("\n" + "=" * 78)
            print("### 18.D.1 · TABLA DE ENTRENAMIENTO (deduplicada por texto)")
            print("=" * 78)
            print(f"  textos distintos            : {_res['n']:>12,}")
            print(f"  · lineas que representan    : {_res['n_lineas_representadas']:>12,}  (factor {(_res['n_lineas_representadas'] / max(_res['n'],1)):.1f}x)")
            print(f"  train (año <= {ANIO_CORTE_TRAIN})        : {_res['n_train']:>12,}")
            print(f"  test  (año >= {ANIO_CORTE_TRAIN + 1})        : {_res['n_test']:>12,}")
            print(f"  clases segmento (2 digitos) : {_res['k_seg2']:>12,}")
            print(f"  clases familia  (4 digitos) : {_res['k_fam4']:>12,}")

            if _res["n_train"] < 1000 or _res["n_test"] < 200:
                print("\n  FRENO 18.D.1: el split por año dejo un lado casi vacio.")
                print("  Revisar ANIO_CORTE_TRAIN antes de entrenar. NO se entrena.")
            elif _res["k_seg2"] < MIN_CLASES:
                print(f"\n  FRENO 18.D.1: solo {_res['k_seg2']} clases de segmento. Algo esta mal.")
            else:
                # =============================================================
                # 18.E · EL PIPELINE (Clase 10) + metricas honestas
                # =============================================================
                _stop_es = StopWordsRemover.loadDefaultStopWords("spanish")

                def _construye_pipeline(col_label, modelo):
                    tok = RegexTokenizer(inputCol="txt", outputCol="_tok",
                                         pattern=r"\W+", minTokenLength=2)
                    swr = StopWordsRemover(inputCol="_tok", outputCol="_tok2",
                                           stopWords=_stop_es)
                    cv  = CountVectorizer(inputCol="_tok2", outputCol="_tf",
                                          vocabSize=VOCAB_SIZE, minDF=MIN_DF)
                    idf = IDF(inputCol="_tf", outputCol="features")
                    idx = StringIndexer(inputCol=col_label, outputCol="label",
                                        handleInvalid="keep")
                    return Pipeline(stages=[tok, swr, cv, idf, idx, modelo])

                def _macro_micro(pred_df):
                    """Matriz de confusion agregada -> (acc, macro-F1, weighted-F1).

                    Se calcula en el DRIVER, sobre unas pocas miles de filas.
                    Spark llama "f1" al F1 PONDERADO: aqui se calculan los dos y
                    se imprimen los dos, porque sobre cola larga no se parecen.
                    """
                    filas = (pred_df.groupBy("label", "prediction")
                             .agg(F.count(F.lit(1)).alias("n")).collect())
                    tp, fp, fn, sop = {}, {}, {}, {}
                    clases = set()
                    total = 0
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
                    # OJO: se promedia sobre la UNION de clases verdaderas y
                    # predichas, no solo las verdaderas. Una clase que el modelo
                    # predice y que nunca es correcta tiene F1=0 y DEBE bajar el
                    # macro. Promediar solo sobre las verdaderas infla la cifra;
                    # es el mismo criterio que usa sklearn por defecto.
                    f1s = {}
                    for y in clases:
                        _tp, _fp, _fn = tp.get(y, 0), fp.get(y, 0), fn.get(y, 0)
                        prec = _tp / (_tp + _fp) if (_tp + _fp) else 0.0
                        rec  = _tp / (_tp + _fn) if (_tp + _fn) else 0.0
                        f1s[y] = (2 * prec * rec / (prec + rec)) if (prec + rec) else 0.0
                    macro = sum(f1s.values()) / len(f1s) if f1s else 0.0
                    # el ponderado pesa por soporte REAL (sop), asi que las
                    # clases nunca-verdaderas aportan 0 peso: es lo correcto.
                    wtd = (sum(f1s[y] * sop.get(y, 0) for y in f1s) / total) if total else 0.0
                    return acc, macro, wtd, len(sop), total

                def _baseline_mayoritaria(train_df, test_df, col_label):
                    """Predecir SIEMPRE la clase mas frecuente del train.

                    Sin esto, un accuracy no significa nada: si el 40% de las
                    lineas son del segmento 43, acertar el 45% no es aprender.
                    """
                    top = (train_df.groupBy(col_label)
                           .agg(F.count(F.lit(1)).alias("n"))
                           .orderBy(F.col("n").desc()).limit(1).collect())
                    if not top:
                        return None, 0.0
                    clase = top[0][col_label]
                    r = test_df.agg(
                        F.count(F.lit(1)).alias("n"),
                        F.sum(F.when(F.col(col_label) == clase, 1).otherwise(0)).alias("ok"),
                    ).collect()[0]
                    return clase, (100.0 * r["ok"] / r["n"] if r["n"] else 0.0)

                _train = _tab.filter(F.col("anio") <= ANIO_CORTE_TRAIN)
                _test  = _tab.filter(F.col("anio") >  ANIO_CORTE_TRAIN)

                _resultados = []

                def _corre(col_label, etiqueta_humana, nombre_modelo, modelo):
                    print("\n" + "-" * 78)
                    print(f"  entrenando {nombre_modelo} · objetivo = {etiqueta_humana}")
                    print("-" * 78)
                    pipe = _construye_pipeline(col_label, modelo)
                    m = pipe.fit(_train)
                    pr = m.transform(_test)
                    acc, macro, wtd, k, n = _macro_micro(pr)
                    clase_b, acc_b = _baseline_mayoritaria(_train, _test, col_label)
                    _resultados.append(dict(objetivo=etiqueta_humana, modelo=nombre_modelo,
                                            acc=acc, macro=macro, wtd=wtd, k=k, n=n,
                                            base_clase=clase_b, base_acc=acc_b))
                    print(f"    n test            : {n:,} textos · {k} clases vistas")
                    print(f"    accuracy          : {acc:6.2f}%")
                    print(f"    BASELINE mayoritaria (clase {clase_b}) : {acc_b:6.2f}%")
                    print(f"    ganancia sobre baseline               : {acc - acc_b:+6.2f} pp")
                    print(f"    macro-F1          : {macro:6.4f}   <- la cifra honesta")
                    print(f"    F1 ponderado      : {wtd:6.4f}   (lo que Spark llama 'f1')")
                    return m

                # --- SEGMENTO (2 digitos) — NaiveBayes primero, es de 1 pasada -
                print("\n" + "=" * 78)
                print("### 18.F · SEGMENTO (2 digitos)")
                print("=" * 78)
                _corre("seg2", "segmento (2 dig)", "NaiveBayes",
                       NaiveBayes(smoothing=1.0, modelType="multinomial"))
                if CORRE_LOGREG:
                    _corre("seg2", "segmento (2 dig)", "LogisticRegression",
                           LogisticRegression(maxIter=LR_MAX_ITER, regParam=LR_REG_PARAM,
                                              family="multinomial"))

                # --- FAMILIA (4 digitos) — solo si se pidio explicitamente -----
                if CORRE_FAMILIA4:
                    print("\n" + "=" * 78)
                    print("### 18.G · FAMILIA (4 digitos)")
                    print("=" * 78)
                    _corre("fam4", "familia (4 dig)", "NaiveBayes",
                           NaiveBayes(smoothing=1.0, modelType="multinomial"))
                    if CORRE_LOGREG:
                        _corre("fam4", "familia (4 dig)", "LogisticRegression",
                               LogisticRegression(maxIter=LR_MAX_ITER, regParam=LR_REG_PARAM,
                                                  family="multinomial"))
                else:
                    print("\n### 18.G · FAMILIA (4 digitos) — NO se corrio.")
                    print("    CORRE_FAMILIA4 = False a proposito. Mirar el resultado de")
                    print("    segmento primero; si convence, poner True y volver a correr")
                    print("    SOLO esta celda. Asi el cupo se gasta en dos decisiones, no")
                    print("    en una apuesta.")

                # =============================================================
                # 18.H · TABLA FINAL + COMO SE CITA
                # =============================================================
                print("\n" + "=" * 78)
                print(f"### 18.H · RESUMEN · RUN_ID_N3 = {RUN_ID_N3}")
                print("=" * 78)
                print(f"  {'objetivo':<18} {'modelo':<20} {'acc':>8} {'base':>8} {'gan.':>8} {'macroF1':>9} {'wtdF1':>8}")
                for r in _resultados:
                    print(f"  {r['objetivo']:<18} {r['modelo']:<20} {r['acc']:>7.2f}% "
                          f"{r['base_acc']:>7.2f}% {r['acc'] - r['base_acc']:>+7.2f} "
                          f"{r['macro']:>9.4f} {r['wtd']:>8.4f}")

                print(f"\n  Referencia externa [DOC], NO resultado propio:")
                print(f"    CLiC-it 2023 (misma tarea, CPV europeo): acc {REF_CLICIT_ACC}% · "
                      f"macro-F1 {REF_CLICIT_MACRO}")
                print("    Re-verificar en fuente primaria antes de citarla (regla 2).")

                print("\n  COMO SE CITA ESTO (regla 1):")
                print(f"    · marca [MEDIDO], run_id = {RUN_ID_N3}, fecha de esta corrida")
                print(f"    · muestra: {_res['n']:,} textos distintos de nivel A estricto,")
                print(f"      no circulares, split por año (train <= {ANIO_CORTE_TRAIN})")
                print("    · SIEMPRE con el baseline al lado. Un accuracy solo, no.")
                print("    · SIEMPRE macro-F1, no solo accuracy: la cola larga se predice mal")
                print("      y el accuracy la esconde.")
                print("    · el sesgo de A_ESTRICTO (compras de un solo codigo) va declarado.")
                print("\n  QUE NO SE PUEDE DECIR CON ESTO:")
                print("    · que una linea en desacuerdo con el modelo este mal codificada.")
                print("      Es una ALERTA, y el macro-F1 dice cuanta desconfianza merece.")
                print("    · nada sobre las lineas SIN texto evaluable (18,13% del universo):")
                print("      este modelo no las ve, por construccion.")
                print(f"\nCAP-18 termina. RUN_ID_N3 = {RUN_ID_N3}")
