# =============================================================================
#  CAP14_oc030_vs_oferentes.py — E1 del estudio de asociacion
#  Capstone Big Data · Mercado Publico nacional 2017-2026
#
#  PREGUNTA: las licitaciones que terminan comprando algo DISTINTO a lo que
#  especificaron (discordancia ONU licitacion <-> orden de compra, la regla
#  OC-030 del modulo de cotejo), ¿son las que atrajeron MENOS oferentes?
#
#  ---------------------------------------------------------------------------
#  POR QUE ESTE ES E1 Y NO "LA" PREGUNTA
#  ---------------------------------------------------------------------------
#  La pregunta original era: ¿hay correlacion entre discordancia de codigos y
#  licitaciones DESIERTAS o CANCELADAS? Esa version, tal cual, NO es computable,
#  y la razon es de diseno, no de datos:
#
#      Una licitacion desierta o cancelada NUNCA genera orden de compra.
#      Entonces la variable "el codigo de la licitacion difiere del de la OC"
#      no esta DEFINIDA justo para el grupo que se quiere comparar. Predictor y
#      desenlace viven en conjuntos disjuntos. Es un colisionador: se estaria
#      comparando adjudicadas contra adjudicadas.
#
#  Se parte en dos estudios, y los dos son legitimos:
#    · E1 (ESTA CELDA): solo adjudicadas con OC. Predictor = discordancia ONU.
#      Desenlace = numero de oferentes / oferente unico. Corre HOY, con los
#      datos congelados, sin re-descarga.
#    · E2 (otra celda, otro dia): todas las licitaciones. Predictor = calidad
#      del codigo DENTRO de la licitacion (generico, ausente, incoherente con
#      el texto segun el tamiz de CAP-11/CAP-12). Desenlace = desierta /
#      cancelada / oferentes. E2 NO es valido sin la re-descarga: en el corte
#      viejo, "no adjudicada" y "todavia no adjudicada a la fecha de descarga"
#      son indistinguibles (ver `auditoria_vintage.py`, bloque V-2).
#
#  ---------------------------------------------------------------------------
#  QUE SE PUEDE Y QUE NO SE PUEDE DECIR CON EL RESULTADO
#  ---------------------------------------------------------------------------
#  ASOCIACION, NUNCA EFECTO. La causalidad inversa es igual de plausible: un
#  unico oferente tiene mas margen para que lo que finalmente se compra termine
#  siendo otra cosa. Con corte transversal las dos direcciones no se separan.
#  La palabra correcta en el informe es "asociada a", no "provoca" ni "explica".
#
#  ---------------------------------------------------------------------------
#  COSTO Y SEGURIDAD DE EJECUCION (Databricks Free Edition, cupo diario)
#  ---------------------------------------------------------------------------
#  · UNA (1) sola accion de Spark en toda la celda: un unico .collect() sobre la
#    tabla de contingencia agregada. Todo lo demas (odds ratios, Mantel-Haenszel,
#    placebo, impresion) se calcula en el driver sobre unas pocas miles de filas.
#  · SIN .persist() / .cache() / .unpersist() (regla §1.14).
#  · SIN .show() sobre DataFrames grandes.
#  · El placebo viaja DENTRO del mismo groupBy, para no gastar una segunda
#    accion. Duplica la cardinalidad de la tabla chica, no del trabajo de Spark.
#  · Lee `df_oc_linked` (16.9M filas), NO `df_oc_all` (51.4M): el estudio solo
#    tiene sentido sobre lineas con licitacion de origen.
#  · Guarda de dependencias: si falta un DataFrame o una columna, ABORTA
#    imprimiendo lo que hay. No inventa sustitutos (leccion de la sesion 7).
#
#  ---------------------------------------------------------------------------
#  ESTADO DE VERIFICACION — LEER ANTES DE CORRER (regla §1.15)
#  ---------------------------------------------------------------------------
#  · La matematica del driver (Mantel-Haenszel, IC de Robins-Breslow-Greenland,
#    descarte de estratos, veredicto contra los criterios predeclarados) SI se
#    valido: 9 escenarios, incluidos paradoja de Simpson, estratos chicos,
#    estratos sin variacion, celdas en cero, direccion inversa y lista vacia. El
#    IC coincide con Woolf al 4to decimal en el caso de un solo estrato.
#    -> `test_cap14_mh.py`, incluido en el paquete. Corre en 1 segundo, sin Spark.
#  · Las expresiones de SPARK (el join, los collect_set, los prefijos, el
#    groupBy) NO se pudieron validar en PySpark local: el entorno donde se
#    escribio esta celda no pudo instalar pyspark. Quedan `[POR VERIFICAR]`.
#    Consecuencia practica: la PRIMERA corrida de esta celda es una prueba, no
#    un resultado. Correrla sola, mirar 14.C.1 antes que nada, y si el numero de
#    licitaciones pareadas no se parece a las 964.171 de la Pregunta 1, parar y
#    revisar el join ANTES de gastar mas cupo.
#
#  CORRER DESPUES DE: CAP-0 y CAP-0-BIS (definen RUN_ID_CAP, cap_resolver,
#  cap_global, cap_num, cap_pct) y de las celdas 1-8 de la Etapa 2.
#
#  ADVERTENCIA DE LECTURA (§1.16): ningun veredicto impreso aqui es una
#  conclusion. Cada uno viene con su tabla de magnitudes al lado y hay que
#  releerlo contra ella antes de copiarlo a ninguna parte.
# =============================================================================

# %% ==========================================================================
# CAP-14 · E1 — DISCORDANCIA ONU (OC-030) vs. NUMERO DE OFERENTES
# =============================================================================

import math

from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------------------------------------------------------
# CRITERIOS PREDECLARADOS — se fijan ANTES de mirar el resultado (§1.17)
# -----------------------------------------------------------------------------
# 1. Se declara ASOCIACION solo si el odds ratio ajustado de Mantel-Haenszel
#    (OR_MH) se aleja de 1 mas alla de este umbral Y su intervalo de confianza
#    del 95% no contiene 1.
UMBRAL_OR = 1.20          # o su reciproco 1/1.20 = 0.833 en la direccion opuesta

# 2. El PLACEBO debe dar un OR_MH DENTRO de [1/1.10, 1.10]. Si el placebo se
#    sale de ahi, el diseno esta roto y el resultado principal NO se cita —
#    la misma disciplina que descarto CAP-12B.
UMBRAL_PLACEBO = 1.10

# 3. El OR crudo y el OR ajustado deben conservar el SIGNO (los dos > 1 o los
#    dos < 1). Si cambian de signo al estratificar, es Simpson y se reporta el
#    ajustado explicando el cambio, nunca solo el crudo.

# 4. Estratos con menos de este numero de licitaciones se excluyen del pooling
#    (aportan ruido, no informacion) y se reportan aparte.
MIN_LIC_POR_ESTRATO = 30

# 5. "Oferente unico" = numero_oferentes <= 1. Es el corte del catalogo de Open
#    Contracting, no una eleccion de este proyecto.
CORTE_OFERENTE_UNICO = 1

RUN_ID_14 = cap_global("RUN_ID_CAP", "SIN_RUN_ID")

print("=" * 78)
print("CAP-14 · E1 — discordancia ONU (OC-030) vs. numero de oferentes")
print("RUN_ID_CAP = %s" % RUN_ID_14)
print("=" * 78)

# -----------------------------------------------------------------------------
# 14.A · GUARDA DE DEPENDENCIAS Y RESOLUCION DE COLUMNAS
# -----------------------------------------------------------------------------
_oc = cap_global("df_oc_linked") or cap_global("df_oc_all")
_lic = cap_global("df_lic_all")

_ABORTA = False
if _oc is None:
    print("ABORTA: falta df_oc_linked / df_oc_all en el kernel.")
    _ABORTA = True
if _lic is None:
    print("ABORTA: falta df_lic_all en el kernel.")
    _ABORTA = True

if not _ABORTA:
    c_key_oc = cap_resolver(_oc, ["codigo_licitacion", "_lic_key"], "clave lic en OC")
    c_onu_oc = cap_resolver(_oc, ["onu_oc", "codigo_producto_onu",
                                  "CodigoProductoONU"], "ONU (OC)")
    c_sector = cap_resolver(_oc, ["_sinclasificar_sector", "sector"], "sector")
    c_anio_oc = cap_resolver(_oc, ["_year", "anio", "year"], "anio (OC)")

    c_key_lic = cap_resolver(_lic, ["codigo_externo", "CodigoExterno"], "clave lic")
    c_onu_lic = cap_resolver(_lic, ["onu_lic", "onu", "CodigoProductoONU"], "ONU (lic)")
    c_ofer = cap_resolver(_lic, ["numero_oferentes", "NumeroOferentes"], "n oferentes")
    c_monto = cap_resolver(_lic, ["_sinclasificar_MontoEstimado", "MontoEstimado"],
                           "monto estimado")

    _faltan = [n for n, v in [("clave lic en OC", c_key_oc), ("ONU OC", c_onu_oc),
                              ("clave lic", c_key_lic), ("ONU lic", c_onu_lic),
                              ("numero_oferentes", c_ofer)] if v is None]
    if _faltan:
        print("\nABORTA: no se resolvieron estas columnas: %s" % ", ".join(_faltan))
        print("Columnas de OC : %s" % ", ".join(_oc.columns[:40]))
        print("Columnas de LIC: %s" % ", ".join(_lic.columns[:40]))
        _ABORTA = True
    else:
        print("\n  resueltas -> OC: key=%s onu=%s sector=%s anio=%s"
              % (c_key_oc, c_onu_oc, c_sector, c_anio_oc))
        print("               LIC: key=%s onu=%s oferentes=%s monto=%s"
              % (c_key_lic, c_onu_lic, c_ofer, c_monto))

# -----------------------------------------------------------------------------
# 14.B · CONSTRUCCION — todo perezoso, sin ninguna accion todavia
# -----------------------------------------------------------------------------
if not _ABORTA:

    def _k(col):
        """Normaliza la clave de licitacion en los DOS lados. 6D-3 probo que el
        formato NO explica las ausencias, pero normalizar igual es gratis y
        evita reintroducir el bug del join de la Celda 6 (que comparaba con ==
        exacto y sin trim del lado de la OC)."""
        return F.upper(F.trim(col.cast("string")))

    def _onu8(col):
        """Codigo ONU a 8 digitos, solo digitos. Devuelve NULL si no tiene 8."""
        s = F.regexp_replace(F.trim(col.cast("string")), r"\D", "")
        return F.when(F.length(s) == 8, s).otherwise(F.lit(None))

    # --- lado OC: un registro por (licitacion, codigo ONU distinto) -----------
    oc = (_oc
          .select(_k(F.col(c_key_oc)).alias("lic_key"),
                  _onu8(F.col(c_onu_oc)).alias("onu8"),
                  (F.col(c_sector) if c_sector else F.lit(None)).alias("sector"),
                  (F.col(c_anio_oc) if c_anio_oc else F.lit(None)).alias("anio"))
          .filter(F.col("lic_key").isNotNull() & (F.col("lic_key") != "")))

    oc_agg = (oc.groupBy("lic_key")
              .agg(F.collect_set("onu8").alias("set_onu_oc"),
                   F.max("sector").alias("sector"),
                   F.min("anio").alias("anio_oc"),
                   F.count(F.lit(1)).alias("n_lineas_oc")))

    # --- lado LIC: un registro por licitacion --------------------------------
    lic = (_lic
           .select(_k(F.col(c_key_lic)).alias("lic_key"),
                   _onu8(F.col(c_onu_lic)).alias("onu8"),
                   cap_num(F.col(c_ofer)).alias("n_oferentes"),
                   (cap_num(F.col(c_monto)) if c_monto else F.lit(None).cast("double")
                    ).alias("monto_est"))
           .filter(F.col("lic_key").isNotNull() & (F.col("lic_key") != "")))

    lic_agg = (lic.groupBy("lic_key")
               .agg(F.collect_set("onu8").alias("set_onu_lic"),
                    # `numero_oferentes` se repite en todas las filas de la misma
                    # licitacion (df_lic_all trae una fila por linea Y por oferta);
                    # max() es un dedup seguro, no una agregacion con significado.
                    F.max("n_oferentes").alias("n_oferentes"),
                    F.max("monto_est").alias("monto_est")))

    par = oc_agg.join(lic_agg, "lic_key", "inner")

    # --- predictor: nivel de coincidencia entre los dos conjuntos de codigos --
    # Se mide en el prefijo mas profundo compartido, que es lo que en UNSPSC
    # separa "el mismo producto" de "la misma familia" de "otro rubro":
    #   8 = codigo identico          6 = misma clase        4 = misma familia
    #   2 = mismo segmento           0 = sin nada en comun
    # Se toma el MEJOR emparejamiento posible: si algun codigo de la OC calza a
    # 8 digitos con alguno de la licitacion, el nivel es 8. Es la version
    # CONSERVADORA (favorece declarar concordancia), a proposito.
    def _prefijos(col_set, n):
        return F.array_distinct(F.transform(
            F.filter(col_set, lambda x: x.isNotNull()),
            lambda x: F.substring(x, 1, n)))

    for n in (8, 6, 4, 2):
        par = par.withColumn("_p_oc_%d" % n, _prefijos(F.col("set_onu_oc"), n))
        par = par.withColumn("_p_li_%d" % n, _prefijos(F.col("set_onu_lic"), n))

    par = par.withColumn(
        "nivel",
        F.when(F.size(F.array_intersect("_p_oc_8", "_p_li_8")) > 0, F.lit(8))
         .when(F.size(F.array_intersect("_p_oc_6", "_p_li_6")) > 0, F.lit(6))
         .when(F.size(F.array_intersect("_p_oc_4", "_p_li_4")) > 0, F.lit(4))
         .when(F.size(F.array_intersect("_p_oc_2", "_p_li_2")) > 0, F.lit(2))
         .otherwise(F.lit(0)))

    # Universo informativo: las dos partes tienen que tener AL MENOS un codigo
    # ONU valido, si no el "nivel 0" seria ausencia de dato, no discordancia.
    # Es la misma distincion que en la Pregunta 1 separa el universo A/B/C.
    par = par.withColumn(
        "evaluable",
        (F.size(F.filter("set_onu_oc", lambda x: x.isNotNull())) > 0) &
        (F.size(F.filter("set_onu_lic", lambda x: x.isNotNull())) > 0))

    # Predictor binario: "muy diferente" = ni siquiera comparten el segmento.
    par = par.withColumn("discordante",
                         F.when(F.col("nivel") == 0, F.lit(1)).otherwise(F.lit(0)))

    # --- desenlace ------------------------------------------------------------
    par = par.withColumn(
        "oferente_unico",
        F.when(F.col("n_oferentes").isNull(), F.lit(None).cast("int"))
         .when(F.col("n_oferentes") <= F.lit(CORTE_OFERENTE_UNICO), F.lit(1))
         .otherwise(F.lit(0)))

    # --- estratos de control --------------------------------------------------
    # Sin estos, se mide "salud compra distinto que municipalidades", no lo que
    # se cree medir.
    par = par.withColumn("e_sector", F.coalesce(F.col("sector"), F.lit("(sin sector)")))
    par = par.withColumn("e_anio", F.coalesce(F.col("anio_oc").cast("string"),
                                              F.lit("(sin anio)")))
    par = par.withColumn(
        "e_monto",
        F.when(F.col("monto_est").isNull(), F.lit("0 sin monto"))
         .when(F.col("monto_est") < 100, F.lit("1 <100 UTM"))
         .when(F.col("monto_est") < 1000, F.lit("2 100-1k"))
         .when(F.col("monto_est") < 5000, F.lit("3 1k-5k"))
         .otherwise(F.lit("4 >=5k UTM")))

    # --- PLACEBO --------------------------------------------------------------
    # Un predictor que NO puede tener relacion causal con el numero de oferentes:
    # la paridad del hash de la clave de licitacion. Viaja en el MISMO groupBy
    # para no gastar una segunda accion de Spark. Si este da una asociacion,
    # el diseno esta roto.
    par = par.withColumn("placebo", (F.abs(F.hash(F.col("lic_key"))) % 2).cast("int"))

    # --- TABLA DE CONTINGENCIA (la unica accion de Spark de la celda) --------
    contingencia = (par
                    .filter(F.col("evaluable") & F.col("oferente_unico").isNotNull())
                    .groupBy("e_sector", "e_anio", "e_monto",
                             "nivel", "discordante", "placebo", "oferente_unico")
                    .agg(F.count(F.lit(1)).alias("n"),
                         F.sum("n_oferentes").alias("suma_oferentes")))

    print("\n  Lanzando la UNICA accion de Spark de la celda (collect de la tabla")
    print("  de contingencia agregada)...")
    filas = contingencia.collect()        # <<<< ACCION DE SPARK Nº 1 (y unica)
    print("  ...lista. %d celdas de contingencia recibidas en el driver.\n" % len(filas))

# -----------------------------------------------------------------------------
# 14.C · ANALISIS EN EL DRIVER — cero Spark de aqui en adelante
# -----------------------------------------------------------------------------
if not _ABORTA and filas:

    def _mh(celdas, clave_pred):
        """Mantel-Haenszel sobre una lista de dicts con
        (estrato, pred, desenlace, n). Devuelve (OR_MH, ic_lo, ic_hi, tablas).

        MH es el ajuste correcto aqui y no una regresion logistica porque:
        (1) no gasta una segunda accion de Spark — se calcula sobre la tabla ya
        colectada; (2) es transparente de auditar celda por celda en la defensa;
        (3) con predictor y desenlace binarios no pierde nada frente a una
        logistica sin interacciones."""
        estr = {}
        for c in celdas:
            k = (c["e_sector"], c["e_anio"], c["e_monto"])
            t = estr.setdefault(k, {(0, 0): 0, (0, 1): 0, (1, 0): 0, (1, 1): 0})
            t[(c[clave_pred], c["oferente_unico"])] += c["n"]

        num = den = 0.0
        usados = descartados = 0
        detalle = []
        for k, t in sorted(estr.items()):
            a = t[(1, 1)]   # discordante  & oferente unico
            b = t[(1, 0)]   # discordante  & varios oferentes
            c_ = t[(0, 1)]  # concordante  & oferente unico
            d = t[(0, 0)]   # concordante  & varios oferentes
            N = a + b + c_ + d
            if N < MIN_LIC_POR_ESTRATO or (a + b) == 0 or (c_ + d) == 0:
                descartados += 1
                continue
            usados += 1
            num += (a * d) / float(N)
            den += (b * c_) / float(N)
            # varianza de Robins-Breslow-Greenland para el IC del log(OR_MH)
            P = (a + d) / float(N)
            Q = (b + c_) / float(N)
            R = (a * d) / float(N)
            S = (b * c_) / float(N)
            detalle.append((k, a, b, c_, d, N, R, S, P, Q))
        if den <= 0 or num <= 0:
            return None, None, None, detalle, usados, descartados
        OR = num / den
        sR = sum(x[6] for x in detalle)
        sS = sum(x[7] for x in detalle)
        t1 = sum(x[8] * x[6] for x in detalle) / (2 * sR * sR) if sR else 0.0
        t2 = sum(x[8] * x[7] + x[9] * x[6] for x in detalle) / (2 * sR * sS) if sR and sS else 0.0
        t3 = sum(x[9] * x[7] for x in detalle) / (2 * sS * sS) if sS else 0.0
        var = t1 + t2 + t3
        if var <= 0:
            return OR, None, None, detalle, usados, descartados
        se = math.sqrt(var)
        return (OR, math.exp(math.log(OR) - 1.96 * se),
                math.exp(math.log(OR) + 1.96 * se), detalle, usados, descartados)

    celdas = [r.asDict() for r in filas]
    total_lic = sum(c["n"] for c in celdas)

    # --- 14.C.1 · magnitudes primero, veredicto despues (§1.16) --------------
    print("=" * 78)
    print("14.C.1 · DISTRIBUCION DEL PREDICTOR — el universo antes de la tasa")
    print("=" * 78)
    por_nivel = {}
    for c in celdas:
        d = por_nivel.setdefault(c["nivel"], {"n": 0, "unico": 0, "sum_of": 0.0})
        d["n"] += c["n"]
        d["unico"] += c["n"] if c["oferente_unico"] == 1 else 0
        d["sum_of"] += (c["suma_oferentes"] or 0)
    ET = {8: "8 dig · codigo identico", 6: "6 dig · misma clase",
          4: "4 dig · misma familia", 2: "2 dig · mismo segmento",
          0: "0 dig · SIN NADA EN COMUN"}
    print("  %-28s %12s %9s %14s %12s" %
          ("nivel de coincidencia", "licitac.", "% univ.", "% of. unico", "of. medio"))
    print("  " + "-" * 76)
    for nv in (8, 6, 4, 2, 0):
        d = por_nivel.get(nv)
        if not d:
            print("  %-28s %12s %9s %14s %12s" % (ET[nv], 0, "-", "-", "-"))
            continue
        print("  %-28s %12s %8.2f%% %13.2f%% %12.2f" %
              (ET[nv], "{:,}".format(d["n"]).replace(",", "."),
               100.0 * d["n"] / total_lic,
               100.0 * d["unico"] / d["n"] if d["n"] else 0,
               d["sum_of"] / d["n"] if d["n"] else 0))
    print("  " + "-" * 76)
    print("  TOTAL evaluable: %s licitaciones pareadas con oferentes conocidos"
          % "{:,}".format(total_lic).replace(",", "."))
    print("\n  LEER PRIMERO ESTA TABLA. Si el nivel 0 es una fraccion minuscula del")
    print("  universo, cualquier odds ratio que salga abajo describe una esquina")
    print("  del problema, no el problema — es la misma leccion de la Pregunta 1,")
    print("  donde el titular resulto ser la exclusion (80,08%) y no la tasa.")

    # --- 14.C.2 · crudo -------------------------------------------------------
    a = sum(c["n"] for c in celdas if c["discordante"] == 1 and c["oferente_unico"] == 1)
    b = sum(c["n"] for c in celdas if c["discordante"] == 1 and c["oferente_unico"] == 0)
    c_ = sum(c["n"] for c in celdas if c["discordante"] == 0 and c["oferente_unico"] == 1)
    d = sum(c["n"] for c in celdas if c["discordante"] == 0 and c["oferente_unico"] == 0)
    print("\n" + "=" * 78)
    print("14.C.2 · TABLA 2x2 CRUDA (sin controles — NO citar sola)")
    print("=" * 78)
    print("  %-24s %14s %14s %12s" % ("", "of. unico", "≥2 oferentes", "% unico"))
    print("  %-24s %14s %14s %11.2f%%" %
          ("discordante (nivel 0)", "{:,}".format(a).replace(",", "."),
           "{:,}".format(b).replace(",", "."), 100.0 * a / (a + b) if (a + b) else 0))
    print("  %-24s %14s %14s %11.2f%%" %
          ("concordante (nivel>0)", "{:,}".format(c_).replace(",", "."),
           "{:,}".format(d).replace(",", "."), 100.0 * c_ / (c_ + d) if (c_ + d) else 0))
    or_crudo = ((a * d) / float(b * c_)) if (b and c_) else None
    print("\n  OR crudo = %s" % ("%.4f" % or_crudo if or_crudo else "no calculable"))

    # --- 14.C.3 · ajustado por sector x anio x tramo de monto ----------------
    OR, lo, hi, det, usados, desc = _mh(celdas, "discordante")
    print("\n" + "=" * 78)
    print("14.C.3 · ODDS RATIO AJUSTADO (Mantel-Haenszel)")
    print("        estratos = sector x anio x tramo de monto estimado")
    print("=" * 78)
    print("  estratos usados      : %d   (con >= %d licitaciones)"
          % (usados, MIN_LIC_POR_ESTRATO))
    print("  estratos descartados : %d   (chicos o sin variacion en el predictor)" % desc)
    if OR is None:
        print("\n  [NO CORRIBLE] no hay estratos con variacion suficiente.")
    else:
        print("\n  OR_MH = %.4f" % OR)
        if lo:
            print("  IC 95%% = [%.4f , %.4f]   (Robins-Breslow-Greenland)" % (lo, hi))
        print("\n  Lectura: OR > 1 significa que, DENTRO del mismo sector, mismo anio y")
        print("  mismo tramo de monto, las licitaciones cuyo codigo ONU no comparte ni")
        print("  el segmento con el de su orden de compra tienen mas probabilidad de")
        print("  haber tenido un solo oferente.")

    # --- 14.C.4 · placebo -----------------------------------------------------
    ORp, lop, hip, _detp, usp, descp = _mh(celdas, "placebo")
    print("\n" + "=" * 78)
    print("14.C.4 · PLACEBO — paridad del hash de la clave de licitacion")
    print("=" * 78)
    print("  Un predictor sin ninguna relacion posible con el numero de oferentes.")
    print("  Corre por la MISMA maquinaria y sobre los MISMOS estratos.")
    if ORp is None:
        print("\n  [NO CORRIBLE] el placebo no se pudo estimar.")
    else:
        print("\n  OR_MH placebo = %.4f%s" %
              (ORp, ("   IC 95%% = [%.4f , %.4f]" % (lop, hip)) if lop else ""))

    # --- 14.C.5 · VEREDICTO contra los criterios predeclarados ---------------
    print("\n" + "=" * 78)
    print("14.C.5 · VEREDICTO contra los criterios PREDECLARADOS")
    print("=" * 78)
    print("  criterio 1 · |OR_MH| supera %.2f (o 1/%.2f) y su IC excluye 1"
          % (UMBRAL_OR, UMBRAL_OR))
    print("  criterio 2 · OR_MH del placebo dentro de [%.4f , %.2f]"
          % (1.0 / UMBRAL_PLACEBO, UMBRAL_PLACEBO))
    print("  criterio 3 · OR crudo y OR ajustado conservan el signo")
    print("  " + "-" * 74)

    ok_placebo = (ORp is not None and
                  (1.0 / UMBRAL_PLACEBO) <= ORp <= UMBRAL_PLACEBO)
    ok_magnitud = (OR is not None and (OR >= UMBRAL_OR or OR <= 1.0 / UMBRAL_OR))
    ok_ic = (OR is not None and lo is not None and not (lo <= 1.0 <= hi))
    ok_signo = (OR is not None and or_crudo is not None and
                ((OR > 1) == (or_crudo > 1)))

    for et, ok in [("placebo limpio", ok_placebo), ("magnitud suficiente", ok_magnitud),
                   ("IC excluye 1", ok_ic), ("signo estable", ok_signo)]:
        print("    [%s] %s" % ("OK" if ok else "NO", et))

    print("  " + "-" * 74)
    if not ok_placebo:
        print("  VEREDICTO: NO CITAR. El placebo se salio del rango, asi que la")
        print("  maquinaria produce asociaciones donde no puede haberlas. El problema")
        print("  esta en el diseno (estratos, dedup o clave), no en los datos.")
    elif ok_magnitud and ok_ic and ok_signo:
        print("  VEREDICTO: ASOCIACION SOSTENIDA. Escribirla como asociacion, con la")
        print("  tabla de 14.C.1 al lado y la salvedad de causalidad inversa en la")
        print("  misma frase. NUNCA como efecto.")
    elif ok_placebo and not ok_magnitud:
        print("  VEREDICTO: SIN ASOCIACION RELEVANTE. Esto es un resultado, no un")
        print("  fracaso: dice que la discordancia de codigo NO marca las licitaciones")
        print("  de un solo oferente, y por lo tanto que OC-030 mide un problema de")
        print("  registro y no un sintoma de baja competencia. Es citable tal cual.")
    else:
        print("  VEREDICTO: NO CONCLUYENTE. Reportar los numeros y decir que no")
        print("  alcanzan el criterio predeclarado. No reescribir el criterio.")

    print("\n  RECORDATORIO OBLIGATORIO PARA EL INFORME:")
    print("  · Esto es E1: SOLO licitaciones adjudicadas que generaron OC. Las")
    print("    desiertas y canceladas estan fuera POR CONSTRUCCION, porque sin OC")
    print("    el predictor no existe. La pregunta sobre desiertas es E2 y necesita")
    print("    la re-descarga (ver auditoria_vintage.py, bloque V-2).")
    print("  · `numero_oferentes` viene del corte descargado. Si una licitacion fue")
    print("    adjudicada DESPUES de la fecha de descarga, su numero de oferentes")
    print("    puede venir vacio, y esas licitaciones salen del denominador. Esa")
    print("    exclusion NO es aleatoria: la cota esta en el bloque V-2.")
    print("\nCAP-14 termina. RUN_ID_CAP=%s" % RUN_ID_14)

elif not _ABORTA:
    print("ABORTA: la tabla de contingencia vino vacia. Revisar el join y el")
    print("filtro `evaluable` antes de volver a gastar cupo.")
