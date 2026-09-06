# -*- coding: utf-8 -*-
# =============================================================================
# PASO 0-E · INVENTARIO FISICO DEL VOLUMEN — CERO ACCIONES DE SPARK
# Capstone Big Data · Mercado Publico · sesion 25 (28-ago-2026)
#
# CELDA DE DATABRICKS. Pegar como celda nueva y correr. `dbutils.fs.ls` lista
# el directorio: es una llamada al sistema de archivos, NO un job de Spark. No
# consume cupo de escaneo. Se puede correr las veces que haga falta.
#
# -----------------------------------------------------------------------------
# QUE CONTESTA (F25 §2)
# -----------------------------------------------------------------------------
# El corte viejo tiene ~3 veces las filas que le corresponden entre 2024-11 y
# 2025-10: 44,0 filas por licitacion en la particion 2025 contra 17-18 en todo
# el resto del proyecto, en los dos cortes y en los cuatro anios medidos.
# Aislando los meses afectados el factor da 2,74x en 2024 y 3,04x en 2025 —
# dos calculos independientes, el mismo numero.
#
# Hay dos formas de que eso pase, y esta celda las distingue mirando el disco:
#
#   1) LA PARTICION TIENE MAS DE UN PARQUET. El notebook lee la carpeta entera,
#      asi que dos archivos = filas duplicadas. Se ve de inmediato abajo, y se
#      arregla borrando el sobrante (o leyendo un solo archivo).
#   2) HAY UN SOLO PARQUET Y YA VIENE INFLADO. Entonces la triplicacion entro
#      al construirlo, o sea que la lista de CSV de origen traia cada mes
#      varias veces. Eso NO se arregla en Databricks: se arregla reconstruyendo
#      la particion, y hay que revisar la carpeta de descarga en el PC.
#
# La regla del protocolo — UN archivo parquet por particion — es exactamente
# esto. Esta celda la verifica en las 40 particiones, no solo en la sospechosa.
# =============================================================================

VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"

CATEGORIAS = {
    "lic_historico": list(range(2017, 2026)),
    "oc_historico":  list(range(2017, 2026)),
    "lic_vigente":   [2026],
    "oc_vigente":    [2026],
}

# Filas declaradas en REFERENCIA_FILAS del notebook maestro v18 (Celda 2).
REFERENCIA_FILAS = {
    2017: {"lic_historico": 3471351, "oc_historico": 6609983},
    2018: {"lic_historico": 2976218, "oc_historico": 6614357},
    2019: {"lic_historico": 2770490, "oc_historico": 6209651},
    2020: {"lic_historico": 1885769, "oc_historico": 4408740},
    2021: {"lic_historico": 1672960, "oc_historico": 4266941},
    2022: {"lic_historico": 2161440, "oc_historico": 4728085},
    2023: {"lic_historico": 2542605, "oc_historico": 5167807},
    2024: {"lic_historico": 3070577, "oc_historico": 5344111},
    2025: {"lic_historico": 4812550, "oc_historico": 5150919},
    2026: {"lic_vigente":    948445, "oc_vigente":   2950934},
}

print("=" * 92)
print("PASO 0-E · INVENTARIO FISICO DEL VOLUMEN   (dbutils.fs.ls — cero acciones de Spark)")
print("=" * 92)
print(f"  base: {VOLUMEN_BASE}\n")
print(f"  {'particion':<34}{'parquets':>9}{'otros':>7}{'MB':>12}"
      f"{'filas ref':>13}{'bytes/fila':>12}")
print("  " + "-" * 90)

sospechosas, multiples, ausentes = [], [], []
resumen = {}

for cat, anios in CATEGORIAS.items():
    for anio in anios:
        ruta = f"{VOLUMEN_BASE}/categoria={cat}/year={anio}"
        try:
            items = dbutils.fs.ls(ruta)                     # noqa: F821
        except Exception as e:                              # noqa: BLE001
            print(f"  {cat + '/' + str(anio):<34}   [no se pudo listar: {type(e).__name__}]")
            ausentes.append((cat, anio))
            continue
        pq = [i for i in items if i.name.lower().endswith(".parquet")]
        otros = [i for i in items if not i.name.lower().endswith(".parquet")]
        mb = sum(i.size for i in pq) / 1e6
        ref = REFERENCIA_FILAS.get(anio, {}).get(cat)
        bpf = (sum(i.size for i in pq) / ref) if ref else None
        resumen[(cat, anio)] = (len(pq), mb, bpf)
        print(f"  {cat + '/' + str(anio):<34}{len(pq):>9}{len(otros):>7}{mb:>12,.1f}"
              f"{(f'{ref:,}' if ref else '-'):>13}"
              f"{(f'{bpf:,.1f}' if bpf else '-'):>12}")
        if len(pq) > 1:
            multiples.append((cat, anio, len(pq)))
            for i in pq:
                print(f"        -> {i.name:<52} {i.size/1e6:>10,.1f} MB")
        if len(pq) == 0:
            sospechosas.append((cat, anio, "sin parquet"))

# -----------------------------------------------------------------------------
# LECTURA
# -----------------------------------------------------------------------------
print("\n" + "=" * 92)
print("LECTURA")
print("=" * 92)

if multiples:
    print("\n  *** PARTICIONES CON MAS DE UN PARQUET — cada una duplica filas al leerse:")
    for cat, anio, n in multiples:
        print(f"        {cat}/year={anio}: {n} archivos")
    print("\n  Esta es la explicacion directa del x3 de F25 §2 si aparece year=2025.")
    print("  Antes de borrar nada: los archivos sobrantes son la unica copia de la")
    print("  descarga rodante 2024-11..2025-10, que trae ~11.402 licitaciones que la")
    print("  re-descarga NO tiene. Copiar fuera del Volumen antes de tocar.")
else:
    print("\n  Una sola parte por particion en todas. Entonces el x3 de F25 §2 NO es")
    print("  un archivo duplicado al lado: viene DENTRO del parquet, o sea que se")
    print("  construyo con la lista de CSV repetida. Se confirma en el PC con el")
    print("  inventario de la carpeta de descarga (F25 §6), y se arregla")
    print("  reconstruyendo esa particion, no borrando archivos aca.")

# bytes por fila: si una particion pesa por fila MUCHO menos que sus vecinas,
# es que declara mas filas de las que su tamano justifica -> filas repetidas
# comprimen muy bien y delatan la duplicacion.
lic = {a: v for (c, a), v in resumen.items() if c.startswith("lic") and v[2]}
if len(lic) >= 3:
    vals = sorted(v[2] for v in lic.values())
    med = vals[len(vals) // 2]
    print(f"\n  bytes por fila declarada (licitaciones) — mediana {med:,.1f}:")
    for anio in sorted(lic):
        bpf = lic[anio][2]
        marca = ""
        if bpf < 0.6 * med:
            marca = "  *** pesa muy poco por fila: filas repetidas comprimen"
        elif bpf > 1.6 * med:
            marca = "  *** pesa mucho por fila: revisar"
        print(f"      year={anio}: {bpf:>8,.1f}{marca}")
    print("\n  Las filas duplicadas casi no ocupan espacio (snappy las comprime muy")
    print("  bien), asi que una particion inflada se delata por pesar poco por fila")
    print("  DECLARADA. Es indicio, no prueba: la prueba es el conteo distinto.")

if ausentes:
    print(f"\n  particiones que no se pudieron listar: {ausentes}")

print("\nPASO 0-E termina. Cero acciones de Spark consumidas.")
print("Pegar esta salida completa en el chat.")
