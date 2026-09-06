# -*- coding: utf-8 -*-
# =============================================================================
# EXPORTAR ESQUEMA DEL CORTE VIEJO  —  celda de Databricks, CERO acciones de Spark
# Capstone Big Data · sesion 25 (28-ago-2026)
#
# `spark.read.parquet(...).schema` lee el footer del archivo: es metadato, no
# lanza job y no consume cupo.
#
# Produce `esquema_v1.json`, que es el CONTRATO que `preparar_corte_v2.py` tiene
# que cumplir en tu PC. Sin este archivo el script local no corre — a proposito:
# adivinar el esquema es exactamente el bug de la Celda 4 de v17 (F19 hallazgo 1),
# donde un nombre de columna que no existia se resolvio en silencio.
#
# Despues de correr esto: Catalog -> Volumes -> descargar esquema_v1.json al PC.
# =============================================================================

import json

VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"
RUTA_SALIDA = f"{VOLUMEN_BASE}/capstone_salidas/esquema_v1.json"

PARTICIONES = {
    "lic_historico": 2024,
    "oc_historico":  2024,
    "lic_vigente":   2026,
    "oc_vigente":    2026,
}

esquema = {}
for cat, anio in PARTICIONES.items():
    ruta = f"{VOLUMEN_BASE}/categoria={cat}/year={anio}"
    try:
        sch = spark.read.parquet(ruta).schema  # noqa: F821  (metadato, sin job)
    except Exception as e:                     # noqa: BLE001
        print(f"[aviso] no se pudo leer {ruta}: {type(e).__name__}: {e}")
        continue
    esquema[cat] = {
        "particion_muestreada": anio,
        "n_columnas": len(sch.fields),
        "columnas": [{"nombre": f.name,
                      "tipo": f.dataType.simpleString(),
                      "nullable": f.nullable} for f in sch.fields],
    }
    print(f"{cat:<16} {len(sch.fields):>4} columnas   (muestreado en year={anio})")

# Conteos de referencia del corte viejo, copiados de REFERENCIA_FILAS del
# notebook maestro v18 (Celda 2). Viajan con el esquema para que el script
# local pueda contrastar sin depender de que te acuerdes.
esquema["_referencia_filas_v1"] = {
    "2017": {"lic_historico": 3471351, "oc_historico": 6609983},
    "2018": {"lic_historico": 2976218, "oc_historico": 6614357},
    "2019": {"lic_historico": 2770490, "oc_historico": 6209651},
    "2020": {"lic_historico": 1885769, "oc_historico": 4408740},
    "2021": {"lic_historico": 1672960, "oc_historico": 4266941},
    "2022": {"lic_historico": 2161440, "oc_historico": 4728085},
    "2023": {"lic_historico": 2542605, "oc_historico": 5167807},
    "2024": {"lic_historico": 3070577, "oc_historico": 5344111},
    "2025": {"lic_historico": 4812550, "oc_historico": 5150919},
    "2026": {"lic_vigente":    948445, "oc_vigente":   2950934},
}

dbutils.fs.mkdirs(f"{VOLUMEN_BASE}/capstone_salidas".replace("/Volumes", "dbfs:/Volumes"))  # noqa: F821
with open(RUTA_SALIDA, "w", encoding="utf-8") as fh:
    json.dump(esquema, fh, ensure_ascii=False, indent=2)

print(f"\nEscrito: {RUTA_SALIDA}")
print("Descargalo al PC (Catalog -> Volumes) y pasaselo a preparar_corte_v2.py")
print("con --esquema-v1 esquema_v1.json")
