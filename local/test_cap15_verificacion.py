# Verificacion real de las expresiones de Spark de CAP-15 contra un par de
# cortes sinteticos con verdad conocida, construidos en el layout del Volumen.
import os, shutil, random
from pyspark.sql import SparkSession, functions as F, types as T

BASE = "/tmp/sptest/volumen"
shutil.rmtree(BASE, ignore_errors=True)
spark = (SparkSession.builder.master("local[2]").appName("cap15")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

ESQ_LIC = T.StructType([
    T.StructField("_sinclasificar_CodigoExterno", T.StringType()),
    T.StructField("_sinclasificar_FechaAdjudicacion", T.StringType()),
    T.StructField("_sinclasificar_RutProveedor", T.StringType()),
    T.StructField("numero_oferentes", T.IntegerType()),
])
ESQ_OC = T.StructType([
    T.StructField("_sinclasificar_Codigo", T.StringType()),
    T.StructField("_sinclasificar_NroLicitacion", T.StringType()),
])

random.seed(11)
VERDAD = {}

def construir(anio, n_comun, n_solo_v1, n_solo_v2, n_gana, n_pierde, filas_extra_v2):
    """Verdad conocida por construccion. `n_gana` licitaciones comunes tienen el
    campo vacio en v1 y lleno en v2; `n_pierde` al reves (patologia)."""
    v1, v2 = [], []
    for i in range(n_comun):
        cod = f"C{anio}-{i}"
        gana = i < n_gana
        pierde = n_gana <= i < n_gana + n_pierde
        a1 = None if gana else "2024-05-01"
        a2 = None if pierde else "2024-05-01"
        # varias filas por licitacion, como el archivo real
        for _ in range(random.choice([1, 2])):
            v1.append((cod, a1, None if gana else "76.000.000-1", 3))
            v2.append((cod, a2, None if pierde else "76.000.000-1", 3))
    for i in range(n_solo_v1):
        v1.append((f"V1{anio}-{i}", "2023-01-01", "76.000.000-1", 2))
    for i in range(n_solo_v2):
        v2.append((f"V2{anio}-{i}", "2025-09-01", "77.000.000-2", 4))
    for i in range(filas_extra_v2):          # lineas nuevas de licitaciones comunes
        v2.append((f"C{anio}-0", "2024-05-01", "76.000.000-1", 3))
    cat = "lic_vigente" if anio == 2026 else "lic_historico"
    for corte, filas in (("", v1), ("_v2", v2)):
        (spark.createDataFrame(filas, ESQ_LIC).coalesce(1).write.mode("overwrite")
         .parquet(f"{BASE}/categoria={cat}{corte}/year={anio}"))
    ocs = [(f"OC{anio}-{i}", f"C{anio}-{i%max(n_comun,1)}") for i in range(50)]
    ocat = "oc_vigente" if anio == 2026 else "oc_historico"
    for corte, extra in (("", 0), ("_v2", 7)):
        (spark.createDataFrame(ocs + [(f"OCX{anio}-{j}", None) for j in range(extra)], ESQ_OC)
         .coalesce(1).write.mode("overwrite").parquet(f"{BASE}/categoria={ocat}{corte}/year={anio}"))
    VERDAD[anio] = dict(comunes=n_comun, solo_v1=n_solo_v1, solo_v2=n_solo_v2,
                        gana=n_gana, pierde=n_pierde,
                        filas_v1=len(v1), filas_v2=len(v2))

construir(2023, 400, 0,   5,   3,    0, 10)     # control sano
construir(2024, 500, 0,   8,   9,    0, 12)     # control sano
construir(2025, 300, 0, 120, 150,    0, 40)     # tratamiento: gana mucho
construir(2026, 100, 7,  60,  20,    4,  5)     # patologico: desaparecen y pierden campo

src = open("/tmp/paq/notebooks/CAP15_comparador_de_cortes.py", encoding="utf-8").read()
src = src.replace('VOLUMEN_BASE = "/Volumes/workspace/default/mercado_publico"',
                  f'VOLUMEN_BASE = "{BASE}"')
g = {"spark": spark, "__name__": "__main__"}
try:
    exec(compile(src, "CAP15", "exec"), g)
except AssertionError as e:
    print("AssertionError:", e)

M = g["M"]
print("\n" + "=" * 78)
print("CONTRASTE CONTRA LA VERDAD CONOCIDA POR CONSTRUCCION")
print("=" * 78)
fallos = 0
for anio, v in VERDAD.items():
    for clave, esperado in [("lic_comunes", v["comunes"]), ("lic_solo_v1", v["solo_v1"]),
                            ("lic_solo_v2", v["solo_v2"]), ("gano_fecha_adj", v["gana"]),
                            ("gano_proveedor", v["gana"]), ("perdio_fecha_adj", v["pierde"]),
                            ("filas_v1", v["filas_v1"]), ("filas_v2", v["filas_v2"])]:
        obt = M.get(("lic", anio, clave), 0)
        ok = obt == esperado
        fallos += 0 if ok else 1
        print(f"  [{'OK ' if ok else 'FALLA'}] {anio} {clave:<18} esperado={esperado:<6} obtenido={obt}")
print(f"\nFALLOS: {fallos}")
spark.stop()
