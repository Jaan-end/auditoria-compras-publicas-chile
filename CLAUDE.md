# Que compra y cuanto paga el Estado

Auditoria nacional de la capa de producto en las compras publicas de Chile, 2017-2026 (Capstone Big Data, SIC 2026). **Repositorio publico.**

## Comandos
- Instalar: `.venv\Scripts\python.exe -m pip install -r requirements.txt`
- Pruebas (scripts que salen con 0 si pasan, ~20 s): `local/test_cap14_mh.py`, `local/test_cap18_etiquetas.py`, `local/test_cap18_metricas.py`. `local/test_cap15_verificacion.py` necesita PySpark (solo Databricks).
- Lint: `.venv\Scripts\python.exe -m ruff check <archivo>`
- El pipeline entregado (`notebooks/etapa2_ANEXO_PROFESOR_v19.py`) corre en Databricks, no en el PC.

## Convenciones que no se deducen del codigo
- Toda cifra se copia de una salida cruda y lleva su `run_id`; `resultados/` nunca se edita (`resultados/LEEME.txt` dice cuales se citan).
- Etiquetas: `[MEDIDO]`, `[FUENTE]`, `[POR VERIFICAR]`, `[DESCARTADO]`. Resultados descartados quedan con sufijo `_DESCARTADO`.
- "Alerta, no infraccion": se mide trazabilidad, no fraude.
- Cifras con coma decimal y punto de miles (`94,44 %`).
- Formato automatico desactivado: cambios pequenos y faciles de revisar.

## Trampas conocidas
- Es publico: nada de JUNJI, rutas locales, credenciales ni datos crudos en commits.
- Los datos de origen (decenas de GB) no se redistribuyen.

## Al compactar
Conserva la lista de archivos modificados, los comandos de prueba usados y las decisiones tomadas.
