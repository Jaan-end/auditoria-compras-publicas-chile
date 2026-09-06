# Capstone · Mercado Público de Chile, 2017-2026

Análisis del dato abierto de compras públicas chilenas: **51,4 millones de líneas
de orden de compra** y **26,3 millones de registros de licitación**, diez años,
procesados en Databricks/Spark.

## Qué se pregunta

1. **¿El código de producto (ONU/UNSPSC) que declara una orden de compra es
   consistente con el que declaró la licitación que la originó?**
   Respuesta: sí — 94,44 % — pero contra un **baseline nulo permutado de 1,38 %**,
   y sólo el **19,73 %** del universo permite hacerse la pregunta. El hallazgo es
   la exclusión, no la tasa.
2. **¿La entrada en vigencia del art. 20 bis de la Ley 19.886 (12-dic-2024) mejoró
   la codificación?** Respuesta: no. +0,28 pp, +0,30 pp restringido a los mismos
   organismos. Y un **placebo** —la cobertura de un campo que la norma no manda—
   se mueve igual, así que el movimiento de la serie no es cumplimiento normativo.
3. **¿La especificación de texto de una línea es evidencia independiente del
   código, o es una copia de él?** Es circular en una proporción que crece de
   7,43 pp (2019) a 22,60 pp (2026), y el mecanismo es identificable: Convenio
   Marco pasa de 6,65 % a 99,86 %.

La doctrina del proyecto es **"alerta, no infracción"**: que una línea no sea
auditable con lo publicado no prueba que se haya comprado otra cosa.

## Cómo se trabaja

Tres reglas explican casi todas las decisiones del repositorio:

- **Toda cifra se cita con su `RUN_ID` y se copia de la salida cruda, nunca se
  recalcula** (regla §1.20). Las salidas crudas están en `resultados/`.
- **Régimen de evidencia obligatorio.** Cada afirmación lleva etiqueta:
  `[MEDIDO]` · `[MEDIDO · recálculo declarado]` · `[FUENTE]` (con URL) ·
  `[VERIFICADO]` · `[POR VERIFICAR]` · `[PRUEBA]` · `[PROPUESTA]`.
- **Una corrida de Databricks por día** (§1.18: la cuenta es gratuita y tiene tope
  diario). Por eso los gráficos, los tamices y las auditorías corren en el PC.

## Mapa del repositorio

| Carpeta | Qué hay |
|---|---|
| `docs/` | Cierre de cifras y auditoría: `01-RESULTADOS-CORRIDA-1314c4f6d481.md`, `02-CIERRE-DE-LAS-CIFRAS-EN-PESOS.md`, `F26-AUDITORIA-Y-CIERRE-DE-FLANCOS-28-AGO.md`. **Empezar por `F26`** — audita el propio pipeline y referencia de dónde sale cada cifra citada. |
| `notebooks/` | Celdas de Databricks: notebook maestro **`etapa2_ANEXO_PROFESOR_v19.py`** (el que se entrega), `etapa2_RESPALDO_COMPLETO_v18.py` (respaldo con los apéndices de diagnóstico), bloques `CAP-*`, pasos de auditoría de corte |
| `local/` | Todo lo que corre en el PC sin gastar cupo: figuras, tamices, auditorías, comparadores de corte |
| `resultados/` | Salidas crudas de cada corrida, con `LEEME.txt` que dice qué es citable |
| `csv/` | Insumos de las figuras, con su procedencia declarada |
| `figuras/` | PNG y PDF generados por `local/graficos_capstone.py` |
| `datos/` | Catálogo ONU, muestras, pesos de estratos |

**Punto de entrada operativo:** `docs/F26-AUDITORIA-Y-CIERRE-DE-FLANCOS-28-AGO.md`, y desde ahí a `01-RESULTADOS` / `02-CIERRE` según la cifra que se necesite trazar.

> Esta es una versión curada del paquete de trabajo completo (ver `SOBRE-ESTA-COPIA.md`): mantiene todo el código, los datos y las salidas crudas necesarias para reproducir y verificar cada cifra citada, y deja fuera la bitácora sesión-a-sesión del desarrollo.

## Lo que este proyecto hace y casi ningún trabajo de datos hace

- Mide el azar antes de celebrar una tasa (baseline nulo permutado).
- Predeclara placebos y reporta cuando fallan.
- Descartó su propio resultado alternativo (CAP-12B) por no replicar un criterio
  predeclarado, teniendo ya el número en la mano.
- **Encontró un defecto de triplicación de filas en su propio corte de datos, lo
  midió con tres métodos independientes y demostró —trazando el código, no
  argumentando— que no afecta ninguna cifra publicada** (`docs/F26`).
