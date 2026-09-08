# Sobre esta copia del proyecto

Este repositorio es una versión preparada, a partir del paquete de trabajo completo
del autor (`capstone-mercado-publico-03sep2026-v3`, 124 archivos), para publicarse en
GitHub. El criterio fue: dejar todo lo necesario para que el proyecto **funcione y sea
comprobable** — código, catálogos, muestras, salidas crudas de cada corrida — y sacar
la bitácora interna de desarrollo y cualquier información personal. Nada de lo listado
abajo afecta ninguna cifra citada en el informe: son documentos de proceso, versiones
superadas de código, o metadatos de máquina local.

## Qué se dejó fuera

- **`historia/`** (10 archivos, ~560 KB): versiones anteriores de los notebooks
  (`celdas_capstone_v11/v14/v15/v16/v17`, `etapa2_databricks_v17_COMPLETO`) ya
  reemplazadas por lo que hay en `notebooks/`, y traspasos de sesión de chat
  (`00-TRASPASO-*`, `00-BITACORA-*`, `00-INSTRUCCIONES-DATABRICKS*`) que no aportan
  código ni datos, solo el registro de cómo se coordinó el trabajo entre sesiones.
  Ningún script de `notebooks/` o `local/` importa ni depende de esta carpeta.
- **`docs/00-EMPEZAR-AQUI.md`**, **`docs/00-PLAN-19-DIAS-Y-FICHAS-DE-TAREA.md`**,
  **`docs/00-SEGUNDA-OPINION-Y-CORRECCIONES.md`** y los documentos de sesión
  `F11`–`F25`, `F27`–`F31` (23 archivos): son la bitácora sesión-a-sesión —
  decisiones, correcciones y calendario internos. Se conservan `01-RESULTADOS`,
  `02-CIERRE-DE-LAS-CIFRAS-EN-PESOS` y `F26` porque son los documentos a los que el
  informe final remite para trazar sus cifras.
- **`GUIA-PASO-A-PASO.html`**: checklist operativo personal del autor (comandos de
  PowerShell contra su carpeta local), no necesario para reproducir el análisis.
- **`local/VERIFICAR-2024-11.txt`**: nota de un chequeo puntual ya resuelto e
  integrado en `docs/F26` (§1.3); su instrucción original apuntaba además a una ruta
  de archivo local (ver abajo).
- **`local/manifest_esquema_comun_649df6092fb4.json`** y
  **`local/manifest_esquema_comun_6de961bbf738.json`**: metadatos de una corrida de
  reconciliación de esquema. Ningún script los lee; su contenido es casi enteramente
  la ruta local del computador del autor (ver abajo).

## Qué se editó, y por qué

- **`local/fig7_maduracion_v2.py`** (líneas 82-83): las dos llamadas a
  `fig.savefig(...)` apuntaban a una ruta absoluta de un contenedor de trabajo
  (`/home/claude/...`) que no existe en el computador del autor. Se cambiaron a rutas
  relativas (`figuras/fig7_maduracion_v2.png` / `.pdf`), consistentes con el resto del
  proyecto. No cambia ningún cálculo, solo dónde se guarda la figura.
- **`resultados/corrida_del_28_paso0BDE_salida_cruda.txt`** (líneas 4 y 45): esta
  salida cruda es citable (fuente de las cifras de `F26`) y por eso se conserva
  íntegra, pero traía impresa la ruta local del computador del autor — usuario de
  Windows y carpeta de OneDrive corporativa. Se reemplazó esa ruta por el marcador
  `<CARPETA_LOCAL>` en esas dos líneas. **No se tocó ninguna otra línea, cifra ni
  mensaje del archivo.**

## Qué NO se sacó, y por qué

- Los datos de licitaciones y órdenes de compra (`datos/`, `csv/`, `resultados/`,
  incluidos RUT y razón social de proveedores adjudicados) son **dato abierto ya
  publicado** por ChileCompra en mercadopublico.cl — no es información personal, es
  justamente el objeto del análisis. Esto es distinto de la doctrina "alerta, no
  infracción" del proyecto (regla §4), que rige cómo se *presentan* hallazgos en el
  informe y el dashboard (sin ranking nominal ni señalamiento individualizado), no si
  se conserva el dato crudo de origen público que sostiene el análisis.
- El nombre del autor (`Jeancarlo Cuesta`) se mantiene donde ya figuraba como autoría
  del código (p. ej. cabecera de `notebooks/etapa2_ANEXO_PROFESOR_v19.py`): es trabajo
  individual firmado, no un dato a ocultar.

## Sobre las referencias a `F13`, `F18`, `F22`–`F25` dentro de los documentos que sí quedaron

`F26` (y, de paso, `01-RESULTADOS` y `02-CIERRE`) citan varias veces documentos de
sesión que no están en este repositorio (`F13`, `F18`, `F22`, `F23`, `F24`, `F25`). Se
revisó cada mención: en todos los casos `F26` cita textualmente lo que decía el
documento anterior y explica en el mismo párrafo qué corrige, cierra o confirma — no
hace falta abrir el original para seguir el argumento. La única excepción es la
corrección documental de `F13` (una cita académica pendiente de dos minutos, mencionada
en `01-RESULTADOS` y `02-CIERRE`): no cambia ninguna cifra, solo queda como pendiente de
redacción.

## Dónde está el resto

El paquete de trabajo completo (124 archivos, con la bitácora íntegra) sigue en el
disco del autor y no se publica. Si hace falta releer un documento de sesión
específico que no está aquí, se puede volver a adjuntar ese archivo puntual.
