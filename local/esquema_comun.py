#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Esquema comun 2017-2026 -- Capstone Big Data Mercado Publico
==============================================================

Que es este archivo:
  Config pura (sin dependencias de pandas ni Spark). Define el esquema comun
  al que se mapean las 4 carpetas de insumos de Mercado Publico
  (Licitaciones_Nacional_Historico/Vigente, OC_Nacional_Historico/Vigente)
  para poder tratarlas como una sola serie 2017-2026, pese a que ChileCompra
  cambio nombres/columnas entre años.

  Es el mismo mapeo alias->columna que ya se valido en gate_g1_g6.py
  (corrida run_id=d4a8e22e1764, 11-ago-2026, [MEDIDO] sobre 2017/2020/2023/2026
  completos), mas la tabla de drift de esquema por año que ese mismo gate
  encontro (hallazgo G5). No se agrego NINGUN alias nuevo que no estuviera ya
  en gate_g1_g6.py: agregar un alias no verificado violaria la regla del
  proyecto "no se infiere, se comprueba" (ver claude/00-arranque.md #1.1).

  Este archivo lo importan (o deberian importar) TODOS los scripts del
  pipeline que necesiten saber "que columna real corresponde a que campo
  canonico" -- hoy construir_capa_esquema_comun.py (pandas), y desde la
  Clase 7 (17-ago) en adelante, el job PySpark de la Semana 1. Un solo lugar
  para el mapeo evita que dos scripts se desincronicen.

Por que hay campos marcados [POR VERIFICAR]:
  El gate G5 (11-ago) detecto QUE hay drift en ciertas categorias de campos
  (ej. "criterios ambientales" en licitacion) pero el registro de esa corrida
  (claude/00-arranque.md) no dejo anotado el NOMBRE LITERAL de columna nuevo
  en 2026. Ese nombre esta en el CSV de esa corrida
  (gate_report_d4a8e22e1764.csv, filas gate=G5, metrica=columnas_nuevas_vs_referencia)
  que el usuario tiene archivado. Hasta pegar ese nombre literal aqui, el
  campo queda fuera del mapeo automatico (no se inventa el nombre).
"""

# --------------------------------------------------------------------------
# Carpetas de origen (identico a gate_g1_g6.py -- no cambiar sin actualizar
# tambien el gate, para que ambos scripts sigan hablando de las mismas 4
# carpetas)
# --------------------------------------------------------------------------

CATEGORY_FOLDERS = {
    "lic_historico": "Licitaciones_Nacional_Historico",
    "lic_vigente": "Licitaciones_Nacional_Vigente",
    "oc_historico": "OC_Nacional_Historico",
    "oc_vigente": "OC_Nacional_Vigente",
}
LIC_CATEGORIES = {"lic_historico", "lic_vigente"}
OC_CATEGORIES = {"oc_historico", "oc_vigente"}

# Alcance temporal decidido por el usuario el 15-ago-2026: 2017-2026 completo
# (ver claude/bitacora.md, entrada 2026-08-15). Cambiar aqui si se recorta.
ANIOS_PROYECTO = list(range(2017, 2027))

# --------------------------------------------------------------------------
# Esquema comun -- LICITACIONES
# Formato: nombre_canonico: [alias_1, alias_2, ...]
# Fuente: LIC_ALIASES de gate_g1_g6.py, [MEDIDO] contra Muestra.xlsx y contra
# la corrida real run_id=d4a8e22e1764 (11-ago-2026).
# --------------------------------------------------------------------------

ESQUEMA_LIC = {
    "codigo_externo": ["CodigoExterno"],
    "onu_lic": ["CodigoProductoONU"],
    "numero_oferentes": ["NumeroOferentes"],
    "oferta_seleccionada": ["Oferta seleccionada", "OfertaSeleccionada"],
    "estado_oferta": ["Estado Oferta", "EstadoOferta"],
    "codigo_moneda": ["CodigoMoneda"],
    "moneda_adquisicion": ["Moneda Adquisicion", "MonedaAdquisicion"],
    "fecha_cierre": ["FechaCierre"],
    "cantidad_ofertada": ["Cantidad Ofertada", "CantidadOfertada"],
}

# --------------------------------------------------------------------------
# Esquema comun -- ORDENES DE COMPRA
# Fuente: OC_ALIASES de gate_g1_g6.py, [MEDIDO] igual que arriba, mas
# confirmado [FUENTE] contra DocumentacionAPIMercadoPublicooc.pdf (14-ago-2026)
# para codigo_licitacion, cantidad_oc y precio_neto_oc.
# --------------------------------------------------------------------------

ESQUEMA_OC = {
    "codigo_oc": ["Codigo"],
    "codigo_licitacion": ["CodigoLicitacion"],
    "codigo_convenio_marco": ["Codigo_ConvenioMarco"],
    "procedencia_oc": ["ProcedenciaOC"],
    "tipo_oc": ["Tipo"],
    "onu_oc": ["codigoProductoONU", "CodigoProductoONU"],
    "cantidad_oc": ["cantidad", "Cantidad"],
    "precio_neto_oc": ["precioNeto", "PrecioNeto"],
    "monto_total_oc_clp": ["MontoTotalOC_PesosChilenos"],
    "moneda_item": ["monedaItem", "MonedaItem"],
    "fecha_creacion": ["FechaCreacion"],
}

CM_MARKERS_PROCEDENCIA = ["convenio marco"]  # comparacion case-insensitive

# --------------------------------------------------------------------------
# Drift de esquema conocido por año -- [MEDIDO] en el gate G5 (11-ago-2026),
# ver claude/00-arranque.md #4. Esto es LO QUE YA SE SABE que un año puede no
# traer; el script constructor de la capa (construir_capa_esquema_comun.py)
# usa esta tabla solo para no marcar como "hallazgo nuevo" un drift que ya se
# conoce, pero de todas formas reporta huecos no listados aqui.
#
# Formato: {campo_canonico: {"ausente_en": [anios], "nota": "..."}}
# --------------------------------------------------------------------------

DRIFT_CONOCIDO_LIC = {
    # [POR VERIFICAR] nombre literal de columna: el gate detecto que 2026
    # trae campos de "criterios ambientales" que 2017/2020/2023 no traen,
    # pero el nombre literal de esa columna no quedo registrado en
    # 00-arranque.md. Pegar el nombre real (desde gate_report_d4a8e22e1764.csv,
    # gate=G5 / categoria=lic / metrica=columnas_nuevas_vs_referencia, year=2026
    # NO -- ese campo compara contra referencia=2026, hay que mirar la fila
    # con year=2017/2020/2023 y columnas_faltantes_vs_referencia) antes de
    # activar esta entrada.
    # "criterios_ambientales": {"ausente_en": [2017, 2020, 2023], "nota": "[POR VERIFICAR] nombre literal de columna"},
    "valor_tiempo_renovacion": {
        "aliases": ["ValorTiempoRenovacion"],
        "ausente_en": [2026],
        "nota": "[MEDIDO] gate G5 11-ago-2026: presente en 2017/2020/2023, no en 2026 (Vigente).",
    },
}

DRIFT_CONOCIDO_OC = {
    "codigo_convenio_marco": {
        "aliases": ["Codigo_ConvenioMarco"],
        "ausente_en": [2017],
        "nota": "[MEDIDO] gate G5 11-ago-2026: no existe como columna en archivos de 2017; "
                "aparece desde 2020. Por eso G1/G2/G6 usan ProcedenciaOC/Tipo para excluir "
                "Convenio Marco de forma uniforme 2017-2026, NUNCA Codigo_ConvenioMarco solo.",
    },
}

# Hallazgo de DRIFT DE VALOR (no de columna): el tipo de OC 'AG' (Compra Agil)
# no aparece como VALOR en la columna Tipo/tipo_oc en la muestra de 2017, pero
# la columna en si probablemente exista igual ese año. [MEDIDO] 182.900 OC con
# tipo AG desde 2020. Explicacion historica [POR VERIFICAR] -- no usar en
# informe sin verificar. No se modela aqui como ausencia de columna.
NOTA_DRIFT_VALOR_TIPO_AG = (
    "[POR VERIFICAR] tipo_oc='AG' (Compra Agil) sin explicacion historica confirmada "
    "de por que no aparece en 2017. Ver claude/00-arranque.md #4."
)


def resolver_alias(columnas_reales, aliases):
    """Dado el listado de columnas reales de un archivo y una lista de alias
    candidatos, devuelve el nombre REAL de columna que calza (case-insensitive,
    espacios colapsados) o None si ninguno calzo -- idem find_col() de
    gate_g1_g6.py, para que ambos scripts resuelvan alias exactamente igual."""
    import re
    norm_map = {}
    for c in columnas_reales:
        key = re.sub(r"\s+", " ", str(c).strip()).lower()
        norm_map[key] = c
    for alias in aliases:
        key = re.sub(r"\s+", " ", alias.strip()).lower()
        if key in norm_map:
            return norm_map[key]
    return None
