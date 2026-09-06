# -*- coding: utf-8 -*-
"""
auditoria_vintage.py — Capstone Big Data · Mercado Publico 2017-2026
====================================================================
AUDITORIA DE VINTAGE (corte de descarga) DE LOS ARCHIVOS DE LICITACION.

CORRE EN TU PC, NO EN DATABRICKS. Cero cupo de Spark. Solo biblioteca estandar
de Python 3.8+ (csv, zipfile, datetime). No requiere pandas.

--------------------------------------------------------------------
QUE PREGUNTA CONTESTA
--------------------------------------------------------------------
Los archivos abiertos de ChileCompra son un CORTE CON FECHA, no un archivo
historico inmutable: una licitacion publicada en 2023 y adjudicada en 2025 no
tenia adjudicacion que publicar el dia que bajaste el archivo. En tu corte
aparece sin lineas adjudicadas y sin proveedor. Bajada hoy, si las trae.

Eso implica dos cosas que este script mide:

  (A) CIERRE DEL PENDIENTE DE LA SESION 9. Los cuatro codigos de la Celda 6D-3
      (5839-14-LR23, 3789-22-LQ24, 1058086-50-LR24, 1057554-79-LQ23) no estaban
      en el universo cargado. 6D-3 refuto la hipotesis de formato (0 match con
      trim() y con upper()). Si esos codigos SI aparecen en la re-descarga, la
      causa queda identificada y el pendiente se cierra con evidencia directa.
      -> BLOQUE V-1

  (B) CUANTIFICACION DE LA CENSURA. Con el corte NUEVO se puede reconstruir,
      contrafactualmente, cuanta adjudicacion faltaba en el corte VIEJO: una
      licitacion estaba censurada si su fecha de adjudicacion es POSTERIOR a la
      fecha en que bajaste el archivo. Esto NO necesita el corte viejo: basta
      el nuevo mas la fecha de descarga original.
      -> BLOQUE V-2   (el bloque mas importante del script)

  (C) DIFF DIRECTO entre los dos cortes, si todavia conservas los archivos
      viejos: cuantas licitaciones aparecen ahora y antes no, y cuantas de las
      que ya estaban ganaron fecha de adjudicacion o proveedor.
      -> BLOQUE V-3   (opcional, solo si pasas --viejo)

--------------------------------------------------------------------
COMO SE USA
--------------------------------------------------------------------
  python auditoria_vintage.py --nuevo "D:/ruta/descarga_2026"

  python auditoria_vintage.py --nuevo "D:/ruta/descarga_2026" \
                              --viejo "D:/ruta/descarga_2025" \
                              --salida "D:/ruta/salida_vintage"

  # Si sabes la fecha exacta en que bajaste el corte viejo, pasala. Si no, el
  # script usa la fecha de modificacion de los archivos viejos como proxy, y lo
  # dice explicitamente en el informe.
  python auditoria_vintage.py --nuevo NUEVO --fecha-descarga-viejo 2025-09-15

Acepta .csv y .zip que contengan .csv (los busca recursivamente). Detecta
encoding (utf-8-sig / cp1252 / latin-1) y separador (; o ,) por archivo.

--------------------------------------------------------------------
DISCIPLINA DE EVIDENCIA (reglas del proyecto)
--------------------------------------------------------------------
- Este script MIDE. No concluye. Cada bloque imprime su tabla de magnitudes al
  lado del veredicto, y el veredicto hay que releerlo contra la tabla (regla
  §1.16).
- Nada de lo que imprime toca ninguna cifra congelada de RUN_ID 1314c4f6d481 /
  70680deaec66 / fb7c3b125bf7. Es una auditoria del corte, no una re-corrida.
- Si una columna no se resuelve, el bloque correspondiente se declara NO
  CORRIBLE e imprime las columnas disponibles. No inventa un sustituto.
- La fecha de descarga por mtime es un PROXY. Si copiaste, moviste o
  descomprimiste los archivos, el mtime puede no ser la fecha real de descarga.
  El informe lo marca. Usa --fecha-descarga-viejo si la sabes.
"""

import argparse
import csv
import io
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, date

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# =====================================================================
# CONFIGURACION
# =====================================================================

# Los cuatro codigos de la muestra 6D-2 que la Celda 6D-3 no encontro en el
# universo cargado (run_id=5fc3ded29565): 0 match exacto, 0 con trim(), 0 con
# trim()+upper(). Si quieres agregar mas, agregalos aqui.
CODIGOS_6D3 = [
    "5839-14-LR23",
    "3789-22-LQ24",
    "1058086-50-LR24",
    "1057554-79-LQ23",
]

# Resolvedor de columnas: primer candidato que exista, comparando en forma
# normalizada (minusculas, sin guiones bajos, sin espacios). Es el mismo patron
# que usa cap_resolver()/_c4_resolver() en el notebook, porque el esquema real
# trae el prefijo `_sinclasificar_` y PascalCase.
CANDIDATOS = {
    "codigo_externo": ["codigo_externo", "CodigoExterno", "codigoexterno",
                       "_sinclasificar_CodigoExterno", "codigo_licitacion",
                       "NroLicitacion", "numero_licitacion"],
    "fecha_publicacion": ["_sinclasificar_FechaPublicacion", "FechaPublicacion",
                          "fecha_publicacion", "FechaPublicacionLicitacion"],
    "fecha_adjudicacion": ["_sinclasificar_FechaAdjudicacion", "FechaAdjudicacion",
                           "fecha_adjudicacion", "FechaAdjudicacionLicitacion"],
    "fecha_cierre": ["fecha_cierre", "_sinclasificar_FechaCierre", "FechaCierre"],
    "estado": ["_sinclasificar_Estado", "Estado", "estado", "CodigoEstado",
               "_sinclasificar_CodigoEstado", "estado_licitacion",
               "DescripcionEstado"],
    "numero_oferentes": ["numero_oferentes", "NumeroOferentes",
                         "_sinclasificar_NumeroOferentes", "n_oferentes",
                         "CantidadOferentes"],
    "rut_proveedor": ["_sinclasificar_RutProveedor", "RutProveedor",
                      "rut_proveedor"],
    "oferta_seleccionada": ["oferta_seleccionada", "OfertaSeleccionada",
                            "_sinclasificar_OfertaSeleccionada"],
    "codigo_item": ["_sinclasificar_Codigoitem", "Codigoitem", "CodigoItem",
                    "codigo_item"],
    "onu": ["onu_lic", "onu", "CodigoProductoONU", "codigo_producto_onu",
            "_sinclasificar_CodigoProductoONU", "codigo_onu"],
    "monto_estimado": ["_sinclasificar_MontoEstimado", "MontoEstimado",
                       "monto_estimado"],
    "rut_unidad": ["_sinclasificar_RutUnidad", "RutUnidad", "rut_unidad"],
    "codigo_organismo": ["_sinclasificar_CodigoOrganismoPublico",
                         "CodigoOrganismoPublico", "codigo_organismo"],
}

# Estados que, en el vocabulario de ChileCompra, cierran una licitacion sin
# adjudicar. Se comparan en forma normalizada y por substring.
ESTADOS_SIN_ADJUDICAR = ["desierta", "revocada", "cancelada", "anulada",
                         "suspendida", "no adjudicada"]
ESTADOS_ADJUDICADA = ["adjudicada", "adjudicado"]


# =====================================================================
# UTILIDADES DE LECTURA
# =====================================================================

def _norm_nombre(s):
    """'_sinclasificar_RutProveedor' y 'rut_proveedor' comparten forma normal.
    Ese es exactamente el bug que tenia la Celda 4 de v17."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def resolver(cabecera, clave, verbose=True):
    """Devuelve el nombre REAL de la columna en `cabecera` para `clave`, o None."""
    reales = {_norm_nombre(c): c for c in cabecera}
    for cand in CANDIDATOS.get(clave, []):
        n = _norm_nombre(cand)
        if n in reales:
            return reales[n]
    # segunda pasada: coincidencia parcial, y se AVISA (no se resuelve en silencio)
    for cand in CANDIDATOS.get(clave, []):
        n = _norm_nombre(cand)
        for rn, real in reales.items():
            if n and n in rn:
                if verbose:
                    print("      [resolver] '%s' -> '%s' por coincidencia PARCIAL "
                          "con '%s'. VERIFICAR que sea la correcta." % (clave, real, cand))
                return real
    return None


def _decodificar(raw_bytes):
    """Devuelve (texto, encoding_usado). ChileCompra exporta en cp1252 casi
    siempre, pero hay archivos en utf-8 con BOM."""
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            return raw_bytes.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("latin-1", errors="replace"), "latin-1/replace"


def _detectar_sep(primera_linea):
    """Separador por conteo: ';' es el de ChileCompra, pero no siempre."""
    if primera_linea.count(";") >= primera_linea.count(",") and primera_linea.count(";") > 0:
        return ";"
    if primera_linea.count(",") > 0:
        return ","
    if primera_linea.count("\t") > 0:
        return "\t"
    return ";"


def descubrir_archivos(raiz):
    """Lista de (etiqueta, abridor) para cada .csv suelto o dentro de .zip.
    `abridor` es una funcion sin argumentos que devuelve un file-object de texto."""
    salida = []
    if os.path.isfile(raiz):
        rutas = [raiz]
    else:
        rutas = []
        for dirpath, _dirnames, filenames in os.walk(raiz):
            for fn in filenames:
                if fn.lower().endswith((".csv", ".zip", ".txt")):
                    rutas.append(os.path.join(dirpath, fn))
    rutas.sort()

    for ruta in rutas:
        if ruta.lower().endswith(".zip"):
            try:
                with zipfile.ZipFile(ruta) as z:
                    internos = [n for n in z.namelist()
                                if n.lower().endswith((".csv", ".txt"))]
            except (zipfile.BadZipFile, OSError) as e:
                print("   [aviso] no se pudo abrir el zip %s: %s" % (ruta, e))
                continue
            for interno in internos:
                salida.append((
                    "%s::%s" % (ruta, interno),
                    (lambda r=ruta, i=interno: _abrir_zip(r, i)),
                    ruta,
                ))
        else:
            salida.append((ruta, (lambda r=ruta: _abrir_plano(r)), ruta))
    return salida


def _abrir_zip(ruta_zip, interno):
    with zipfile.ZipFile(ruta_zip) as z:
        raw = z.read(interno)
    texto, enc = _decodificar(raw)
    return io.StringIO(texto), enc


def _abrir_plano(ruta):
    with open(ruta, "rb") as fh:
        raw = fh.read()
    texto, enc = _decodificar(raw)
    return io.StringIO(texto), enc


def leer_filas(abridor, columnas_pedidas, verbose=False):
    """Generador de dicts {clave_logica: valor} para las columnas resueltas.
    Devuelve primero un dict de resolucion (clave -> nombre real o None)."""
    fh, enc = abridor()
    primera = fh.readline()
    fh.seek(0)
    sep = _detectar_sep(primera)
    lector = csv.reader(fh, delimiter=sep, quotechar='"')
    try:
        cabecera = next(lector)
    except StopIteration:
        return {}, iter(()), cabecera_vacia(), enc, sep
    cabecera = [c.strip().lstrip("\ufeff") for c in cabecera]
    res = {k: resolver(cabecera, k, verbose=verbose) for k in columnas_pedidas}
    idx = {k: (cabecera.index(v) if v in cabecera else None)
           for k, v in res.items() if v is not None}

    def _gen():
        for fila in lector:
            if not fila:
                continue
            d = {}
            for k, i in idx.items():
                d[k] = fila[i].strip() if i < len(fila) else None
            yield d

    return res, _gen(), cabecera, enc, sep


def cabecera_vacia():
    return []


# =====================================================================
# UTILIDADES DE VALOR
# =====================================================================

_FORMATOS_FECHA = [
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M",
    "%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
    "%Y/%m/%d", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y",
]


def parse_fecha(s):
    """Texto -> date, o None. Tolerante a los formatos de exportacion chilenos.
    NO adivina: si ningun formato calza, devuelve None y el llamador lo cuenta."""
    if s is None:
        return None
    s = s.strip()
    if not s or s.upper() in ("NA", "N/A", "NULL", "NONE", "S/I", "-", ""):
        return None
    s = s.replace("T", " ") if ("T" in s and "-" in s[:11]) else s
    for f in _FORMATOS_FECHA:
        try:
            return datetime.strptime(s[:len(datetime.now().strftime(f))], f).date()
        except (ValueError, TypeError):
            continue
    # ultimo recurso: los primeros 10 caracteres como ISO
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def parse_entero(s):
    """Texto -> int, o None. Tolera formato chileno ('1.234')."""
    if s is None:
        return None
    s = s.strip().replace(".", "").replace(" ", "")
    if not s or not re.match(r"^-?\d+$", s):
        return None
    try:
        return int(s)
    except ValueError:
        return None


def norm_txt(s):
    if s is None:
        return ""
    s = s.strip().lower()
    for a, b in zip("áéíóúüñ", "aeiouun"):
        s = s.replace(a, b)
    return s


def clasificar_estado(s):
    """-> 'adjudicada' | 'sin_adjudicar' | 'otro' | 'sin_dato'"""
    n = norm_txt(s)
    if not n:
        return "sin_dato"
    for e in ESTADOS_ADJUDICADA:
        if e in n:
            return "adjudicada"
    for e in ESTADOS_SIN_ADJUDICAR:
        if e in n:
            return "sin_adjudicar"
    return "otro"


def norm_codigo(s):
    """Normalizacion del codigo de licitacion para comparar: trim + upper.
    Es exactamente lo que 6D-3 probo y que NO resolvio la ausencia."""
    return (s or "").strip().upper()


def fmt_pct(num, den):
    if not den:
        return "n/a"
    return "%.4f%%" % (100.0 * num / den)


def fmt_n(n):
    return "{:,}".format(n).replace(",", ".")


# =====================================================================
# CARGA DE UN CORTE
# =====================================================================

CLAVES = ["codigo_externo", "fecha_publicacion", "fecha_adjudicacion",
          "fecha_cierre", "estado", "numero_oferentes", "rut_proveedor",
          "oferta_seleccionada", "codigo_item", "onu", "monto_estimado",
          "rut_unidad", "codigo_organismo"]


class Corte(object):
    """Un corte = una descarga completa. Se deduplica a nivel de LICITACION
    (codigo_externo), quedandose con el primer valor no vacio de cada campo,
    porque df_lic_all trae varias filas por linea de adquisicion y por oferta
    (trampa conocida del esquema: count(*) NO es el numero de lineas)."""

    def __init__(self, nombre):
        self.nombre = nombre
        self.lic = {}                      # codigo_norm -> dict de campos
        self.filas_leidas = 0
        self.archivos = []                 # (etiqueta, n_filas, enc, sep, ruta_fisica)
        self.resoluciones = {}             # etiqueta -> dict de resolucion
        self.fechas_no_parseadas = Counter()
        self.mtimes = []                   # datetime de modificacion de cada archivo

    def cargar(self, raiz, verbose=True, limite_archivos=None):
        archivos = descubrir_archivos(raiz)
        if not archivos:
            print("   ABORTA: no se encontro ningun .csv/.zip bajo %r" % raiz)
            return False
        if limite_archivos:
            archivos = archivos[:limite_archivos]
        print("   %d archivo(s) a leer bajo %r" % (len(archivos), raiz))

        for etiqueta, abridor, ruta_fisica in archivos:
            try:
                self.mtimes.append(
                    datetime.fromtimestamp(os.path.getmtime(ruta_fisica)))
            except OSError:
                pass
            try:
                res, gen, cabecera, enc, sep = leer_filas(abridor, CLAVES,
                                                          verbose=verbose)
            except Exception as e:                       # noqa: BLE001
                print("   [aviso] no se pudo leer %s: %s" % (etiqueta, e))
                continue

            if res.get("codigo_externo") is None:
                if verbose:
                    print("   [salta] %s — no tiene columna de codigo de licitacion."
                          % os.path.basename(etiqueta))
                    print("           columnas: %s" % ", ".join(cabecera[:12]))
                continue

            self.resoluciones[etiqueta] = res
            n = 0
            for d in gen:
                n += 1
                cod = norm_codigo(d.get("codigo_externo"))
                if not cod:
                    continue
                reg = self.lic.get(cod)
                if reg is None:
                    reg = {k: None for k in CLAVES}
                    reg["_n_filas"] = 0
                    self.lic[cod] = reg
                reg["_n_filas"] += 1
                for k in CLAVES:
                    v = d.get(k)
                    if v not in (None, "") and reg.get(k) in (None, ""):
                        reg[k] = v
            self.filas_leidas += n
            self.archivos.append((etiqueta, n, enc, sep, ruta_fisica))
            if verbose:
                print("      %-58s %10s filas  [%s, sep='%s']"
                      % (os.path.basename(etiqueta)[:58], fmt_n(n), enc, sep))
        return len(self.lic) > 0

    def fecha_descarga_proxy(self):
        """mtime MAXIMO de los archivos del corte. Proxy de la fecha de descarga."""
        if not self.mtimes:
            return None
        return max(self.mtimes).date()

    def resolucion_consolidada(self):
        """Una vista de que columnas se resolvieron en al menos un archivo."""
        out = {}
        for res in self.resoluciones.values():
            for k, v in res.items():
                if v is not None and k not in out:
                    out[k] = v
        return out


# =====================================================================
# BLOQUE V-0 · INVENTARIO Y RESOLUCION DE ESQUEMA
# =====================================================================

def bloque_v0(corte, fecha_forzada=None):
    print("\n" + "=" * 78)
    print("V-0 · INVENTARIO DEL CORTE '%s'" % corte.nombre)
    print("=" * 78)
    print("  archivos leidos            : %s" % fmt_n(len(corte.archivos)))
    print("  filas leidas (crudas)      : %s" % fmt_n(corte.filas_leidas))
    print("  licitaciones distintas     : %s" % fmt_n(len(corte.lic)))
    if corte.filas_leidas and len(corte.lic):
        print("  filas por licitacion (prom): %.2f"
              % (corte.filas_leidas / float(len(corte.lic))))
        print("     ^ si es >1, el archivo trae varias filas por linea y por oferta:")
        print("       count(*) NO es el numero de lineas. Ya deduplicado por codigo.")

    proxy = corte.fecha_descarga_proxy()
    print("\n  FECHA DE DESCARGA")
    if fecha_forzada:
        print("    declarada por el usuario : %s   [DATO DECLARADO]" % fecha_forzada)
    if proxy:
        print("    mtime maximo (proxy)     : %s   [PROXY — ver salvedad]" % proxy)
    if not fecha_forzada and not proxy:
        print("    NO DETERMINABLE. Pasa --fecha-descarga-viejo para el bloque V-2.")
    print("    SALVEDAD: el mtime es la fecha del archivo en disco, no")
    print("    necesariamente la de descarga. Si copiaste, moviste o")
    print("    descomprimiste, puede estar corrido. Declararlo en el informe.")

    print("\n  RESOLUCION DE COLUMNAS (consolidada sobre todos los archivos)")
    res = corte.resolucion_consolidada()
    for k in CLAVES:
        v = res.get(k)
        marca = "OK " if v else "NO "
        print("    [%s] %-20s -> %s" % (marca, k, v if v else "(no resuelta)"))
    faltan = [k for k in ("fecha_publicacion", "fecha_adjudicacion") if not res.get(k)]
    if faltan:
        print("\n    AVISO: sin %s el bloque V-2 no corre." % " ni ".join(faltan))
    if not res.get("estado"):
        print("    AVISO: no se resolvio 'estado'. El corte por desierta/cancelada")
        print("           queda [NO CORRIBLE] y hay que mirar la cabecera a mano.")
        print("           Cabeceras vistas (primeras 40 columnas del primer archivo):")
        for etiqueta in list(corte.resoluciones)[:1]:
            fh_res = corte.resoluciones[etiqueta]
            print("             archivo: %s" % os.path.basename(etiqueta))
            print("             resueltas: %s" % {k: v for k, v in fh_res.items() if v})
    return res


# =====================================================================
# BLOQUE V-1 · LOS CUATRO CODIGOS DE 6D-3
# =====================================================================

def bloque_v1(corte, codigos=None, salida=None):
    codigos = codigos or CODIGOS_6D3
    print("\n" + "=" * 78)
    print("V-1 · LOS CODIGOS DE LA CELDA 6D-3, ¿ESTAN EN EL CORTE NUEVO?")
    print("=" * 78)
    print("  Contexto: 6D-3 (run_id=5fc3ded29565) dio 0 match exacto, 0 con trim()")
    print("  y 0 con trim()+upper() para estos codigos en el universo cargado. La")
    print("  hipotesis de formato quedo REFUTADA. Si ahora aparecen, la causa de la")
    print("  ausencia era el vintage del corte, no el formato ni el join.\n")

    filas = []
    encontrados = 0
    for cod in codigos:
        reg = corte.lic.get(norm_codigo(cod))
        if reg is None:
            print("  [NO]  %-20s  ausente tambien en el corte nuevo" % cod)
            filas.append({"codigo": cod, "presente": 0, "n_filas": 0,
                          "fecha_publicacion": "", "fecha_adjudicacion": "",
                          "estado": "", "rut_proveedor": "", "numero_oferentes": ""})
            continue
        encontrados += 1
        fp = reg.get("fecha_publicacion") or ""
        fa = reg.get("fecha_adjudicacion") or ""
        est = reg.get("estado") or ""
        prov = reg.get("rut_proveedor") or ""
        nof = reg.get("numero_oferentes") or ""
        print("  [SI]  %-20s  filas=%s  publicada=%s  adjudicada=%s"
              % (cod, fmt_n(reg["_n_filas"]), fp or "(sin dato)", fa or "(sin dato)"))
        print("        estado=%s  proveedor=%s  oferentes=%s"
              % (est or "(sin dato)", prov or "(sin dato)", nof or "(sin dato)"))
        d_p, d_a = parse_fecha(fp), parse_fecha(fa)
        if d_p and d_a:
            print("        rezago publicacion->adjudicacion: %d dias (%.1f meses)"
                  % ((d_a - d_p).days, (d_a - d_p).days / 30.44))
        filas.append({"codigo": cod, "presente": 1, "n_filas": reg["_n_filas"],
                      "fecha_publicacion": fp, "fecha_adjudicacion": fa,
                      "estado": est, "rut_proveedor": prov,
                      "numero_oferentes": nof})

    print("\n  " + "-" * 74)
    print("  RESULTADO: %d de %d codigos presentes en el corte nuevo."
          % (encontrados, len(codigos)))
    if encontrados == len(codigos):
        print("  -> HIPOTESIS DE VINTAGE SOSTENIDA para los cuatro casos. El pendiente")
        print("     de la sesion 9 (Apendice C-3) tiene causa identificada: el archivo")
        print("     mensual es un corte con fecha, y estas licitaciones de ciclo largo")
        print("     no tenian adjudicacion publicada al momento de la descarga original.")
        print("     NO cerrar con esto solo: confirmar contra la documentacion de")
        print("     descarga de ChileCompra que criterio separa Historico de Vigente")
        print("     (regla §1.13) antes de escribirlo como causa unica.")
    elif encontrados == 0:
        print("  -> HIPOTESIS DE VINTAGE DEBILITADA para estos casos. Los codigos siguen")
        print("     ausentes. Buscar otra causa; no forzar esta.")
    else:
        print("  -> RESULTADO MIXTO. Reportar los dos grupos por separado, nunca")
        print("     mezclados. Un caso presente no generaliza a los ausentes.")

    if salida:
        _escribir_csv(os.path.join(salida, "v1_codigos_6d3.csv"), filas)
    return filas


# =====================================================================
# BLOQUE V-2 · RECONSTRUCCION DE LA CENSURA (el bloque principal)
# =====================================================================

def bloque_v2(corte, res, fecha_corte_viejo, salida=None):
    print("\n" + "=" * 78)
    print("V-2 · CENSURA POR FECHA DE DESCARGA — ¿cuanta adjudicacion faltaba")
    print("      en el corte congelado?")
    print("=" * 78)

    if not res.get("fecha_publicacion") or not res.get("fecha_adjudicacion"):
        print("  [NO CORRIBLE] falta fecha de publicacion o de adjudicacion en el")
        print("  esquema. Ver V-0. No se fuerza un sustituto.")
        return None
    if fecha_corte_viejo is None:
        print("  [NO CORRIBLE] no hay fecha de descarga del corte VIEJO. Pasala con")
        print("  --fecha-descarga-viejo YYYY-MM-DD (es la fecha en que bajaste los")
        print("  archivos que alimentaron RUN_ID 1314c4f6d481).")
        return None

    print("  Definicion operativa: una licitacion estaba CENSURADA en el corte viejo")
    print("  si su fecha de adjudicacion (segun el corte nuevo) es POSTERIOR a la")
    print("  fecha de descarga del corte viejo (%s). En ese corte aparecia sin lineas"
          % fecha_corte_viejo)
    print("  adjudicadas y sin proveedor, aunque hoy si los tenga.\n")

    por_anio = defaultdict(lambda: {
        "total": 0, "con_adj": 0, "sin_adj": 0, "censurada": 0, "ausente": 0,
        "sin_fecha_pub": 0, "rezagos": [],
        "est_adjudicada": 0, "est_sin_adjudicar": 0, "est_otro": 0, "est_sin_dato": 0,
    })
    no_parsea_pub = 0
    no_parsea_adj = 0

    for cod, reg in corte.lic.items():
        fp = parse_fecha(reg.get("fecha_publicacion"))
        fa = parse_fecha(reg.get("fecha_adjudicacion"))
        if reg.get("fecha_publicacion") and fp is None:
            no_parsea_pub += 1
        if reg.get("fecha_adjudicacion") and fa is None:
            no_parsea_adj += 1
        if fp is None:
            por_anio["(sin fecha)"]["sin_fecha_pub"] += 1
            continue
        a = fp.year
        d = por_anio[a]
        d["total"] += 1
        cls = clasificar_estado(reg.get("estado"))
        d["est_" + cls] += 1
        # Distincion que importa: una licitacion PUBLICADA despues de la fecha de
        # descarga no estaba CENSURADA en el corte viejo — no estaba, punto. Es
        # ausencia, no dato faltante, y se cuenta aparte.
        ausente = fp > fecha_corte_viejo
        if ausente:
            d["ausente"] += 1
        if fa is None:
            d["sin_adj"] += 1
        else:
            d["con_adj"] += 1
            d["rezagos"].append((fa - fp).days)
            if (not ausente) and fa > fecha_corte_viejo:
                d["censurada"] += 1

    anios = sorted([a for a in por_anio if isinstance(a, int)])
    print("  %-6s %10s %10s %10s %11s %11s %11s" %
          ("anio", "licitac.", "ausentes", "en corte", "c/adjudic.",
           "CENSURAD.", "% censur."))
    print("  " + "-" * 76)
    tot = totc = totcen = totaus = 0
    filas = []
    for a in anios:
        d = por_anio[a]
        en_corte = d["total"] - d["ausente"]
        tot += d["total"]
        totc += d["con_adj"]
        totcen += d["censurada"]
        totaus += d["ausente"]
        print("  %-6s %10s %10s %10s %11s %11s %11s" %
              (a, fmt_n(d["total"]), fmt_n(d["ausente"]), fmt_n(en_corte),
               fmt_n(d["con_adj"]), fmt_n(d["censurada"]),
               fmt_pct(d["censurada"], en_corte)))
        rz = sorted(d["rezagos"])
        filas.append({
            "anio_publicacion": a, "licitaciones": d["total"],
            "ausentes_del_corte_viejo": d["ausente"],
            "presentes_en_corte_viejo": en_corte,
            "con_adjudicacion": d["con_adj"], "sin_adjudicacion": d["sin_adj"],
            "censuradas_en_corte_viejo": d["censurada"],
            "pct_censuradas": (100.0 * d["censurada"] / en_corte) if en_corte else "",
            "rezago_mediano_dias": (rz[len(rz) // 2] if rz else ""),
            "rezago_p90_dias": (rz[int(len(rz) * 0.9)] if rz else ""),
            "rezago_max_dias": (rz[-1] if rz else ""),
            "estado_adjudicada": d["est_adjudicada"],
            "estado_sin_adjudicar": d["est_sin_adjudicar"],
            "estado_otro": d["est_otro"],
            "estado_sin_dato": d["est_sin_dato"],
        })
    print("  " + "-" * 76)
    print("  %-6s %10s %10s %10s %11s %11s %11s" %
          ("TOTAL", fmt_n(tot), fmt_n(totaus), fmt_n(tot - totaus), fmt_n(totc),
           fmt_n(totcen), fmt_pct(totcen, tot - totaus)))

    print("\n  REZAGO PUBLICACION -> ADJUDICACION (dias), por anio de publicacion")
    print("  %-6s %12s %12s %12s %12s" % ("anio", "n", "mediana", "p90", "maximo"))
    print("  " + "-" * 60)
    for a in anios:
        rz = sorted(por_anio[a]["rezagos"])
        if not rz:
            print("  %-6s %12s %12s %12s %12s" % (a, 0, "-", "-", "-"))
            continue
        print("  %-6s %12s %12s %12s %12s" %
              (a, fmt_n(len(rz)), rz[len(rz) // 2],
               rz[int(len(rz) * 0.9)], rz[-1]))

    if por_anio["(sin fecha)"]["sin_fecha_pub"]:
        print("\n  licitaciones sin fecha de publicacion parseable: %s"
              % fmt_n(por_anio["(sin fecha)"]["sin_fecha_pub"]))
    if no_parsea_pub or no_parsea_adj:
        print("  fechas presentes pero NO parseadas — publicacion: %s · adjudicacion: %s"
              % (fmt_n(no_parsea_pub), fmt_n(no_parsea_adj)))
        print("  ^ si esto no es cero, mirar los formatos crudos ANTES de citar nada.")

    print("\n  " + "-" * 74)
    print("  COMO SE LEE (regla §1.16 — el veredicto se relee contra la tabla):")
    print("  · La columna CENSURADAS es una cota INFERIOR del faltante del corte")
    print("    viejo: cuenta solo las que YA estan adjudicadas hoy. Las que siguen")
    print("    sin adjudicar hoy no se pueden clasificar y no suman.")
    print("  · La censura NO es aleatoria: se concentra donde el rezago es largo, o")
    print("    sea en los procesos grandes y plurianuales (LR, LQ). Cualquier cifra")
    print("    del proyecto calculada sobre adjudicaciones esta sesgada en esa")
    print("    direccion, y esta tabla es su cota.")
    print("  · AUSENTES y CENSURADAS son cosas distintas y no se suman: ausente =")
    print("    publicada DESPUES de la descarga, o sea que no estaba en el corte")
    print("    viejo; censurada = si estaba, pero sin su adjudicacion. El % de")
    print("    censura se calcula sobre las presentes, no sobre el total.")
    print("  · El anio mas reciente esta SIEMPRE inflado en 'sin adjudicacion':")
    print("    es censura del corte NUEVO, no del viejo. No leerlo como patologia.")

    if salida:
        _escribir_csv(os.path.join(salida, "v2_censura_por_anio.csv"), filas)
    return filas


# =====================================================================
# BLOQUE V-3 · DIFF DIRECTO ENTRE CORTES
# =====================================================================

def bloque_v3(nuevo, viejo, salida=None):
    print("\n" + "=" * 78)
    print("V-3 · DIFF DIRECTO: corte VIEJO vs corte NUEVO")
    print("=" * 78)

    set_n = set(nuevo.lic)
    set_v = set(viejo.lic)
    solo_n = set_n - set_v
    solo_v = set_v - set_n
    comun = set_n & set_v

    print("  licitaciones en el corte VIEJO   : %s" % fmt_n(len(set_v)))
    print("  licitaciones en el corte NUEVO   : %s" % fmt_n(len(set_n)))
    print("  solo en el NUEVO (aparecieron)   : %s   (%s del nuevo)"
          % (fmt_n(len(solo_n)), fmt_pct(len(solo_n), len(set_n))))
    print("  solo en el VIEJO (desaparecieron): %s   (%s del viejo)"
          % (fmt_n(len(solo_v)), fmt_pct(len(solo_v), len(set_v))))
    print("  en ambos                         : %s" % fmt_n(len(comun)))
    if solo_v:
        print("  ^ que una licitacion DESAPAREZCA del corte nuevo es un hallazgo")
        print("    distinto y mas grave que una que aparece. Mirar una muestra a mano.")

    gano_adj = gano_prov = gano_ofer = 0
    for cod in comun:
        rv, rn = viejo.lic[cod], nuevo.lic[cod]
        if not rv.get("fecha_adjudicacion") and rn.get("fecha_adjudicacion"):
            gano_adj += 1
        if not rv.get("rut_proveedor") and rn.get("rut_proveedor"):
            gano_prov += 1
        if not rv.get("numero_oferentes") and rn.get("numero_oferentes"):
            gano_ofer += 1

    print("\n  DE LAS QUE YA ESTABAN EN AMBOS CORTES, cuantas GANARON dato:")
    print("    fecha de adjudicacion : %s   (%s de las comunes)"
          % (fmt_n(gano_adj), fmt_pct(gano_adj, len(comun))))
    print("    RUT de proveedor      : %s   (%s)"
          % (fmt_n(gano_prov), fmt_pct(gano_prov, len(comun))))
    print("    numero de oferentes   : %s   (%s)"
          % (fmt_n(gano_ofer), fmt_pct(gano_ofer, len(comun))))
    print("\n  ^ ESTE ES EL NUMERO PARA EL INFORME. No es una estimacion: es el")
    print("    conteo directo de registros que cambiaron de contenido entre dos")
    print("    descargas del mismo periodo. Es la evidencia mas fuerte de que el")
    print("    dato abierto de ChileCompra es un corte con fecha.")

    # muestra de aparecidas, por anio y con su rezago
    por_anio_ap = Counter()
    muestra = []
    for cod in solo_n:
        reg = nuevo.lic[cod]
        fp = parse_fecha(reg.get("fecha_publicacion"))
        if fp:
            por_anio_ap[fp.year] += 1
        if len(muestra) < 30:
            muestra.append({"codigo": cod,
                            "fecha_publicacion": reg.get("fecha_publicacion") or "",
                            "fecha_adjudicacion": reg.get("fecha_adjudicacion") or "",
                            "estado": reg.get("estado") or "",
                            "numero_oferentes": reg.get("numero_oferentes") or ""})
    if por_anio_ap:
        print("\n  LICITACIONES QUE APARECIERON, por anio de publicacion:")
        for a in sorted(por_anio_ap):
            print("    %-6s %12s" % (a, fmt_n(por_anio_ap[a])))

    if salida:
        _escribir_csv(os.path.join(salida, "v3_muestra_aparecidas.csv"), muestra)
        _escribir_csv(os.path.join(salida, "v3_resumen.csv"), [{
            "lic_corte_viejo": len(set_v), "lic_corte_nuevo": len(set_n),
            "solo_nuevo": len(solo_n), "solo_viejo": len(solo_v),
            "comunes": len(comun), "ganaron_fecha_adjudicacion": gano_adj,
            "ganaron_rut_proveedor": gano_prov,
            "ganaron_numero_oferentes": gano_ofer,
        }])
    return {"solo_nuevo": len(solo_n), "solo_viejo": len(solo_v),
            "comunes": len(comun), "gano_adj": gano_adj}


def _escribir_csv(ruta, filas):
    if not filas:
        return
    os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
    with open(ruta, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=list(filas[0].keys()), delimiter=";")
        w.writeheader()
        w.writerows(filas)
    print("     -> escrito %s" % ruta)


# =====================================================================
# MAIN
# =====================================================================

def main(argv=None):
    p = argparse.ArgumentParser(
        description="Auditoria de vintage de los archivos de licitacion de "
                    "ChileCompra. Corre en local, sin Databricks.")
    p.add_argument("--nuevo", required=True,
                   help="carpeta (o archivo) de la re-descarga 2023-2026")
    p.add_argument("--viejo", default=None,
                   help="carpeta del corte original, si todavia lo tienes "
                        "(habilita el bloque V-3)")
    p.add_argument("--fecha-descarga-viejo", default=None,
                   help="YYYY-MM-DD, la fecha en que bajaste el corte original. "
                        "Si no la pasas y hay --viejo, se usa su mtime como proxy.")
    p.add_argument("--salida", default="salida_vintage",
                   help="carpeta donde escribir los CSV de resultado")
    p.add_argument("--limite-archivos", type=int, default=None,
                   help="leer solo los primeros N archivos (para una prueba rapida)")
    p.add_argument("--silencioso", action="store_true",
                   help="no imprimir el detalle archivo por archivo")
    a = p.parse_args(argv)

    verbose = not a.silencioso
    os.makedirs(a.salida, exist_ok=True)

    print("=" * 78)
    print("AUDITORIA DE VINTAGE — Capstone Mercado Publico")
    print("corrida: %s" % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 78)
    print("\nCARGANDO CORTE NUEVO...")
    nuevo = Corte("nuevo")
    if not nuevo.cargar(a.nuevo, verbose=verbose,
                        limite_archivos=a.limite_archivos):
        print("\nABORTA: no se cargo ninguna licitacion del corte nuevo.")
        return 2

    res = bloque_v0(nuevo, fecha_forzada=a.fecha_descarga_viejo)
    bloque_v1(nuevo, salida=a.salida)

    fecha_vieja = None
    if a.fecha_descarga_viejo:
        fecha_vieja = parse_fecha(a.fecha_descarga_viejo)
        if fecha_vieja is None:
            print("\n[aviso] no pude parsear --fecha-descarga-viejo=%r"
                  % a.fecha_descarga_viejo)

    viejo = None
    if a.viejo:
        print("\nCARGANDO CORTE VIEJO...")
        viejo = Corte("viejo")
        if not viejo.cargar(a.viejo, verbose=verbose,
                            limite_archivos=a.limite_archivos):
            print("   [aviso] el corte viejo no cargo; se salta V-3.")
            viejo = None
        elif fecha_vieja is None:
            fecha_vieja = viejo.fecha_descarga_proxy()
            if fecha_vieja:
                print("   fecha de descarga del corte viejo (PROXY por mtime): %s"
                      % fecha_vieja)

    bloque_v2(nuevo, res, fecha_vieja, salida=a.salida)
    if viejo is not None:
        bloque_v3(nuevo, viejo, salida=a.salida)

    print("\n" + "=" * 78)
    print("FIN. Resultados en %r" % os.path.abspath(a.salida))
    print("Pega esta salida completa en el chat para leerla juntos.")
    print("Recordatorio: nada de esto cambia ninguna cifra congelada. Es una")
    print("auditoria del corte, no una re-corrida del pipeline.")
    print("=" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())
