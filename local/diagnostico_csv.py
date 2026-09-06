# -*- coding: utf-8 -*-
"""
DIAGNOSTICO DE UN CSV QUE ROMPE EL LECTOR
=========================================
Encuentra las lineas fisicas que hacen fallar a csv.reader y dice cual de
los dos arreglos corresponde. No modifica nada.

    python diagnostico_csv.py "C:\\...\\new\\Lic\\lic_2026-3.csv"
    python diagnostico_csv.py "C:\\...\\new\\Lic"        (carpeta entera)
"""
import csv, io, os, sys

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


def decodificar(raw):
    for e in ENCODINGS:
        try:
            return raw.decode(e), e
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", "replace"), "latin-1/replace"


def detectar_sep(linea):
    return max((";", ",", "\t", "|"), key=linea.count)


def revisar(ruta):
    print("=" * 78)
    print(os.path.basename(ruta))
    print("=" * 78)
    with open(ruta, "rb") as fh:
        raw = fh.read()
    texto, enc = decodificar(raw)
    sep = detectar_sep(texto.split("\n", 1)[0])
    n_lineas_fisicas = texto.count("\n")
    n_comillas = texto.count('"')
    cr_sueltos = texto.count("\r") - texto.count("\r\n")
    print("   encoding=%s  sep=%r  lineas fisicas=%s  comillas totales=%s"
          % (enc, sep, format(n_lineas_fisicas, ","), format(n_comillas, ",")))
    print("   retornos de carro SUELTOS dentro de campos: %s"
          % format(cr_sueltos, ","))
    if cr_sueltos:
        i = texto.find("\r")
        while i != -1 and texto[i:i + 2] == "\r\n":
            i = texto.find("\r", i + 2)
        if i != -1:
            print("        ejemplo: ...%s[\\r]%s..."
                  % (texto[max(0, i - 45):i].replace("\n", "|"),
                     texto[i + 1:i + 46].replace("\n", "|")))

    # --- pasada 1: lectura normal, contando errores y su posicion ---------
    lector = csv.reader(io.StringIO(texto), delimiter=sep, quotechar='"')
    n_ok = n_err = 0
    ejemplos = []
    ncols = None
    while True:
        try:
            fila = next(lector)
        except StopIteration:
            break
        except csv.Error as e:
            n_err += 1
            if len(ejemplos) < 5:
                ejemplos.append((lector.line_num, str(e)))
            if n_err > 20000:
                ejemplos.append((lector.line_num, "...abortado, demasiados"))
                break
            continue
        if ncols is None:
            ncols = len(fila)
        n_ok += 1

    print("   [normal    ] filas leidas=%s  errores=%s  columnas cabecera=%s"
          % (format(n_ok, ","), format(n_err, ","), ncols))
    for ln, msg in ejemplos:
        print("        linea ~%s : %s" % (format(ln, ","), msg))

    # --- pasada 2: QUOTE_NONE ---------------------------------------------
    lector2 = csv.reader(io.StringIO(texto), delimiter=sep,
                         quoting=csv.QUOTE_NONE)
    n_ok2 = n_err2 = 0
    anchos = {}
    while True:
        try:
            fila = next(lector2)
        except StopIteration:
            break
        except csv.Error:
            n_err2 += 1
            continue
        n_ok2 += 1
        anchos[len(fila)] = anchos.get(len(fila), 0) + 1

    top = sorted(anchos.items(), key=lambda kv: -kv[1])[:4]
    print("   [QUOTE_NONE] filas leidas=%s  errores=%s"
          % (format(n_ok2, ","), format(n_err2, ",")))
    print("        anchos de fila mas frecuentes: %s"
          % ", ".join("%d cols x%s" % (k, format(v, ",")) for k, v in top))

    # --- veredicto ---------------------------------------------------------
    # --- pasada 3: con los saltos normalizados (el parche) ----------------
    normal = texto.replace("\r\n", "\n").replace("\r", " ")
    lector3 = csv.reader(io.StringIO(normal), delimiter=sep, quotechar='"')
    n_ok3 = n_err3 = 0
    while True:
        try:
            next(lector3)
        except StopIteration:
            break
        except csv.Error:
            n_err3 += 1
            continue
        n_ok3 += 1
    print("   [normalizado] filas leidas=%s  errores=%s   <- lo que hace el parche"
          % (format(n_ok3, ","), format(n_err3, ",")))

    print("   " + "-" * 74)
    if n_err == 0 and cr_sueltos == 0:
        print("   VEREDICTO: este archivo NO es el que rompe. Seguir con otro.")
    elif n_err3 == 0 and cr_sueltos > 0:
        print("   VEREDICTO: la causa son los %s retorno(s) de carro sueltos."
              % format(cr_sueltos, ","))
        print("   Alguien tipeo un salto de linea dentro del nombre o la")
        print("   descripcion de una licitacion. No es corrupcion.")
        print("   ARREGLO: parche_lector_tolerante.py. No se pierde ninguna fila.")
    elif n_err2 == 0 and top and top[0][0] == ncols and \
            n_ok2 >= n_lineas_fisicas * 0.99:
        print("   VEREDICTO: el archivo NO usa comillas de verdad. Hay un '\"'")
        print("   suelto en algun campo de texto y csv.reader entra en modo")
        print("   'campo entrecomillado' y se come lineas.")
        print("   ARREGLO: leer con QUOTE_NONE  ->  MP_QUOTE_NONE=1")
        print("   Y OJO: con lectura normal se perdian %s filas en silencio."
              % format(max(0, n_ok2 - n_ok), ","))
    else:
        print("   VEREDICTO: hay filas rotas de verdad (saltos de linea dentro")
        print("   de un campo sin comillas). QUOTE_NONE no las salva.")
        print("   ARREGLO: el lector tolerante las omite y las cuenta.")
        print("   Revisar que el total omitido sea despreciable antes de citar.")
    print()
    return n_err, n_err2


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    destino = sys.argv[1]
    if os.path.isfile(destino):
        rutas = [destino]
    else:
        rutas = sorted(os.path.join(dp, fn)
                       for dp, _dn, fns in os.walk(destino)
                       for fn in fns if fn.lower().endswith(".csv"))
    for r in rutas:
        try:
            revisar(r)
        except Exception as e:
            print("   [error] %s: %s\n" % (r, e))
    return 0


if __name__ == "__main__":
    sys.exit(main())
