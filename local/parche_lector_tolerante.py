# -*- coding: utf-8 -*-
"""
PARCHE · lector robusto para auditoria_vintage_v2.py
====================================================
CAUSA DEL ERROR de lic_2026-3.csv
    _csv.Error: new-line character seen in unquoted field

Un campo de texto trae un RETORNO DE CARRO SUELTO (\\r sin \\n). El texto se
mete en io.StringIO(), que con su newline por defecto NO trata el \\r solo
como fin de linea, asi que el caracter queda DENTRO del campo y csv.reader
aborta. No es un archivo corrupto: es un salto de linea que alguien escribio
en el nombre o la descripcion de una licitacion.

QUE HACE ESTE PARCHE
    1. _decodificar()  normaliza los saltos: \\r\\n -> \\n  y  \\r suelto -> ' '.
       Esto arregla la causa, sin perder ni una fila.  <- el arreglo de verdad
    2. leer_filas()    ademas queda tolerante: si aun asi una fila es
       ilegible, la omite, la cuenta y la declara, en vez de botar la corrida.
    3. MP_QUOTE_NONE=1 fuerza QUOTE_NONE por si aparece el otro patron
       (una comilla suelta al inicio de un campo).

Como curva_maduracion.py importa de este modulo, arregla los dos scripts.

    python parche_lector_tolerante.py "C:\\...\\Codigos\\auditoria_vintage_v2.py"

Deja copia en <archivo>.bak-lector antes de tocar nada.
"""
import io, os, re, shutil, sys

NUEVO_DECOD = '''def _decodificar(raw_bytes):
    """Devuelve (texto, encoding_usado). ChileCompra exporta en cp1252 casi
    siempre, pero hay archivos en utf-8 con BOM.

    PARCHE 29-ago-2026 · normalizacion de saltos de linea.
    Algunos campos de texto traen un \\r suelto (un salto tipeado dentro del
    nombre de la licitacion). io.StringIO no lo trata como fin de linea, el
    caracter queda dentro del campo y csv.reader aborta con
    'new-line character seen in unquoted field'. Se normaliza aqui, en el
    unico punto por el que pasan todos los archivos:
        \\r\\n -> \\n   (fin de linea Windows, se respeta)
        \\r    -> ' '  (control suelto dentro de un campo, se neutraliza)
    No se pierde ninguna fila y el conteo de lineas no cambia."""
    texto = None
    enc_usado = "latin-1/replace"
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            texto = raw_bytes.decode(enc)
            enc_usado = enc
            break
        except UnicodeDecodeError:
            continue
    if texto is None:
        texto = raw_bytes.decode("latin-1", errors="replace")

    n_cr = texto.count("\\r")
    n_crlf = texto.count("\\r\\n")
    sueltos = n_cr - n_crlf
    if sueltos > 0:
        CR_SUELTOS["total"] += sueltos
        print("   [normalizado] %s retorno(s) de carro suelto(s) dentro de "
              "campos; se reemplazan por espacio." % format(sueltos, ","))
    texto = texto.replace("\\r\\n", "\\n").replace("\\r", " ")
    return texto, enc_usado
'''

NUEVO_LEER = '''def leer_filas(abridor, columnas_pedidas, verbose=False, quoting=None):
    """Generador de dicts {clave_logica: valor} para las columnas resueltas.
    Devuelve primero un dict de resolucion (clave -> nombre real o None).

    PARCHE 29-ago-2026 · tolerante a filas rotas. Un csv.Error en una fila ya
    no aborta la corrida: la fila se omite, se cuenta en FILAS_OMITIDAS y se
    declara en pantalla. Con MP_QUOTE_NONE=1 se lee con QUOTE_NONE, que es lo
    correcto cuando el CSV no usa comillas de verdad y trae un '"' suelto."""
    fh, enc = abridor()
    primera = fh.readline()
    fh.seek(0)
    sep = _detectar_sep(primera)

    if quoting is None and os.environ.get("MP_QUOTE_NONE", "") == "1":
        quoting = csv.QUOTE_NONE
    if quoting == csv.QUOTE_NONE:
        lector = csv.reader(fh, delimiter=sep, quoting=csv.QUOTE_NONE)
    else:
        lector = csv.reader(fh, delimiter=sep, quotechar='"')

    try:
        cabecera = next(lector)
    except StopIteration:
        return {}, iter(()), cabecera_vacia(), enc, sep
    except csv.Error as e:
        print("   [ABORTA ARCHIVO] la cabecera misma esta rota: %s" % e)
        return {}, iter(()), cabecera_vacia(), enc, sep
    cabecera = [c.strip().lstrip("\\ufeff") for c in cabecera]
    res = {k: resolver(cabecera, k, verbose=verbose) for k in columnas_pedidas}
    idx = {k: (cabecera.index(v) if v in cabecera else None)
           for k, v in res.items() if v is not None}

    def _gen():
        malas = 0
        while True:
            try:
                fila = next(lector)
            except StopIteration:
                break
            except csv.Error as e:
                malas += 1
                if malas <= 3:
                    print("   [aviso] fila ilegible omitida (linea ~%s): %s"
                          % (getattr(lector, "line_num", "?"), e))
                elif malas == 4:
                    print("   [aviso] ...se omiten los avisos siguientes.")
                if malas > LIMITE_FILAS_OMITIDAS:
                    print("   [ABORTA ARCHIVO] mas de %d filas ilegibles. "
                          "Correr diagnostico_csv.py sobre este archivo antes "
                          "de citar cualquier cifra de este mes."
                          % LIMITE_FILAS_OMITIDAS)
                    break
                continue
            if not fila:
                continue
            d = {}
            for k, i in idx.items():
                d[k] = fila[i].strip() if i < len(fila) else None
            yield d
        if malas:
            FILAS_OMITIDAS["total"] += malas
            print("   [RESUMEN] %s fila(s) omitida(s) por ilegibles en este "
                  "archivo. Declararlo antes de citar este mes."
                  % format(malas, ","))

    return res, _gen(), cabecera, enc, sep
'''

CABECERA_EXTRA = '''
# --- parche 29-ago-2026 · lectura robusta ------------------------------------
# CR_SUELTOS    : retornos de carro dentro de campos, neutralizados al decodificar.
# FILAS_OMITIDAS: filas que aun asi resultaron ilegibles y se saltaron.
# Las dos cifras se imprimen al final: si alguna es grande, el mes no se cita
# sin declararlo.
LIMITE_FILAS_OMITIDAS = 5000
CR_SUELTOS = Counter()
FILAS_OMITIDAS = Counter()
'''


def _reemplazar(src, nombre, nuevo, sigue):
    pat = r'^def %s\(.*?(?=\n\ndef %s)' % (nombre, sigue)
    m = re.search(pat, src, re.S | re.M)
    if not m:
        return None
    return src[:m.start()] + nuevo.rstrip("\n") + src[m.end():]


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return 2
    ruta = sys.argv[1]
    if not os.path.isfile(ruta):
        print("no existe: %s" % ruta)
        return 2

    with io.open(ruta, "r", encoding="utf-8") as fh:
        src = fh.read()

    if "LIMITE_FILAS_OMITIDAS" in src:
        print("[ya parchado] este archivo ya tiene la lectura robusta.")
        return 0

    paso1 = _reemplazar(src, "_decodificar", NUEVO_DECOD, "_detectar_sep")
    if paso1 is None:
        print("[ABORTA] no encontre _decodificar(). No se toco nada.")
        return 1
    paso2 = _reemplazar(paso1, "leer_filas", NUEVO_LEER, "cabecera_vacia")
    if paso2 is None:
        print("[ABORTA] no encontre leer_filas(). No se toco nada.")
        return 1

    ancla = "\ndef _decodificar("
    if ancla not in paso2:
        print("[ABORTA] no encontre donde insertar las constantes.")
        return 1
    final = paso2.replace(ancla, CABECERA_EXTRA + ancla, 1)

    shutil.copyfile(ruta, ruta + ".bak-lector")
    with io.open(ruta, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(final)

    print("[OK] parchado. Copia de seguridad en %s.bak-lector" % ruta)
    print()
    print("Volver a correr, sin mas:")
    print('   python curva_maduracion.py --nuevo "...\\new\\Lic"')
    print()
    print("Si aun asi falla, diagnosticar el archivo:")
    print('   python diagnostico_csv.py "...\\new\\Lic\\lic_2026-3.csv"')
    print('   y si dice QUOTE_NONE:   $env:MP_QUOTE_NONE="1"')
    return 0


if __name__ == "__main__":
    sys.exit(main())
