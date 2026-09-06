# -*- coding: utf-8 -*-
"""
graficos_capstone.py — Capstone Big Data · Mercado Publico 2017-2026
====================================================================
FIGURAS DEL INFORME, DEL DASHBOARD Y DE LA DEFENSA.

CORRE FUERA DE DATABRICKS. Cero cupo de Spark. Se alimenta de (a) las cifras
congeladas, escritas aqui con su RUN_ID de procedencia, y (b) los CSV que ya
exporta CAP-6 y los que escribe `auditoria_vintage.py`.

    pip install matplotlib
    python graficos_capstone.py --csv ./csv --salida ./figuras

Cada figura se salta sola, con un mensaje claro, si le falta su CSV. Ninguna
figura inventa un dato: si no esta, no se dibuja.

--------------------------------------------------------------------
POR QUE FUERA DEL NOTEBOOK
--------------------------------------------------------------------
El cupo de Databricks Free Edition corta corridas a media celda (§1.18: una
corrida por dia). Un grafico no puede costar una accion de Spark. CAP-6 ya deja
las tablas citadas en CSV sin computo extra; los graficos se hacen desde ahi, se
versionan en el repositorio y se regeneran gratis cuantas veces haga falta.

--------------------------------------------------------------------
DISCIPLINA DE PROCEDENCIA
--------------------------------------------------------------------
Toda cifra escrita a mano en este archivo lleva al lado el RUN_ID que la
produjo. Si una cifra cambia en el informe, se cambia AQUI y se regenera; no se
edita la imagen. Las cifras sin RUN_ID son de fuente externa y llevan su URL.
"""

import argparse
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# =====================================================================
# PALETA — validada para daltonismo, orden fijo, nunca ciclada
# =====================================================================
# Slots 1-3 de la paleta categorica de referencia. Se usan en ORDEN FIJO: el
# color sigue a la entidad, no a su posicion en el ranking. Ninguna figura de
# aqui pasa de 3 series, que es el limite validado para pares no adyacentes.
AZUL     = "#2a78d6"   # slot 1
NARANJA  = "#eb6834"   # slot 2
AQUA     = "#1baf7a"   # slot 3
TINTA    = "#0b0b0b"
TINTA_2  = "#52514e"
TINTA_3  = "#8a8880"
SUPERF   = "#fcfcfb"
REJILLA  = "#e4e3de"

plt.rcParams.update({
    "figure.facecolor": SUPERF,
    "axes.facecolor": SUPERF,
    "savefig.facecolor": SUPERF,
    "font.family": "DejaVu Sans",
    "font.size": 10.5,
    "axes.edgecolor": REJILLA,
    "axes.labelcolor": TINTA_2,
    "text.color": TINTA,
    "xtick.color": TINTA_2,
    "ytick.color": TINTA_2,
    "xtick.labelsize": 9.5,
    "ytick.labelsize": 9.5,
    "axes.titlesize": 12.5,
    "axes.titleweight": "bold",
    "axes.grid": False,
    "figure.dpi": 130,
})


def cl_pct(v, dec=2):
    """Porcentaje en formato chileno: coma decimal."""
    return ("%.*f%%" % (dec, v)).replace(".", ",")


def cl_num(v, dec=0):
    """Numero en formato chileno: punto de miles, coma decimal."""
    s = "{:,.{d}f}".format(v, d=dec)
    return s.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def _limpiar(ax, ejes=("top", "right")):
    for e in ejes:
        ax.spines[e].set_visible(False)
    for e in ("left", "bottom"):
        ax.spines[e].set_color(REJILLA)
    ax.tick_params(length=0)


def _pie(ax, texto, dy=-0.16):
    ax.annotate(texto, xy=(0, 0), xytext=(0, dy), xycoords="axes fraction",
                textcoords="axes fraction", fontsize=8, color=TINTA_3,
                va="top", ha="left", wrap=True)


def _guardar(fig, salida, nombre):
    os.makedirs(salida, exist_ok=True)
    for ext in ("png", "pdf"):
        ruta = os.path.join(salida, "%s.%s" % (nombre, ext))
        fig.savefig(ruta, bbox_inches="tight", pad_inches=0.28)
    plt.close(fig)
    print("   [OK] %s.png / .pdf" % nombre)


def _leer_csv(carpeta, nombre):
    """Devuelve lista de dicts, o None si el archivo no esta."""
    if not carpeta:
        return None
    ruta = os.path.join(carpeta, nombre)
    if not os.path.exists(ruta):
        return None
    for enc in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with open(ruta, newline="", encoding=enc) as fh:
                muestra = fh.readline()
                fh.seek(0)
                sep = ";" if muestra.count(";") >= muestra.count(",") else ","
                return list(csv.DictReader(fh, delimiter=sep))
        except UnicodeDecodeError:
            continue
    return None


def _num(s):
    if s is None:
        return None
    s = str(s).strip().replace(".", "").replace(",", ".") if "," in str(s) else str(s).strip()
    try:
        return float(s)
    except ValueError:
        return None


def _saltar(nombre, motivo):
    print("   [salta] %s — %s" % (nombre, motivo))


# =====================================================================
# FIGURA 1 · Composicion del universo de la Pregunta 1
# =====================================================================
def fig1_universo_p1(salida):
    """El titular de la Pregunta 1 es la EXCLUSION, no la tasa. El grafico
    tiene que mostrar la exclusion. [MEDIDO] RUN_ID_CAP=1314c4f6d481,
    confirmado por 70680deaec66."""
    A, B, C = 66.75, 13.33, 19.73          # % del universo de 964.171 licitaciones
    nA, nB, nC = 643562, 128543, 190232

    fig, ax = plt.subplots(figsize=(9.6, 3.1))
    izq = 0.0
    piezas = [
        ("A · monolinea legitima\n1 linea, <=1 codigo", A, nA, AZUL),
        ("B · testimonial multilinea\n>=2 lineas, <=1 codigo", B, nB, NARANJA),
        ("C · informativo\n>=2 codigos distintos", C, nC, AQUA),
    ]
    # Las etiquetas de B y C se escalonan en dos filas: los segmentos son
    # angostos y en una sola fila el texto se pisa.
    filas_y = [-0.42, -1.16, -0.42]
    for i, (etiqueta, pct, n, color) in enumerate(piezas):
        ax.barh([0], [pct], left=[izq], height=0.46, color=color,
                edgecolor=SUPERF, linewidth=2.0)   # 2px de aire entre segmentos
        centro = izq + pct / 2.0
        ax.text(centro, 0, cl_pct(pct), ha="center", va="center",
                color="white", fontsize=11.5, fontweight="bold")
        y_et = filas_y[i]
        if y_et < -0.9:      # fila baja: se dibuja una guia hasta la barra
            ax.plot([centro, centro], [-0.30, y_et + 0.05], color=REJILLA, lw=1.0)
        ax.text(centro, y_et, etiqueta, ha="center", va="top",
                color=TINTA_2, fontsize=8.6, linespacing=1.35)
        ax.text(centro, 0.40, cl_num(n), ha="center", va="bottom",
                color=TINTA_3, fontsize=8.4)
        izq += pct

    # llave que marca el hallazgo: A+B = 80,08%
    ax.plot([0, A + B], [0.72, 0.72], color=TINTA, lw=1.4)
    ax.plot([0, 0], [0.66, 0.72], color=TINTA, lw=1.4)
    ax.plot([A + B, A + B], [0.66, 0.72], color=TINTA, lw=1.4)
    ax.text((A + B) / 2.0, 0.82,
            "80,08% declara A LO MAS UN codigo de producto distinto",
            ha="center", va="bottom", fontsize=10.2, fontweight="bold", color=TINTA)

    ax.set_xlim(0, 100)
    ax.set_ylim(-1.85, 1.35)
    ax.axis("off")
    ax.set_title("La tasa de consistencia ONU describe solo al 19,73% del universo",
                 loc="left", pad=26)
    _pie(ax, dy=-0.06, texto="964.171 licitaciones pareadas · [MEDIDO] RUN_ID_CAP=1314c4f6d481 "
             "(confirmado por 70680deaec66).\nLa patologia real —varias lineas y un "
             "solo codigo— es el 13,33% (B), no el 80%: una licitacion de una linea "
             "con un codigo no es una falla.")
    _guardar(fig, salida, "fig1_universo_pregunta1")


# =====================================================================
# FIGURA 2 · Embudo de trazabilidad
# =====================================================================
def fig2_embudo(salida, csv_dir):
    """Una serie -> un solo tono, sin leyenda (el titulo la nombra)."""
    filas = _leer_csv(csv_dir, "cap9_cascada.csv")
    if filas:
        etiquetas = [f.get("peldano") or f.get("etapa") or "" for f in filas]
        valores = [_num(f.get("n") or f.get("lineas")) for f in filas]
        etiquetas = [e for e, v in zip(etiquetas, valores) if v]
        valores = [v for v in valores if v]
    else:
        # Fallback con las cifras congeladas documentadas.
        etiquetas = ["Universo de lineas de OC",
                     "Con licitacion de origen declarada\n(subconjunto enlazable)"]
        valores = [51451528.0, 16901188.0]
        print("   [aviso] fig2 usa solo los dos peldanos documentados: no se")
        print("           encontro 'cap9_cascada.csv' con la cascada completa.")

    fig, ax = plt.subplots(figsize=(9.2, 0.95 * len(valores) + 2.0))
    y = list(range(len(valores)))[::-1]
    ax.barh(y, valores, height=0.5, color=AZUL)
    for yy, v, et in zip(y, valores, etiquetas):
        ax.text(v * 1.012, yy, cl_num(v),
                va="center", ha="left", fontsize=10, color=TINTA, fontweight="bold")
        pct = 100.0 * v / valores[0]
        ax.text(v * 1.012, yy - 0.30, "%s del universo" % cl_pct(pct),
                va="center", ha="left", fontsize=8.4, color=TINTA_3)
    ax.set_yticks(y)
    ax.set_yticklabels(etiquetas, fontsize=9.6, color=TINTA_2, linespacing=1.4)
    ax.set_xlim(0, max(valores) * 1.30)
    ax.set_xticks([])
    _limpiar(ax, ("top", "right", "bottom"))
    ax.set_title("Trazabilidad: cuanto del universo declara de donde viene",
                 loc="left", pad=16)
    _pie(ax, "Trazabilidad no circular peldano 2->3: 73,78% por CONTEO, 72,63% por "
             "MONTO. [MEDIDO] RUN_ID_CAP=1314c4f6d481.\nNO usar el G6 de 99,83% "
             "(CAP-3.B): es circular. Ancla externa [FUENTE] Soylu et al. (2022), "
             "Information 13(2):99 — 9% de los avisos de adjudicacion enlazan "
             "explicitamente licitacion y contrato.")
    _guardar(fig, salida, "fig2_embudo_trazabilidad")


# =====================================================================
# FIGURA 3 · Reconciliacion externa 2024
# =====================================================================
def fig3_reconciliacion(salida):
    """Por que se lidera con el conteo y no con los montos. Una sola serie:
    la razon contra la cifra oficial. La linea de 1,00 es la referencia."""
    varas = [
        ("Numero de ordenes de compra", 0.9825, False),
        ("precio x cantidad, saneado", 0.7425, False),
        ("saneado x 1,19 (con IVA)", 0.8836, False),
        ("monto_total_oc_clp", 1.9007, True),      # descartada
    ]
    fig, ax = plt.subplots(figsize=(9.2, 3.9))
    y = list(range(len(varas)))[::-1]
    for yy, (et, v, descartada) in zip(y, varas):
        if descartada:
            ax.barh([yy], [v], height=0.5, color="none", edgecolor=TINTA_3,
                    linewidth=1.3, hatch="////")
        else:
            ax.barh([yy], [v], height=0.5, color=AZUL)
        # columna fija de valores a la derecha: pegados a la barra chocarian
        # con la linea de referencia del 100
        ax.text(2.26, yy, cl_num(v * 100, 2), va="center", ha="right",
                fontsize=10, color=TINTA, fontweight="bold")
        if descartada:
            ax.text(v / 2.0, yy - 0.44,
                    "DESCARTADA — su razon oscila entre 1,26 y 6,56 segun el ano",
                    va="top", ha="center", fontsize=8.3, color=TINTA_3)
    ax.axvline(1.0, color=TINTA, lw=1.2, ls="--", zorder=0)
    ax.text(1.0, len(varas) - 0.52, " cifra oficial de ChileCompra",
            fontsize=8.6, color=TINTA_2, ha="left", va="bottom")
    ax.set_yticks(y)
    ax.set_yticklabels([v[0] for v in varas], fontsize=9.6, color=TINTA_2)
    ax.set_xlim(0, 2.30)
    ax.set_ylim(-1.25, len(varas) - 0.05)
    ax.set_xlabel("razon pipeline / cifra oficial 2024 (100 = coincidencia exacta)",
                  fontsize=9.3, labelpad=8)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _p: cl_num(x * 100)))
    _limpiar(ax)
    ax.set_title("El pipeline captura el 98,25% de las ordenes de compra de 2024",
                 loc="left", pad=16)
    _pie(ax, "1.996.187 OC del pipeline vs 2.031.670 oficiales. [MEDIDO] "
             "RUN_ID_CAP=1314c4f6d481.\n[FUENTE] ChileCompra, 09-jun-2025: "
             "\"durante el ano 2024 las entidades emitieron 2.031.670 ordenes de "
             "compra\".\nUna orden de compra no depende del IVA, del tipo de cambio "
             "ni de si el monto oficial incluye cargos: por eso se lidera con el conteo.")
    _guardar(fig, salida, "fig3_reconciliacion_2024")


# =====================================================================
# FIGURA 4 · Pregunta 2 — serie interrumpida (necesita CSV)
# =====================================================================
def fig4_pregunta2(salida, csv_dir):
    filas = _leer_csv(csv_dir, "cap2_serie_onu_por_anio.csv")
    if not filas:
        _saltar("fig4_pregunta2_serie_interrumpida",
                "falta 'cap2_serie_onu_por_anio.csv' (exportalo desde CAP-6 con "
                "columnas: anio, pct_completa, pct_restringida)")
        return
    anios, comp, restr = [], [], []
    for f in filas:
        a = _num(f.get("anio") or f.get("_year"))
        if a is None:
            continue
        anios.append(int(a))
        comp.append(_num(f.get("pct_completa") or f.get("pct")))
        restr.append(_num(f.get("pct_restringida")))
    if not anios:
        _saltar("fig4_pregunta2_serie_interrumpida", "el CSV no tiene columna 'anio'")
        return

    fig, ax = plt.subplots(figsize=(9.2, 4.4))
    ax.plot(anios, comp, color=AZUL, lw=2.0, marker="o", ms=5.5,
            label="serie completa")
    if any(v is not None for v in restr):
        ax.plot(anios, restr, color=NARANJA, lw=2.0, marker="o", ms=5.5,
                label="restringida a los 1.079 organismos presentes en 2024 y 2025")
    ax.axvline(2024.95, color=TINTA_3, lw=1.2, ls="--", zorder=0)
    ax.text(2024.95, ax.get_ylim()[1], " art. 20 bis · 12-dic-2024",
            fontsize=8.8, color=TINTA_2, ha="left", va="top")
    ax.set_ylabel("% de lineas con codigo ONU valido", fontsize=9.3)
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)
    leg = ax.legend(frameon=False, fontsize=9, loc="upper left")
    for t in leg.get_texts():
        t.set_color(TINTA_2)
    ax.set_title("Pregunta 2 — la mejora no aparece, y no es por cambio de poblacion",
                 loc="left", pad=16)
    _pie(ax, "Delta 2024->2025: +0,28 pp sin restringir, +0,30 pp restringido a los "
             "mismos organismos. [MEDIDO] RUN_ID_CAP=70680deaec66.\nEl confundidor "
             "del 12-dic-2024 se probo y el indicador sobrevive: el cambio de "
             "poblacion NO explica la ausencia de mejora.")
    _guardar(fig, salida, "fig4_pregunta2_serie_interrumpida")


# =====================================================================
# FIGURA 5 · OCDS — la degradacion de la publicacion de hitos
# =====================================================================
def fig5_ocds_milestones(salida):
    """[FUENTE·SA] F21 §2.1 — data.open-contracting.org/en/publication/10 y /144.
    Consultado el 27-ago-2026. Regla 5: abrir la pagina una vez antes de citar."""
    fig, ax = plt.subplots(figsize=(8.4, 4.2))
    etiquetas = ['Publicacion "Historical"\ndic-2018 a abr-2022\n(ya no se actualiza)',
                 'Publicacion activa\nene-2022 a may-2026']
    valores = [42922, 0]
    x = [0, 1]
    ax.bar(x, valores, width=0.42, color=[AZUL, AZUL])
    for xx, v in zip(x, valores):
        ax.text(xx, v + 1600, cl_num(v), ha="center",
                va="bottom", fontsize=13, fontweight="bold", color=TINTA)
    ax.text(1, 9000, "cero hitos publicados\ndesde 2022", ha="center", va="bottom",
            fontsize=9.4, color=NARANJA, fontweight="bold", linespacing=1.35)
    ax.set_xticks(x)
    ax.set_xticklabels(etiquetas, fontsize=9.2, color=TINTA_2, linespacing=1.5)
    ax.tick_params(axis="x", pad=8)
    ax.set_ylabel("hitos de ejecucion contractual (milestones)", fontsize=9.3)
    ax.set_ylim(0, 56000)
    ax.set_xlim(-0.6, 1.6)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: cl_num(v)))
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)
    ax.set_title("Chile publicaba hitos de ejecucion contractual. Dejo de hacerlo.",
                 loc="left", pad=16)
    _pie(ax, dy=-0.30, texto="[FUENTE·SA] data.open-contracting.org/en/publication/10 y /144, "
             "consultado 27-ago-2026 (F21 §2.1).\nSALVEDAD OBLIGATORIA: la "
             "publicacion \"Historical\" solo cubre licitaciones (no Convenio Marco "
             "ni Compra Agil,\nque son la mayoria del volumen). Es evidencia sobre "
             "el ecosistema de publicacion, no sobre el pipeline propio.")
    _guardar(fig, salida, "fig5_ocds_milestones")


# =====================================================================
# FIGURA 6 · Auditoria _year vs fecha_creacion (necesita CSV)
# =====================================================================
def fig6_auditoria_year(salida, csv_dir):
    filas = _leer_csv(csv_dir, "cap9bis_auditoria_year.csv")
    if not filas:
        _saltar("fig6_auditoria_year",
                "falta 'cap9bis_auditoria_year.csv' (columnas: anio, pct_discrepante)")
        return
    anios, pct = [], []
    for f in filas:
        a = _num(f.get("anio") or f.get("_year"))
        p = _num(f.get("pct_discrepante") or f.get("pct"))
        if a is None or p is None:
            continue
        anios.append(int(a))
        pct.append(p)
    if not anios:
        _saltar("fig6_auditoria_year", "el CSV no trae anio/pct utilizables")
        return
    fig, ax = plt.subplots(figsize=(9.0, 3.6))
    ax.bar(anios, pct, width=0.6, color=AZUL)
    for a, p in zip(anios, pct):
        ax.text(a, p + max(pct) * 0.03, cl_pct(p), ha="center", va="bottom",
                fontsize=8.8, color=TINTA)
    ax.axhline(0.5982, color=NARANJA, lw=1.6, ls="--")
    ax.text(anios[0], 0.5982, " 0,5982% del universo completo", fontsize=8.8,
            color=NARANJA, va="bottom", ha="left")
    ax.set_ylabel("% de lineas donde _year no coincide con fecha_creacion",
                  fontsize=9.3)
    ax.set_xticks(anios)
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)
    ax.set_title("_year viene de la carpeta de descarga, no de una fecha del registro",
                 loc="left", pad=16)
    _pie(ax, "307.775 de 51.451.528 lineas = 0,5982%. [MEDIDO] "
             "RUN_ID_CAP=70680deaec66. 2026 es ano parcial (1,79%).\nAcompanar con "
             "esta tabla cualquier serie 'por ano' del informe.")
    _guardar(fig, salida, "fig6_auditoria_year")


# =====================================================================
# FIGURA 7 · Censura por fecha de descarga (necesita el CSV del vintage)
# =====================================================================
def fig7_censura(salida, csv_dir):
    filas = _leer_csv(csv_dir, "v2_censura_por_anio.csv")
    if not filas:
        _saltar("fig7_censura_por_fecha_descarga",
                "falta 'v2_censura_por_anio.csv' (lo escribe auditoria_vintage.py)")
        return
    anios, pct, n = [], [], []
    for f in filas:
        a = _num(f.get("anio_publicacion"))
        p = _num(f.get("pct_censuradas"))
        pres = _num(f.get("presentes_en_corte_viejo"))
        if a is None or p is None:
            continue
        anios.append(int(a))
        pct.append(p)
        n.append(int(pres or 0))
    if not anios:
        _saltar("fig7_censura_por_fecha_descarga", "el CSV vino vacio")
        return
    fig, ax = plt.subplots(figsize=(9.0, 3.9))
    ax.bar(anios, pct, width=0.6, color=NARANJA)
    for a, p, nn in zip(anios, pct, n):
        ax.text(a, p + max(pct) * 0.035, cl_pct(p, 1), ha="center", va="bottom",
                fontsize=9.2, color=TINTA, fontweight="bold")
        # el n va DENTRO de la barra: fuera del eje choca con la etiqueta del eje
        ax.text(a, max(pct) * 0.02, "n=%s" % cl_num(nn), ha="center", va="bottom",
                fontsize=8.0, color="white")
    ax.set_ylabel("% de licitaciones presentes en el corte viejo\npero SIN su "
                  "adjudicacion", fontsize=9.3, linespacing=1.4)
    ax.set_xticks(anios)
    ax.set_xlabel("ano de publicacion de la licitacion", fontsize=9.3, labelpad=8)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: cl_pct(v, 0)))
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)
    ax.set_title("El dato abierto es un corte con fecha: cuanta adjudicacion faltaba",
                 loc="left", pad=16)
    _pie(ax, "Censurada = adjudicada DESPUES de la fecha de descarga del corte "
             "original. Cota INFERIOR: solo cuenta las ya adjudicadas hoy.\nNo es "
             "aleatoria: se concentra en los procesos de ciclo largo (LR, LQ). "
             "Fuente: auditoria_vintage.py, bloque V-2.")
    _guardar(fig, salida, "fig7_censura_por_fecha_descarga")



# =====================================================================
# FIGURA 8 · El corte es un artefacto de descarga
# =====================================================================
# [MEDIDO] paso 0 RUN_ID_P0=770618aa84a4 (corte viejo, Databricks) y paso 0-B
# (re-descarga, PC, 28-ago-2026). Salida cruda:
# resultados/corrida_del_28_paso0BDE_salida_cruda.txt · documento F25 seccion 1.
#
# Cada barra es un mes de publicacion: cuanto mas (o menos) trae la re-descarga
# de agosto de 2026 respecto del corte de trabajo, en licitaciones distintas.
# Tres regimenes, y ninguno es un fenomeno del mercado: son fechas de descarga.
DELTA_MENSUAL = [
    # (anio, mes, viejo, nuevo)
    (2023, 1, 10232, 10233), (2023, 2, 10789, 10792), (2023, 3, 12872, 12877),
    (2023, 4, 11610, 11619), (2023, 5, 13531, 13535), (2023, 6, 12810, 12815),
    (2023, 7, 12881, 12887), (2023, 8, 14002, 14016), (2023, 9, 9088, 9101),
    (2023, 10, 14563, 14577), (2023, 11, 16556, 16636), (2023, 12, 10477, 10496),
    (2024, 1, 11685, 11712), (2024, 2, 12439, 12458), (2024, 3, 12166, 12189),
    (2024, 4, 14848, 14871), (2024, 5, 14096, 14122), (2024, 6, 12596, 12616),
    (2024, 7, 13939, 13955), (2024, 8, 14497, 14516), (2024, 9, 11496, 11510),
    (2024, 10, 15410, 15431), (2024, 11, 15068, 13587), (2024, 12, 8230, 7175),
    (2025, 1, 7775, 7000), (2025, 2, 8475, 7534), (2025, 3, 9302, 8484),
    (2025, 4, 10321, 9383), (2025, 5, 9575, 8725), (2025, 6, 9516, 8711),
    (2025, 7, 10320, 9354), (2025, 8, 9611, 8722), (2025, 9, 8765, 7879),
    (2025, 10, 10041, 9043), (2025, 11, 8627, 8627), (2025, 12, 7070, 7070),
    (2026, 1, 7198, 7199), (2026, 2, 7526, 7526), (2026, 3, 7895, 7900),
    (2026, 4, 7782, 7819), (2026, 5, 7458, 7558), (2026, 6, 8368, 8615),
    (2026, 7, 6317, 7814), (2026, 8, 59, 3050),
]
TOPE_Y = 30.0          # el ultimo mes se sale de escala a proposito; se rotula


def fig8_regimenes_de_descarga(salida):
    """Una serie por regimen, color fijo por regimen (no por posicion)."""
    pct, etiquetas, colores = [], [], []
    for a, m, v, n in DELTA_MENSUAL:
        d = 100.0 * (n - v) / v
        pct.append(d)
        etiquetas.append("%d-%02d" % (a, m))
        if (a, m) >= (2024, 11) and (a, m) <= (2025, 10):
            colores.append(NARANJA)          # ventana de la descarga rodante
        elif d > 0.5:
            colores.append(AZUL)             # el corte nuevo es posterior
        else:
            colores.append(TINTA_3)          # calzan

    x = list(range(len(pct)))
    dibujado = [min(p, TOPE_Y) for p in pct]

    fig, ax = plt.subplots(figsize=(11.6, 4.6))
    ax.bar(x, dibujado, width=0.72, color=colores)
    ax.axhline(0, color=TINTA, lw=1.0)

    # el mes que se sale de escala se rotula con su valor real
    for i, (p, d) in enumerate(zip(pct, dibujado)):
        if p > TOPE_Y:
            ax.text(i, TOPE_Y + 1.0, "▲\n%s" % cl_pct(p, 0), ha="center", va="bottom",
                    fontsize=7.6, color=AZUL, fontweight="bold", linespacing=0.9)

    # marcas de los tres regimenes, sobre la ventana naranja
    i0 = next(i for i, (a, m, *_r) in enumerate(DELTA_MENSUAL) if (a, m) == (2024, 11))
    i1 = next(i for i, (a, m, *_r) in enumerate(DELTA_MENSUAL) if (a, m) == (2025, 10))
    ax.annotate("", xy=(i0 - 0.4, -14.6), xytext=(i1 + 0.4, -14.6),
                arrowprops=dict(arrowstyle="<->", color=NARANJA, lw=1.3))
    ax.text((i0 + i1) / 2.0, -15.4,
            "12 meses contiguos · −9,80% de media (desv. 1,1 pp) · 11.402 licitaciones",
            ha="center", va="top", fontsize=8.8, color=NARANJA, fontweight="bold")

    ax.set_xticks(x[::3])
    ax.set_xticklabels([etiquetas[i] for i in x[::3]], rotation=45, ha="right",
                       fontsize=8.4)
    ax.set_ylim(-19.5, TOPE_Y + 6.0)
    ax.set_ylabel("diferencia de la re-descarga contra el corte de trabajo\n"
                  "(licitaciones distintas por mes de publicacion)",
                  fontsize=9.0, linespacing=1.4)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: cl_pct(v, 0)))
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)

    from matplotlib.patches import Patch
    leg = ax.legend(handles=[
        Patch(facecolor=TINTA_3, label="calzan (misma descarga masiva, ago-2026)"),
        Patch(facecolor=NARANJA, label="ventana de la descarga mes a mes 2024-2025"),
        Patch(facecolor=AZUL, label="el corte nuevo es 11 dias posterior"),
    ], frameon=False, fontsize=8.8, loc="upper left", ncol=1)
    for t in leg.get_texts():
        t.set_color(TINTA_2)

    ax.set_title("El corte de datos no es una foto: es un calendario de descargas",
                 loc="left", pad=16)
    _pie(ax, dy=-0.32, texto=
         "[MEDIDO] corte viejo: paso 0, RUN_ID_P0=770618aa84a4 · re-descarga: paso 0-B, "
         "28-ago-2026 (PC, sin cupo de Spark).\nUna tasa constante con bordes duros no es "
         "un fenomeno del mercado: es un evento de descarga. Los meses que calzan dan "
         "delta cero exacto,\nlo que muestra que la fuente ES reproducible para meses ya "
         "asentados. El ultimo mes esta fuera de escala (59 vs 3.050 licitaciones): "
         "fecha los dos cortes en 1-ago y 11-ago de 2026.")
    _guardar(fig, salida, "fig8_regimenes_de_descarga")


# =====================================================================
# FIGURA 9 · La particion inflada, en una sola vara
# =====================================================================
# [MEDIDO] filas: REFERENCIA_FILAS_V1 (notebook maestro, Celda 2) y paso 0-B.
# Licitaciones distintas: paso 0 RUN_ID_P0=770618aa84a4 y paso 0-B. F26 seccion 1.3.
FILAS_POR_LIC = [
    # (anio, viejo, nuevo)
    (2023, 17.0, 17.1),
    (2024, 19.6, 16.3),
    (2025, 44.0, 17.2),
    (2026, 18.0, 18.1),
]


def fig9_filas_por_licitacion(salida):
    """Dos series -> dos tonos fijos, leyenda presente y valor sobre cada barra."""
    anios = [r[0] for r in FILAS_POR_LIC]
    viejo = [r[1] for r in FILAS_POR_LIC]
    nuevo = [r[2] for r in FILAS_POR_LIC]
    x = list(range(len(anios)))
    w = 0.36

    fig, ax = plt.subplots(figsize=(8.8, 4.4))
    ax.axhspan(17.0, 18.1, color=REJILLA, zorder=0)
    ax.text(len(anios) - 0.42, 17.55, "banda observada en todo\nel resto del periodo",
            fontsize=8.4, color=TINTA_3, va="center", ha="left", linespacing=1.3)

    b1 = ax.bar([i - w / 2 for i in x], viejo, width=w - 0.02, color=AZUL,
                label="corte de trabajo (Parquet del Volumen)")
    b2 = ax.bar([i + w / 2 for i in x], nuevo, width=w - 0.02, color=NARANJA,
                label="re-descarga de agosto de 2026 (CSV)")
    for barras, vals in ((b1, viejo), (b2, nuevo)):
        for b, v in zip(barras, vals):
            ax.text(b.get_x() + b.get_width() / 2, v + 0.7, cl_num(v, 1),
                    ha="center", va="bottom", fontsize=9.4, color=TINTA,
                    fontweight="bold")

    ax.annotate("2,6 veces la densidad\nde cualquier otro anio",
                xy=(2 - w / 2 - 0.20, 42.5), xytext=(0.18, 43.2),
                fontsize=9.0, color=TINTA_2, linespacing=1.35,
                arrowprops=dict(arrowstyle="->", color=TINTA_3, lw=1.1))

    ax.set_xticks(x)
    ax.set_xticklabels([str(a) for a in anios], fontsize=10)
    ax.set_xlim(-0.62, len(anios) + 0.95)
    ax.set_ylim(0, 52)
    ax.set_ylabel("filas del archivo por licitacion distinta", fontsize=9.3)
    ax.grid(axis="y", color=REJILLA, lw=0.8)
    ax.set_axisbelow(True)
    _limpiar(ax)
    leg = ax.legend(frameon=False, fontsize=8.6, loc="upper right",
                    bbox_to_anchor=(1.005, 1.0))
    for t in leg.get_texts():
        t.set_color(TINTA_2)
    ax.set_title("La particion 2025 del corte de trabajo declara el triple de filas",
                 loc="left", pad=16)
    _pie(ax, dy=-0.20, texto=
         "[MEDIDO] F26 seccion 1.3. Tres estimaciones independientes coinciden: aritmetica "
         "mes a mes (2,74x en 2024 · 3,04x en 2025),\nfilas por licitacion (esta figura) y "
         "bytes por fila del Parquet (63,4 contra una mediana de 131,8). Las filas repetidas "
         "comprimen,\npor eso el archivo no crece. Ninguna cifra del informe se calcula "
         "sobre ese conteo: ver F26 seccion 2.")
    _guardar(fig, salida, "fig9_filas_por_licitacion")

# =====================================================================
def main(argv=None):
    p = argparse.ArgumentParser(description="Figuras del capstone. Sin Spark.")
    p.add_argument("--csv", default="csv",
                   help="carpeta con los CSV de CAP-6 y de auditoria_vintage.py")
    p.add_argument("--salida", default="figuras", help="carpeta de salida")
    a = p.parse_args(argv)

    print("Generando figuras en %r (CSV desde %r)\n" % (a.salida, a.csv))
    fig1_universo_p1(a.salida)
    fig2_embudo(a.salida, a.csv)
    fig3_reconciliacion(a.salida)
    fig4_pregunta2(a.salida, a.csv)
    fig5_ocds_milestones(a.salida)
    fig6_auditoria_year(a.salida, a.csv)
    fig7_censura(a.salida, a.csv)
    fig8_regimenes_de_descarga(a.salida)
    fig9_filas_por_licitacion(a.salida)
    print("\nListo. Las figuras que se saltaron necesitan su CSV: exportalo con")
    print("CAP-6 (cero computo de Spark) o corriendo auditoria_vintage.py.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
