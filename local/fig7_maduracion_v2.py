import csv, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

SRC="/root/.claude/uploads/ba1bb9d7-19c7-5976-9eab-05ff139f033c/91c59b76-curva_maduracion.csv"
S1="#2a78d6"; S2="#eb6834"; INK="#0b0b0b"; INK2="#52514e"; MUT="#8a8880"; SURF="#fcfcfb"

rows=[]
for r in csv.DictReader(open(SRC), delimiter=";"):
    rows.append(dict(edad=int(r["antiguedad_meses"]),
                     conf=float(r["pct_con_fecha_adjudicacion"]),
                     adj=float(r["pct_estado_adjudicada"]),
                     fut=float(r["pct_adjudicacion_en_futuro"])))
rows.sort(key=lambda d:-d["edad"])
x=[d["edad"] for d in rows]

plt.rcParams.update({"font.family":"DejaVu Sans","font.size":9,
                     "axes.edgecolor":MUT,"axes.linewidth":.8,
                     "figure.facecolor":SURF,"axes.facecolor":SURF})
fig,(a,b)=plt.subplots(2,1,figsize=(7.4,6.4),sharex=True,
                       gridspec_kw=dict(height_ratios=[1.35,1],hspace=.28))

# --- Panel A -----------------------------------------------------------
a.fill_between(x,[d["adj"] for d in rows],[d["conf"] for d in rows],
               color=S2,alpha=.13,linewidth=0,zorder=1)
a.plot(x,[d["conf"] for d in rows],color=S1,lw=2,marker="o",ms=3.5,
       mec=SURF,mew=.8,zorder=3,label="con FechaAdjudicacion poblada")
a.plot(x,[d["adj"] for d in rows],color=S2,lw=2,marker="s",ms=3.5,
       mec=SURF,mew=.8,zorder=3,label="en estado Adjudicada")
a.set_ylim(70,102); a.set_yticks([75,80,85,90,95,100])
a.axvline(24,color=MUT,lw=.8,ls=(0,(4,3)),zorder=0)
a.annotate("desde 2024-08 la columna\nviene poblada al 100 %",(24,100),
           xytext=(38,93.2),fontsize=7.6,color=INK2,ha="left",
           arrowprops=dict(arrowstyle="-",color=MUT,lw=.7))
a.annotate("brecha 17,1 pp de media (DE 1,6):\nregistros con fecha poblada y sin adjudicacion",
           (30,91.5),xytext=(30,72.0),fontsize=7.6,color=INK2,ha="center",
           arrowprops=dict(arrowstyle="-",color=MUT,lw=.7))
a.set_title("A · La columna esta completa; la adjudicacion no",
            loc="left",fontsize=10.5,color=INK,fontweight="bold",pad=8)
a.set_ylabel("% de las licitaciones del mes",fontsize=8.5,color=INK2)
a.legend(frameon=False,fontsize=8,loc="lower right",handlelength=1.6,
         labelcolor=INK2,ncol=1,bbox_to_anchor=(1.0,-.03))

# --- Panel B -----------------------------------------------------------
b.plot(x,[d["fut"] for d in rows],color=S1,lw=2,marker="o",ms=3.5,
       mec=SURF,mew=.8,zorder=3)
b.set_ylim(-.02,.42)
b.annotate("0,297 %",(8,.297),xytext=(12.5,.355),fontsize=8,color=S1,
           fontweight="bold",arrowprops=dict(arrowstyle="-",color=S1,lw=.8))
b.annotate("el mes mas joven de este corte ya tiene 8 meses.\n"
           "El tramo 0-7 meses —donde `lic_2026-7` marco 38,3 %—\n"
           "no esta en la carpeta y es el unico que importa.",
           (9.2,.10),xytext=(38,.30),fontsize=7.8,color=INK2,ha="left",
           arrowprops=dict(arrowstyle="->",color=MUT,lw=.8,
                           connectionstyle="arc3,rad=.18"))
b.set_title("B · Fechas de adjudicacion que apuntan al futuro",
            loc="left",fontsize=10.5,color=INK,fontweight="bold",pad=8)
b.set_ylabel("% del mes",fontsize=8.5,color=INK2)
b.set_xlabel("antiguedad del mes al momento de la descarga (meses)",
             fontsize=8.5,color=INK2)

for ax in (a,b):
    ax.set_xlim(44.5,6.5)
    ax.grid(axis="y",color="#e6e5e0",lw=.7,zorder=0)
    ax.set_axisbelow(True)
    for s in ("top","right"): ax.spines[s].set_visible(False)
    ax.tick_params(colors=INK2,length=3)
    ax.yaxis.set_major_formatter(FuncFormatter(
        lambda v,_: f"{v:g} %".replace(".",",")))

fig.suptitle("Figura 7 · Maduracion del archivo mensual de licitaciones",
             x=.02,y=.985,ha="left",fontsize=12.5,color=INK,fontweight="bold")
fig.text(.02,.028,
 "[MEDIDO] curva_maduracion.py sobre la re-descarga (36 meses, 2023-01 a 2025-12); "
 "fecha de referencia 2026-08-27 [PROXY por mtime].\n"
 "Panel A: la brecha naranja es estable en toda la ventana, luego NO es maduracion: "
 "es un relleno estructural de la columna.\nPanel B: el 38,3 % de `lic_2026-7` (paso 0-D) "
 "queda fuera de escala y fuera del corte. La figura es honesta pero esta incompleta.",
 fontsize=7.3,color=INK2,va="bottom",linespacing=1.5)
fig.subplots_adjust(left=.10,right=.985,top=.90,bottom=.185)
fig.savefig("figuras/fig7_maduracion_v2.png",dpi=220)
fig.savefig("figuras/fig7_maduracion_v2.pdf")
print("ok")
