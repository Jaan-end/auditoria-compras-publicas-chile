# CAP-14 · Discordancia de código ONU vs. oferente único — ficha citable

`[MEDIDO]` · **`run_id = bc06eb233fc5`** · 3-sep-2026 (sesión 37) · celda:
`notebooks/CAP14_oc030_vs_oferentes.py` · salida cruda: `resultados/cap14_oc030_salida_cruda.txt`

**Cómo se cita:** siempre como **asociación**, con la tabla 14.C.1 al lado y la salvedad de
causalidad inversa **en la misma frase**. Nunca como efecto, nunca el OR crudo solo.

## La cifra

| | |
|---|---|
| Gate 14.C.1 | 962.336 licitaciones pareadas vs. 964.171 de la Pregunta 1 · Δ = 1.835 (**0,19 %**) → **pasa** |
| OR crudo (2×2) | 2,1111 — **no citable solo**, confundido por sector/año/monto |
| **OR de Mantel-Haenszel** | **1,9989** · IC 95 % **[1,9159 – 2,0856]** (Robins-Breslow-Greenland) |
| Estratos | 128 usados (≥ 30 licitaciones) · 214 descartados por chicos o sin variación |
| Placebo (paridad del hash de la clave) | OR_MH **0,9962** · IC 95 % [0,9864 – 1,0061] · umbral predeclarado [0,9091 – 1,10] → **limpio** |
| Veredicto vs. 4 criterios predeclarados | **ASOCIACIÓN SOSTENIDA** (magnitud · IC excluye 1 · signo estable · placebo limpio) |

## La tabla que se lee primero (14.C.1)

| Nivel de coincidencia lic ↔ OC | licitaciones | % universo | % oferente único |
|---|---:|---:|---:|
| 8 díg · código idéntico | 942.316 | 97,92 % | **20,42 %** |
| 6 díg · misma clase | 3.490 | 0,36 % | 29,43 % |
| 4 díg · misma familia | 3.402 | 0,35 % | 30,04 % |
| 2 díg · mismo segmento | 3.479 | 0,36 % | 29,26 % |
| 0 díg · **sin nada en común** | 9.649 | 1,00 % | **35,28 %** |

## Lo que la tabla dice y el odds ratio binario no

**No hay gradiente dosis-respuesta: hay un escalón.** Los tres niveles intermedios son
indistinguibles entre sí (29,26 % · 29,43 % · 30,04 %, sobre n ≈ 3.400 cada uno). El salto grande
está entre **código idéntico (20,42 %)** y **cualquier otra cosa (≈ 29-35 %)**, con un segundo
escalón menor en la discordancia total.

Consecuencia operativa: el corte binario actual agrupa los tres niveles intermedios **del lado
concordante**, junto a las 942.316 idénticas, y compara 35,28 % vs 20,52 %. Un corte alternativo
—**idéntico vs. todo lo demás** (19.020 licitaciones)— separa mejor lo que los datos muestran.
**Pendiente:** correr el mismo estimador con ese corte y con el corte a 4 dígitos, y reportar los
tres. Si el veredicto se sostiene en los tres, la elección del corte deja de ser un flanco
(ver `SOLIDEZ-METODOLOGICA.md` A14).

## Las tres salvedades, obligatorias

1. **Asociación, nunca efecto.** Diseño transversal: la causalidad inversa es igual de plausible.
   Un rubro difícil de codificar puede atraer pocos oferentes *y* producir códigos discordantes.
2. **Solo E1** — licitaciones adjudicadas que generaron OC. Las desiertas y canceladas quedan fuera
   **por construcción**: sin OC el predictor no existe. Eso es E2, `[POR VERIFICAR]`, no medido.
   Es un colisionador identificado, no un descuido.
3. **Exclusión no aleatoria en el denominador.** `numero_oferentes` viene del corte descargado; una
   licitación adjudicada después de esa fecha puede quedar fuera de las 962.336. **No cuantificada.**
   La cota está en el bloque V-2 de `local/auditoria_vintage.py`.

## Estado del archivo de evidencia

⚠ `resultados/cap14_oc030_salida_cruda.txt` es una **transcripción**, no una exportación byte a
byte (no hubo `dbutils.fs.put` ni redirección de stdout). Su propia nota de proveniencia lo
declara, y los saltos de línea colapsados se conservaron sin reconstruir. El **contenido** —cifras,
etiquetas, umbrales, textos de veredicto— fue cotejado palabra por palabra contra la celda, y la
aritmética interna cuadra (942.316 + 3.490 + 3.402 + 3.479 + 9.649 = 962.336; OR crudo recalculado
= 2,1111).

Es el **tercer** archivo del proyecto en esta condición, junto a los dos de CAP-18.
Ver `SOLIDEZ-METODOLOGICA.md` A12: los tres se cierran re-ejecutando y exportando.
