# Auditoría adversarial: los quince ataques, y qué falta para resistirlos

> **Estado de este documento.** Es la auditoría adversarial escrita el 11 de septiembre de 2026, dos días
> después de la defensa, y se publica tal como se escribió: es la lista de ataques, no sus resultados. Las
> menciones a documentos que no están en este repositorio apuntan al repositorio de trabajo, que no es público.

**Propósito.** No es una lista de mejoras. Es la lista de **por dónde se cae el proyecto** si un
revisor competente y hostil lo lee con tiempo. Cada entrada tiene: el ataque en la voz del crítico,
qué defensa ya existe, y qué falta exactamente.

**Contexto que juega a favor.** Este proyecto ya hace cuatro cosas que casi ningún trabajo de datos
hace: mide el azar antes de celebrar una tasa, predeclara placebos y reporta cuando fallan, descartó
un resultado propio (CAP-12B) por no replicar un criterio predeclarado teniendo el número en la mano,
y encontró un defecto en su propio corte y lo publicó. **Eso no es el punto de partida habitual.**
Los ataques de abajo son los que quedan.

**Prioridad:** 🔴 cae el hallazgo · 🟠 obliga a matizar · 🟡 objeción de forma.

---

## A1 🔴 · "Tu 94,44 % está calculado sobre la parte fácil"

**El ataque.** Solo el 19,73 % del universo permite hacerse la pregunta. Ese 19,73 % no es una
muestra aleatoria del 100 %: son, presumiblemente, las licitaciones públicas grandes, bien
documentadas, de organismos con capacidad. La tasa alta se mide justo donde se espera que sea alta.

**Lo que ya existe.** El proyecto **lo dice**, y lo dice fuerte: *"el hallazgo es la exclusión, no la
tasa"*. Eso desarma el ataque de mala fe, no el de buena fe.

**Lo que falta, y es lo primero que haría un referee.** Una **tabla de balance entre incluidos y
excluidos** sobre observables: tipo de proceso, tramo de monto, sector, región, año, número de
oferentes, modalidad. Con diferencias estandarizadas, no solo medias. Si los dos grupos difieren
sistemáticamente —que es lo esperado— eso **es un resultado publicable por sí solo**: describe qué
parte del gasto público es opaca. Hoy el proyecto tiene el porcentaje de exclusión pero no el
**retrato de lo excluido**. Es el hueco más grande del trabajo y el más barato de llenar.

---

## A2 🔴 · "Tu baseline nulo permutado está inflado a tu favor"

**El ataque.** El contraste 94,44 % vs 1,38 % impresiona porque el baseline es diminuto. Si la
permutación se hizo **globalmente** —barajando códigos contra el universo entero— el baseline ignora
que las compras se concentran en pocos rubros: dos líneas al azar del mismo hospital tienen mucho
más de 1,38 % de probabilidad de compartir segmento ONU. El baseline correcto se permuta
**dentro de estrato** (mismo año, mismo rubro, mismo tipo de proceso).

**Lo que falta.** Reportar **los dos baselines**, el global y el estratificado, uno al lado del otro.
Si el estratificado sube a, digamos, 15 %, el hallazgo sigue en pie (94,44 vs 15 es enorme) pero la
cifra honesta es otra. Si sube a 70 %, el hallazgo cambia de tamaño y hay que saberlo antes que el
revisor. **No hacer esta prueba es la decisión más riesgosa que quedaría en pie.**

---

## A3 🔴 · "Tus intervalos de confianza son demasiado angostos: las líneas no son independientes"

**El ataque.** Las líneas de una misma licitación comparten redactor, bases, catálogo y criterio de
codificación. Tratarlas como observaciones independientes subestima el error estándar, a veces por
un factor de 3 a 10. Todo IC calculado sobre n = millones de líneas es, bajo este ataque, ficción.

**Lo que falta.** Recalcular **todo** IC y toda prueba con errores estándar **agrupados por
licitación** (y en el nivel organismo, agrupados por organismo), o con bootstrap por clúster.
Reportar el **efecto de diseño** (deff) junto a cada cifra. Es mecánico, es una tarde de trabajo,
y convierte la objeción técnica más letal del set en una fortaleza declarada.

Nota: CAP-11 ya hace muestreo estratificado con reponderación y reporta **n efectivo 49,2** sobre
300 — o sea, el proyecto **ya sabe** que el n nominal engaña. Falta aplicar el mismo criterio al
resto de las cifras.

---

## A4 🔴 · "El catálogo ONU con el que evalúas 2017 no es el que existía en 2017"

**El ataque.** UNSPSC se versiona: se crean, fusionan y retiran códigos. Evaluar diez años de series
contra un catálogo descargado en 2026 introduce error sistemático creciente hacia atrás, y produce
"códigos malformados" que en su momento eran válidos.

**Lo que ya existe.** El proyecto tiene la señal: **11.402 códigos ausentes** entre cortes (paso 0-C,
`run_id 770618aa84a4`), y una pregunta Q4 sobre códigos malformados.

**Lo que falta.** (a) Fijar la **versión** del catálogo usado y declararla; (b) una prueba de
sensibilidad: ¿cuánto cambia la Pregunta 1 si se excluyen las líneas cuyo código no existe en el
catálogo vigente al año de la transacción? Si el cambio es < 1 pp, se declara y se cierra el flanco
para siempre en dos líneas.

---

## A5 🟠 · "Tu corte tiene fecha, y eso sesga en una dirección conocida"

**El ataque.** Una licitación publicada en 2023 y adjudicada en 2025 no aparece completa en un corte
descargado antes. Los procesos **lentos** están subrepresentados en los años recientes. Cualquier
serie temporal que termine cerca de la fecha de descarga tiene censura por la derecha. La Pregunta 2
—cuyo corte es diciembre 2024, cerca del borde— es la más expuesta.

**Lo que ya existe.** Está descubierto, documentado y medido: la re-descarga 2023-2026, la ventana
2024-11..2025-10 con −9,80 %, `local/auditoria_vintage.py`, la figura 7 de maduración. **Es uno de
los aciertos del proyecto.**

**Lo que falta.** Pasar de "existe y lo medimos" a **"corregimos por él"**: usar la curva de
maduración para **reponderar** o para truncar la serie en el punto donde la maduración llega a
≥ 95 %, y rehacer la Pregunta 2 sobre la serie truncada. Si el resultado no cambia, la conclusión
pasa de robusta-por-argumento a robusta-por-prueba.

---

## A6 🟠 · "Tu muestra de validación es de 300 líneas con n efectivo 49"

**El ataque.** El intervalo [3,60 % – 21,21 %] es de un factor 6 entre extremos. Toda la magnitud
del problema descansa ahí.

**Lo que falta.** Ampliar a **n ≈ 1.000** con el mismo diseño estratificado — ahora que no hay cupo
de Databricks ni fecha de defensa, es tiempo de persona, no de cómputo. Y lo más importante:
**dos revisores independientes** sobre al menos 200 líneas, con **κ inter-evaluador reportado**. Hoy
el etiquetado humano es de una sola persona y no tiene medida de fiabilidad — es la única parte del
proyecto donde se aplica un estándar más bajo que el que el propio proyecto exige a los algoritmos
(a los que sí les midió la κ).

---

## A7 🟠 · "'Circularidad' es una definición tuya. ¿Quién dice que mide lo que dices?"

**El ataque.** Es el constructo central del hallazgo 3 y es una construcción propia. Sin validación,
es una regla que produce un número, no una medición de un fenómeno.

**Lo que falta.** **Validación de constructo**: tomar 200 líneas, que dos personas juzguen a ciegas
si el texto es una copia del código, y comparar contra la regla automática. Sensibilidad,
especificidad y κ. Con eso, "circularidad" deja de ser una definición y pasa a ser un instrumento
medido. Es el mismo trabajo de A6 y se hace en la misma pasada.

Argumento de apoyo que ya existe y es fuerte: el mecanismo está **identificado** (Convenio Marco
pasa de 6,65 % a 99,86 %). Una regla que aísla limpiamente un mecanismo institucional conocido es
mucho más creíble que una regla suelta.

---

## A8 🟠 · "Dices que predeclaraste los criterios, pero el archivo lo escribiste tú y lo puedes editar"

**El ataque.** Todo el régimen de evidencia del proyecto descansa en "esto se fijó antes de mirar".
La prueba de ese *antes* son documentos propios en un zip. No es verificable.

**Lo que falta, y es barato.** **Historia de Git con commits fechados**, y —para lo que venga— un
preregistro con sello externo: OSF, o el hash SHA-256 del plan de análisis publicado antes de correr.
Esto convierte la mejor práctica del proyecto, que hoy es un acto de fe, en algo verificable.

Es la razón más importante para publicar el repositorio, por encima del CV: **cada commit fechado es
un notario**. Y de aquí en adelante, todo criterio predeclarado se commitea **antes** de la corrida
que evalúa.

---

## A9 🟠 · "Hiciste muchas pruebas. Alguna tenía que salir"

**El ataque.** Tres preguntas, múltiples cortes, varios métodos de similitud, dos niveles de ONU,
subgrupos. Sin declarar la familia de pruebas, cualquier resultado significativo es sospechoso.

**Lo que falta.** Un **inventario explícito de todas las pruebas corridas**, incluidas las que no se
reportaron (CAP-12B ya está, y es un precedente excelente), y la distinción clara entre lo
**confirmatorio** (predeclarado) y lo **exploratorio**. Corrección por multiplicidad donde
corresponda. El proyecto tiene la cultura; le falta la tabla.

**A favor:** la conclusión principal de la Pregunta 2 es **negativa** ("no mejoró"), y las
conclusiones negativas no sufren de dragado de datos — nadie draga para no encontrar nada. Decirlo
explícitamente desarma buena parte de este ataque.

---

## A10 🟠 · "Con n de millones, todo es significativo. ¿+0,30 pp es relevante?"

**El ataque.** La Pregunta 2 se responde con un Δ de +0,28 / +0,30 pp. Con millones de líneas, un
efecto de ese tamaño puede ser estadísticamente significativo y sustantivamente nulo — o al revés,
declararlo "no significativo" con poca potencia sería otro error.

**Lo que falta.** Declarar el **tamaño de efecto mínimo relevante** antes de mirar (p. ej. 5 pp) y
usar una **prueba de equivalencia (TOST)**. El resultado deja de ser "no encontramos efecto" —que es
ausencia de evidencia— y pasa a ser **"demostramos que el efecto es menor que X"**, que es evidencia
de ausencia. Es una afirmación mucho más fuerte, casi nadie la hace, y aquí el n gigante juega a
favor en vez de en contra.

---

## A11 🟠 · "¿Deflactaste?"

**El ataque.** Cualquier cifra en pesos que compare 2017 con 2026 sin deflactar está mal por ~40 %
de inflación acumulada. Chile tiene además la UF y la UTM como unidades de cuenta habituales en
compras públicas.

**Lo que falta.** Verificar qué hace hoy `docs/02-CIERRE-DE-LAS-CIFRAS-EN-PESOS.md`, y si no
deflacta: hacerlo con IPC del INE (o UF), declarar el año base, y publicar la serie en las **dos**
unidades — nominal y real. Si ya deflacta, decirlo en la primera línea de esa sección.

---

## A12 🟠 · "Tres de tus salidas crudas son transcripciones"

Ya declarado por el propio proyecto: las dos de CAP-18 / N3 **y la de CAP-14** (`bc06eb233fc5`), cada una con su nota de proveniencia. Sostienen tres cifras titulares, entre ellas el odds ratio. Se cierra re-ejecutando con `SEED_N3 = 20260907`
y exportando byte a byte. Mientras no se cierre, **es el único punto donde el proyecto no cumple su
propia regla**, y un revisor que lo encuentre va a preguntarse qué más.

---

## A13 🟡 · "Nadie puede reproducir esto"

**El ataque.** No hay entorno fijado, ni versiones de librerías, ni checksums de los datos de
entrada. Los CSV de ChileCompra cambian entre descargas — el propio proyecto lo demostró. Sin hash
del corte, "reproducible" es una palabra.

**Lo que falta, y es la pieza de ingeniería más valiosa que se puede agregar:**
1. `requirements.txt` / `environment.yml` con versiones **fijadas**.
2. Un **manifiesto de datos**: por cada archivo del corte, nombre, tamaño, fecha de descarga y
   **SHA-256**. Ya existe la mitad (`manifest_esquema_comun_*.json` trae rutas y tamaños); falta el
   hash.
3. Una **semilla global** declarada y usada en todo lo estocástico.
4. Un `Makefile` o un script único que reproduzca las figuras desde los CSV publicados.

Con eso, el repositorio deja de ser un archivo de código y pasa a ser un **artefacto reproducible**,
que es exactamente lo que un empleador técnico busca y lo que un referee exige.

---

## A14 🟡 · "Definiste 'discordancia' como no compartir ni el segmento de 2 dígitos. ¿Por qué 2?"

Toda regla de corte necesita una **curva de sensibilidad**: cómo cambia el resultado con 2, 4 y 8
dígitos. Si el hallazgo se mantiene en los tres niveles, la elección deja de importar y el ataque
muere. CAP-18 ya corrió en dos niveles (segmento y familia) y encontró algo interesante — extenderlo
a la regla de discordancia es la misma idea.

**Y aquí hay munición ya medida.** La tabla 14.C.1 muestra que los tres niveles intermedios (6, 4 y
2 dígitos) tienen tasas de oferente único indistinguibles entre sí (29,26 % · 29,43 % · 30,04 %):
**no hay gradiente, hay un escalón** entre "código idéntico" (20,42 %) y "cualquier otra cosa". El
corte binario actual mete los tres intermedios del lado concordante. Correr el estimador con el
corte en *idéntico vs. resto* y con el corte a 4 dígitos, y reportar los tres, convierte A14 de
objeción abierta en prueba de sensibilidad cerrada. Es media hora. Ver `docs/CAP14-RESULTADO.md`.

---

## A15 🟡 · "Confundiste a ChileCompra con los organismos"

**El ataque.** El proyecto mide una falla de trazabilidad, pero no distingue si la causa está en el
**diseño de la plataforma** (Convenio Marco publica texto circular por construcción), en el
**catálogo** (códigos ONU que no representan lo que se compra en Chile) o en la **conducta del
organismo**. Sin esa separación, cualquier lectura se convierte en acusación al actor equivocado.

**Lo que falta.** Una descomposición **shift-share**, aplicada a nivel
nacional: cuánto de la opacidad es mix (qué se compra y por qué canal) y cuánto es conducta. Es el
análisis que convierte el proyecto de denuncia difusa en diagnóstico accionable, **y ya está medio
implementado** en la corrida del 24-ago.

---

## Orden recomendado de ataque

| Orden | Ítems | Por qué |
|---|---|---|
| 1 | **A2, A3** | Son recálculos sobre lo ya construido. Si alguno mueve una cifra titular, todo lo demás cambia de prioridad. **Hacerlos antes de publicar nada** |
| 2 | **A1, A15** | Producen resultados nuevos, no solo defensas. El retrato de lo excluido y el shift-share nacional son hallazgos por derecho propio |
| 3 | **A8, A13** | Infraestructura. Baratos, y desbloquean el repositorio y el preregistro de todo lo que venga |
| 4 | **A4, A5, A10, A11, A12, A14** | Pruebas de sensibilidad. Cada una cierra un flanco con 2-3 párrafos y una tabla |
| 5 | **A6, A7** | Tiempo de persona, no de cómputo. Es la misma pasada de etiquetado y cierra los dos |
| 6 | **A9** | Se escribe al final, cuando se sabe cuántas pruebas hubo en total |

---

## La regla que conviene adoptar de aquí en adelante

El proyecto ya tiene un régimen de evidencia por afirmación (`[MEDIDO]` · `[FUENTE]` ·
`[POR VERIFICAR]` · `[PRUEBA]` · `[PROPUESTA]`). Le falta una sola etiqueta más, y es la que cierra
todos los ataques de la familia A8-A9:

> **`[CONFIRMATORIO]` vs `[EXPLORATORIO]`**, decidido y commiteado **antes** de correr.

Un hallazgo exploratorio bien etiquetado no es un hallazgo débil: es un hallazgo honesto. Lo que
hunde un trabajo es un exploratorio presentado como confirmatorio.
