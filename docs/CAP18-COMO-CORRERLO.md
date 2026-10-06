# CAP-18 (N3) — cómo correrlo, qué mirar, y cómo se cita

> ## ⚠ ESTE DOCUMENTO SE ESCRIBIÓ ANTES DE CORRER. YA CORRIÓ.
>
> **7-sep-2026, `run_id 5a53ec4a1f30`.** Todo lo que abajo dice «[POR VERIFICAR]», «la primera
> corrida es una prueba» o «puede terminar en no se cita» **quedó resuelto**: la celda corrió, las
> cifras están en `resultados/cap18_n3_salida_cruda.txt` y
> `resultados/cap18b_familia4_salida_cruda.txt`, y el informe §4.2 ya las publica.
>
> **Se conserva tal como se escribió**, sin maquillar, porque documenta el criterio con el que se
> decidió correrlo y los frenos que se fijaron **antes** de conocer el resultado — que es
> exactamente lo que el régimen de evidencia de este proyecto pide poder mostrar.
>
> **Resultado, en dos líneas:** segmento (2 díg.) LogReg **63,93 %** contra baseline 15,62 %,
> macro-F1 **0,4687**. Familia (4 díg.) LogReg **56,58 %** contra baseline 7,61 %, macro-F1
> **0,2739**. Cifras completas y reglas de citación en `claude/F35-cap18-n3-resultado.md`.
>
> **Lo único que falló** fue escribir a `/tmp/...`: Databricks Free Edition tiene el DBFS root
> público deshabilitado (`DBFS_DISABLED` / SQLSTATE 56038). La celda ya materializa en una tabla de
> Unity Catalog.

---

**Escrito el lunes 7-sep-2026, 21:30 (hora Santiago). Faltan ~45 h para la defensa.**
Léelo entero antes de abrir Databricks: son tres minutos y te ahorra una corrida perdida.

---

## 0 · Lo primero: esto puede terminar en "no se cita", y eso está bien

Esta celda existe porque la tabla de cobertura curricular del propio proyecto
(`evaluacion-capstone-bigdata-mp-nacional.md` §11, marca `[DOC]`) asigna la **Clase 10
(ML con Spark)** al reclasificador N3, y esa casilla está vacía. Llenarla es lo que
estás intentando.

Pero la **Regla 1 manda sobre eso**: si la cifra no alcanza a verificarse, **no se cita**,
y el informe se queda con la declaración de trabajo futuro que ya tiene escrita en §4.2 —
que es una declaración limpia y defendible, no un agujero. **Abandonar a tiempo es un
resultado, no un fracaso.** El peor desenlace posible no es no correrlo: es correrlo,
sacar un número, y citarlo sin haberlo verificado.

---

## 1 · Los tres archivos

| Archivo | Qué es | Dónde va |
|---|---|---|
| `CAP18_n3_clasificador_onu.py` | La celda de Databricks. Se pega al final del notebook `etapa2_ANEXO_PROFESOR_v19.py` | Databricks + repo |
| `test_cap18_metricas.py` | Prueba la aritmética del driver (accuracy, macro-F1, baseline) **contra sklearn**. 12/12 | Local + repo |
| `test_cap18_etiquetas.py` | Prueba el embudo de etiquetas y los tres filtros de fuga sobre un fixture de 17 líneas. 20/20 | Local + repo |

**Corre las dos pruebas antes de tocar Databricks.** Tardan 2 segundos juntas y no
necesitan Spark:

```
python test_cap18_metricas.py
python test_cap18_etiquetas.py
```

Si alguna falla, **no corras la celda**: significa que algo cambió y el número que salga
no vale.

---

## 2 · Estado de verificación, sin adornos

- ✅ **La matemática del driver está validada.** 12/12, y accuracy, macro-F1 y F1 ponderado
  coinciden con `sklearn` hasta el decimal 9 sobre un corpus sintético desbalanceado.
- ✅ **La lógica del embudo de etiquetas está validada.** 20/20 sobre un fixture donde la
  respuesta correcta de cada línea está escrita a mano.
- ⚠️ **Las expresiones de Spark y el Pipeline de ML NO están validados.** `pyspark` no se
  pudo instalar en el entorno donde se escribió la celda (mismo caso que CAP-14, que lo
  declara igual en su cabecera). **Quedan `[POR VERIFICAR]`.**

**Consecuencia práctica: la primera corrida es una PRUEBA, no un resultado.**

---

## 3 · El orden de corrida, y dónde está el freno

La celda depende de que ya estén en memoria `df_oc_linked`, `df_lic_all`, y las utilidades
de `CAP-0`, `CAP-0-BIS` y `CAP-0-G7`. **Correrla en la misma sesión de kernel** donde
corrieron las celdas 1-8. Si falta algo, aborta limpio e imprime qué falta — no inventa
sustitutos.

Los parámetros arrancan así **a propósito**:

```python
CORRE_FAMILIA4 = False     # primero segmento (2 díg.), después se decide
CORRE_LOGREG   = True      # NaiveBayes corre siempre y va primero
TOPE_ENTRENAMIENTO = 600_000
ANIO_CORTE_TRAIN   = 2023  # train ≤ 2023 · test ≥ 2024
```

### Lo primero que tienes que mirar: el bloque **18.C.1**

Imprime el embudo de etiquetas. **Si `lineas de OC enlazadas, con ONU valido y con texto`
no está en el orden de los millones, el join está mal y nada de lo que sigue sirve.
Para ahí.** Es el mismo control que CAP-14 pone en 14.C.1, por la misma razón.

Después mira **18.D.1**: cuántos textos distintos quedaron, y cómo se repartieron entre
train y test. Hay dos frenos automáticos:

- menos de **100.000** filas entrenables → **no entrena** y te lo dice;
- el split por año deja un lado casi vacío → **no entrena** y te lo dice.

Que se dispare un freno **es un hallazgo citable**: significa que la fracción del universo
con etiqueta limpia y texto no circular es demasiado chica para sostener un clasificador.
Eso se declara y se acabó.

---

## 4 · Por qué esta celda escribe a disco (y por qué eso no es un descuido)

Databricks Free Edition serverless no soporta `.persist()` / `.cache()` — es la regla §3.8
del proyecto y ya te costó tiempo antes. Pero entrenar `LogisticRegression` con
`maxIter=15` **sin** cache recomputaría el join de 16,9 M de filas quince veces.

La celda materializa la tabla de entrenamiento **una vez** a Parquet
(`/tmp/capstone_n3/<run_id>`) y la vuelve a leer. Es el reemplazo estándar de `.cache()`
cuando `.cache()` no existe, y es la diferencia entre minutos y horas. Está declarado en
la cabecera; no es un efecto secundario escondido.

**`NaiveBayes` corre primero** porque es de una sola pasada. Si su número es absurdo, paras
ahí y no gastas el cupo en `LogisticRegression`.

---

## 5 · Cómo se lee el resultado (esto es lo que decide si se cita)

La celda imprime, para cada modelo:

```
accuracy          :  xx.xx%
BASELINE mayoritaria (clase N) :  xx.xx%
ganancia sobre baseline        :  +x.xx pp
macro-F1          : 0.xxxx   <- la cifra honesta
F1 ponderado      : 0.xxxx   (lo que Spark llama 'f1')
```

**Las tres reglas de lectura:**

1. **Un accuracy sin su baseline al lado no es una cifra, es una decoración.** Si el
   segmento 43 concentra el 40 % de las líneas, acertar el 45 % no es haber aprendido nada.
   Lo que se cita es la **ganancia sobre el baseline**.
2. **El macro-F1 es la cifra honesta, y va a ser bajo.** Es lo esperable: la cola larga del
   catálogo se predice mal. La referencia externa (`[DOC]`, CLiC-it 2023 sobre CPV europeo:
   77,7 % de accuracy con macro-F1 de **0,40**) dice exactamente eso. **Reportar un macro-F1
   bajo con honestidad vale más ante un jurado técnico que un accuracy inflado** — y esa
   referencia hay que **re-verificarla en fuente primaria** antes de citarla (Regla 2).
3. **Spark llama `f1` al F1 ponderado, que no es el macro.** Sobre cola larga el ponderado
   se ve bien y esconde justo lo que interesa. La celda calcula los dos e imprime los dos;
   la prueba `test_cap18_metricas.py` #3 y #4 demuestra la brecha con un caso construido.

---

## 6 · Qué se puede decir y qué no

**Sí se puede decir**, si la corrida verifica:

- "Entrenamos un clasificador supervisado sobre TF-IDF que, a partir del texto libre,
  predice el segmento ONU con X % de accuracy contra un baseline de Y %, y macro-F1 de Z."
- "El diseño excluye el texto circular, deduplica por texto y parte por año, precisamente
  para que la cifra no sea un artefacto."

**No se puede decir, nunca:**

- Que una línea en desacuerdo con el modelo **esté mal codificada**. Es una **alerta
  estadística** (Regla 4), y el macro-F1 dice cuánta desconfianza merece.
- Nada sobre el **18,13 % de líneas sin texto evaluable**: el modelo no las ve, por
  construcción.
- Que el resultado se generalice al universo completo sin declarar el **sesgo de
  A_ESTRICTO** — entrena solo con líneas cuya licitación declaró un único código, o sea
  sobre-representa compras simples de un producto. Va declarado como limitación en el
  informe, no se descubre en la defensa.

---

## 7 · Si sale bien, dónde entra

**Reemplaza el §4.2 del informe**, que hoy declara N3 como trabajo futuro con su diseño.
Pasa de "esto es lo que haríamos" a "esto es lo que hicimos, con estas métricas y estas
limitaciones". No toca ninguna otra cifra del informe y **no obliga a rehacer las láminas**
— cabe como respuesta de Q&A y como una línea en la lámina de método.

**Si sale mal o no alcanza:** el §4.2 se queda exactamente como está. No se toca nada.

---

## 8 · El corte

Tenías dos horas y el acuerdo era: **si no funciona, se descarta.** Ese acuerdo sigue en
pie y no lo decide el entusiasmo, lo decide el reloj. Lo que **no** cae por esto, bajo
ninguna circunstancia (§4 del plan de continuidad del 7-sep):

**repositorio público · los tres ensayos cronometrados · el régimen de evidencia.**
