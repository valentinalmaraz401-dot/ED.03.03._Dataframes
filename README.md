# ED.03.03. DataFrames en Apache Spark (PySpark)

Bienvenido a este repositorio. Aquí encontrarás el desarrollo completo de la práctica **ED.03.03. DataFrames**, donde aprendemos a procesar, limpiar, transformar y analizar grandes volúmenes de información utilizando **Apache Spark** con **Python (PySpark)**.

---

## ¿De qué trata este proyecto? (Explicación sencilla)

Imagina que tienes una hoja de cálculo de Excel, pero en lugar de tener 100 o 1,000 filas, tiene **casi un millón de registros** (841,147 ventas exactamente). Si intentaras abrir y procesar un archivo así en herramientas tradicionales, tu computadora se volvería muy lenta o se bloquearía.

Aquí es donde entra **Apache Spark**:
* **¿Qué es Apache Spark?** Es un motor de procesamiento de datos ultrarrápido diseñado para trabajar con *Big Data* (grandes volúmenes de información). Divide el trabajo en partes pequeñas para procesarlo en segundos.
* **¿Qué es PySpark?** Es la herramienta que nos permite controlar Apache Spark escribiendo código en **Python**, uno de los lenguajes de programación más amigables y utilizados en ciencia de datos.
* **¿Qué es un DataFrame?** Es simplemente una **tabla organizada en filas y columnas** (muy parecida a una tabla de Excel o de una base de datos SQL), pero optimizada para que Spark pueda filtrar, sumar, agrupar y cruzar millones de datos a gran velocidad.
* **¿Qué es JupyterLab?** Es el cuaderno interactivo (`ED_03_03_DataFrames.ipynb`) donde escribimos el código por bloques (celdas) y vemos inmediatamente los resultados en pantalla.

En este proyecto primero practicamos con **8 ejercicios cortos** usando una tabla de empleados ficticios para dominar las herramientas básicas, y después aplicamos todo lo aprendido en un **proyecto real de análisis de ventas internacionales** (`pdi_sales.csv`).

---

## Estructura del Repositorio

```
ED.03.03.-DataFrames/
├── DATOS/
│   └── pdi_sales.csv              # Archivo CSV con 841,147 registros de ventas internacionales
├── capturas/                      # Evidencias visuales paso a paso de toda la ejecución
│   ├── 01_spark_setup.png         # Configuración inicial de SparkSession
│   ├── 02_dataset_download.png    # Descarga y verificación del dataset
│   ├── 03_ejercicios_df_1.png     # Ejercicio 1: Creación y ordenación
│   ├── 03_ejercicios_df_2.png     # Ejercicio 2: Selección y filtrado lógico
│   ├── 03_ejercicios_df_3.png     # Ejercicio 3: Creación y modificación de columnas
│   ├── 03_ejercicios_df_4.png     # Ejercicio 4: Agrupaciones y estadísticas
│   ├── 03_ejercicios_df_5.png     # Ejercicio 5: Funciones de ventana (Window)
│   ├── 03_ejercicios_df_6.png     # Ejercicio 6: Cruce de tablas (Left Join)
│   ├── 03_ejercicios_df_7.png     # Ejercicio 7: Consultas con SQL puro
│   ├── 03_ejercicios_df_8.png     # Ejercicio 8: Limpieza y unión de textos
│   ├── 04_proyecto_df_1.png       # Proyecto Paso 1: Carga y esquema de pdi_sales.csv
│   ├── 04_proyecto_df_2.png       # Proyecto Paso 2: Ingresos totales por país
│   ├── 04_proyecto_df_3.png       # Proyecto Paso 3: Ventas en Alemania (> 1 unidad)
│   ├── 04_proyecto_df_4.png       # Proyecto Paso 4: Códigos postales con más ventas en Alemania
│   └── 04_proyecto_df_5.png       # Proyecto Paso 5: Precio unitario y Top 5 productos
├── ED_03_03_DataFrames.ipynb      # Cuaderno principal de JupyterLab con código y resultados
├── ED_03_03_DataFrames.html       # Exportación web del cuaderno ejecutado
├── ED_03_03_DataFrames.pdf        # Versión en documento PDF del cuaderno
└── README.md                      # Guía explicativa del proyecto
```

---

## Contenido Paso a Paso

### Fase 1: Preparación del Entorno
1. **Encendido del motor (`SparkSession`):** Configuramos Spark en modo local (`local[*]`) para aprovechar todos los núcleos del procesador y silenciamos los mensajes técnicos innecesarios (`setLogLevel("ERROR")`) para mantener los resultados limpios.
2. **Obtención de datos:** Descargamos el archivo `pdi_sales.csv` dentro de la carpeta `DATOS/` y comprobamos que tuviera toda su información intacta.

### Fase 2: Los 8 Ejercicios Prácticos (Tabla de Empleados)
Antes de analizar el archivo grande, resolvimos 8 retos fundamentales sobre una tabla de empleados (`nombre`, `edad`, `departamento`, `salario`):

1. **Creación y ordenación (`printSchema` y `orderBy`):** Creamos la tabla desde cero, revisamos qué tipo de dato tiene cada columna (texto o número) y ordenamos a los empleados desde el salario más alto hasta el más bajo.
2. **Filtros inteligentes (`filter` con `&`, `|` e `isin`):** Buscamos personas que cumplieran varias condiciones a la vez (tener 30 años o más **Y** trabajar en `IT` **O** `Ventas`).
3. **Nuevas columnas y reglas (`withColumn` y `when`):** Calculamos automáticamente un bono del 12 % para cada empleado y creamos una etiqueta que clasifica su sueldo como `"Alto"` (si supera 45,000) o `"Estándar"`.
4. **Resúmenes por grupo (`groupBy` y `agg`):** Agrupamos a los empleados por departamento para saber cuántos hay en cada área, cuál es su sueldo promedio y cuál es el sueldo máximo.
5. **Rankings internos (`Window` y `rank`):** Sin perder el detalle de cada persona, creamos un ranking dentro de cada departamento para ver quién ocupa el puesto #1, #2, etc., según su salario.
6. **Cruce de información (`Left Join`):** Unimos la tabla de empleados con una tabla de departamentos. Comprobamos qué ocurre cuando un empleado (Marta) tiene un código de departamento que no existe en el catálogo: Spark conserva su fila y marca el nombre del departamento como `NULL` (vacío).
7. **Uso de SQL tradicional (`createOrReplaceTempView` y `spark.sql`):** Demostramos que en Spark también se puede escribir lenguaje **SQL clásico** (`SELECT ... FROM ... WHERE ...`) convirtiendo el DataFrame en una vista temporal.
8. **Limpieza de textos (`trim` y `concat_ws`):** Corregimos nombres y apellidos que venían con espacios extra al principio o al final (`" Juan "`) y los unimos en una sola columna limpia (`"Juan García"`).

### Fase 3: Proyecto Final — Análisis de Ventas (`pdi_sales.csv`)
Con las herramientas dominadas, analizamos el historial completo de **841,147 ventas** para responder 5 preguntas clave de negocio:

* **Paso 1 (Exploración inicial):** Cargamos el archivo respetando su separador de punto y coma (`;`), confirmamos los tipos de columnas (`ProductID`, `Date`, `Zip`, `Units`, `Revenue`, `Country`) y verificamos los **841,147 registros**.
* **Paso 2 (Ingresos por país):** Sumamos el dinero recaudado (`Revenue`) en cada país y descubrimos el ranking de mercados:
  1. **Francia (`France`):** 148,490,193.30
  2. **Alemania (`Germany`):** 131,930,510.10
  3. **México (`Mexico`):** 103,645,802.60
  4. **Canadá (`Canada`):** 34,114,930.90
* **Paso 3 (Compras múltiples en Alemania):** Filtramos las ventas ocurridas en `Germany` donde se compró más de una unidad (`Units > 1`), encontrando exactamente **7,426 operaciones**.
* **Paso 4 (Zonas con más ventas en Alemania):** Contamos cuántas ventas tuvo cada código postal (`Zip`) alemán, descubriendo que el código **50169** lidera con **2,130 ventas**.
* **Paso 5 (Precio unitario y productos estrella):** Calculamos cuánto costó cada unidad vendida (`Precio_Unitario = Revenue / Units`) e identificamos los **5 productos que más dinero generaron** en toda la historia de la empresa (encabezados por el producto `475` con más de 12.66 millones en ingresos).

---

## Requisitos y Cómo Ejecutarlo

Si deseas abrir y ejecutar este proyecto en tu propia computadora, necesitas tener instalado:
* **Sistema Operativo:** Linux (Ubuntu) o WSL2 en Windows.
* **Java (JDK 11 o 17):** Necesario para que el motor de Apache Spark funcione.
* **Python 3 + PySpark 3.5.3 + JupyterLab.**

### Pasos para correr el cuaderno:
1. Clona este repositorio en tu equipo:
   ```bash
   git clone https://github.com/valentinalmaraz401-dot/ED.03.03._Dataframes.git
   cd ED.03.03._Dataframes
   ```
2. Inicia JupyterLab desde la terminal:
   ```bash
   jupyter lab ED_03_03_DataFrames.ipynb
   ```
3. Ejecuta las celdas en orden de arriba hacia abajo (`Shift + Enter`) para reproducir todos los resultados y finalizar limpiamente con `spark.stop()`.