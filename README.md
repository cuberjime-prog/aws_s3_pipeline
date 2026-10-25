# Pipeline End-to-End de Ingeniería de Datos: AWS S3 + Databricks

Pipeline de ingeniería de datos que implementa una arquitectura Medallion (Bronze, Silver y Gold) para procesar datos de ventas y clientes, desde la ingesta de archivos CSV almacenados en Amazon S3 hasta la generación de indicadores analíticos en Databricks.

El proyecto utiliza Auto Loader para la ingesta de archivos, PySpark para la transformación, Delta Lake para el almacenamiento de tablas y Databricks Jobs para automatizar la ejecución del pipeline.

## Objetivo del proyecto

Construir un pipeline de datos reproducible y automatizado que permita transformar datos de ventas con errores intencionales en información estructurada, confiable y lista para el análisis.

El conjunto de datos contiene información sobre ventas, clientes, productos, cantidades, precios, descuentos, métodos de pago, canales comerciales y provincias de Costa Rica.

El proyecto contempla problemas habituales de calidad de datos, como valores nulos, registros duplicados, formatos inconsistentes, cantidades negativas, descuentos fuera de rango y totales de venta incorrectos.

## Arquitectura del proyecto


### Flujo de procesamiento

1. Los archivos CSV se almacenan en un bucket de Amazon S3.
2. Auto Loader detecta y procesa los archivos disponibles.
3. Bronze conserva los datos de origen junto con metadatos de ingesta.
4. Silver normaliza los datos, aplica reglas de calidad y separa los registros rechazados.
5. Gold genera tablas agregadas para el análisis de ventas.
6. Las pruebas de calidad verifican reglas de negocio y la consistencia entre Silver y Gold.
7. Un Databricks Job ejecuta las etapas en el orden definido.

## Tecnologías utilizadas

| Tecnología | Uso en el proyecto |
|---|---|
| Amazon S3 | Almacenamiento de archivos de entrada. |
| Databricks Free Edition | Entorno de desarrollo y ejecución del pipeline. |
| Python | Lenguaje para la lógica de ingesta y procesamiento. |
| PySpark | Limpieza, transformación y agregación de datos. |
| Spark SQL | Consultas y validaciones analíticas. |
| Auto Loader | Detección e ingesta incremental de archivos. |
| Delta Lake | Almacenamiento de tablas en formato Delta. |
| Databricks Jobs | Orquestación y automatización de tareas. |
| Git y GitHub | Control de versiones y documentación del proyecto. |

## Arquitectura Medallion

### 1. Bronze — Ingesta de datos

**Tabla:** `aws_s3.ventas.bronze_ventas_raw`

Esta capa almacena los datos recibidos desde los archivos CSV de Amazon S3.

Implementación:

- Ingesta mediante Auto Loader con `cloudFiles`.
- Lectura de archivos CSV con encabezados.
- Configuración de una ubicación para el esquema de Auto Loader.
- Uso de un checkpoint persistente para registrar el progreso de la ingesta.
- Registro del archivo de origen y la fecha de ingesta.
- Almacenamiento en Delta Lake.

**Resultado:** datos de origen disponibles para su posterior procesamiento y auditoría.

### 2. Silver — Limpieza y calidad

**Tablas:**

- `aws_s3.ventas.silver_ventas`
- `aws_s3.ventas.silver_ventas_rechazadas`

Esta capa aplica reglas de limpieza y validación para mejorar la calidad de los datos.

Transformaciones implementadas:

- Normalización de cadenas y valores vacíos.
- Conversión de fechas a un formato consistente.
- Limpieza de símbolos monetarios y conversión de precios a valores numéricos.
- Conversión y validación de cantidades y descuentos.
- Detección de identificadores de venta duplicados.
- Validación de campos obligatorios.
- Cálculo del total esperado de cada venta después de aplicar el descuento.
- Identificación de totales originales que requieren revisión.
- Separación de registros válidos y registros rechazados.

**Resultado:** datos preparados para el análisis, con una salida separada para los registros que incumplen las reglas definidas.

### 3. Gold — Indicadores de negocio

Las tablas Gold contienen agregaciones que facilitan el análisis comercial mediante SQL y dashboards.

| Tabla | Propósito |
|---|---|
| `aws_s3.ventas.gold_ventas_diarias` | Ventas, unidades, ingresos y ticket promedio por fecha. |
| `aws_s3.ventas.gold_ventas_producto` | Rendimiento comercial por producto. |
| `aws_s3.ventas.gold_ventas_provincia` | Comparación de resultados entre provincias. |
| `aws_s3.ventas.gold_ventas_canales` | Análisis por método de pago y canal de venta. |

Indicadores disponibles:

- Cantidad de ventas.
- Unidades vendidas.
- Ingresos calculados en colones costarricenses (CRC).
- Ticket promedio.
- Descuento promedio.
- Rendimiento por producto, provincia, canal y método de pago.

Los importes analíticos se calculan a partir de los valores limpios y del total esperado, en lugar de depender directamente de los totales originales potencialmente incorrectos.

## Ingesta incremental con Auto Loader

Auto Loader se utiliza para descubrir y procesar los archivos disponibles en la ubicación de entrada de Amazon S3.

Configuraciones relevantes:

- `cloudFiles.format`: formato de los archivos de origen.
- `cloudFiles.schemaLocation`: ubicación persistente de la información del esquema.
- `cloudFiles.schemaEvolutionMode`: estrategia de evolución del esquema, según la configuración del notebook.
- `checkpointLocation`: ubicación persistente para mantener el progreso de la ingesta.
- `trigger(availableNow=True)`: procesa los archivos disponibles y finaliza la ejecución.

Las ubicaciones de esquema y checkpoint se mantienen separadas de los datos de entrada.

La persistencia del checkpoint permite que Auto Loader gestione el seguimiento de los archivos procesados entre ejecuciones. Para mantener este comportamiento, no se debe eliminar ni cambiar arbitrariamente el checkpoint.

**Nota:** el procesamiento incremental de Auto Loader no implica que todas las etapas posteriores sean incrementales. En esta implementación, las tablas Silver y Gold se reconstruyen mediante `overwrite`. Por lo tanto, el diseño actual combina ingesta incremental con transformaciones posteriores de reconstrucción completa.

## Pruebas de calidad de datos

El notebook `06_calidad_datos` implementa validaciones para comprobar el cumplimiento de reglas de negocio.

| Prueba | Regla esperada |
|---|---|
| Campos obligatorios | Sin valores nulos en los campos críticos seleccionados. |
| Identificadores duplicados | Sin identificadores de venta duplicados en Silver. |
| Cantidades | Valores positivos y no nulos. |
| Descuentos | Valores dentro del rango de 0 a 100 %. |
| Precios | Valores no nulos y no negativos. |
| Conciliación financiera | Diferencia entre los ingresos calculados desde Silver y los agregados en Gold inferior al umbral configurado. |

Los resultados se guardan en la tabla:

`aws_s3.ventas.resultados_calidad`

Los estados `PASS` y `FAIL` permiten identificar qué reglas se cumplen y cuáles necesitan investigación.

La implementación actual registra los resultados de la última ejecución mediante una escritura `overwrite`. Un historial de ejecuciones y alertas automáticas puede añadirse como una mejora futura.

Las pruebas verifican las reglas programadas; no garantizan por sí solas la ausencia de todos los posibles errores en los datos.

## Automatización con Databricks Jobs

El pipeline se ejecuta mediante un Job de Databricks configurado para ejecutar los notebooks en secuencia.

| Orden | Notebook | Responsabilidad |
|---|---|---|
| 1 | `01_ingesta_bronze` | Ingesta de datos con Auto Loader. |
| 2 | `02_limpieza_silver` | Limpieza y validación inicial. |
| 3 | `03_transformacion_gold` | Generación de tablas analíticas. |
| 4 | `04_calidad_datos` | Ejecución de las pruebas de calidad. |

Dependencias de ejecución:

- Silver depende de Bronze.
- Gold depende de Silver.
- Calidad depende de Gold.

El Job fue creado y ejecutado correctamente en el entorno del proyecto. Las pruebas de calidad también se ejecutaron con estado `PASS`.

La configuración de ejecución programada, los reintentos y las notificaciones dependen de las opciones configuradas en el Job.

## Mejoras futuras

Las siguientes funcionalidades pueden ampliar el alcance del proyecto:

- Incorporar procesamiento incremental de Silver y Gold.
- Implementar pruebas automatizadas que interrumpan el Job cuando fallen reglas críticas.
- Añadir historial de ejecuciones de calidad, métricas y alertas.
- Desarrollar dashboards analíticos de ventas.
- Implementar monitoreo de duración, volumen de registros y fallos.
- Añadir pruebas de idempotencia y evolución del esquema.
- Documentar la arquitectura con un diagrama exportado y capturas de las ejecuciones reales.

## Autor

**[Moises Cubero]**


