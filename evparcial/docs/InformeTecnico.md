# Informe Técnico — Evaluación Parcial 1
## Caso 5: Educación Universitaria

**Asignatura:** SCY1101 Programación para la Ciencia de Datos  
**Framework:** Kedro  
**Caso:** Educación Universitaria  
**Archivos utilizados:** `estudiantes.csv`, `inscripciones.csv`, `calificaciones.csv`, `asistencia.csv`

---

## 1. Resumen ejecutivo

El presente proyecto tuvo como objetivo desarrollar un flujo profesional de transformación de datos utilizando Kedro sobre un caso de educación universitaria. El dataset estuvo compuesto por cuatro archivos CSV relacionados con estudiantes, inscripciones, calificaciones y asistencia.

Se implementó un proyecto Kedro modular con cuatro pipelines: `data_ingestion`, `data_cleaning`, `data_transform` y `data_validation`. El flujo permitió cargar los datos crudos, detectar problemas de calidad, limpiar inconsistencias, integrar tablas, generar variables derivadas y validar la integridad del resultado final.

Como resultado, se obtuvo un dataset integrado y reproducible, junto con reportes de diagnóstico, comparación antes/después de la limpieza, auditoría y validación del esquema. Además, se generaron visualizaciones que permiten identificar patrones de asistencia, rendimiento académico y riesgo estudiantil.

---

## 2. Descripción del problema

El caso simula un sistema universitario con información distribuida en distintas tablas. El desafío principal consistió en integrar correctamente las fuentes y resolver problemas de calidad de datos bajo un flujo reproducible y profesional.

Los cuatro archivos representan:
- **estudiantes.csv**: información base del estudiante.
- **inscripciones.csv**: asignaturas inscritas por cada estudiante.
- **calificaciones.csv**: notas y ponderaciones por evaluación.
- **asistencia.csv**: registros de asistencia por clase.

El objetivo fue transformar estos datos en un dataset final útil para análisis posterior y con trazabilidad completa.

---

## 3. Metodología general

El proyecto se estructuró con Kedro, separando configuración y lógica de negocio mediante:
- `catalog.yml` para definir datasets.
- `parameters.yml` para externalizar parámetros.
- pipelines modulares por etapa.
- outputs persistidos en carpetas intermedias y de reporting.

Esta elección permite reproducibilidad, trazabilidad, organización y facilidad de mantenimiento.

---

## 4. Pipeline 1 — Data Ingestion

En esta etapa se cargaron los cuatro archivos CSV desde `data/01_raw/` y se generó un diagnóstico inicial del estado de los datos.

Se inspeccionaron:
- número de filas y columnas,
- tipos de datos,
- valores nulos,
- duplicados,
- columnas problemáticas.

El output principal de esta etapa fue `diagnostico_inicial.csv`, que permitió documentar el estado base del dataset antes de cualquier transformación.

---

## 5. Pipeline 2 — Data Cleaning

En la etapa de limpieza se abordaron problemas clásicos de calidad de datos:

### 5.1 Valores faltantes
Se aplicó imputación diferenciada:
- variables numéricas: mediana,
- variables categóricas: moda o valores por defecto de negocio.

La mediana fue preferida en columnas numéricas porque es más robusta frente a valores extremos.

### 5.2 Duplicados
Se eliminaron duplicados exactos y se consolidaron registros por claves principales cuando correspondía.

### 5.3 Tipos mixtos
Se corrigieron columnas numéricas almacenadas como texto, incluyendo notas y ponderaciones.

### 5.4 Fechas
Se estandarizaron fechas con formatos mixtos, permitiendo su uso posterior en análisis y control de consistencia.

### 5.5 Strings inconsistentes
Se normalizaron mayúsculas, minúsculas, espacios y nombres categóricos, reduciendo ruido en columnas como carrera, docente, asignatura y estado de asistencia.

### 5.6 Outliers
Las notas se restringieron al rango válido definido por regla de negocio. Esto evita que errores de digitación afecten métricas posteriores.

El resultado de esta etapa quedó documentado en `comparacion_limpieza.csv`, permitiendo cuantificar el impacto del proceso.

---

## 6. Pipeline 3 — Data Transformation

En esta etapa se integraron las tablas limpias para construir el dataset final.

### 6.1 Integración de tablas
Se realizaron joins entre:
- inscripciones y estudiantes,
- calificaciones agregadas por inscripción,
- asistencia agregada por inscripción.

### 6.2 Transformaciones aplicadas
Se utilizaron técnicas alineadas con lo solicitado en la evaluación:
- `groupby`
- agregaciones
- cálculo de métricas derivadas
- normalización de variables numéricas
- codificación de variables categóricas
- creación de features académicas

### 6.3 Variables derivadas
Se generaron, entre otras:
- `nota_promedio`
- `nota_final_ponderada`
- `cantidad_evaluaciones`
- `tasa_asistencia`
- `carga_asignaturas`
- `antiguedad_ingreso`
- `riesgo_academico`
- `resultado_academico`
- `aprobado`

Estas transformaciones agregan valor analítico porque convierten tablas operacionales en variables útiles para modelado y toma de decisiones.

---

## 7. Pipeline 4 — Data Validation

La etapa de validación se diseñó para asegurar integridad y procedencia de los datos.

### 7.1 Auditoría
Se generó un reporte global de datasets con:
- cantidad de filas,
- columnas,
- nulos,
- duplicados,
- checksums.

### 7.2 Integridad referencial
Se verificó que:
- las inscripciones correspondieran a estudiantes existentes,
- las calificaciones correspondieran a inscripciones válidas,
- la asistencia correspondiera a inscripciones válidas.

### 7.3 Validación de esquema
Se contrastó el esquema real del dataset final contra el esquema esperado definido en parámetros.

### 7.4 Comparación antes/después
Se documentó el efecto de la limpieza en términos de nulos, duplicados y filas afectadas.

Esta etapa refuerza la confiabilidad del flujo y la trazabilidad del procesamiento.

---

## 8. Resultados del análisis exploratorio

A partir del notebook EDA se obtuvieron los siguientes hallazgos:

- El dataset final consolidó la información académica y de asistencia en una estructura lista para análisis.
- La asistencia mostró relación con el rendimiento académico.
- Fue posible identificar carreras con mejores promedios de asistencia y aprobación.
- También se identificaron asignaturas con mayor y menor tasa de aprobación.
- La variable `riesgo_academico` permitió clasificar estudiantes según desempeño y asistencia.

**Incorpora aquí los valores concretos obtenidos en tu notebook**, por ejemplo:
- cantidad total de registros,
- nota promedio global,
- tasa de asistencia promedio global,
- carrera con mejor asistencia,
- carrera con mejor aprobación,
- asignatura con mayor y menor aprobación.

---

## 9. Justificación técnica de decisiones

Las decisiones tomadas en limpieza y transformación se justifican por criterios de calidad, consistencia y utilidad analítica.

- La imputación evitó pérdida excesiva de información.
- La eliminación de duplicados impidió sesgos en el conteo de registros.
- La estandarización de strings redujo cardinalidad artificial.
- La corrección de tipos mixtos permitió cálculos válidos.
- La generación de variables derivadas facilitó el análisis del rendimiento.
- La validación final permitió evidenciar que el flujo no solo transforma, sino que también controla la calidad del resultado.

---

## 10. Conclusiones

El proyecto cumplió con el objetivo de construir un flujo profesional de datos usando Kedro, integrando carga, limpieza, transformación y validación en una arquitectura reproducible.

Se logró:
- estructurar el proyecto según buenas prácticas,
- limpiar e integrar múltiples fuentes,
- construir variables relevantes para análisis académico,
- generar reportes y visualizaciones,
- validar integridad y esquema del resultado final.

La principal fortaleza del enfoque fue la reproducibilidad y separación clara entre configuración, lógica de negocio y reporting.

---

## 11. Recomendaciones y mejoras futuras

Como mejoras futuras se propone:
- incorporar validaciones automáticas más estrictas por reglas de negocio,
- agregar tests unitarios sobre nodos críticos,
- ampliar el análisis temporal del rendimiento,
- incluir dashboards o visualización interactiva,
- explorar modelos predictivos sobre riesgo académico o aprobación.

---

## 12. Evidencia de reproducibilidad

El proyecto incluye:
- `catalog.yml`
- `parameters.yml`
- cuatro pipelines modulares
- `requirements.txt`
- `README.md`
- notebook EDA
- reportes finales en `data/08_reporting/`

La ejecución completa se realiza mediante:

```bash
kedro run