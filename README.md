# 🧬 Sistema de Análisis Genómico - HDFS + Spark + Gradio

Sistema completo de procesamiento y análisis de datos genómicos utilizando Big Data technologies.

## 📋 Arquitectura

```
┌─────────────┐      ┌──────────┐      ┌─────────────┐      ┌───────────────┐
│   Producer  │ ───> │  Kafka   │ ───> │ HDFS Loader │ ───> │     HDFS      │
│  (Genomas)  │      │ Topics   │      │  (Parquet)  │      │ (Distributed) │
└─────────────┘      └──────────┘      └─────────────┘      └───────────────┘
                                                                      │
                                                                      ▼
┌─────────────┐      ┌──────────┐      ┌─────────────┐      ┌───────────────┐
│  Dashboard  │ <─── │  Spark   │ <─── │    YARN     │ <─── │   Analytics   │
│   (Gradio)  │      │ Jobs/SQL │      │  Resource   │      │   & Queries   │
└─────────────┘      └──────────┘      └─────────────┘      └───────────────┘
```

## 🚀 Componentes

### 1. **Producer** - Generador de Datos Genómicos
- Genera familias con datos de padres, madres e hijos
- Publica a Kafka en topics separados
- 3 productores en paralelo (escalable)

### 2. **HDFS** - Almacenamiento Distribuido
- **NameNode**: Gestión de metadatos (puerto 9870)
- **DataNodes** (3): Almacenamiento distribuido
- Datos en formato **Parquet** (comprimido y columnar)

### 3. **YARN** - Gestión de Recursos
- **ResourceManager**: Orquestación de recursos (puerto 8088)
- **NodeManagers** (2): Ejecución de tareas
- **HistoryServer**: Historial de jobs (puerto 8188)

### 4. **Spark** - Procesamiento Distribuido
- **Master**: Coordinador (puerto 8080, 7077)
- **Workers** (2): Ejecutores de tareas
- Análisis paralelo de genomas

### 5. **HDFS Loader**
- Consume datos de Kafka
- Escribe batches en HDFS como Parquet
- Buffer de 10,000 registros

### 6. **Dashboard Gradio**
- Interfaz web interactiva (puerto 7860)
- Gráficos con Plotly
- Jobs Spark desde UI

## 📦 Instalación y Configuración

### Requisitos Previos
- Docker & Docker Compose
- 8 GB RAM mínimo (16 GB recomendado)
- 20 GB espacio en disco

### Paso 1: Levantar Producer (Kafka + ZooKeeper)

```bash
cd producer
docker-compose up --scale genomic-producer=3 --build -d
```

Esto levanta:
- ✅ Kafka (puerto 9092)
- ✅ ZooKeeper (puerto 2181)
- ✅ 3 Productores genómicos

### Paso 2: Levantar Consumer (HDFS + YARN + Spark + Dashboard)

```bash
cd cosumer
docker-compose up --build -d
```

Esto levanta:
- ✅ HDFS NameNode + 3 DataNodes
- ✅ YARN ResourceManager + 2 NodeManagers
- ✅ Spark Master + 2 Workers
- ✅ HDFS Loader (consume Kafka → HDFS)
- ✅ Dashboard Gradio

### Paso 3: Verificar que todo está funcionando

```bash
# Ver logs del consumer
docker-compose -f cosumer/docker-compose.yml logs -f genomic-consumer

# Ver logs del producer
docker-compose -f producer/docker-compose.yml logs -f genomic-producer
```

## 🌐 URLs de Acceso

| Servicio | URL | Descripción |
|----------|-----|-------------|
| **Dashboard Gradio** | http://localhost:7860 | Interfaz principal de análisis |
| **Spark Master UI** | http://localhost:8080 | Estado del cluster Spark |
| **HDFS NameNode** | http://localhost:9870 | Explorador de archivos HDFS |
| **YARN ResourceManager** | http://localhost:8088 | Gestión de recursos YARN |
| **History Server** | http://localhost:8188 | Historial de jobs MapReduce |

## 📊 Uso del Dashboard

### Tab 1: 🏠 Overview
- **Selecciona tipo de miembro**: Padres / Madres / Hijos
- **Ajusta límite de datos**: Cantidad de SNPs a cargar
- **Click "Cargar Datos"**: Visualiza distribuciones y métricas

**Gráficos disponibles:**
- 📊 Distribución de SNPs por Cromosoma
- 🔬 Top 20 Genotipos más Frecuentes
- 🌈 Diversidad Genética por Cromosoma

### Tab 2: 🗺️ Heatmap Cromosómico
- Visualiza densidad de SNPs a lo largo de regiones genómicas
- Selecciona tipo de miembro y límite de datos
- Heatmap interactivo con 50 bins por cromosoma

### Tab 3: 👨‍👩‍👧 Análisis Familiar
- Estadísticas de familias completadas
- Promedio de hijos por familia
- Tiempos de generación
- Correlación SNPs vs Tamaño familiar

### Tab 4: ⚡ Spark Jobs
Ejecuta análisis masivos sobre TODOS los datos en HDFS:

1. **Estadísticas Globales**
   - Total SNPs por tipo de miembro
   - Familias únicas
   - Cromosomas y genotipos únicos

2. **Análisis Cromosómico**
   - Distribución completa por cromosoma
   - Min/Max/Media de posiciones
   - Diversidad de genotipos

3. **Diversidad Genética**
   - Índice de diversidad por cromosoma
   - Correlación SNPs vs Genotipos

### Tab 5: ℹ️ Información
- Configuración del sistema
- Arquitectura completa
- Rutas en HDFS
- Tecnologías utilizadas

## 🔧 Comandos Útiles

### Verificar datos en HDFS

```bash
# Entrar al NameNode
docker exec -it namenode bash

# Listar archivos
hdfs dfs -ls /genomic_data
hdfs dfs -ls /genomic_data/fathers
hdfs dfs -ls /genomic_data/mothers
hdfs dfs -ls /genomic_data/children
hdfs dfs -ls /genomic_data/families_metadata

# Ver tamaño de datos
hdfs dfs -du -h /genomic_data
```

### Ejecutar Spark Shell

```bash
# Entrar al Spark Master
docker exec -it spark-master bash

# Lanzar Spark Shell (Scala)
spark-shell --master spark://spark-master:7077

# Lanzar PySpark (Python)
pyspark --master spark://spark-master:7077
```

### Consultar datos con Spark SQL

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GenomicQuery") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

# Leer datos
df = spark.read.parquet("hdfs://namenode:9000/genomic_data/fathers/*.parquet")

# Mostrar esquema
df.printSchema()

# Contar registros
df.count()

# Ver muestra
df.show(10)

# Análisis por cromosoma
df.groupBy("chromosome").count().show()
```

## 📁 Estructura de Datos en HDFS

```
/genomic_data/
├── fathers/
│   ├── batch_20251019_120000_0.parquet
│   ├── batch_20251019_120030_1.parquet
│   └── ...
├── mothers/
│   ├── batch_20251019_120000_0.parquet
│   └── ...
├── children/
│   ├── batch_20251019_120000_0.parquet
│   └── ...
└── families_metadata/
    ├── family_FAM_ABC123_20251019_120000.json
    └── ...
```

### Formato Parquet Schema

```
root
 |-- family_id: string
 |-- person_id: string
 |-- member_type: string
 |-- chromosome: string
 |-- position: long
 |-- genotype: string
 |-- timestamp: string
 |-- gender: string
```

## 🛑 Detener el Sistema

```bash
# Detener consumer
cd cosumer
docker-compose down

# Detener producer
cd producer
docker-compose down
```

## 🔥 Limpiar todo (incluye volúmenes)

```bash
# Detener y eliminar volúmenes del consumer
cd cosumer
docker-compose down -v

# Detener y eliminar volúmenes del producer
cd producer
docker-compose down -v
```

⚠️ **Advertencia**: Esto eliminará TODOS los datos almacenados en HDFS.

## 📈 Rendimiento y Escalabilidad

### Configuración Actual
- **Producers**: 3 instancias
- **DataNodes**: 3 nodos
- **NodeManagers**: 2 nodos
- **Spark Workers**: 2 workers (2 cores, 2GB cada uno)
- **Batch Size**: 10,000 SNPs

### Escalar Horizontalmente

#### Más Producers
```bash
docker-compose up --scale genomic-producer=5 --build -d
```

#### Más DataNodes (editar docker-compose.yml)
```yaml
datanode4:
  image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
  # ... configuración similar a datanode1
```

#### Más Spark Workers
```yaml
spark-worker-3:
  image: bde2020/spark-worker:3.3.0-hadoop3.3
  # ... configuración similar a spark-worker-1
```

## 🐛 Troubleshooting

### El dashboard no carga datos

1. Verificar que HDFS tenga datos:
```bash
docker exec -it namenode hdfs dfs -ls /genomic_data/fathers
```

2. Verificar logs del loader:
```bash
docker-compose logs -f genomic-consumer | grep "HDFS Loader"
```

### Spark Jobs fallan

1. Verificar memoria disponible en workers:
```bash
# Acceder a Spark Master UI: http://localhost:8080
```

2. Ajustar configuración en docker-compose.yml:
```yaml
YARN_CONF_yarn_nodemanager_resource_memory__mb=8192
SPARK_WORKER_MEMORY=4g
```

### HDFS NameNode no arranca

1. Verificar que no haya conflictos de puertos:
```bash
lsof -i :9870
lsof -i :9000
```

2. Limpiar y reiniciar:
```bash
docker-compose down -v
docker-compose up -d
```

## 📚 Referencias

- [Hadoop HDFS Documentation](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Gradio Documentation](https://www.gradio.app/docs/)
- [Apache Parquet Format](https://parquet.apache.org/docs/)

## 👥 Autores

Proyecto académico de Procesamiento de Grandes Volúmenes de Datos

---

🧬 **Genomic Analysis Platform** | Big Data Technologies | 2025
