# 🧬 PGVD - Genomic Data Processing with Apache Spark & Kafka

**Proyecto Académico de Procesamiento de Datos Genómicos en Tiempo Real usando Apache Spark, Kafka y HDFS**

## 📋 Descripción del Proyecto

PGVD es una plataforma de **procesamiento distribuido en tiempo real** de datos genómicos usando:

- **Apache Spark**: Procesamiento distribuido de datos genéticos
- **Apache Kafka**: Ingesta en tiempo real de datos desde productores
- **HDFS**: Almacenamiento distribuido de datasets
- **Flask Dashboard**: Visualización avanzada con métricas de streaming genético
- **Docker Compose**: Orquestación completa de servicios

### Características Principales

✅ **Procesamiento de Datos Genómicos**
- Análisis de familias (Padre, Madre, Hijos)
- Detección de variantes genéticas
- Cálculo de orientación genética (Dominante/Recesivo/Heterocigoto)

✅ **Streaming en Tiempo Real**
- Ingesta desde Kafka
- Ventanas de tiempo para análisis
- Detección de anomalías genéticas

✅ **Métricas Avanzadas**
- Tasa de mutación en tiempo real
- Distribución de genotipos
- Top genes y variantes detectados
- Diversidad genética
- Tendencias de mutaciones

✅ **Monitoreo del Cluster**
- Métricas de Spark (Masters, Workers, Jobs)
- Estado de HDFS (DataNodes, Storage)
- Rendimiento de Executors

---

## 🏗️ Arquitectura del Proyecto

```
┌─────────────────────────────────────────────────────────────┐
│                     PRODUCER (producer/)                     │
│  - family_generator.py: Genera familias genéticas           │
│  - producer.py: Envía datos a Kafka                         │
│  - streaming_manager.py: Gestiona el flujo                  │
└────────────────┬────────────────────────────────────────────┘
                 │ [KAFKA TOPICS]
┌────────────────▼────────────────────────────────────────────┐
│                  CONSUMER (cosumer/)                         │
│  - spark_consumer.py: Consume desde Kafka                   │
│  - Procesa datos genómicos en Spark                         │
│  - Almacena en HDFS                                         │
│  - Envía métricas al Dashboard                              │
└────────────────┬────────────────────────────────────────────┘
                 │ [REST API]
┌────────────────▼────────────────────────────────────────────┐
│           DASHBOARD (cosumer/dashboard/)                     │
│  - dashboard_advanced.py: Backend Flask                     │
│  - dashboard.js: Gráficos interactivos                      │
│  - index.html: UI moderna                                   │
│  - Visualización de métricas en tiempo real                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Guía de Inicio Rápido

### Requisitos Previos

- **Docker** y **Docker Compose** instalados
- Mínimo **8GB RAM** para el cluster Spark
- Datos genómicos en `producer/data/archive-2/` (5 archivos CSV)

### 1️⃣ Preparar Datos

Coloca los archivos genómicos en `producer/data/archive-2/`:
```
producer/data/archive-2/
├── Father Genome.csv
├── Mother Genome.csv
├── Child 1 Genome.csv
├── Child 2 Genome.csv
└── Child 3 Genome.csv
```

**Dataset**: [Family Genome Dataset - Kaggle](https://www.kaggle.com/datasets/zusmani/family-genome-dataset)

### 2️⃣ Desplegar con Docker

```bash
git clone https://github.com/ALbertE03/PGVD.git

```bash
# Iniciar todos los servicios
./start.sh

# Monitorear logs en tiempo real
docker-compose logs -f

# Ver estado de servicios
docker-compose ps
```

### 3️⃣ Acceder al Dashboard

```
http://localhost:5000
```

**Navegación del Dashboard:**
- 🖥️ **Cluster Metrics**: Estado de Spark, HDFS y Jobs
- 🧬 **Genetic Streaming**: Métricas avanzadas de datos genómicos
- 📊 **Data Analysis**: Estadísticas de procesamiento

### 4️⃣ Detener Servicios

```bash
./stop.sh
```

---

## 📁 Estructura del Proyecto

```
PGVD/
├── README.md                          # Este archivo
├── start.sh                           # Script de inicio
├── stop.sh                            # Script de parada
│
├── producer/                          # Ingesta de datos
│   ├── producer.py                    # Envía datos a Kafka
│   ├── family_generator.py            # Generador de datos genómicos
│   ├── streaming_manager.py           # Gestor de flujos
│   ├── config.py                      # Configuración
│   ├── requirements.txt               # Dependencias Python
│   ├── Dockerfile                     # Imagen Docker
│   ├── docker-compose.yml             # Orquestación
│   └── data/
│       └── archive-2/                 # Datos genómicos (CSV)
│
├── cosumer/                           # Procesamiento y dashboard
│   ├── spark_consumer.py              # Consumer Spark
│   ├── requirements.txt               # Dependencias Python
│   ├── requirements_dashboard.txt     # Deps del dashboard
│   ├── entrypoint.sh                  # Script de inicio
│   ├── docker-compose.yml             # Orquestación
│   ├── dockerfile                     # Imagen base
│   ├── Dockerfile.driver              # Spark Driver
│   ├── Dockerfile.master              # Spark Master
│   ├── Dockerfile.worker              # Spark Worker
│   ├── Dockerfile.dashboard           # Dashboard Flask
│   ├── driver-entrypoint.sh           # Inicio del driver
│   ├── worker-entrypoint.sh           # Inicio del worker
│   │
│   ├── dashboard/
│   │   ├── dashboard_advanced.py      # Backend Flask (NEW)
│   │   ├── static/
│   │   │   └── dashboard.js           # Gráficos avanzados (UPDATED)
│   │   └── templates/
│   │       └── index.html             # UI moderna (UPDATED)
│   │
│   └── models/
│       ├── __init__.py
│       └── data_models.py             # Modelos de datos
│
├── informe/
│   └── informe_pgvd.tex               # Documentación académica
│
└── photos/                            # Capturas de pantalla
```

---

## 🛠️ Scripts de Control

### `start.sh` - Inicia todos los servicios

```bash
./start.sh
```

**Qué hace:**
1. Construye imágenes Docker
2. Inicia Producer (Kafka, Zookeeper)
3. Inicia Consumer (Spark, HDFS, Dashboard)
4. Inicia generador de datos
5. Monitorea logs en tiempo real

### `stop.sh` - Detiene todos los servicios

```bash
./stop.sh
```

**Qué hace:**
1. Detiene y elimina contenedores
2. Elimina volúmenes (opcional)
3. Limpia recursos de Docker

---

## 🔌 Servicios y Puertos

### Producer (Puerto 9092)
- **Kafka Broker**: `kafka:9092`
- **Zookeeper**: `zookeeper:2181`
- **Jupyter/Generador**: `http://localhost:8888`

### Consumer - Cluster Spark
- **Spark Master**: `http://spark-master-1:8080`
- **Spark Worker 1**: `http://spark-worker-1:8081`
- **Spark Worker 2**: `http://spark-worker-2:8082`
- **Spark Worker 3**: `http://spark-worker-3:8083`
- **Spark Driver UI**: `http://localhost:4040`

### Consumer - HDFS
- **NameNode**: `http://namenode:9870`
- **DataNode 1**: `http://datanode-1:9864`
- **DataNode 2**: `http://datanode-2:9864`
- **DataNode 3**: `http://datanode-3:9864`

### Dashboard
- **Flask Dashboard**: `http://localhost:5000`
- **API REST**: `http://localhost:5000/api/*`

---

## 📊 Endpoints de API

### Métricas de Procesamiento
```
GET /api/stats                    # Conteo de familias procesadas
GET /api/cluster_stats            # Estado del cluster Spark/HDFS
GET /api/processing_history       # Historial de procesamiento
GET /api/task_times               # Tiempos de tareas completadas
```

### Métricas Genéticas (NUEVAS ✨)
```
GET /api/genetic_metrics          # Métricas de streaming genético
GET /api/genetic_trends           # Tendencias de mutaciones
POST /api/genetic_alert           # Detectar anomalías genéticas
```

### Métricas de Spark
```
GET /api/spark_jobs               # Jobs y executors activos
```

---

## 📝 Cómo Funciona el Streaming Genético

### 1. Producer envía datos a Kafka

```python
# producer.py
kafka_message = {
    "member_type": "fathers",
    "total_records": 100,
    "genetic_data": {
        "variant_type": "SNP",
        "gene": "BRCA1",
        "genotype": "0/1",
        "chromosome": 17,
        "position": 41196312,
        "quality": 99.0
    }
}
```

### 2. Consumer recibe y procesa

```python
# spark_consumer.py
# 1. Consume mensajes de Kafka
# 2. Analiza datos genéticos
# 3. Calcula métricas en ventanas de tiempo
# 4. Envía a Dashboard vía API REST
```

### 3. Dashboard muestra en tiempo real

- **Tasa de Mutación**: Mutaciones por segundo en ventana de 60s
- **Distribución de Genotipos**: Dominante/Recesivo/Heterocigoto
- **Top Genes**: 5 genes más frecuentes
- **Top Variantes**: 5 variantes más comunes
- **Anomalías**: Desviaciones de tasa esperada

---

## 🔍 Métricas Avanzadas Explicadas

### 1️⃣ Tasa de Mutación
**Fórmula**: Número de variantes genéticas / segundos en ventana

```
Ventana: 60 segundos
Si se reciben 45 variantes en 60s → Tasa = 0.75 variantes/seg
```

### 2️⃣ Orientación Genética

| Genotipo | Orientación | Descripción |
|----------|-------------|------------|
| 0/0 | Recesivo | Dos alelos recesivos |
| 0/1 | Heterocigoto | Un alelo de cada tipo |
| 1/1 | Dominante | Dos alelos dominantes |

### 3️⃣ Detección de Anomalías

```
Anomalía detectada si:
|Tasa actual - Tasa esperada| > 2.5σ (desviaciones estándar)
```

### 4️⃣ Diversidad Genética

```
Diversidad = Número de genes únicos / Total de variantes
Rango: 0-1 (1 = máxima diversidad)
```

---

## 🐛 Troubleshooting

### ❌ "Connection refused" en Kafka

```bash
# Verificar que Kafka está corriendo
docker-compose -f producer/docker-compose.yml ps

# Reiniciar
docker-compose -f producer/docker-compose.yml restart kafka zookeeper
```

### ❌ "No space left on device"

```bash
# Limpiar imágenes y volúmenes
docker system prune -a --volumes
./start.sh
```

### ❌ Dashboard no carga gráficos

```bash
# Verificar que Flask está corriendo
docker-compose -f cosumer/docker-compose.yml ps

# Ver logs
docker-compose -f cosumer/docker-compose.yml logs dashboard
```

### ❌ Spark sin workers conectados

```bash
# Reiniciar cluster Spark
docker-compose -f cosumer/docker-compose.yml restart spark-master-1
docker-compose -f cosumer/docker-compose.yml restart spark-worker-1 spark-worker-2 spark-worker-3
```

---

## 📈 Ejemplos de Consultas

### Obtener todas las métricas genéticas

```bash
curl http://localhost:5000/api/genetic_metrics
```

**Respuesta:**
```json
{
  "window_size": 156,
  "mutation_rate": 0.0234,
  "mutation_rate_percent": 2.34,
  "genotype_distribution": {
    "dominant": 42,
    "recessive": 35,
    "heterozygous": 79
  },
  "top_genes": [
    {"gene": "BRCA1", "count": 23},
    {"gene": "TP53", "count": 18}
  ],
  "anomaly_count": 0
}
```

### Obtener tendencias

```bash
curl http://localhost:5000/api/genetic_trends
```

**Respuesta:**
```json
{
  "current_mutation_rate": 0.0234,
  "rate_change_percent": 5.2,
  "trend_direction": "up",
  "genotype_percentages": {
    "dominant": 26.92,
    "recessive": 22.44,
    "heterozygous": 50.64
  },
  "genetic_diversity": 0.45,
  "anomaly_rate": 0.0
}
```

### Detectar anomalía

```bash
curl -X POST http://localhost:5000/api/genetic_alert \
  -H "Content-Type: application/json" \
  -d '{"anomaly_threshold": 2.5}'
```

---

## 🎯 Justificación del Uso de Streaming

✅ **¿Por qué Apache Kafka?**
- Ingesta de datos genómicos en tiempo real (no batch)
- Tolerancia a fallos con replicación
- Escalabilidad horizontal

✅ **¿Por qué Apache Spark?**
- Procesamiento distribuido de ventanas de tiempo
- Cálculo eficiente de agregaciones
- Integración con HDFS y Kafka

✅ **¿Por qué HDFS?**
- Almacenamiento distribuido de datasets genómicos
- Alta disponibilidad (replicación 3x)
- Acceso paralelo desde Spark

✅ **¿Por qué Dashboard en tiempo real?**
- Monitoreo instantáneo de tasa de mutación
- Detección de anomalías genéticas
- Análisis de tendencias en directo

---

## 📚 Referencias

- [Apache Spark Documentation](https://spark.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache HDFS Architecture](https://hadoop.apache.org/docs/r3.3.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)
- [Family Genome Dataset - Kaggle](https://www.kaggle.com/datasets/zusmani/family-genome-dataset)

---
