# PGVD - Plataforma de Generación y Visualización de Datos Genómicos

## 📋 Descripción del Proyecto

Implementa una arquitectura de streaming distribuida utilizando **Kafka** como broker de mensajes, **Spark Structured Streaming** para procesamiento en tiempo real, y un dashboard interactivo para visualización.

### Características Principales
- ✅ **Generación masiva de SNPs genómicos** en múltiples threads paralelos
- ✅ **Procesamiento distribuido en tiempo real** con Spark Structured Streaming
- ✅ **Análisis de variantes genéticas** por familia
- ✅ **Dashboard interactivo** para monitoreo y visualización
- ✅ **Arquitectura dockerizada** para facilitar despliegue

---

## 📁 Estructura del Repositorio

```
PGVD/
├── producer/                 # Producer: Generador de datos genómicos
│   ├── producer.py          # Script principal de producción
│   ├── family_generator.py  # Generador de familias y SNPs
│   ├── streaming_manager.py # Gestor de streaming Kafka
│   ├── config.py            # Configuración del producer
│   ├── family/              # Modelos de generación familiar
│   │   ├── base_generator.py
│   │   ├── father.py
│   │   ├── mother.py
│   │   └── childs.py
│   ├── models/              # Modelos de datos
│   │   ├── __init__.py
│   │   └── data_models.py
│   ├── data/                # Datos de entrada
│   │   └── archive-2/       # Genomas de referencia (Kaggle)
│   ├── docker-compose.yml   # Orquestación de contenedores
│   ├── Dockerfile           # Imagen del producer
│   ├── requirements.txt      # Dependencias Python
│   └── aed.ipynb            # Análisis exploratorio de datos
│
├── cosumer/                 # Consumer: Procesamiento en tiempo real
│   ├── spark_consumer.py    # Consumer principal con Spark Streaming
│   ├── docker-compose.yml   # Orquestación de contenedores
│   ├── dockerfile           # Imagen del consumer
│   ├── requirements.txt      # Dependencias Python
│   ├── requirements_dashboard.txt  # Dependencias del dashboard
│   ├── dashboard/           # Dashboard web interactivo
│   │   ├── dashboard_advanced.py         # Backend avanzado
│   │   ├── dashboard_genomic_advanced.py # Lógica genómica
│   │   ├── static/
│   │   │   └── dashboard.js
│   │   └── templates/
│   │       ├── index.html
│   │       └── index_advanced.html
│   ├── entrypoint.sh        # Script de entrada del contenedor
│   └── requirements_dashboard.txt
│
├── start.sh                 # 🚀 Script para iniciar el proyecto completo
├── stop.sh                  # 🛑 Script para detener el proyecto
├── README.md                # Este archivo
└── improve.md               # Mejoras futuras y notas técnicas

Dataset de entrada: [Family Genome Dataset - Kaggle](https://www.kaggle.com/datasets/zusmani/family-genome-dataset)
```

---

## 🚀 Inicio Rápido

### Requisitos Previos
- **Docker** (versión 20.10+)
- **Docker Compose** (versión 1.29+)
- **Git**
- **Variables de entorno** configuradas en `.env.sh`

### Instalación y Ejecución

#### 1️⃣ Clonar el repositorio
```bash
git clone https://github.com/ALbertE03/PGVD.git
cd PGVD
```

#### 2️⃣ Configurar variables de entorno
Crear archivo `.env.sh` en la raíz del proyecto:
```bash
#!/bin/bash
export PRODUCER_PATH="$(dirname "$0")/producer"
export CONSUMER_PATH="$(dirname "$0")/cosumer"
export KAFKA_BROKER="kafka:9092"
export SPARK_MASTER="spark://spark-master:7077"
```

Hacer el archivo ejecutable:
```bash
chmod +x .env.sh
```

#### 3️⃣ Ejecutar el proyecto completo

**Opción A: Script de inicio automático (RECOMENDADO)**
```bash
chmod +x start.sh
./start.sh
```

**Opción B: Ejecutar componentes manualmente**
```bash
# Terminal 1: Producer
cd producer && docker compose up --scale genomic-producer=2 -d

# Terminal 2: Consumer
cd cosumer && docker compose up -d
```

#### 4️⃣ Acceder al Dashboard
Una vez que los servicios estén ejecutándose:
```
http://localhost:5000
```

#### 5️⃣ Detener el proyecto
```bash
./stop.sh
```

---

## 📊 Componentes del Sistema

### Producer (Generador de Datos)
**Ubicación**: `producer/`

Genera datos genómicos sintéticos a partir del dataset de Kaggle. Características:
- Crea familias (padre, madre, hijos)
- Genera SNPs (Single Nucleotide Polymorphisms)
- Produce mensajes a tópics de Kafka especializados (`fathers`, `mothers`, `children`)
- Ejecuta múltiples instancias en paralelo para simular carga masiva

**Scripts de ingesta**:
- `producer.py` - Punto de entrada principal
- `family_generator.py` - Lógica de generación de familias
- `streaming_manager.py` - Gestor de flujo a Kafka

**Archivos relacionados**:
- `family/base_generator.py` - Clase base de generación
- `family/father.py`, `mother.py`, `childs.py` - Modelos genéticos
- `models/data_models.py` - Esquemas y estructuras de datos

---

### Consumer (Spark Structured Streaming)
**Ubicación**: `cosumer/`

Procesa el flujo de datos genómicos en tiempo real utilizando Apache Spark. Características:
- Lectura de Kafka en tiempo real
- Procesamiento con ventanas deslizantes (5 segundos)
- Agregaciones y análisis de variantes genéticas
- Almacenamiento de resultados

**Script principal**: `spark_consumer.py`

---

### Dashboard Interactivo
**Ubicación**: `cosumer/dashboard/`

Visualización web en tiempo real de los análisis genómicos. Características:
- Gráficos de variantes por familia
- Estadísticas de SNPs
- Monitoreo de salud del sistema
- Interfaz responsive

**Acceso**: `http://localhost:5000`

**Versiones disponibles**:
- `dashboard.py` - Versión estándar
- `dashboard_advanced.py` - Versión avanzada con análisis complejos
- `dashboard_genomic_advanced.py` - Análisis genómico especializado

---

## 🐳 Docker & Docker Compose

### Estructura Docker

**Producer Stack**:
```yaml
producer/docker-compose.yml
├── genomic-producer × 2  # Instancias paralelas del generador
├── kafka                # Broker de mensajes
└── zookeeper           # Coordinador Kafka
```

**Consumer Stack**:
```yaml
cosumer/docker-compose.yml
├── spark-master        # Coordinador Spark
├── spark-worker        # Nodos de procesamiento distribuido
└── dashboard          # Servidor Flask de visualización
```

### Comandos Docker Útiles

**Ver logs en tiempo real**:
```bash
# Producer
cd producer && docker compose logs -f

# Consumer
cd cosumer && docker compose logs -f
```

**Listar contenedores activos**:
```bash
docker ps -a
```

**Detener servicios específicos**:
```bash
# Producer
cd producer && docker compose down

# Consumer
cd cosumer && docker compose down
```

**Reconstruir imágenes**:
```bash
cd producer && docker compose build --no-cache
cd cosumer && docker compose build --no-cache
```

**Ver recursos utilizados**:
```bash
docker stats
```

---

## 📈 Pipeline de Datos

```
┌─────────────────────┐
│  Datos Kaggle       │
│ (Genomas Familiares)│
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│     Producer        │
│  (x2 paralelo)      │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│   Kafka Topics      │
│ fathers,mothers,... │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Spark Consumer     │
│  Streaming Analysis │
└──────────┬──────────┘
           ↓
┌─────────────────────┐
│  Dashboard Web      │
│ http://localhost:5000
└─────────────────────┘
```

---

## 🔧 Configuración

### Variables de Entorno Clave
```bash
PRODUCER_PATH="./producer"          # Ruta al directorio del producer
CONSUMER_PATH="./cosumer"           # Ruta al directorio del consumer
KAFKA_BROKER="kafka:9092"           # Dirección del broker Kafka
SPARK_MASTER="spark://spark-master:7077"  # URL del master de Spark
```

### Archivos de Configuración
- `producer/config.py` - Parámetros del producer
- `.env.sh` - Variables de entorno globales
- `producer/docker-compose.yml` - Servicios del producer
- `cosumer/docker-compose.yml` - Servicios del consumer

---

## 📚 Consultas y Análisis Disponibles

El sistema ejecuta análisis automáticos sobre:

### 1. Estadísticas de SNPs por Familia
- Recuento total de variantes genéticas
- Frecuencia de alelos (A/T/G/C)
- Patrones de herencia familiar

### 2. Análisis de Variabilidad Genética
- Diversidad genómica dentro de familias
- Correlaciones entre padres e hijos
- Detección de anomalías genéticas

### 3. Métricas de Rendimiento del Sistema
- Mensajes procesados por segundo
- Latencia de procesamiento (ms)
- Disponibilidad del cluster Spark

---
