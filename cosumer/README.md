# 🧬 Genomic Data Consumer Dashboard

Dashboard interactivo en tiempo real para consumir y visualizar datos genómicos desde Apache Kafka.

## 🚀 Características

- **Consumo en Tiempo Real**: Consume mensajes de Kafka de forma continua y eficiente
- **Interfaz Interactiva**: Dashboard web con Gradio para visualización de datos
- **Almacenamiento en HDFS**: Persistencia automática de datos en HDFS
- **Métricas en Vivo**: Seguimiento de tasa de consumo y estadísticas en tiempo real
- **Visualizaciones Dinámicas**: Gráficos interactivos con Plotly

## 📊 Visualizaciones Disponibles

1. **Distribución por Población**: Gráfico de barras mostrando la distribución de muestras por población genética
2. **Estado de QC**: Gráfico circular con el porcentaje de muestras que pasaron/fallaron el control de calidad
3. **Distribución de Coverage**: Histograma de la profundidad de cobertura de secuenciación
4. **Plataformas de Secuenciación**: Distribución de muestras por plataforma tecnológica utilizada

## 🏗️ Arquitectura

```
┌─────────────┐      ┌─────────────┐      ┌──────────────┐
│   Kafka     │─────▶│  Consumer   │─────▶│    HDFS      │
│   Broker    │      │  Dashboard  │      │   Storage    │
└─────────────┘      └─────────────┘      └──────────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │   Gradio    │
                     │  Dashboard  │
                     └─────────────┘
```

## 🔧 Configuración

### Variables de Entorno

- `KAFKA_BROKER`: Dirección del broker de Kafka (default: `kafka:9092`)
- `KAFKA_TOPIC`: Topic de Kafka a consumir (default: `genomic-data`)
- `HDFS_NAMENODE_URL`: URL del namenode de HDFS (default: `http://namenode:9870`)
- `HDFS_USER`: Usuario para HDFS (default: `root`)

### Componentes del Sistema

- **Kafka Consumer**: Consume mensajes del topic `genomic-data`
- **Buffer en Memoria**: Mantiene los últimos 10,000 registros en memoria para acceso rápido
- **HDFS Writer**: Escribe batches de datos a HDFS cada 60 segundos o cada 100 registros
- **Gradio Dashboard**: Interfaz web en el puerto 7860

## 🐳 Despliegue con Docker

### Construir y Ejecutar

```bash
# Desde el directorio cosumer/
docker-compose up -d --build
```

### Ver Logs

```bash
docker logs -f genomic-consumer
```

### Detener

```bash
docker-compose down
```

## 📈 Acceso al Dashboard

Una vez que el contenedor esté ejecutándose, accede al dashboard en:

```
http://localhost:7860
```

## 📋 Métricas Disponibles

El dashboard muestra las siguientes métricas en tiempo real:

- **Mensajes Consumidos**: Total de mensajes procesados
- **Buffer en Memoria**: Cantidad de registros actualmente en el buffer
- **Tasa Actual**: Mensajes por segundo (últimos 5 segundos)
- **Tasa Promedio**: Promedio de mensajes por segundo desde el inicio
- **Tiempo Transcurrido**: Tiempo total de ejecución

## 🔄 Auto-Refresh

El dashboard se actualiza automáticamente cada 5 segundos. También puedes usar el botón "🔄 Actualizar Datos" para refrescar manualmente.

## 📦 Estructura de Datos

Cada mensaje consumido contiene información sobre:

- **Identificación**: Sample ID, Population, Platform
- **Métricas de Calidad**: QC Status, Coverage, Mapping Quality
- **Métricas de Secuenciación**: Read Length, GC Content, Duplication Rate
- **Variantes**: SNPs, INDELs, Structural Variants
- **Información Clínica**: Gender, Age, Phenotype
- **Datos Técnicos**: Sequencer ID, Flow Cell, Pipeline Version

## 🛠️ Desarrollo

### Estructura de Archivos

```
cosumer/
├── consumer_dashboard.py   # Aplicación principal
├── dockerfile              # Imagen Docker
├── docker-compose.yml      # Orquestación de servicios
├── entrypoint.sh          # Script de inicio
├── requirements.txt       # Dependencias Python
└── README.md             # Este archivo
```

### Dependencias Principales

- `kafka-python`: Cliente de Kafka para Python
- `gradio`: Framework para crear la interfaz web
- `plotly`: Biblioteca de visualización interactiva
- `pandas`: Manipulación de datos
- `hdfs`: Cliente HDFS para Python

## 🐛 Troubleshooting

### El consumer no se conecta a Kafka

1. Verifica que el servicio de Kafka esté ejecutándose:
   ```bash
   docker ps | grep kafka
   ```

2. Revisa los logs del consumer:
   ```bash
   docker logs genomic-consumer
   ```

3. Verifica la conectividad de red:
   ```bash
   docker exec genomic-consumer nc -zv kafka 9092
   ```

### No hay datos en el dashboard

1. Verifica que el producer esté ejecutándose y enviando datos
2. Revisa los logs para ver si hay mensajes de error
3. Verifica que el topic de Kafka sea el correcto

### Error de conexión a HDFS

El consumer seguirá funcionando sin HDFS, pero no persistirá los datos. Verifica:

1. Que el namenode de HDFS esté ejecutándose
2. Que la URL del namenode sea correcta
3. Los logs para mensajes de advertencia específicos

## 📝 Notas

- El buffer en memoria es circular con capacidad para 10,000 registros
- Los datos se escriben a HDFS en batches para optimizar el rendimiento
- El dashboard usa auto-refresh para mantener los datos actualizados
- El consumer usa `latest` offset por defecto (solo mensajes nuevos)

## 🤝 Integración con el Ecosistema

Este consumer está diseñado para trabajar con:

- **Producer**: Genera datos genómicos sintéticos y los envía a Kafka
- **Kafka**: Broker de mensajería para streaming de datos
- **HDFS**: Sistema de archivos distribuido para almacenamiento persistente
- **Hadoop/YARN**: Procesamiento distribuido de datos (futuro)

## 📄 Licencia

Este proyecto es parte del sistema PGVD (Processing Genomic Variant Data).
