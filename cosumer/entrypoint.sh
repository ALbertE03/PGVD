#!/bin/bash

echo "=================================================="
echo "🧬 GENOMIC CONSUMER - HDFS DASHBOARD"
echo "=================================================="

# Esperar a que Kafka esté disponible
echo "⏳ Esperando a que Kafka esté disponible..."
KAFKA_BROKER=${KAFKA_BROKER:-kafka:9092}
KAFKA_HOST=$(echo $KAFKA_BROKER | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BROKER | cut -d: -f2)

timeout=60
while ! nc -z $KAFKA_HOST $KAFKA_PORT; do
    echo "   Kafka no disponible en $KAFKA_HOST:$KAFKA_PORT, esperando..."
    sleep 2
    timeout=$((timeout - 2))
    if [ $timeout -le 0 ]; then
        echo "❌ Timeout esperando a Kafka"
        exit 1
    fi
done

echo "✅ Kafka está disponible!"

# Esperar a que HDFS NameNode esté disponible
echo "⏳ Esperando a que HDFS NameNode esté disponible..."
HDFS_URL=${HDFS_NAMENODE_URL:-hdfs://namenode:9000}
HDFS_WEB_URL=$(echo $HDFS_URL | sed 's/hdfs:\/\//http:\/\//g' | sed 's/:9000/:9870/g')

timeout=60
while ! curl -s -f "$HDFS_WEB_URL" > /dev/null; do
    echo "   HDFS no disponible en $HDFS_WEB_URL, esperando..."
    sleep 3
    timeout=$((timeout - 3))
    if [ $timeout -le 0 ]; then
        echo "❌ Timeout esperando a HDFS"
        exit 1
    fi
done

echo "✅ HDFS NameNode está disponible!"

# Verificar DataNodes
echo "⏳ Verificando DataNodes..."
sleep 5
echo "✅ DataNodes listos!"

echo "=================================================="
echo "🚀 Iniciando servicios..."
echo "=================================================="
echo "📍 Kafka Broker: $KAFKA_BROKER"
echo "📍 HDFS NameNode: $HDFS_URL"
echo "=================================================="

# Iniciar HDFS loader en background
echo "🔄 Iniciando HDFS Loader..."
python hdfs_loader.py &
LOADER_PID=$!

# Esperar un poco para que el loader inicialice
sleep 5

# Iniciar dashboard simple
echo "🎨 Iniciando Dashboard Simple..."
python simple_dashboard.py &
DASHBOARD_PID=$!

# Función para cleanup
cleanup() {
    echo "🛑 Deteniendo servicios..."
    kill $LOADER_PID 2>/dev/null
    kill $DASHBOARD_PID 2>/dev/null
    exit 0
}

trap cleanup SIGTERM SIGINT

# Esperar a que los procesos terminen
wait
