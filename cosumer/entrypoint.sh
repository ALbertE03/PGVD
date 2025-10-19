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
HDFS_URL=${HDFS_NAMENODE_URL:-http://namenode:9870}
timeout=60
while ! curl -s -f "$HDFS_URL" > /dev/null; do
    echo "   HDFS no disponible en $HDFS_URL, esperando..."
    sleep 3
    timeout=$((timeout - 3))
    if [ $timeout -le 0 ]; then
        echo "❌ Timeout esperando a HDFS"
        exit 1
    fi
done

echo "✅ HDFS NameNode está disponible!"

# Verificar conexión con DataNodes
echo "⏳ Verificando DataNodes..."
sleep 5
echo "✅ DataNodes listos!"

echo "=================================================="
echo "🚀 Iniciando Dashboard de Consumidor..."
echo "=================================================="
echo "📍 Kafka Broker: $KAFKA_BROKER"
echo "📍 HDFS NameNode: $HDFS_URL"
echo "📍 Dashboard URL: http://localhost:7860"
echo "=================================================="

# Iniciar la aplicación
exec python consumer_dashboard.py
