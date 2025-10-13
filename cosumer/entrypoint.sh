#!/bin/bash

echo "🚀 Iniciando Genomic Consumer..."

# Esperar a que Kafka esté disponible
echo "⏳ Esperando a que Kafka esté disponible..."
timeout=60
counter=0
until nc -z kafka 9092; do
    counter=$((counter+1))
    if [ $counter -gt $timeout ]; then
        echo "❌ Timeout esperando a Kafka"
        exit 1
    fi
    echo "Intento $counter/$timeout: Kafka no está listo..."
    sleep 2
done
echo "✅ Kafka está disponible!"

# Esperar a que HDFS Namenode esté disponible
echo "⏳ Esperando a que HDFS Namenode esté disponible..."
counter=0
until curl -sf http://namenode:9870/ > /dev/null; do
    counter=$((counter+1))
    if [ $counter -gt $timeout ]; then
        echo "⚠️ Timeout esperando a HDFS (continuando de todas formas)..."
        break
    fi
    echo "Intento $counter/$timeout: HDFS no está listo..."
    sleep 2
done
echo "✅ HDFS está disponible!"

# Iniciar la aplicación
echo "🎯 Iniciando consumer dashboard..."
exec python consumer_dashboard.py
