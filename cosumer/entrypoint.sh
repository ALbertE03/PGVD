#!/bin/bash
# Entrypoint script para Spark Consumer

echo "🧬 Genomic Data Consumer - Spark Streaming Mode"
echo "======================================"
echo ""
echo "▶️  Iniciando Spark Streaming Consumer..."
echo ""

exec python3 /app/spark_consumer.py
