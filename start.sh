#!/bin/bash

echo "🚀 Iniciando sistema de análisis genómico..."
echo ""

# Iniciar producer (Kafka + Zookeeper + Genomic Producer)
echo "📡 Iniciando Producer (Kafka + Zookeeper)..."
cd /Users/alberto/Desktop/PGVD/producer && docker-compose up --scale genomic-producer=1 --build -d

echo ""
echo "🧬 Iniciando Consumer (Hadoop + Spark)..."
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose up --build -d

echo ""
echo "⏳ Esperando que los servicios estén listos..."
sleep 15

echo ""
echo "✅ Sistema iniciado correctamente"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Servicios disponibles:"
echo "  • HDFS NameNode UI:    http://localhost:9870"
echo "  • YARN ResourceManager: http://localhost:8088"
echo "  • Spark UI:            http://localhost:4040"
echo "  • MapReduce History:   http://localhost:8188"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 Spark está procesando datos en tiempo real"
echo "💡 Para ver la consola interactiva, ejecuta:"
echo "   cd cosumer && ./console.sh"
echo ""
