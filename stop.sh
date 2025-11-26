#!/bin/bash

echo "🛑 Deteniendo sistema y eliminando volúmenes..."
echo ""

echo "⏹️  Deteniendo producer..."
cd /Users/alberto/Desktop/PGVD/producer && docker-compose down -v

echo ""
echo "⏹️  Deteniendo consumer..."
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose down -v

