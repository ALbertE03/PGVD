#!/bin/bash

set -e

# Cargar variables privadas
source "$(dirname "$0")/.env.sh"

echo "🛑 Deteniendo sistema y eliminando volúmenes..."
echo ""

echo "⏹️  Deteniendo producer..."
cd "$PRODUCER_PATH" && docker-compose down -v

echo ""
echo "⏹️  Deteniendo consumer..."
cd "$CONSUMER_PATH" && docker-compose down -v
