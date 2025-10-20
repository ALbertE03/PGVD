#!/bin/bash
# Script para ejecutar la consola interactiva

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║    🧬 Consola Interactiva - Genomic Analyzer               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# Verificar que el sistema esté corriendo
if ! docker ps | grep -q "genomic-consumer"; then
    echo "❌ El sistema no está corriendo"
    echo ""
    echo "Inicia el sistema con:"
    echo "  docker-compose up -d"
    echo ""
    exit 1
fi

echo "✅ Sistema detectado"
echo ""
echo "Ejecutando consola interactiva dentro del contenedor..."
echo ""

# Ejecutar la consola interactiva
docker exec -it genomic-consumer python3 /app/interactive_console.py
