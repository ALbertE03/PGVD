#!/bin/bash

# Iniciar MongoDB replica set
cd /Users/alberto/Desktop/PGVD && docker-compose -f docker-compose.mongo.yml up -d

# Esperar a que MongoDB esté listo
echo "⏳ Esperando a que MongoDB inicie..."
sleep 10

# Iniciar Producer
cd /Users/alberto/Desktop/PGVD/producer && docker-compose up --scale genomic-producer=1 -d

# Iniciar Consumer
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose up -d
