#!/bin/bash


# Iniciar Producer
cd /Users/alberto/Desktop/PGVD/producer && docker-compose up --scale genomic-producer=1 -d

# Iniciar Consumer
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose up -d