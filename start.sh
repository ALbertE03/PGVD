#!/bin/bash


# Iniciar Producer
cd /Users/alberto/Desktop/PGVD/producer && docker-compose up --scale genomic-producer=2 -d

# Iniciar Consumer
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose up -d