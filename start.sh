#!/bin/bash

cd /Users/alberto/Desktop/PGVD/producer && docker-compose up --scale genomic-producer=1 --build -d
cd /Users/alberto/Desktop/PGVD/cosumer && docker-compose up --build -d
