#!/bin/bash

set -e

# Cargar variables privadas
source "$(dirname "$0")/.env.sh"

# Iniciar Producer
cd "$PRODUCER_PATH" && docker-compose up --scale genomic-producer=1 -d

# Iniciar Consumer
cd "$CONSUMER_PATH" && docker-compose up  -d