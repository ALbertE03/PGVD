#!/bin/bash

set -e

source "$(dirname "$0")/.env.sh"

cd "$PRODUCER_PATH" && docker-compose up --scale genomic-producer=1 -d

cd "$CONSUMER_PATH" && docker-compose up  -d