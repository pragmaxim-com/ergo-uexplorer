#!/bin/bash

set -euo pipefail

docker compose -f docker-compose.yml -f docker-compose.stargate.yml up -d --no-recreate stargate