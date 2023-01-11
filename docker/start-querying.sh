#!/bin/bash

set -euo pipefail

docker compose -f docker-compose.cassandra.yml -f docker-compose.stargate.yml -f docker-compose.janusgraph.yml up -d --no-recreate stargate