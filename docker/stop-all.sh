#!/bin/bash

./stop-gracefully-ergo.sh
./stop-gracefully-cassandra.sh

docker compose -f docker-compose.yml -f docker-compose.node.yml down