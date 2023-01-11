#!/bin/bash

./drain-cassandra.sh

docker compose -f docker-compose.cassandra.yml -f docker-compose.node.yml -f docker-compose.stargate.yml down