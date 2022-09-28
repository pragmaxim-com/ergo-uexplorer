#!/bin/bash

docker compose stop uexplorer

echo "Flushing Cassandra memtables to disk and then stopping and removing all containers"

docker exec -it cassandra nodetool drain

docker compose stop stargate
docker compose stop cassandra
