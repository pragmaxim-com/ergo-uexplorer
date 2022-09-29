#!/bin/bash

echo "Flushing Cassandra memtables to disk and then stopping and removing all containers"

docker exec -it cassandra nodetool drain
docker exec -it stargate nodetool drain
