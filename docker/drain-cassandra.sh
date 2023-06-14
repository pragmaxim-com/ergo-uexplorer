#!/bin/bash

echo "Flushing Cassandra memtables to disk ..."

docker exec -it cassandra nodetool drain
docker exec -it stargate nodetool drain
