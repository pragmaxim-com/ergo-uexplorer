#!/bin/bash

set -euo pipefail

echo "Flushing Cassandra memtables to disk and then stopping and removing docker container..."

docker exec -it cassandra nodetool drain && docker stop cassandra && docker rm cassandra

echo "Stopping and removing stargate docker container..."

docker stop stargate && docker rm stargate