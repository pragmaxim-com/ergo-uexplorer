#!/bin/bash

set -euo pipefail

echo "Flushing scylla memtables to disk and then stopping and removing docker container..."

docker exec -it ergo-scylla nodetool drain && docker stop ergo-scylla && docker rm ergo-scylla