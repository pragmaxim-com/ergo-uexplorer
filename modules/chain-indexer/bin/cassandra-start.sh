#!/bin/bash

set -euo pipefail

printf "Make sure that:
  - /var/lib/cassandra dir exists
  - /proc/sys/fs/aio-max-nr = 1048576
  - you have at least 16GB of ram
  - your system won't run another memory intensive processes like Browser, IDE, etc. (OOM killer might kick in) \n"

while true; do
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "y/n ?";;
    esac
done

if [ ! -d "/var/lib/cassandra" ]
then
    echo "Directory /var/lib/cassandra DOES NOT exists."
    exit 1
fi

MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo);
if [ "$MEM_TOTAL" -lt 14000000 ]
then
    echo "Cassandra is tested only with 16GB ram, you should have at least 14GB, it will most likely fail otherwise."
    exit 1
fi

AIO_MAX_NR=$(cat /proc/sys/fs/aio-max-nr);
if [ "$AIO_MAX_NR" -lt 1000000 ]
then
    echo "Indexing is tested with /proc/sys/fs/aio-max-nr = 1048576, it will most likely fail otherwise"
    exit 1
fi

docker compose up -d cassandra

(docker compose logs -f cassandra &) | grep -q "Created default superuser role"

echo "Loading db schema"
docker exec -it cassandra cqlsh --file '/tmp/cassandra.cql'

docker compose up -d stargate

echo ""
echo "Waiting for stargate to start up..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8082/health)" != "200" ]]; do
    printf '.'
    sleep 5
done

