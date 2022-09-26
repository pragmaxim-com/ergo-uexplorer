#!/bin/bash

set -euo pipefail

if [ ! -d "/var/lib/cassandra" ]
then
    echo "Please create /var/lib/cassandra directory before proceeding."
fi

export CASSANDRA_HEAP_NEWSIZE=3G
export CASSANDRA_MAX_HEAP_SIZE=12G

while true; do
    read -p "Are you going to sync blockchain from scratch? " yn
    case $yn in
        [Yy]* )
          MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo);
          if [ "$MEM_TOTAL" -lt 14000000 ]
          then
              echo "You should have at least 14GB of RAM, exiting ..."
              exit 1
          elif [ "$MEM_TOTAL" -lt 17000000 ]
          then
            echo "Please close all memory intensive processes like Browser, IDE, etc. (OOM killer might kick in) until syncing finishes"
          fi
          break;;
        [Nn]* )
          export CASSANDRA_HEAP_NEWSIZE=1G
          export CASSANDRA_MAX_HEAP_SIZE=4G
          break;;
        * ) echo "y/n ?";;
    esac
done

docker compose up -d cassandra

(docker compose logs -f cassandra &) | grep -q -e "Created default superuser role" -e "Startup complete"

echo "Loading db schema"
docker exec -it cassandra cqlsh --file '/tmp/cassandra.cql'

docker compose up -d stargate

echo ""
echo "Waiting for stargate to start up..."
while [[ "$(curl -s -o /dev/null -w ''%{http_code}'' http://localhost:8082/health)" != "200" ]]; do
    printf '.'
    sleep 5
done

