#!/bin/bash

set -euo pipefail

export CASSANDRA_HEAP_NEWSIZE=2G
export CASSANDRA_MAX_HEAP_SIZE=8G
export ERGO_MAX_HEAP=1G
export BACKEND_INDEXING_PARALLELISM=1

while true; do
    read -p "Are you going to sync blockchain from scratch? " yn
    case $yn in
        [Yy]* )
          MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo);
          if [ "$MEM_TOTAL" -lt 10000000 ]
          then
              echo "You should have at least 12GB of RAM, exiting ..."
              exit 1
          elif [ "$MEM_TOTAL" -lt 12000000 ]
          then
            echo "Please close all memory intensive processes like Browser, IDE, etc. (OOM killer might kick in) until syncing finishes"
          elif [ "$MEM_TOTAL" -gt 16000000 ]
          then
            export CASSANDRA_HEAP_NEWSIZE=2G
            export CASSANDRA_MAX_HEAP_SIZE=10G
            export ERGO_MAX_HEAP=2G
            V_CPU_COUNT=$(nproc --all)
            if [ "$V_CPU_COUNT" -ge 16 ]; then export BACKEND_INDEXING_PARALLELISM=2; fi
          fi
          break;;
        [Nn]* )
          export CASSANDRA_HEAP_NEWSIZE=512M
          export CASSANDRA_MAX_HEAP_SIZE=2G
          break;;
        * ) echo "y/n ?";;
    esac
done

while true; do
    read -p "Do you want to create Ergo Node docker container? " yn
    case $yn in
        [Yy]* )
          echo "Starting Ergo node, cassandra, stargate and uexplorer..."
          docker compose -f docker-compose.yml -f docker-compose.node.yml up -d --no-recreate
          break;;
        [Nn]* )
          echo "Starting cassandra, stargate and uexplorer..."
          docker compose up -d --no-recreate
          break;;
        * ) echo "y/n ?";;
    esac
done
