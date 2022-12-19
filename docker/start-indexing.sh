#!/bin/bash

set -euo pipefail

V_CPU_COUNT=$(nproc --all)

while true; do
    read -p "Are you going to sync blockchain from scratch? " yn
    case $yn in
        [Yy]* )
          MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo);
          if [ "$MEM_TOTAL" -lt 16000000 ]
          then
              echo "You should have at least 16GB of RAM, exiting ..."
              exit 1
          elif [ "$V_CPU_COUNT" -ge 16 ] && [ "$MEM_TOTAL" -gt 30000000 ]
          then
            echo "This is not tested yet and it probably requires changing GC from CMS to G1"
            export CASSANDRA_HEAP_NEWSIZE=2G
            export CASSANDRA_MAX_HEAP_SIZE=16G
            export ERGO_MAX_HEAP=4G
            export BACKEND_INDEXING_PARALLELISM=2
          elif [ "$MEM_TOTAL" -gt 16000000 ]
          then
            echo "Please close all memory intensive processes like Browser, IDE, etc. (OOM killer might kick in) until syncing finishes"
            export CASSANDRA_HEAP_NEWSIZE=1G
            export CASSANDRA_MAX_HEAP_SIZE=9G
            export ERGO_MAX_HEAP=2G
            export BACKEND_INDEXING_PARALLELISM=1
          fi
          break;;
        [Nn]* )
          export CASSANDRA_HEAP_NEWSIZE=512M
          export CASSANDRA_MAX_HEAP_SIZE=1G
          export ERGO_MAX_HEAP=1G
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
