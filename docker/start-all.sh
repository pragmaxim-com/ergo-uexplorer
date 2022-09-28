#!/bin/bash

set -euo pipefail

export CASSANDRA_HEAP_NEWSIZE=3G
export CASSANDRA_MAX_HEAP_SIZE=12G
export ERGO_MAX_HEAP=4G

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
          docker compose -f docker-compose.yml -f docker-compose.node.yml up -d
          break;;
        [Nn]* )
          echo "Starting cassandra, stargate and uexplorer..."
          docker compose up -d
          break;;
        * ) echo "y/n ?";;
    esac
done
