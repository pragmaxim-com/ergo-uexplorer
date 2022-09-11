#!/bin/bash

set -euo pipefail

printf "Make sure that:
  - /var/lib/scylla dir exists
  - /proc/sys/fs/aio-max-nr = 1048576
  - you have at least 14GB of ram
  - your system won't run another memory intensive processes like Browser, IDE, etc. (OOM killer might kick in) \n"

while true; do
    read -p "Do you want to continue? " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "y/n ?";;
    esac
done

if [ ! -d "/var/lib/scylla" ]
then
    echo "Directory /var/lib/scylla DOES NOT exists."
    exit 1
fi

MEM_TOTAL=$(awk '/^MemTotal:/{print $2}' /proc/meminfo);
if [ "$MEM_TOTAL" -lt 14000000 ]
then
    echo "Scylla is tested only with 16GB ram, you should have at least 14GB, it will most likely fail otherwise."
    exit 1
fi

AIO_MAX_NR=$(cat /proc/sys/fs/aio-max-nr);
if [ "$AIO_MAX_NR" -lt 1000000 ]
then
    echo "Indexing is tested with /proc/sys/fs/aio-max-nr = 1048576, it will most likely fail otherwise"
    exit 1
fi

VCPU_AVAILABLE=$(nproc --all)
SCYLLA_SMP=$((VCPU_AVAILABLE / 2))

docker run \
   --memory 14G \
   --oom-kill-disable \
   --name ergo-scylla \
   -p 9042:9042 -p 10000:10000 -p 5090:5090 -p 9180:9180 -p 9100:9100 \
   --volume /var/lib/scylla:/var/lib/scylla \
   --volume ${PWD}/scylla.cql:/tmp/scylla.cql \
   --hostname ergo-scylla \
   -d scylladb/scylla:5.1 --overprovisioned 1 --developer-mode 1 --memory 11G --reserve-memory 3G --smp $SCYLLA_SMP

echo "Waiting for scylla to initialize..."
sleep 45;

echo "Loading db schema"
docker exec -it ergo-scylla cqlsh --file '/tmp/scylla.cql'
