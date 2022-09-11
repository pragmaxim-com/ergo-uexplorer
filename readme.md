# uExplorer

Supplementary ergo explorer with ScyllaDB backend with rapid indexing speed but limited querying possibilities.

## Chain Indexer

Chain indexer syncs with Ergo node and then keeps polling new blocks while discarding superseded forks.

**Requirements:**
  - local/remote ScyllaDB, installation script expects :
      - `/var/lib/scylla` dir exists
      - `/proc/sys/fs/aio-max-nr` has value 1048576
  - at least 32GB of ram (`scyllaDB = 25GB`, `ergo-node = 1GB`, `chain-indexer = 1GB`, `system = 5GB` to avoid OOM killer)
  - at least 4vCPUs but the whole stack was tested only on 8vCPUs machine (half of the cores is allocated for scylla)
  - local fully synced Ergo Node is running if you are syncing from scratch, polling new blocks is done from peer network

### Build

```
$ sbt stage
$ tree dist
dist
├── bin
│   ├── chain-indexer      # runs chain-indexer, expects ScyllaDB + Ergo Node running (in case of initial sync)
│   ├── chain-indexer.bat
│   ├── scylla.cql         # db schema sourced from scylla.sh
│   └── scylla-start.sh    # starts scylla in a docker container (either setup for sync or just for polling)
│   └── scylla-stop.sh     # flushes scylla memtables to disk and then stops and removes docker container
├── conf
│   ├── application.ini    # memory settings (default well tested)
│   └── chain-indexer.conf # local/remote ergo node address and scylla address must be defined
└── lib
    └── chain-indexer.jar  # fat jar of all dependencies
```

### Run

Have fully synced ergo node running locally for initial explorer sync,
start scylla in syncing mode and then chain-indexer syncs in ~ 90 minutes.
```
$ cd fully-synced-ergo-node
$ nohup java -Xmx1g -jar ergo.jar --mainnet -c ergo.conf &

$ cd ergo-uexplorer/dist/bin
$ ./scylla-start.sh
Do you want to start scylla for initial sync (s) or polling (p)? s
4098518a35b0cdc74bf598dc52bcf032d952a2c30271a43064c1c353adb5fd6d
Waiting for scylla to initialize...
Loading db schema

$ ./chain-indexer
11:43:19 Initiating indexing of 816 epochs ...
11:43:21 Persisted Epochs: 1[0], Blocks cache size (heights): 1538[1 - 1538], Invalid Epochs: 0
11:43:23 Persisted Epochs: 2[0 - 1], Blocks cache size (heights): 1538[1025 - 2562], Invalid Epochs: 0
11:43:24 Persisted Epochs: 3[0 - 2], Blocks cache size (heights): 1538[2049 - 3586], Invalid Epochs: 0
.......................................................................
13:09:50 Persisted Epochs: 814[0 - 813], Blocks cache size (heights): 1538[832513 - 834050], Invalid Epochs: 0
13:09:57 Persisted Epochs: 815[0 - 814], Blocks cache size (heights): 1544[833537 - 835080], Invalid Epochs: 0
13:10:07 Initiating polling...
13:10:08 Going to index 644 blocks starting at height 835585
13:11:51 Persisted Epochs: 816[0 - 815], Blocks cache size (heights): 1536[834561 - 836096], Invalid Epochs: 0
13:12:59 Going to index 3 blocks starting at height 836229
13:13:44 Going to index 1 blocks starting at height 836232
13:14:35 Going to index 1 blocks starting at height 836233
```

**Troubleshooting:**
  1. ScyllaDB crashes :
  
    - reason : most likely OOM killer kicked in and killed docker container and scylla process consequently
    - solution : avoid running another memory intensive processes (Browser, IDE),
                 chain-indexer is tested on a dedicated server (laptop is unstable environment)
        ```
        $ docker logs ergo-scylla 2>&1 | grep -i kill
        2022-09-10 09:08:51,142 INFO exited: scylla (terminated by SIGKILL; not expected)
        ```
  2. Chain-indexer crashes :
  
    - almost exclusively due to scylla connection problems if OOM killer kills it
    - chain-indexer recovers during polling. During initial sync, if scylla is killed by OOM killer without being properly shutdown,
      there might be data loss as it is eventually consistent database, so you have to start syncing from scratch with empty DB !!!

**Cleanup:**
```
$ ./scylla-stop.sh
$ sudo rm /var/lib/scylla/* -rf
```

## Rest/Graphql API

TODO
