# uExplorer

Supplementary, lightweight Ergo explorer with ScyllaDB backend :
  - rapid indexing speed
  - low memory requirements (designed for machines with 16GB)
  - limited querying possibilities (in comparison to RDBMS)

## Chain Indexer

Chain indexer syncs with Node and keeps polling blocks while discarding superseded forks.

**Requirements:**
  - `OpenJDK 11.x` is best for both `chain-indexer` and `ergo-node`
  - local/remote ScyllaDB, installation script expects :
      - `/var/lib/scylla` dir exists
      - `/proc/sys/fs/aio-max-nr` has value `1048576`
      - we run `docker` distro which is for playing only, install [Debian](https://github.com/scylladb/scylladb/tree/60e8f5743cc777882c6b53fa04a0e82c8ae862b2/dist/debian) distro if you can
  - `14GB+` of RAM (`scyllaDB=11GB`, `ergo-node=1GB`, `chain-indexer=512MB`, `system = 1.5GB`)
      - if you have more RAM, change `scylla-start.sh` script to avoid memory issues
  - `4vCPU+` but the whole stack was tested on `8vCPU` machine (1/2 cores allocated for scylla)
  - local fully synced Ergo Node is running if you are syncing from scratch
      - polling new blocks is done only from peer network

### Build

```
$ sbt stage
$ tree dist
dist
├── bin
│   ├── chain-indexer      # runs chain-indexer, expects ScyllaDB + Ergo Node running
│   ├── chain-indexer.bat
│   ├── scylla.cql         # db schema sourced from scylla.sh
│   ├── scylla.yaml        # for overriding default scylla server-side settings
│   └── scylla-start.sh    # starts scylla in a docker container
│   └── scylla-stop.sh     # flushes scylla memtables to disk and then removes container
├── conf
│   ├── application.ini    # memory settings (default well tested)
│   └── chain-indexer.conf # local/remote ergo node and scylla address must be defined
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
4098518a35b0cdc74bf598dc52bcf032d952a2c30271a43064c1c353adb5fd6d
Waiting for scylla to initialize...
Loading db schema

$ ./chain-indexer
11:43:19 Initiating indexing of 816 epochs ...
11:43:21 Persisted Epochs: 1[0], Blocks cache size (heights): 1538[1 - 1538]
11:43:23 Persisted Epochs: 2[0 - 1], Blocks cache size (heights): 1538[1025 - 2562]
11:43:24 Persisted Epochs: 3[0 - 2], Blocks cache size (heights): 1538[2049 - 3586]
.......................................................................
13:09:50 Persisted Epochs: 814[0 - 813], Blocks cache size (heights): 1538[832513 - 834050]
13:09:57 Persisted Epochs: 815[0 - 814], Blocks cache size (heights): 1544[833537 - 835080]
13:10:07 Initiating polling...
13:10:08 Going to index 644 blocks starting at height 835585
13:11:51 Persisted Epochs: 816[0 - 815], Blocks cache size (heights): 1536[834561 - 836096]
13:12:59 Going to index 3 blocks starting at height 836229
13:13:44 Going to index 1 blocks starting at height 836232
13:14:35 Going to index 1 blocks starting at height 836233
```

**Troubleshooting:**

-  ScyllaDB crashes :
    - reason : most likely OOM killer kicked in and killed scylla process
    - solution : avoid running another memory intensive processes (Browser, IDE),
                 chain-indexer is tested on a dedicated server (laptop is unstable environment)
        ```
        $ docker logs ergo-scylla 2>&1 | grep -i kill
        2022-09-10 09:08:51,142 INFO exited: scylla (terminated by SIGKILL; not expected)
        ```
    - docker version of scylla is not prod-ready, ie. OOM killer prone, etc. It is [covered](https://github.com/scylladb/scylladb/blob/60e8f5743cc777882c6b53fa04a0e82c8ae862b2/dist/common/systemd/scylla-server.service#L25)
      in prod-ready [Debian](https://github.com/scylladb/scylladb/tree/60e8f5743cc777882c6b53fa04a0e82c8ae862b2/dist/debian) distro

- Chain-indexer crashes :
    - reason: almost exclusively due to scylla connection problems if OOM killer kills it
    - solution :
        - nothing should happen during polling when chain-indexer recovers on its own
        - if scylla is killed during heavy initial sync by OOM killer,
          there might be data loss as it is eventually consistent database
          which requires proper shutdown. Please start syncing from scratch with empty DB.
        - if indexing crashes but scylla logs do not contain `SIGKILL`,
          run `./chain-indexer` and it will continue when it stopped (no data gets lost)

**Cleanup:**
```
$ ./scylla-stop.sh
$ sudo rm /var/lib/scylla/* -rf
```

## Rest/Graphql API

TODO
