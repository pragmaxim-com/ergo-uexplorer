# uExplorer

Supplementary, lightweight Ergo explorer with CassandraDB backend :
  - rapid indexing speed
  - low memory requirements (designed for machines with 16GB)
  - limited querying possibilities (in comparison to RDBMS)
  - shares the same model and schema as [Ergo explorer](https://github.com/ergoplatform/explorer-backend)
  - resilient
    - it primarily uses local node (good for initial sync) with a fallback to peer network

## Chain Indexer

Chain indexer syncs with Node and keeps polling blocks while discarding superseded forks.

**Requirements:**
  - `SBT 1.7.x` for building and `OpenJDK 11.x` for running both `chain-indexer` and `ergo-node`
  - local/remote CassandraDB, installation script expects :
      - `/var/lib/cassandra` dir exists
      - `/proc/sys/fs/aio-max-nr` has value `1048576`
      - we run `docker` distro which is for playing only, install prod-ready distro if you can
  - `16GB+` of RAM (`cassandraDB=14GB`, `ergo-node=512MB`, `chain-indexer=512MB`, `system=1GB`)
      - if you have more RAM, change `cassandra-start.sh` script to avoid memory issues
  - `8vCPU+` for initial sync, polling and querying is not that demanding
  - local fully synced Ergo Node is running if you are syncing from scratch
      - polling new blocks automatically falls back to peer-network if local node is not available

### Build

```
$ sbt stage
$ tree dist
dist
├── bin
│   ├── chain-indexer      # runs chain-indexer, expects cassandraDB + Ergo Node running
│   ├── chain-indexer.bat
│   ├── cassandra.cql         # db schema sourced from cassandra.sh
│   ├── cassandra.yaml        # for overriding default cassandra server-side settings
│   ├── cassandra-start.sh    # starts cassandra in a docker container
│   └── cassandra-stop.sh     # flushes cassandra memtables to disk and then removes container
├── conf
│   ├── application.ini    # memory settings (default well tested)
│   └── chain-indexer.conf # local/remote ergo node and cassandra address must be defined
└── lib
    └── chain-indexer.jar  # fat jar of all dependencies
```

### Run

Have fully synced ergo node running locally for initial explorer sync,
start cassandra in syncing mode and then chain-indexer syncs in ~ 90 minutes.
```
$ cd fully-synced-ergo-node
$ nohup java -Xmx1g -jar ergo.jar --mainnet -c ergo.conf &

$ cd ergo-uexplorer/dist/bin
$ ./cassandra-start.sh
4098518a35b0cdc74bf598dc52bcf032d952a2c30271a43064c1c353adb5fd6d
Waiting for cassandra to initialize...
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

-  cassandraDB crashes :
    - reason : most likely OOM killer kicked in and killed cassandra process
    - solution : avoid running another memory intensive processes (Browser, IDE),
                 chain-indexer is tested on a dedicated server (laptop is unstable environment)
        ```
        $ docker logs cassandra 2>&1 | grep -i kill
        2022-09-10 09:08:51,142 INFO exited: cassandra (terminated by SIGKILL; not expected)
        ```
    - docker version of cassandra is not prod-ready, ie. OOM killer prone, etc.

- Chain-indexer crashes :
    - reason: almost exclusively due to cassandra connection problems if OOM killer kills it
    - solution :
        - nothing should happen during polling when chain-indexer recovers on its own
        - if cassandra is killed during heavy initial sync by OOM killer,
          there might be data loss as it is eventually consistent database
          which requires proper shutdown. Please start syncing from scratch with empty DB.
        - if indexing crashes but cassandra logs do not contain `SIGKILL`,
          run `./chain-indexer` and it will continue when it stopped (no data gets lost)

**Cleanup:**
```
$ ./cassandra-stop.sh
$ sudo rm /var/lib/cassandra/* -rf
```

## Graphql API

When `./cassandra-start.sh` script finishes, go to http://localhost:8085/playground and
copy/paste the auth token from following snippet to `HTTP HEADERS` at bottom-left of the playground
and follow [documentation](https://stargate.io/docs/latest/develop/graphql.html).

```
curl -L -X POST 'http://localhost:8081/v1/auth' \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "username": "cassandra",
    "password": "cassandra"
}'
```
```
{"authToken":"{auth-token-here}"}
```