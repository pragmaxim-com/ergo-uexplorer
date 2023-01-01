# uExplorer

Supplementary, lightweight Ergo explorer with CassandraDB backend :
  - rapid indexing speed (30mins on `16vCPU`/`20GB RAM` server to 90mins on `4vCPU+`/`16GB RAM`
  - whole utxo state accessible in real-time (4GB of RAM)
  - [stargate](https://stargate.io/) graphql server over cassandra schema
  - shares the same model and schema as [Ergo explorer](https://github.com/ergoplatform/explorer-backend)
  - resilient
    - it primarily uses local node (good for initial sync) with a fallback to peer network

## Chain Indexer

Chain indexer syncs with Node and keeps polling blocks while discarding superseded forks.

**Requirements:**
  - `SBT 1.7.x` for building and `OpenJDK 11.x` for running both `chain-indexer` and `ergo-node`
  - `16GB+` of RAM and `4vCPU+` for rapid sync from local Ergo Node
      - `start-indexing.sh` script asks you if you are syncing chain from scratch or not
      - ergo-node = 2GB
      - cassandraDB = 9GB
      - chain-indexer = 4GB
  - `8GB+` of RAM and `4vCPU+` for slow sync from peer-network, polling and querying
      - `start-querying.sh` (note that this script creates db indexes which are being built some time)
      - ergo-node = 1GB
      - cassandraDB = 1GB
      - stargate = 1GB
      - chain-indexer = 4GB
  - local fully synced Ergo Node is running if you are syncing from scratch
      - polling new blocks automatically falls back to peer-network if local node is not available

### Build

Not necessary as all docker images from docker-compose are publicly available :
```
$ docker build . -t pragmaxim/uexplorer-chain-indexer:latest
```

```
# tree
  ├── schema-tables.cql             # db schema applied at start-indexing.sh phase
  ├── schema-indexes.cql            # db indexes applied at start-querying.sh phase
  ├── ergo.conf                     # expects global env variable SCOREX_REST_API_KEY_HASH
  ├── chain-indexer.conf            # no need to change anything
  ├── docker-compose.yml            # base for minimal indexing with locally running Ergo Node
  ├── docker-compose.node.yml       # apply to base for running also Ergo Node
  ├── docker-compose.stargate.yml   # apply to base for applying indexes and graphql querying
  ├── start-indexing.sh             # starts indexing, feel free to start up services individually
  ├── start-querying.sh             # applies indexes and starts stargate for graphql querying
  ├── stop-all.sh                   # safely stops everything running
  └── drain-cassandra.sh            # cassandra must be stopped gracefully
```

### Run

```
$ cd docker
$ start-indexing.sh
$ docker compose logs uexplorer-chain-indexer
11:43:19 Initiating indexing of 816 epochs ...
11:43:21 New epoch 0 created, utxo count: 1025, non-empty-address count: 122, persisted Epochs: 1[0], Blocks cache size (heights): 33[1 - 1538]
11:43:22 New epoch 1 created, utxo count: 2049, non-empty-address count: 134, persisted Epochs: 2[0 - 1], Blocks cache size (heights): 33[1025 - 2562]
11:43:23 New epoch 2 created, utxo count: 3073, non-empty-address count: 235, persisted Epochs: 3[0 - 2], Blocks cache size (heights): 33[2049 - 3586]

$ start-querying.sh
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
        - if indexing crashes but cassandra logs do not contain `SIGKILL`, no data gets lost

**Cleanup:**
```
$ ./stop-all.sh
$ docker volume ls # cassandra and ergo volumes contain a lot of data
```

## Graphql API

When `./start-all.sh` script finishes, go to http://localhost:8085/playground and
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
and copy `{"authToken": "secret"}` paste `{"x-cassandra-token": "secret"}`

### Examples

There is an address bar in the playground UI where you select either `DDL (/graphql-schema)` or `DML (/graphql/<keyspace>)` :

**DDL (/graphql-schema)**
```
query {
  keyspace(name: "uexplorer") {
    node_outputs: table(name: "node_outputs") { columns { name } }
  }
}
```

**DML (/graphql/uexplorer)**
```
query {
  node_outputs(filter: {header_id: {eq: "b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b"}}){
    values {
      address
    }
  }
}
```
```
query {
  node_outputs(filter: {address: {eq: "88dhgzEuTXaVTz3coGyrAbJ7DNqH37vUMzpSe2vZaCEeBzA6K2nKTZ2JQJhEFgoWmrCQEQLyZNDYMby5"}}){
    values {
      box_id
    }
  }
}
```