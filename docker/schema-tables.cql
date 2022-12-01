CREATE KEYSPACE IF NOT EXISTS uexplorer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TYPE IF NOT EXISTS uexplorer.block_info
(
    block_size             INT,
    block_coins            BIGINT,
    block_mining_time      BIGINT,
    txs_count              INT,
    txs_size               INT,
    miner_address          VARCHAR,
    miner_reward           BIGINT,
    miner_revenue          BIGINT,
    block_fee              BIGINT,
    block_chain_total_size BIGINT,
    total_txs_count        BIGINT,
    total_coins_issued     BIGINT,
    total_mining_time      BIGINT,
    total_fees             BIGINT,
    total_miners_reward    BIGINT,
    total_coins_in_txs     BIGINT,
    max_tx_gix             BIGINT,
    max_box_gix            BIGINT,
);

CREATE TABLE IF NOT EXISTS uexplorer.node_headers
(
    header_id         VARCHAR,
    parent_id         VARCHAR,
    height            INT,
    timestamp         BIGINT,
    difficulty        DECIMAL,
    version           TINYINT,
    n_bits            BIGINT,
    state_root        VARCHAR,
    ad_proofs_root    VARCHAR,
    ad_proofs_bytes   VARCHAR,
    ad_proofs_digest  VARCHAR,
    extensions_digest VARCHAR,
    extensions_fields VARCHAR,
    transactions_root VARCHAR,
    extension_hash    VARCHAR,
    miner_pk          VARCHAR,
    w                 VARCHAR,
    n                 VARCHAR,
    d                 VARCHAR,
    votes             VARCHAR,
    main_chain        BOOLEAN,
    block_info        frozen<block_info>,
    PRIMARY KEY (header_id)
);

/* ^^   Tables having one row per partition representing a block    */
/* ---------------------------------------------------------------- */
/* vv   Tables having many rows per partition representing a block  */

CREATE TABLE IF NOT EXISTS uexplorer.node_transactions
(
    header_id        VARCHAR,
    tx_id            VARCHAR,
    inclusion_height INT,
    coinbase         BOOLEAN,
    timestamp        BIGINT,
    size             INT,
    idx              INT,
    global_index     BIGINT,
    main_chain       BOOLEAN,
    PRIMARY KEY ((header_id), tx_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_inputs
(
    header_id   VARCHAR,
    box_id      VARCHAR,
    tx_id       VARCHAR,
    proof_bytes VARCHAR,
    extension   VARCHAR,
    idx         INT,
    main_chain  BOOLEAN,
    PRIMARY KEY ((header_id), box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_outputs
(
    header_id               VARCHAR,
    box_id                  VARCHAR,
    tx_id                   VARCHAR,
    value                   BIGINT,
    creation_height         INT,
    settlement_height       INT,
    idx                     INT,
    global_index            BIGINT,
    ergo_tree               VARCHAR,
    ergo_tree_template_hash VARCHAR,
    address                 VARCHAR,
    timestamp               BIGINT,
    main_chain              BOOLEAN,
    PRIMARY KEY ((header_id), box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_assets
(
    header_id VARCHAR,
    token_id  VARCHAR,
    box_id    VARCHAR,
    idx       INT,
    value     BIGINT,
    PRIMARY KEY ((header_id), token_id, box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_registers
(
    header_id        VARCHAR,
    id               VARCHAR,
    box_id           VARCHAR,
    value_type       VARCHAR,
    serialized_value VARCHAR,
    PRIMARY KEY ((header_id), id, box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_tokens
(
    header_id       VARCHAR,
    token_id        VARCHAR,
    box_id          VARCHAR,
    emission_amount BIGINT,
    name            VARCHAR,
    description     VARCHAR,
    type            VARCHAR,
    decimals        INT,
    PRIMARY KEY ((header_id), token_id, box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_epoch_last_headers
(
    epoch_index    INT,
    last_header_id VARCHAR,
    PRIMARY KEY (epoch_index)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_epoch_inputs
(
    epoch_index    INT,
    box_id         VARCHAR,
    PRIMARY KEY ((epoch_index), box_id)
);

CREATE TABLE IF NOT EXISTS uexplorer.node_epoch_outputs
(
    epoch_index INT,
    address     VARCHAR,
    box_id      VARCHAR,
    value       BIGINT,
    PRIMARY KEY ((epoch_index), address, box_id)
);
