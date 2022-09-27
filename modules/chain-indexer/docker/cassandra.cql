CREATE KEYSPACE IF NOT EXISTS uexplorer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

CREATE TABLE IF NOT EXISTS uexplorer.node_headers
(
    header_id         VARCHAR,
    parent_id         VARCHAR,
    version           TINYINT,
    height            INT,
    n_bits            BIGINT,
    difficulty        DECIMAL,
    timestamp         BIGINT,
    state_root        BLOB,
    ad_proofs_root    BLOB,
    ad_proofs_bytes   BLOB,
    ad_proofs_digest  BLOB,
    extensions_digest BLOB,
    extensions_fields VARCHAR,
    transactions_root BLOB,
    extension_hash    BLOB,
    miner_pk          BLOB,
    w                 BLOB,
    n                 BLOB,
    d                 VARCHAR,
    votes             VARCHAR,
    main_chain        BOOLEAN,
    PRIMARY KEY (header_id)
);

CREATE INDEX IF NOT EXISTS "node_headers__parent_id" ON uexplorer.node_headers (parent_id);
CREATE INDEX IF NOT EXISTS "node_headers__height" ON uexplorer.node_headers (height);
CREATE INDEX IF NOT EXISTS "node_headers__ts" ON uexplorer.node_headers (timestamp);

CREATE TABLE IF NOT EXISTS uexplorer.blocks_info
(
    header_id              VARCHAR,
    parent_id              VARCHAR,
    timestamp              BIGINT,
    height                 INT,
    difficulty             BIGINT,
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
    main_chain             BOOLEAN,
    PRIMARY KEY (header_id)
);

CREATE INDEX IF NOT EXISTS "blocks_info__height" ON uexplorer.blocks_info (height);
CREATE INDEX IF NOT EXISTS "blocks_info__ts" ON uexplorer.blocks_info (timestamp);


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

CREATE INDEX IF NOT EXISTS "node_transactions__tx_id" ON uexplorer.node_transactions (tx_id);
CREATE INDEX IF NOT EXISTS "node_transactions__timestamp" ON uexplorer.node_transactions (timestamp);
CREATE INDEX IF NOT EXISTS "node_transactions__inclusion_height" ON uexplorer.node_transactions (inclusion_height);


CREATE TABLE IF NOT EXISTS uexplorer.node_inputs
(
    header_id   VARCHAR,
    box_id      VARCHAR,
    tx_id       VARCHAR,
    proof_bytes BLOB,
    extension   VARCHAR,
    idx         INT,
    main_chain  BOOLEAN,
    PRIMARY KEY ((header_id), box_id)
);

CREATE INDEX IF NOT EXISTS "node_inputs__box_id" ON uexplorer.node_inputs (box_id);
CREATE INDEX IF NOT EXISTS "node_inputs__tx_id" ON uexplorer.node_inputs (tx_id);


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
    ergo_tree               BLOB,
    ergo_tree_template_hash BLOB,
    address                 VARCHAR,
    additional_registers    VARCHAR,
    timestamp               BIGINT,
    main_chain              BOOLEAN,
    PRIMARY KEY ((header_id), box_id)
);

CREATE INDEX IF NOT EXISTS "node_outputs__box_id" ON uexplorer.node_outputs (box_id);
CREATE INDEX IF NOT EXISTS "node_outputs__tx_id" ON uexplorer.node_outputs (tx_id);
CREATE INDEX IF NOT EXISTS "node_outputs__timestamp" ON uexplorer.node_outputs (timestamp);
CREATE INDEX IF NOT EXISTS "node_outputs__address" ON uexplorer.node_outputs (address);


CREATE TABLE IF NOT EXISTS uexplorer.node_assets
(
    header_id VARCHAR,
    token_id  BLOB,
    box_id    VARCHAR,
    idx       INT,
    value     BIGINT,
    PRIMARY KEY ((header_id), token_id, box_id)
);

CREATE INDEX IF NOT EXISTS "node_assets__box_id" ON uexplorer.node_assets (box_id);
CREATE INDEX IF NOT EXISTS "node_assets__token_id" ON uexplorer.node_assets (token_id);


CREATE TABLE IF NOT EXISTS uexplorer.node_registers
(
    header_id        VARCHAR,
    id               VARCHAR,
    box_id           VARCHAR,
    value_type       VARCHAR,
    serialized_value BLOB,
    rendered_value   VARCHAR,
    PRIMARY KEY ((header_id), id, box_id)
);

CREATE INDEX IF NOT EXISTS "node_registers__box_id" ON uexplorer.node_registers (box_id);


CREATE TABLE IF NOT EXISTS uexplorer.node_tokens
(
    header_id       VARCHAR,
    token_id        BLOB,
    box_id          VARCHAR,
    emission_amount BIGINT,
    name            VARCHAR,
    description     VARCHAR,
    type            VARCHAR,
    decimals        INT,
    PRIMARY KEY ((header_id), token_id, box_id)
);

CREATE INDEX IF NOT EXISTS "node_tokens__box_id" ON uexplorer.node_tokens (box_id);


CREATE TABLE IF NOT EXISTS uexplorer.node_epochs
(
    epoch_index     INT,
    last_header_id  VARCHAR,
    PRIMARY KEY (epoch_index)
);
