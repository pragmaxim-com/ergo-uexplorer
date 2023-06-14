CREATE KEYSPACE IF NOT EXISTS uexplorer WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

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
    idx              SMALLINT,
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
    idx         SMALLINT,
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
    idx                     SMALLINT,
    tx_idx                  SMALLINT,
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

/* height index on header table and box index on output table are needed for UtxoState construction, let's build it on the fly */

CREATE INDEX IF NOT EXISTS "node_headers__height" ON uexplorer.node_headers (height);
CREATE INDEX IF NOT EXISTS "node_outputs__box_id" ON uexplorer.node_outputs (box_id);