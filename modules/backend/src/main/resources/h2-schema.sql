
create table if not exists Block (
    blockId              VARCHAR(64) NOT NULL PRIMARY KEY,
    parentId             VARCHAR(64) NOT NULL,
    revision             BIGINT NOT NULL,
    timestamp            BIGINT NOT NULL,
    height               INT NOT NULL,
    blockSize            INT NOT NULL,
    blockCoins           BIGINT NOT NULL,
    blockMiningTime      BIGINT NOT NULL,
    txsCount             INT NOT NULL,
    txsSize              INT NOT NULL,
    minerAddress         VARCHAR NOT NULL,
    minerReward          BIGINT NOT NULL,
    minerRevenue         BIGINT NOT NULL,
    blockFee             BIGINT NOT NULL,
    blockChainTotalSize  BIGINT NOT NULL,
    totalTxsCount        BIGINT NOT NULL,
    totalCoinsIssued     BIGINT NOT NULL,
    totalMiningTime      BIGINT NOT NULL,
    totalFees            BIGINT NOT NULL,
    totalMinersReward    BIGINT NOT NULL,
    totalCoinsInTxs      BIGINT NOT NULL,
    maxTxGix             BIGINT NOT NULL,
    maxBoxGix            BIGINT NOT NULL
);

create index if not exists block_height ON Block (height);

create table if not exists ErgoTree (
    hash                 VARCHAR(64) NOT NULL PRIMARY KEY,
    blockId              VARCHAR(64) NOT NULL REFERENCES Block (blockId) ON DELETE CASCADE,
    hex                  VARCHAR NOT NULL
);

create table if not exists ErgoTreeT8 (
    hash                 VARCHAR(64) NOT NULL PRIMARY KEY,
    blockId              VARCHAR(64) NOT NULL REFERENCES Block (blockId) ON DELETE CASCADE,
    hex                  VARCHAR NOT NULL
);

create table if not exists Box (
    boxId                VARCHAR(64) NOT NULL PRIMARY KEY,
    txId                 VARCHAR(64) NOT NULL,
    blockId              VARCHAR(64) NOT NULL REFERENCES Block (blockId) ON DELETE CASCADE,
    creationHeight       INT NOT NULL,
    settlementHeight     INT NOT NULL,
    ergoTreeHash         VARCHAR(64) NOT NULL REFERENCES ErgoTree (hash) ON DELETE CASCADE,
    ergoTreeT8Hash       VARCHAR(64) REFERENCES ErgoTreeT8 (hash) ON DELETE CASCADE,
    ergValue             BIGINT NOT NULL,
    index                INT NOT NULL,
    r4                   VARCHAR,
    r5                   VARCHAR,
    r6                   VARCHAR,
    r7                   VARCHAR,
    r8                   VARCHAR,
    r9                   VARCHAR
);

create index if not exists box_ergoTreeHash ON Box (ergoTreeHash);
create index if not exists box_blockId ON Box (blockId);

create table if not exists Asset (
    tokenId              VARCHAR(64) NOT NULL PRIMARY KEY,
    blockId              VARCHAR(64) NOT NULL REFERENCES Block (blockId) ON DELETE CASCADE
);

create table if not exists Asset2Box (
    tokenId              VARCHAR(64) NOT NULL REFERENCES Asset (tokenId) ON DELETE CASCADE,
    boxId                VARCHAR(64) NOT NULL REFERENCES Box (boxId) ON DELETE CASCADE,
    index                INT NOT NULL,
    amount               BIGINT NOT NULL,
    name                 VARCHAR,
    description          VARCHAR,
    type                 VARCHAR,
    decimals             INT
);

create index if not exists asset2box_tokenId ON Asset2Box (tokenId);
create index if not exists asset2box_boxId ON Asset2Box (boxId);

create table if not exists Utxo (
    boxId                VARCHAR(64) NOT NULL PRIMARY KEY REFERENCES Box (boxId) ON DELETE CASCADE,
    txId                 VARCHAR(64) NOT NULL,
    blockId              VARCHAR(64) NOT NULL REFERENCES Block (blockId) ON DELETE CASCADE,
    creationHeight       INT NOT NULL,
    settlementHeight     INT NOT NULL,
    ergoTreeHash         VARCHAR(64) NOT NULL REFERENCES ErgoTree (hash) ON DELETE CASCADE,
    ergoTreeT8Hash       VARCHAR(64) REFERENCES ErgoTreeT8 (hash) ON DELETE CASCADE,
    ergValue             BIGINT NOT NULL,
    index                INT NOT NULL,
    r4                   VARCHAR,
    r5                   VARCHAR,
    r6                   VARCHAR,
    r7                   VARCHAR,
    r8                   VARCHAR,
    r9                   VARCHAR
);

create index if not exists utxo_ergoTreeHash ON Utxo (ergoTreeHash);
create index if not exists utxo_blockId ON Utxo (blockId);
