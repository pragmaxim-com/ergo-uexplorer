create table if not exists OUTPUTS (
    boxId varchar(64) not null primary key,
    blockId varchar(64) not null,
    creationHeight int not null,
    txId varchar(64) not null,
    ergoTreeHex varchar not null,
    ergoTreeT8Hex varchar,
    val bigint not null
)
