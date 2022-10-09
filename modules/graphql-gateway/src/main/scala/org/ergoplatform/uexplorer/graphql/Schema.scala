package org.ergoplatform.uexplorer.graphql

import cql4s.dsl.*
import org.ergoplatform.uexplorer.Address
import org.ergoplatform.uexplorer.db.BlockInfo

import java.util.Currency

// ------------
// Custom types
// ------------

trait addressType
object addressType:
  given DataTypeCodec[addressType, String, Address] =
    DataType.textCodec.map(_.unwrapped, Address.fromStringUnsafe)


// ------------------
// User defined types
// ------------------

class blockInfoType extends udt[
  BlockInfo,    // model case class
  "uexplorer",  // keyspace
  "block_info", // udt name
  (
    "block_size"              :=: int,
    "block_coins"             :=: bigint,
    "block_mining_time"       :=: nullable[bigint],
    "txs_count"               :=: int,
    "txs_size"                :=: int,
    "miner_address"           :=: addressType,
    "miner_reward"            :=: bigint,
    "miner_revenue"           :=: bigint,
    "block_fee"               :=: bigint,
    "block_chain_total_size"  :=: bigint,
    "total_txs_count"         :=: bigint,
    "total_coins_issued"      :=: bigint,
    "total_mining_time"       :=: bigint,
    "total_fees"              :=: bigint,
    "total_miners_reward"     :=: bigint,
    "total_coins_in_txs"      :=: bigint,
    "max_tx_gix"              :=: bigint,
    "max_box_gix"             :=: bigint
  )
]

// ------
// Tables
// ------

/*
object headers extends Table[
  "uexplorer",  // keyspace
  "node_headers", // table name
  (
    "header_id"         :=: varchar,
    "parent_id"         :=: varchar,
    "height"            :=: int,
    "timestamp"         :=: bigint,
    "difficulty"        :=: decimal,
    "version"           :=: tinyint,
    "n_bits"            :=: bigint,
    "state_root"        :=: varchar,
    "ad_proofs_root"    :=: varchar,
    "ad_proofs_bytes"   :=: varchar,
    "ad_proofs_digest"  :=: varchar,
    "extensions_digest" :=: varchar,
    "extensions_fields" :=: varchar,
    "transactions_root" :=: varchar,
    "extension_hash"    :=: varchar,
    "miner_pk"          :=: varchar,
    "w"                 :=: varchar,
    "n"                 :=: varchar,
    "d"                 :=: varchar,
    "votes"             :=: varchar,
    "main_chain"        :=: boolean,
    "block_info"        :=: blockInfoType
  )
]
*/
