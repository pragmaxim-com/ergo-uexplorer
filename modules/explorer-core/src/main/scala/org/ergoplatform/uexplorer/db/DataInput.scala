package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.{BlockId, BoxId, TxId}

/** Represents `node_data_inputs` table.
  */
final case class DataInput(
  boxId: BoxId,
  txId: TxId,
  headerId: BlockId,
  index: Int, // index  of the input in the transaction
  mainChain: Boolean // chain status, `true` if this input resides in main chain.
)
