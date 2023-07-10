package org.ergoplatform.uexplorer.backend

import org.ergoplatform.uexplorer.{BlockId, BoxId}
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.*
import zio.*

trait Repo:

  def persistBlockInTx(
    block: Block,
    outputs: OutputRecords,
    inputs: Iterable[BoxId]
  ): Task[BlockId]

object Repo:
  def persistBlockInTx(
    block: Block,
    outputs: OutputRecords,
    inputs: Iterable[BoxId]
  ): ZIO[Repo, Throwable, BlockId] =
    ZIO.serviceWithZIO[Repo](_.persistBlockInTx(block, outputs, inputs))
