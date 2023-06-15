package org.ergoplatform.uexplorer.db

import org.ergoplatform.uexplorer.BlockId

sealed trait Insertable

case class BestBlockInserted(lightBlock: LightBlock, fullBlockOpt: Option[FullBlock]) extends Insertable

case class ForkInserted(newFork: List[BestBlockInserted], supersededFork: Map[BlockId, VersionedBlock]) extends Insertable
