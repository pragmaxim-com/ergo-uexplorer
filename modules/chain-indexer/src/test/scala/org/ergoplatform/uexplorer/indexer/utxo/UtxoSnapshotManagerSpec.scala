package org.ergoplatform.uexplorer.indexer.utxo

import org.ergoplatform.uexplorer.indexer.Rest
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.db.BlockBuilder
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

class UtxoSnapshotManagerSpec extends AnyFreeSpec with Matchers {

  implicit private val protocol: ProtocolSettings = ChainIndexerConf.loadDefaultOrThrow.protocol

  private val utxoSnapshotManager =
    new UtxoSnapshotManager(
      Paths.get(System.getProperty("java.io.tmpdir"), "ergo-snapshots").toFile
    )

  private lazy val block = BlockBuilder(Rest.blocks.getByHeight(1024), None).get

  "it should persist utxo snapshot" in {
    val addressByUtxo = block.outputs.map(o => o.boxId -> o.address).toMap
    val utxosByAddress =
      block.outputs
        .map(o => (o.address, o.boxId, o.value))
        .groupBy(_._1)
        .view
        .mapValues(_.map(o => o._2 -> o._3).toMap)
        .toMap
    val utxoState = UtxoState(
      addressByUtxo,
      utxosByAddress,
      Set.empty
    )
    utxoSnapshotManager.saveSnapshot(UtxoSnapshot.Deserialized(1, utxoState)).get
    val snapshot = utxoSnapshotManager.getLatestSnapshotByIndex.get
    snapshot.epochIndex shouldBe 1
    snapshot.utxoState shouldBe utxoState
  }
}
