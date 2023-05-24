package org.ergoplatform.uexplorer.utxo

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.ProtocolSettings
import org.ergoplatform.uexplorer.db.BlockBuilder
import org.ergoplatform.uexplorer.indexer.Rest
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.scalatest.freespec.{AnyFreeSpec, AsyncFreeSpec}
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths
import scala.collection.immutable.TreeMap

class DiskUtxoSnapshotManagerSpec extends AsyncFreeSpec with Matchers {

  private val testKit                                     = ActorTestKit()
  implicit private val protocol: ProtocolSettings         = ChainIndexerConf.loadDefaultOrThrow.protocol
  implicit private val addressEncoder: ErgoAddressEncoder = protocol.addressEncoder
  implicit private val sys: ActorSystem[_]                = testKit.internalSystem

  private val utxoSnapshotManager =
    new DiskUtxoSnapshotManager(
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
      Map.empty,
      TreeMap.empty,
      TopAddresses.empty
    )
    utxoSnapshotManager.saveSnapshot(UtxoSnapshot.Deserialized(1, utxoState)).flatMap { _ =>
      utxoSnapshotManager.getLatestSnapshotByIndex.map { snapshot =>
        snapshot.get.epochIndex shouldBe 1
        snapshot.get.utxoState shouldBe utxoState
      }
    }
  }
}
