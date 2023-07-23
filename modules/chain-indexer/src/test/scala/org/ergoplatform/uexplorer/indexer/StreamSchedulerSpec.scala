package org.ergoplatform.uexplorer.indexer

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.blocks.PersistentBlockRepo
import org.ergoplatform.uexplorer.backend.boxes.PersistentBoxRepo
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo}
import org.ergoplatform.uexplorer.config.ExplorerConfig
import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.chain.*
import org.ergoplatform.uexplorer.indexer.chain.Initializer.ChainEmpty
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.{Backend, GraphBackend}
import org.ergoplatform.uexplorer.indexer.mempool.{MemPool, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import org.ergoplatform.uexplorer.{BlockId, ProtocolSettings, ReadableStorage}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.capabilities.WebSockets
import sttp.capabilities.zio.ZioStreams
import sttp.client3.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.testing.SttpBackendStub
import zio.*
import zio.config.typesafe.TypesafeConfigProvider
import zio.test.*

import java.nio.file.Paths
import scala.collection.immutable.{ListMap, TreeMap}
import scala.concurrent.Future
import scala.concurrent.duration.*

object StreamSchedulerSpec extends ZIOSpecDefault with TestSupport {

  private val testingBackend: SttpBackendStub[Task, ZioStreams] =
    HttpClientZioBackend.stub
      .whenRequestMatches { r =>
        r.uri.path.endsWith(List("info"))
      }
      .thenRespondCyclicResponses(
        (1 to 3).map(_ => Response.ok(getPeerInfo(Rest.info.poll))): _*
      )
      .whenRequestMatchesPartial {
        case r if r.uri.path.endsWith(List("transactions", "unconfirmed")) =>
          Response.ok(getUnconfirmedTxs)
        case r if r.uri.path.endsWith(List("peers", "connected")) =>
          Response.ok(getConnectedPeers)
        case r if r.uri.path.startsWith(List("blocks", "at")) =>
          val chainHeight = r.uri.path.last.toInt
          Response.ok(s"""["${Rest.blockIds.byHeight(chainHeight)}"]""")
        case r if r.uri.path.startsWith(List("blocks")) && r.uri.params.get("offset").isDefined =>
          val offset = r.uri.params.get("offset").get.toInt
          val limit  = r.uri.params.get("limit").getOrElse("50").toInt
          Response.ok(Rest.blocks.forOffset(offset, limit).map(blockId => s""""$blockId"""") mkString ("[", ",", "]"))
        case r if r.uri.path.startsWith(List("blocks")) =>
          val blockId = r.uri.path.last
          Response.ok(Rest.blocks.byId(BlockId.fromStringUnsafe(blockId)))
      }

  def spec =
    suite("meta")(
      test(Rest.info.sync) {
        (for {
          nodePoolFiber  <- ZIO.serviceWithZIO[StreamScheduler](_.validateAndSchedule(Schedule.once))
          lastHeight     <- ZIO.serviceWith[ReadableStorage](_.getLastHeight)
          missingHeights <- ZIO.serviceWith[ReadableStorage](_.findMissingHeights)
          memPoolState   <- ZIO.serviceWithZIO[MemPool](_.getTxs)
        } yield assertTrue(
          lastHeight.get == 4200,
          missingHeights.isEmpty,
          memPoolState.underlyingTxs.size == 9
        )).provide(
          ChainIndexerConf.layer,
          NodePoolConf.layer,
          MvStoreConf.layer,
          MemPool.layer,
          NodePool.layer,
          UnderlyingBackend.layerFor(testingBackend),
          SttpNodePoolBackend.layer,
          Backend.layerH2,
          MetadataHttpClient.layer,
          BlockHttpClient.layer,
          PersistentBlockRepo.layer,
          PersistentBoxRepo.layer,
          PersistentRepo.layer,
          StreamScheduler.layer,
          PluginManager.layerNoPlugins,
          MvStorage.zlayerWithTempDir,
          StreamExecutor.layer,
          MempoolSyncer.layer,
          Initializer.layer,
          BlockReader.layer,
          BlockWriter.layer,
          GraphBackend.layer
        )
      } @@ TestAspect.debug
    )
}
