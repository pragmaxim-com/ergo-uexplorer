package org.ergoplatform.uexplorer.indexer

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, ActorSystem}
import org.ergoplatform.uexplorer.indexer.api.InMemoryBackend
import org.ergoplatform.uexplorer.indexer.config.{ChainIndexerConf, ProtocolSettings}
import org.ergoplatform.uexplorer.indexer.http.{BlockHttpClient, LocalNodeUriMagnet, MetadataHttpClient, RemoteNodeUriMagnet}
import org.ergoplatform.uexplorer.indexer.chain.{ChainState, ChainSyncer}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer
import org.ergoplatform.uexplorer.indexer.mempool.MempoolSyncer.MempoolState
import org.ergoplatform.uexplorer.indexer.utxo.{UtxoSnapshotManager, UtxoState}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.testing.SttpBackendStub

import scala.collection.immutable.{ListMap, TreeMap}
import scala.concurrent.Future

class IndexerSpec extends AsyncFreeSpec with TestSupport with Matchers with BeforeAndAfterAll with ScalaFutures {

  private val testKit                                           = ActorTestKit()
  implicit private val sys: ActorSystem[_]                      = testKit.internalSystem
  implicit val protocol: ProtocolSettings                       = ChainIndexerConf.loadDefaultOrThrow.protocol
  implicit private val localNodeUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(uri"http://local")
  implicit private val remoteNodeUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(uri"http://remote")

  override def afterAll(): Unit = {
    super.afterAll()
    sys.terminate()
  }

  implicit val chainSyncerRef: ActorRef[ChainSyncer.ChainSyncerRequest] =
    testKit.spawn(new ChainSyncer().initialBehavior, "ChainSyncer")

  implicit val mempoolSyncerSyncerRef: ActorRef[MempoolSyncer.MempoolSyncerRequest] =
    testKit.spawn(MempoolSyncer.behavior(MempoolState(ListMap.empty)), "MempoolSyncer")

  implicit val testingBackend: SttpBackendStub[Future, WebSockets] = SttpBackendStub.asynchronousFuture
    .whenRequestMatches { r =>
      r.uri.path.endsWith(List("info"))
    }
    .thenRespondCyclicResponses(
      (1 to 2).map(_ => Response.ok(getPeerInfo(Rest.info.sync))) ++
      (1 to 100).map(_ => Response.ok(getPeerInfo(Rest.info.poll))): _*
    )
    .whenRequestMatchesPartial({
      case r if r.uri.path.endsWith(List("transactions", "unconfirmed")) =>
        Response.ok(getUnconfirmedTxs)
      case r if r.uri.path.endsWith(List("peers", "connected")) =>
        Response.ok(getConnectedPeers)
      case r if r.uri.path.startsWith(List("blocks", "at")) =>
        val chainHeight = r.uri.path.last.toInt
        Response.ok(s"""["${Rest.blockIds.byHeight(chainHeight)}"]""")
      case r if r.uri.path.startsWith(List("blocks")) =>
        val blockId = r.uri.path.last
        Response.ok(Rest.blocks.byId(blockId))
    })

  val blockClient     = new BlockHttpClient(new MetadataHttpClient[WebSockets](minNodeHeight = Rest.info.minNodeHeight))
  val inMemoryBackend = new InMemoryBackend
  val snapshotManager = new UtxoSnapshotManager()
  val indexer         = new Indexer(inMemoryBackend, blockClient, snapshotManager, List.empty)

  "Indexer should sync from 1 to 4150 and then from 4150 to 4200" in {
    ChainSyncer.initialize(ChainState.empty).flatMap { _ =>
      indexer.periodicSync.flatMap { case (chainState, mempoolState) =>
        chainState.getLastCachedBlock.map(_.height).get shouldBe 4150
        chainState.findMissingIndexes shouldBe empty
        mempoolState.stateTransitionByTx.size shouldBe 9
        indexer.periodicSync.map { case (newChainState, newMempoolState) =>
          newChainState.getLastCachedBlock.map(_.height).get shouldBe 4200
          newChainState.findMissingIndexes shouldBe empty
          newMempoolState.stateTransitionByTx.size shouldBe 0
        }
      }
    }
  }
}
