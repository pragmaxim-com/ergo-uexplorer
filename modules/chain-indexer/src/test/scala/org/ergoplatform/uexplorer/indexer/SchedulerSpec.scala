package org.ergoplatform.uexplorer.indexer

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, ActorSystem}
import akka.stream.{KillSwitches, SharedKillSwitch}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.chain.*
import org.ergoplatform.uexplorer.indexer.mempool.{MempoolStateHolder, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.mempool.MempoolStateHolder.MempoolState
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import sttp.capabilities.WebSockets
import sttp.client3.*
import sttp.client3.testing.SttpBackendStub
import org.ergoplatform.uexplorer.janusgraph.api.InMemoryGraphBackend

import scala.collection.immutable.{ListMap, TreeMap}
import scala.concurrent.Future
import org.ergoplatform.uexplorer.{ProtocolSettings, Storage}
import org.ergoplatform.uexplorer.cassandra.api.InMemoryBackend
import org.ergoplatform.uexplorer.http.{LocalNodeUriMagnet, Rest, TestSupport}
import org.ergoplatform.uexplorer.http.RemoteNodeUriMagnet
import org.ergoplatform.uexplorer.http.BlockHttpClient
import org.ergoplatform.uexplorer.http.MetadataHttpClient
import org.ergoplatform.uexplorer.indexer.chain.Initializer.ChainEmpty
import org.ergoplatform.uexplorer.storage.MvStorage

import java.nio.file.Paths
import scala.concurrent.duration.*

class SchedulerSpec extends AsyncFreeSpec with TestSupport with Matchers with BeforeAndAfterAll with ScalaFutures {

  private val testKit                                           = ActorTestKit()
  implicit private val sys: ActorSystem[_]                      = testKit.internalSystem
  implicit private val protocol: ProtocolSettings               = ChainIndexerConf.loadDefaultOrThrow.protocol
  implicit private val localNodeUriMagnet: LocalNodeUriMagnet   = LocalNodeUriMagnet(uri"http://local")
  implicit private val remoteNodeUriMagnet: RemoteNodeUriMagnet = RemoteNodeUriMagnet(uri"http://remote")
  implicit val killSwitch: SharedKillSwitch                     = KillSwitches.shared("scheduler-kill-switch")

  override def afterAll(): Unit = {
    super.afterAll()
    sys.terminate()
  }

  implicit val mempoolSyncerSyncerRef: ActorRef[MempoolStateHolder.MempoolStateHolderRequest] =
    testKit.spawn(MempoolStateHolder.behavior(MempoolState(ListMap.empty)), "MempoolSyncer")

  implicit val testingBackend: SttpBackendStub[Future, WebSockets] = SttpBackendStub.asynchronousFuture
    .whenRequestMatches { r =>
      r.uri.path.endsWith(List("info"))
    }
    .thenRespondCyclicResponses(
      (1 to 3).map(_ => Response.ok(getPeerInfo(Rest.info.sync))) ++
        (1 to 100).map(_ => Response.ok(getPeerInfo(Rest.info.poll))): _*
    )
    .whenRequestMatchesPartial {
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
    }

  val storage       = MvStorage(64).get
  val blockClient   = new BlockHttpClient(new MetadataHttpClient[WebSockets](minNodeHeight = Rest.info.minNodeHeight))
  val backend       = Some(new InMemoryBackend)
  val graphBackend  = Some(new InMemoryGraphBackend)
  val pluginManager = new PluginManager(List.empty)
  val blockIndexer  = BlockIndexer(storage, graphBackend.isDefined, 1.second)
  val chainIndexer  = new ChainIndexer(backend, graphBackend, blockClient, blockIndexer)
  val mempoolSyncer = new MempoolSyncer(blockClient)
  val initializer   = new Initializer(storage, backend, graphBackend)
  val scheduler     = new Scheduler(pluginManager, chainIndexer, mempoolSyncer, initializer)

  "Scheduler should sync from 1 to 4150 and then from 4150 to 4200" in {
    initializer.init shouldBe ChainEmpty
    scheduler.periodicSync.flatMap { mempoolState =>
      storage.getLastHeight.get shouldBe 4150
      storage.findMissingHeights shouldBe empty
      mempoolState.stateTransitionByTx.size shouldBe 9
      scheduler.periodicSync.map { newMempoolState =>
        storage.getLastHeight.get shouldBe 4200
        storage.findMissingHeights shouldBe empty
        newMempoolState.stateTransitionByTx.size shouldBe 0
      }
    }
  }
}
