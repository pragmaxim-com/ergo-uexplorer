package org.ergoplatform.uexplorer.indexer

import com.zaxxer.hikari.HikariDataSource
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo}
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.{BoxRepo, PersistentBoxRepo}
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.http.{BlockHttpClient, MetadataHttpClient, NodePool, NodePoolConf, SttpNodePoolBackend, UnderlyingBackend}
import org.ergoplatform.uexplorer.indexer.chain.{BlockReader, BlockWriter, Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.GraphBackend
import org.ergoplatform.uexplorer.indexer.mempool.{MemPool, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import sttp.capabilities.zio.ZioStreams
import sttp.client3.testing.SttpBackendStub
import zio.{Task, ZLayer}

trait SpecZioLayers {

  def dbWithTempDirPerEachRun: ZLayer[Any, Throwable, MvStoreConf with MvStorage with HikariDataSource] =
    MvStoreConf.layer >+> MvStorage.zlayerWithTempDirPerEachRun >+> H2Backend.zlayerWithTempDirPerEachRun

  def dbWithTempDirPerJvmRun: ZLayer[Any, Throwable, MvStoreConf with MvStorage with HikariDataSource] =
    MvStoreConf.layer >+> MvStorage.zlayerWithTempDirPerJvmRun >+> H2Backend.zlayerWithTempDirPerJvmRun

  def databaseLayer: ZLayer[
    MvStoreConf with MvStorage with HikariDataSource,
    Throwable,
    ChainIndexerConf with GraphBackend with PersistentBlockRepo with PersistentBoxRepo with PersistentRepo
  ] =
    ChainIndexerConf.layer >+> GraphBackend.layer >+> PersistentBlockRepo.layer >+> PersistentBoxRepo.layer >+> PersistentRepo.layer

  def httpLayer(testingBackend: SttpBackendStub[Task, ZioStreams]): ZLayer[
    Any,
    Throwable,
    NodePoolConf with NodePool with UnderlyingBackend with MetadataHttpClient with SttpNodePoolBackend with BlockHttpClient with MemPool with MempoolSyncer
  ] =
    NodePoolConf.layer >+> NodePool.layer >+> UnderlyingBackend.layerFor(
      testingBackend
    ) >+> MetadataHttpClient.layer >+> SttpNodePoolBackend.layer >+> BlockHttpClient.layer >+> MemPool.layer >+> MempoolSyncer.layer

  def allLayersWithDbPerEachRun(testingBackend: SttpBackendStub[Task, ZioStreams]) = dbWithTempDirPerEachRun >+> databaseLayer >+> httpLayer(
    testingBackend
  ) >+> PluginManager.layerNoPlugins >+> Initializer.layer >+> BlockReader.layer >+> BlockWriter.layer >+> StreamExecutor.layer >+> StreamScheduler.layer

  def allLayersWithDbPerJvmRun(testingBackend: SttpBackendStub[Task, ZioStreams]) = dbWithTempDirPerJvmRun >+> databaseLayer >+> httpLayer(
    testingBackend
  ) >+> PluginManager.layerNoPlugins >+> Initializer.layer >+> BlockReader.layer >+> BlockWriter.layer >+> StreamExecutor.layer >+> StreamScheduler.layer

}
