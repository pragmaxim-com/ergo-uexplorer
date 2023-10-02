package org.ergoplatform.uexplorer.indexer

import com.zaxxer.hikari.HikariDataSource
import org.ergoplatform.uexplorer.backend.blocks.PersistentBlockRepo
import org.ergoplatform.uexplorer.backend.boxes.{PersistentAssetRepo, PersistentBoxRepo}
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo}
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.chain.{BlockReader, BlockWriter, Initializer, StreamExecutor}
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.GraphBackend
import org.ergoplatform.uexplorer.indexer.mempool.{MemPool, MempoolSyncer}
import org.ergoplatform.uexplorer.indexer.plugin.PluginManager
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import zio.ZLayer

trait SpecZioLayers {

  def dbPerEachRun: ZLayer[Any, Throwable, MvStoreConf with MvStorage with HikariDataSource] =
    MvStoreConf.layer >+> MvStorage.zlayerWithTempDirPerEachRun >+> H2Backend.zlayerWithTempDirPerEachRun

  def dbPerJvmRun: ZLayer[Any, Throwable, MvStoreConf with MvStorage with HikariDataSource] =
    MvStoreConf.layer >+> MvStorage.zlayerWithTempDirPerJvmRun >+> H2Backend.zlayerWithTempDirPerJvmRun

  private def databaseLayer: ZLayer[
    MvStoreConf with MvStorage with HikariDataSource,
    Throwable,
    ChainIndexerConf with GraphBackend with PersistentBlockRepo with PersistentBoxRepo with PersistentRepo
  ] =
    ChainIndexerConf.layer >+> GraphBackend.layer >+> PersistentBlockRepo.layer >+> PersistentAssetRepo.layer >+> PersistentBoxRepo.layer >+> PersistentRepo.layer

  private def httpLayer: ZLayer[
    UnderlyingBackend,
    Throwable,
    NodePoolConf with NodePool with MetadataHttpClient with SttpNodePoolBackend with BlockHttpClient
  ] =
    NodePoolConf.layer >+> NodePool.layer >+> MetadataHttpClient.layer >+> SttpNodePoolBackend.layer >+> BlockHttpClient.layer

  def allLayers: ZLayer[
    UnderlyingBackend with MvStoreConf with MvStorage with HikariDataSource,
    Throwable,
    NodePoolConf
      with NodePool
      with MetadataHttpClient
      with SttpNodePoolBackend
      with BlockHttpClient
      with ChainIndexerConf
      with GraphBackend
      with PersistentBlockRepo
      with PersistentBoxRepo
      with PersistentRepo
      with PluginManager
      with Initializer
      with BlockReader
      with BlockWriter
      with StreamExecutor
      with MemPool
      with MempoolSyncer
      with StreamScheduler
  ] =
    httpLayer >+> databaseLayer >+> PluginManager.layerNoPlugins >+> Initializer.layer >+> BlockReader.layer >+> BlockWriter.layer >+> StreamExecutor.layer >+> MemPool.layer >+> MempoolSyncer.layer >+> StreamScheduler.layer

}
