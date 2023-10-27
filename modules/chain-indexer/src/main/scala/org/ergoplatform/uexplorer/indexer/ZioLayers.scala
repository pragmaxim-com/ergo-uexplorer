package org.ergoplatform.uexplorer.indexer

import com.zaxxer.hikari.HikariDataSource
import org.ergoplatform.uexplorer.CoreConf
import org.ergoplatform.uexplorer.backend.blocks.{BlockRepo, BlockService, PersistentBlockRepo}
import org.ergoplatform.uexplorer.backend.boxes.*
import org.ergoplatform.uexplorer.backend.stats.StatsService
import org.ergoplatform.uexplorer.backend.{H2Backend, PersistentRepo}
import org.ergoplatform.uexplorer.db.GraphBackend
import org.ergoplatform.uexplorer.http.*
import org.ergoplatform.uexplorer.indexer.config.ChainIndexerConf
import org.ergoplatform.uexplorer.indexer.db.GraphBackend
import org.ergoplatform.uexplorer.storage.{MvStorage, MvStoreConf}
import zio.ZLayer
import zio.http.{Client, ZClient}

trait ZioLayers {

  protected[indexer] def storageLayer: ZLayer[Any, Throwable, MvStoreConf with MvStorage with HikariDataSource] =
    MvStoreConf.layer >+> MvStorage.zlayerWithDefaultDir >+> H2Backend.zLayerFromConf

  protected[indexer] def databaseLayer: ZLayer[
    MvStoreConf with MvStorage with HikariDataSource,
    Throwable,
    ChainIndexerConf with GraphBackend with PersistentBlockRepo with PersistentBoxRepo with PersistentAssetRepo with PersistentRepo
  ] =
    ChainIndexerConf.layer >+> GraphBackend.layer >+> PersistentBlockRepo.layer >+> PersistentAssetRepo.layer >+> PersistentBoxRepo.layer >+> PersistentRepo.layer

  protected[indexer] def httpLayer: ZLayer[
    Any,
    Throwable,
    NodePoolConf with UnderlyingBackend with Client with NodePool with MetadataHttpClient with SttpNodePoolBackend with BlockHttpClient
  ] =
    NodePoolConf.layer >+> ZClient.default >+> UnderlyingBackend.layer >+> NodePool.layer >+> MetadataHttpClient.layer >+> SttpNodePoolBackend.layer >+> BlockHttpClient.layer

  protected[indexer] def serviceLayers
    : ZLayer[MvStorage with BlockRepo with AssetRepo with BoxRepo, Throwable, CoreConf with StatsService with BlockService with BoxService] =
    CoreConf.layer >+> StatsService.layer >+> BlockService.layer >+> BoxService.layer

}
