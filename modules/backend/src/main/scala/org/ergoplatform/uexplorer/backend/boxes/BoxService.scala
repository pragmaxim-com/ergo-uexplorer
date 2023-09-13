package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.backend.IdParsingException
import org.ergoplatform.uexplorer.db.{Asset, Asset2Box, Box, BoxWithAssets, Utxo}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.{Address, BoxId, CoreConf, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hash, ErgoTreeT8Hex, TokenId}
import zio.http.QueryParams
import zio.{IO, Task, ZIO, ZLayer}

case class BoxService(boxRepo: BoxRepo, assetRepo: AssetRepo, coreConf: CoreConf) {
  import BoxService.allColumns

  def getUnspentBoxes(asset2box: Iterable[(BoxId, Option[Asset2Box])], indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[Any, Throwable, List[BoxWithAssets]] =
    for
      assetsByBox <- ZIO.when(asset2box.nonEmpty) {
                       ZIO
                         .foreachPar(asset2box.groupBy(_._1).toList) { case (boxId, a2b) =>
                           boxRepo.joinUtxoWithErgoTreeAndBlock(boxId, allColumns, indexFilter).map(_.head -> a2b.flatMap(_._2))
                         }
                         .withParallelism(32) // TODO configurable
                     }
      result <- ZIO.when(assetsByBox.nonEmpty)(BoxWithAssets.fromBox(assetsByBox.get))
    yield result.toList.flatten

  def getSpentBoxes(asset2box: Iterable[(BoxId, Option[Asset2Box])], utxoIds: Set[BoxId], indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[Any, Throwable, List[BoxWithAssets]] =
    val spentAsset2box = asset2box.filter { case (boxId, _) => !utxoIds.contains(boxId) }
    for
      assetsByBox <- ZIO.when(spentAsset2box.nonEmpty) {
                       ZIO
                         .foreachPar(spentAsset2box.groupBy(_._1).toList) { case (boxId, a2b) =>
                           boxRepo.joinBoxWithErgoTreeAndBlock(boxId, allColumns, indexFilter).map(_.head -> a2b.flatMap(_._2))
                         }
                         .withParallelism(32) // TODO configurable
                     }
      result <- ZIO.when(assetsByBox.nonEmpty)(BoxWithAssets.fromBox(assetsByBox.get))
    yield result.toList.flatten

  def getAnyBoxes(asset2box: Iterable[(BoxId, Option[Asset2Box])], indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[Any, Throwable, List[BoxWithAssets]] =
    for
      assetsByBox <- ZIO.when(asset2box.nonEmpty) {
                       ZIO
                         .foreachPar(asset2box.groupBy(_._1).toList) { case (boxId, a2b) =>
                           boxRepo.joinBoxWithErgoTreeAndBlock(boxId, allColumns, indexFilter).map(_.head -> a2b.flatMap(_._2))
                         }
                         .withParallelism(32) // TODO configurable
                     }
      result <- ZIO.when(assetsByBox.nonEmpty)(BoxWithAssets.fromBox(assetsByBox.get))
    yield result.toList.flatten

  def getUnspentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      tId       <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      asset2box <- assetRepo.lookupUnspentAssetsByTokenId(tId, allColumns, indexFilter)
      result    <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getSpentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      tId       <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      utxoIds   <- boxRepo.lookupUtxoIdsByTokenId(tId, allColumns, indexFilter)
      asset2box <- assetRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
      result    <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getAnyBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      tId       <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      asset2box <- assetRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
      result    <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      tId     <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      tId     <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      boxIds  <- boxRepo.lookupBoxIdsByTokenId(tId, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId, allColumns, indexFilter)
    yield boxIds.diff(utxoIds)

  def getAnyBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      tId    <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      boxIds <- boxRepo.lookupBoxIdsByTokenId(tId, allColumns, indexFilter)
    yield boxIds

  def getSpentBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getAnyBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      asset2box    <- assetRepo.lookupUnspentAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      boxes        <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes.diff(utxoIds)

  def getAnyBoxIdsByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      boxIds       <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxIds

  def getSpentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getUnspentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      asset2box    <- assetRepo.lookupUnspentAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getAnyBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      boxes        <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes.diff(utxoIds)

  def getAnyBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      boxIds       <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxIds

  def getSpentBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getUnspentBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      asset2box    <- assetRepo.lookupUnspentAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getAnyBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      asset2box    <- assetRepo.lookupAnyAssetsByHash(ergoTreeHash, allColumns, indexFilter)
      result       <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      boxes        <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes.diff(utxoIds)

  def getAnyBoxIdsByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      boxIds       <- boxRepo.lookupBoxIdsByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxIds

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter) // todo filter or not filter ?
      asset2box      <- assetRepo.lookupAnyAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      asset2box      <- assetRepo.lookupUnspentAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      asset2box      <- assetRepo.lookupAnyAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      boxes          <- boxRepo.lookupBoxIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes.diff(utxoIds)

  def getAnyBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      boxIds         <- boxRepo.lookupBoxIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxIds

  def getSpentBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      asset2box      <- assetRepo.lookupAnyAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getSpentBoxes(asset2box, utxoIds, indexFilter)
    yield result

  def getUnspentBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      asset2box      <- assetRepo.lookupUnspentAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getUnspentBoxes(asset2box, indexFilter)
    yield result

  def getAnyBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Iterable[BoxWithAssets]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      asset2box      <- assetRepo.lookupAnyAssetsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      result         <- getAnyBoxes(asset2box, indexFilter)
    yield result

  def getUnspentBoxIdsByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield utxoIds

  def getSpentBoxIdsByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      boxes          <- boxRepo.lookupBoxIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes.diff(utxoIds)

  def getAnyBoxIdsByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[BoxId]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      boxIds         <- boxRepo.lookupBoxIdsByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxIds

  def getUtxo(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Option[BoxWithAssets]] =
    for
      bid       <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      asset2box <- assetRepo.lookupUnspentAssetsByBoxId(bid, allColumns, indexFilter)
      utxos     <- getUnspentBoxes(asset2box, indexFilter)
    yield utxos.headOption

  def getAnyBox(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Option[BoxWithAssets]] =
    for
      bid       <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      asset2box <- assetRepo.lookupAnyAssetsByBoxId(bid, allColumns, indexFilter)
      boxes     <- getAnyBoxes(asset2box, indexFilter)
    yield boxes.headOption

  def getSpentBox(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[Option[BoxWithAssets]] =
    for
      bid       <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      asset2box <- assetRepo.lookupAnyAssetsByBoxId(bid, allColumns, indexFilter)
      utxoIds   <- boxRepo.lookupUtxo(bid, allColumns, indexFilter)
      boxes     <- getSpentBoxes(asset2box, utxoIds.map(_.boxId).toSet, indexFilter)
    yield boxes.headOption

  def getAnyBoxes(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[List[BoxWithAssets]] =
    for
      bids      <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      asset2box <- assetRepo.lookupAnyAssetsByBoxIds(bids, allColumns, indexFilter)
      boxes     <- getAnyBoxes(asset2box, indexFilter)
    yield boxes

  def getUtxos(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[List[BoxWithAssets]] =
    for
      bids      <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      asset2box <- assetRepo.lookupUnspentAssetsByBoxIds(bids, allColumns, indexFilter)
      utxos     <- getUnspentBoxes(asset2box, indexFilter)
    yield utxos

  def getSpentBoxes(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): Task[List[BoxWithAssets]] =
    for
      bids      <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      asset2box <- assetRepo.lookupAnyAssetsByBoxIds(bids, allColumns, indexFilter)
      utxoIds   <- boxRepo.lookupUtxos(bids, allColumns, indexFilter)
      boxes     <- getSpentBoxes(asset2box, utxoIds.map(_.boxId).toSet, indexFilter)
    yield boxes

}

object BoxService {
  val indexWhiteList = List("tokenId", "txId", "r4", "r5", "r6", "r7", "r8", "r9")
  val allColumns     = indexWhiteList ++ List("boxId", "blockId", "ergoTreeHash", "ergoTreeT8Hash", "ergValue", "amount")

  def layer: ZLayer[BoxRepo with AssetRepo with CoreConf, Nothing, BoxService] =
    ZLayer.fromFunction(BoxService.apply _)

  def getUnspentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByTokenId(tokenId, indexFilter))

  def getSpentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByTokenId(tokenId, indexFilter))

  def getAnyBoxesByTokenId(tokenId: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByTokenId(tokenId, indexFilter))

  def getUnspentBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByTokenId(tokenId, indexFilter))

  def getSpentBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByTokenId(tokenId, indexFilter))

  def getAnyBoxIdsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByTokenId(tokenId, indexFilter))

  def getUnspentBoxIdsByAddress(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByAddress(tokenId, indexFilter))

  def getSpentBoxIdsByAddress(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByAddress(tokenId, indexFilter))

  def getAnyBoxIdsByAddress(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByAddress(tokenId, indexFilter))

  def getUnspentBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByErgoTree(ergoTree, indexFilter))

  def getSpentBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByErgoTree(ergoTree, indexFilter))

  def getAnyBoxIdsByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByErgoTree(ergoTree, indexFilter))

  def getUnspentBoxIdsByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByErgoTreeHash(ergoTreeHash, indexFilter))

  def getSpentBoxIdsByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByErgoTreeHash(ergoTreeHash, indexFilter))

  def getAnyBoxIdsByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByErgoTreeHash(ergoTreeHash, indexFilter))

  def getUnspentBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByErgoTreeT8(ergoTreeT8, indexFilter))

  def getSpentBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByErgoTreeT8(ergoTreeT8, indexFilter))

  def getAnyBoxIdsByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByErgoTreeT8(ergoTreeT8, indexFilter))

  def getUnspentBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash, indexFilter))

  def getSpentBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash, indexFilter))

  def getAnyBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxIdsByErgoTreeT8Hash(ergoTreeT8Hash, indexFilter))

  def getUtxo(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, Option[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxo(boxId, indexFilter))

  def getAnyBox(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, Option[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBox(boxId, indexFilter))

  def getSpentBox(boxId: String, indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, Option[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBox(boxId, indexFilter))

  def getAnyBoxes(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, List[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxes(boxIds, indexFilter))

  def getUtxos(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, List[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxos(boxIds, indexFilter))

  def getSpentBoxes(boxIds: Set[String], indexFilter: Map[String, String])(implicit enc: ErgoAddressEncoder): ZIO[BoxService, Throwable, List[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxes(boxIds, indexFilter))

  def getSpentBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByAddress(address, indexFilter))

  def getUnspentBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByAddress(address, indexFilter))

  def getAnyBoxesByAddress(address: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByAddress(address, indexFilter))

  def getSpentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTree(ergoTree, indexFilter))

  def getUnspentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTree(ergoTree, indexFilter))

  def getAnyBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTree(ergoTree, indexFilter))

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String])(implicit
    enc: ErgoAddressEncoder
  ): ZIO[BoxService, Throwable, Iterable[BoxWithAssets]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

}
