package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.IdParsingException
import org.ergoplatform.uexplorer.db.{Asset, Asset2Box, Box, Utxo}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.{Address, BoxId, CoreConf, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hash, ErgoTreeT8Hex, TokenId}
import zio.http.QueryParams
import zio.{IO, Task, ZIO, ZLayer}

case class BoxService(boxRepo: BoxRepo, coreConf: CoreConf) {
  import BoxService.allColumns

  def getUnspentAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Asset2Box]] =
    for
      tId    <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      assets <- boxRepo.lookupUnspentAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets

  def getSpentAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Asset2Box]] =
    for
      tId     <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId)
      assets  <- boxRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets.filter(a => !utxoIds.contains(a.boxId))

  def getAnyAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Asset2Box]] =
    for
      tId    <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      assets <- boxRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets

  def getUnspentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      tId   <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      utxos <- boxRepo.lookupUtxosByTokenId(tId, allColumns, indexFilter)
    yield utxos

  def getSpentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      tId     <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      boxes   <- boxRepo.lookupBoxesByTokenId(tId, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getAnyBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      tId   <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId)).mapError(ex => IdParsingException(tokenId, ex.getMessage))
      utxos <- boxRepo.lookupBoxesByTokenId(tId, allColumns, indexFilter)
    yield utxos

  def getUtxo(boxId: String): Task[Option[Utxo]] =
    for
      bid  <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      utxo <- boxRepo.lookupUtxo(bid)
    yield utxo

  def getAnyBox(boxId: String): Task[Option[Box]] =
    for
      bid <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      box <- boxRepo.lookupBox(bid)
    yield box

  def getAnyBoxes(boxIds: Set[String]): Task[List[Box]] =
    for
      bids  <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      boxes <- boxRepo.lookupBoxes(bids)
    yield boxes

  def getSpentBox(boxId: String): Task[Option[Box]] =
    for
      bid <- ZIO.attempt(BoxId(boxId)).mapError(ex => IdParsingException(boxId, ex.getMessage))
      box <- boxRepo.lookupUtxo(bid).flatMap(_.fold(boxRepo.lookupBox(bid))(_ => ZIO.succeed(Option.empty[Box])))
    yield box

  def getUtxos(boxIds: Set[String]): Task[List[Utxo]] =
    for
      bids  <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      boxes <- boxRepo.lookupUtxos(bids)
    yield boxes

  def getSpentBoxes(boxIds: Set[String]): Task[List[Box]] =
    for
      bids <- ZIO.attempt(boxIds.map(BoxId(_))).mapError(ex => IdParsingException(boxIds.mkString(","), ex.getMessage))
      boxes <- boxRepo
                 .lookupUtxos(bids)
                 .map(_.map(_.boxId).toSet)
                 .flatMap { utxoIds =>
                   boxRepo
                     .lookupBoxes(bids)
                     .map(_.filter(b => !utxoIds.contains(b.boxId)))
                 }
    yield boxes

  def getSpentBoxesByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByAddress(address: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address)).mapError(ex => IdParsingException(address, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree)).mapError(ex => IdParsingException(ergoTree, ex.getMessage))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex).mapError(ex => IdParsingException(ergoTreeHex.value, ex.getMessage))
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTreeHash(etHash: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash)).mapError(ex => IdParsingException(etHash, ex.getMessage))
      utxos        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      utxos          <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8)).mapError(ex => IdParsingException(ergoTreeT8, ex.getMessage))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex).mapError(ex => IdParsingException(ergoTreeT8Hex.value, ex.getMessage))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[Utxo]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      boxes          <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes

  def getAnyBoxesByErgoTreeT8Hash(etT8Hash: String, indexFilter: Map[String, String]): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash)).mapError(ex => IdParsingException(etT8Hash, ex.getMessage))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes

}

object BoxService {
  val indexWhiteList = List("tokenId", "txId", "r4", "r5", "r6", "r7", "r8", "r9")
  val allColumns     = indexWhiteList ++ List("boxId", "blockId", "ergoTreeHash", "ergoTreeT8Hash", "ergValue")

  def layer: ZLayer[BoxRepo with CoreConf, Nothing, BoxService] =
    ZLayer.fromFunction(BoxService.apply _)

  def getUnspentAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Asset2Box]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentAssetsByTokenId(tokenId, indexFilter))

  def getSpentAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Asset2Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentAssetsByTokenId(tokenId, indexFilter))

  def getAnyAssetsByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Asset2Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyAssetsByTokenId(tokenId, indexFilter))

  def getUnspentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByTokenId(tokenId, indexFilter))

  def getSpentBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByTokenId(tokenId, indexFilter))

  def getAnyBoxesByTokenId(tokenId: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByTokenId(tokenId, indexFilter))

  def getUtxo(boxId: String): ZIO[BoxService, Throwable, Option[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxo(boxId))

  def getAnyBox(boxId: String): ZIO[BoxService, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBox(boxId))

  def getAnyBoxes(boxIds: Set[String]): ZIO[BoxService, Throwable, List[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxes(boxIds))

  def getSpentBox(boxId: String): ZIO[BoxService, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBox(boxId))

  def getUtxos(boxIds: Set[String]): ZIO[BoxService, Throwable, List[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxos(boxIds))

  def getSpentBoxes(boxIds: Set[String]): ZIO[BoxService, Throwable, List[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxes(boxIds))

  def getSpentBoxesByAddress(address: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByAddress(address, indexFilter))

  def getUnspentBoxesByAddress(address: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByAddress(address, indexFilter))

  def getAnyBoxesByAddress(address: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByAddress(address, indexFilter))

  def getSpentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTree(ergoTree, indexFilter))

  def getUnspentBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTree(ergoTree, indexFilter))

  def getAnyBoxesByErgoTree(ergoTree: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTree(ergoTree, indexFilter))

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeHash(ergoTreeHash, indexFilter))

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8(ergoTreeT8, indexFilter))

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeHash: String, indexFilter: Map[String, String]): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8Hash(ergoTreeHash, indexFilter))

}
