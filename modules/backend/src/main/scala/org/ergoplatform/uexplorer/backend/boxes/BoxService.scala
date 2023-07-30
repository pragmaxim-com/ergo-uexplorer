package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.db.{Asset, Box, Utxo}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.{Address, BoxId, CoreConf, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hash, ErgoTreeT8Hex, TokenId}
import zio.http.QueryParams
import zio.{Task, ZIO, ZLayer}

case class BoxService(boxRepo: BoxRepo, coreConf: CoreConf) {
  import BoxService.allColumns

  def getUnspentAssetsByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Asset]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      assets <- boxRepo.lookupUnspentAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets

  def getSpentAssetsByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Asset]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId)
      assets  <- boxRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets.filter(a => !utxoIds.contains(a.boxId))

  def getAnyAssetsByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Asset]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      assets <- boxRepo.lookupAnyAssetsByTokenId(tId, allColumns, indexFilter)
    yield assets

  def getUnspentBoxesByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByTokenId(tId, allColumns, indexFilter)
    yield utxos

  def getSpentBoxesByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Box]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByTokenId(tId, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByTokenId(tId)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getAnyBoxesByTokenId(tokenId: String, params: QueryParams): Task[Iterable[Box]] =
    for
      tId <- ZIO.attempt(TokenId.fromStringUnsafe(tokenId))
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupBoxesByTokenId(tId, allColumns, indexFilter)
    yield utxos

  def getUtxo(boxId: BoxId): Task[Option[Utxo]] = boxRepo.lookupUtxo(boxId)

  def getAnyBox(boxId: BoxId): Task[Option[Box]] = boxRepo.lookupBox(boxId)

  def getAnyBoxes(boxIds: Set[BoxId]): Task[List[Box]] = boxRepo.lookupBoxes(boxIds)

  def getSpentBox(boxId: BoxId): Task[Option[Box]] =
    boxRepo
      .lookupUtxo(boxId)
      .flatMap(_.fold(boxRepo.lookupBox(boxId))(_ => ZIO.succeed(Option.empty[Box])))

  def getUtxos(boxIds: Set[BoxId]): Task[List[Utxo]] = boxRepo.lookupUtxos(boxIds)

  def getSpentBoxes(boxIds: Set[BoxId]): Task[List[Box]] =
    boxRepo
      .lookupUtxos(boxIds)
      .map(_.map(_.boxId).toSet)
      .flatMap { utxoIds =>
        boxRepo
          .lookupBoxes(boxIds)
          .map(_.filter(b => !utxoIds.contains(b.boxId)))
      }

  def getSpentBoxesByAddress(address: String, params: QueryParams): Task[Iterable[Box]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByAddress(address: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByAddress(address: String, params: QueryParams): Task[Iterable[Box]] =
    for
      addr         <- ZIO.attempt(Address.fromStringUnsafe(address))
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(addr)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTree(ergoTree: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTree(ergoTree: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTree(ergoTree: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHex  <- ZIO.attempt(ErgoTreeHex.fromStringUnsafe(ergoTree))
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeHash(etHash: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash))
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)
      utxoIds      <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeHash(etHash: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash))
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)
    yield utxos

  def getAnyBoxesByErgoTreeHash(etHash: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etHash))
      utxos        <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)
    yield utxos

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hex  <- ZIO.attempt(ErgoTreeT8Hex.fromStringUnsafe(ergoTreeT8))
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeT8Hash(etT8Hash: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)
      utxoIds        <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8Hash(etT8Hash: String, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash))
      boxes          <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)
    yield boxes

  def getAnyBoxesByErgoTreeT8Hash(etT8Hash: String, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ZIO.attempt(ErgoTreeHash.fromStringUnsafe(etT8Hash))
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)
    yield boxes

}

object BoxService {
  val indexWhiteList = List("tokenId", "txId", "r4", "r5", "r6", "r7", "r8", "r9")
  val allColumns     = indexWhiteList ++ List("boxId", "blockId", "ergoTreeHash", "ergoTreeT8Hash", "ergValue")

  def layer: ZLayer[BoxRepo with CoreConf, Nothing, BoxService] =
    ZLayer.fromFunction(BoxService.apply _)

  def getUnspentAssetsByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Asset]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentAssetsByTokenId(tokenId, params))

  def getSpentAssetsByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Asset]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentAssetsByTokenId(tokenId, params))

  def getAnyAssetsByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Asset]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyAssetsByTokenId(tokenId, params))

  def getUnspentBoxesByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByTokenId(tokenId, params))

  def getSpentBoxesByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByTokenId(tokenId, params))

  def getAnyBoxesByTokenId(tokenId: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByTokenId(tokenId, params))

  def getUtxo(boxId: BoxId): ZIO[BoxService, Throwable, Option[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxo(boxId))

  def getAnyBox(boxId: BoxId): ZIO[BoxService, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBox(boxId))

  def getAnyBoxes(boxIds: Set[BoxId]): ZIO[BoxService, Throwable, List[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxes(boxIds))

  def getSpentBox(boxId: BoxId): ZIO[BoxService, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBox(boxId))

  def getUtxos(boxIds: Set[BoxId]): ZIO[BoxService, Throwable, List[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUtxos(boxIds))

  def getSpentBoxes(boxIds: Set[BoxId]): ZIO[BoxService, Throwable, List[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxes(boxIds))

  def getSpentBoxesByAddress(address: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByAddress(address, params))

  def getUnspentBoxesByAddress(address: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByAddress(address, params))

  def getAnyBoxesByAddress(address: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByAddress(address, params))

  def getSpentBoxesByErgoTree(ergoTree: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTree(ergoTree, params))

  def getUnspentBoxesByErgoTree(ergoTree: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTree(ergoTree, params))

  def getAnyBoxesByErgoTree(ergoTree: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTree(ergoTree, params))

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeHash(ergoTreeHash, params))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeHash(ergoTreeHash, params))

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeHash(ergoTreeHash, params))

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8(ergoTreeT8, params))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8(ergoTreeT8, params))

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8(ergoTreeT8, params))

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8Hash(ergoTreeHash, params))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash, params))

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeHash: String, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8Hash(ergoTreeHash, params))

}
