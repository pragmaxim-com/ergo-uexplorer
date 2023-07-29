package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.db.{Box, Utxo}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import org.ergoplatform.uexplorer.{Address, BoxId, CoreConf, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hash, ErgoTreeT8Hex}
import zio.http.QueryParams
import zio.{Task, ZIO, ZLayer}

case class BoxService(boxRepo: BoxRepo, coreConf: CoreConf) {
  import BoxService.allColumns

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

  def getSpentBoxesByAddress(address: Address, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByAddress(address: Address, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByAddress(address: Address, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTree(ergoTreeHex: ErgoTreeHex, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTree(ergoTreeHex: ErgoTreeHex, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTree(ergoTreeHex: ErgoTreeHex, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): Task[Iterable[Box]] =
    for
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)
      utxoIds <- boxRepo.lookupUtxoIdsByHash(ergoTreeHash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): Task[Iterable[Utxo]] =
    boxRepo.lookupUtxosByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): Task[Iterable[Box]] =
    boxRepo.lookupBoxesByHash(ergoTreeHash, allColumns, params.map.view.mapValues(_.head).toMap)

  def getSpentBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeT8Hex, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes   <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
      utxoIds <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeHex, params: QueryParams): Task[Iterable[Utxo]] =
    for
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      utxos <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield utxos

  def getAnyBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeT8Hex, params: QueryParams): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      indexFilter = params.map.view.mapValues(_.head).toMap
      boxes <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, indexFilter)
    yield boxes

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash, params: QueryParams): Task[Iterable[Box]] =
    for
      boxes   <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)
      utxoIds <- boxRepo.lookupUtxoIdsByT8Hash(ergoTreeT8Hash)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash, params: QueryParams): Task[Iterable[Utxo]] =
    boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash, params: QueryParams): Task[Iterable[Box]] =
    boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash, allColumns, params.map.view.mapValues(_.head).toMap)

}

object BoxService {
  val indexWhiteList = List("tokenId", "txId", "r4", "r5", "r6", "r7", "r8", "r9")
  val allColumns     = indexWhiteList ++ List("boxId", "blockId", "ergoTreeHash", "ergoTreeT8Hash", "ergValue")

  def layer: ZLayer[BoxRepo with CoreConf, Nothing, BoxService] =
    ZLayer.fromFunction(BoxService.apply _)

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

  def getSpentBoxesByAddress(address: Address, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByAddress(address, params))

  def getUnspentBoxesByAddress(address: Address, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByAddress(address, params))

  def getAnyBoxesByAddress(address: Address, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByAddress(address, params))

  def getSpentBoxesByErgoTree(ergoTree: ErgoTreeHex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTree(ergoTree, params))

  def getUnspentBoxesByErgoTree(ergoTree: ErgoTreeHex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTree(ergoTree, params))

  def getAnyBoxesByErgoTree(ergoTree: ErgoTreeHex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTree(ergoTree, params))

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeHash(ergoTreeHash, params))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeHash(ergoTreeHash, params))

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeHash(ergoTreeHash, params))

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8(ergoTreeT8, params))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8(ergoTreeT8, params))

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8(ergoTreeT8, params))

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8Hash(ergoTreeHash, params))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash, params))

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash, params: QueryParams): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8Hash(ergoTreeHash, params))

}
