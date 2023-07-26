package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.{Address, BoxId, CoreConf, ErgoTreeHash, ErgoTreeHex, ErgoTreeT8Hash, ErgoTreeT8Hex}
import org.ergoplatform.uexplorer.db.{Box, ErgoTree, Utxo}
import org.ergoplatform.uexplorer.parser.ErgoTreeParser
import zio.{Task, ZIO, ZLayer}

case class BoxService(boxRepo: BoxRepo, coreConf: CoreConf) {

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

  def getSpentBoxesByAddress(address: Address): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash)
      utxoIds      <- boxRepo.lookupUtxosByHash(ergoTreeHash).map(_.map(_.boxId).toSet)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByAddress(address: Address): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash)
    yield utxos

  def getAnyBoxesByAddress(address: Address): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.base58Address2ErgoTreeHash(address)(coreConf.addressEncoder)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash)
    yield boxes

  def getSpentBoxesByErgoTree(ergoTreeHex: ErgoTreeHex): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash)
      utxoIds      <- boxRepo.lookupUtxosByHash(ergoTreeHash).map(_.map(_.boxId).toSet)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTree(ergoTreeHex: ErgoTreeHex): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      utxos        <- boxRepo.lookupUtxosByHash(ergoTreeHash)
    yield utxos

  def getAnyBoxesByErgoTree(ergoTreeHex: ErgoTreeHex): Task[Iterable[Box]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeHex)
      boxes        <- boxRepo.lookupBoxesByHash(ergoTreeHash)
    yield boxes

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): Task[Iterable[Box]] =
    for
      boxes   <- boxRepo.lookupBoxesByHash(ergoTreeHash)
      utxoIds <- boxRepo.lookupUtxosByHash(ergoTreeHash).map(_.map(_.boxId).toSet)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): Task[Iterable[Utxo]] =
    boxRepo.lookupUtxosByHash(ergoTreeHash)

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): Task[Iterable[Box]] =
    boxRepo.lookupBoxesByHash(ergoTreeHash)

  def getSpentBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeT8Hex): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash)
      utxoIds        <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash).map(_.map(_.boxId).toSet)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeHex): Task[Iterable[Utxo]] =
    for
      ergoTreeHash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      utxos        <- boxRepo.lookupUtxosByT8Hash(ergoTreeHash)
    yield utxos

  def getAnyBoxesByErgoTreeT8(ergoTreeT8Hex: ErgoTreeT8Hex): Task[Iterable[Box]] =
    for
      ergoTreeT8Hash <- ErgoTreeParser.ergoTreeHex2Hash(ergoTreeT8Hex)
      boxes          <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash)
    yield boxes

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash): Task[Iterable[Box]] =
    for
      boxes   <- boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash)
      utxoIds <- boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash).map(_.map(_.boxId).toSet)
    yield boxes.filter(b => !utxoIds.contains(b.boxId))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash): Task[Iterable[Utxo]] =
    boxRepo.lookupUtxosByT8Hash(ergoTreeT8Hash)

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeT8Hash: ErgoTreeT8Hash): Task[Iterable[Box]] =
    boxRepo.lookupBoxesByT8Hash(ergoTreeT8Hash)

}

object BoxService {

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

  def getSpentBoxesByAddress(address: Address): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByAddress(address))

  def getUnspentBoxesByAddress(address: Address): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByAddress(address))

  def getAnyBoxesByAddress(address: Address): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByAddress(address))

  def getSpentBoxesByErgoTree(ergoTree: ErgoTreeHex): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTree(ergoTree))

  def getUnspentBoxesByErgoTree(ergoTree: ErgoTreeHex): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTree(ergoTree))

  def getAnyBoxesByErgoTree(ergoTree: ErgoTreeHex): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTree(ergoTree))

  def getSpentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeHash(ergoTreeHash))

  def getUnspentBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeHash(ergoTreeHash))

  def getAnyBoxesByErgoTreeHash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeHash(ergoTreeHash))

  def getSpentBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8(ergoTreeT8))

  def getUnspentBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8(ergoTreeT8))

  def getAnyBoxesByErgoTreeT8(ergoTreeT8: ErgoTreeT8Hex): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8(ergoTreeT8))

  def getSpentBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getSpentBoxesByErgoTreeT8Hash(ergoTreeHash))

  def getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxService](_.getUnspentBoxesByErgoTreeT8Hash(ergoTreeHash))

  def getAnyBoxesByErgoTreeT8Hash(ergoTreeHash: ErgoTreeHash): ZIO[BoxService, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxService](_.getAnyBoxesByErgoTreeT8Hash(ergoTreeHash))

}
