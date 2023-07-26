package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHash, ErgoTreeT8Hash}
import zio.*

trait BoxRepo:

  def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    utxos: Iterable[Utxo]
  ): Task[Iterable[BoxId]]

  def deleteUtxo(boxId: BoxId): Task[Long]

  def deleteUtxos(boxId: Iterable[BoxId]): Task[Long]

  def lookupBox(boxId: BoxId): Task[Option[Box]]

  def lookupUtxo(boxId: BoxId): Task[Option[Utxo]]

  def lookupBoxes(boxes: Set[BoxId]): Task[List[Box]]

  def lookupBoxesByHash(etHash: ErgoTreeHash): Task[Iterable[Box]]

  def lookupUtxosByHash(etHash: ErgoTreeHash): Task[Iterable[Utxo]]

  def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Iterable[Box]]

  def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Iterable[Utxo]]

  def lookupUtxos(boxes: Set[BoxId]): Task[List[Utxo]]

  def isEmpty: Task[Boolean]

object BoxRepo:
  def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    utxos: Iterable[Utxo]
  ): ZIO[BoxRepo, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.insertUtxos(ergoTrees, ergoTreeT8s, utxos))

  def deleteUtxo(boxId: BoxId): ZIO[BoxRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BoxRepo](_.deleteUtxo(boxId))

  def deleteUtxos(boxIds: Iterable[BoxId]): ZIO[BoxRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BoxRepo](_.deleteUtxos(boxIds))

  def lookupBox(boxId: BoxId): ZIO[BoxRepo, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBox(boxId))

  def lookupUtxo(boxId: BoxId): ZIO[BoxRepo, Throwable, Option[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxo(boxId))

  def lookupBoxesByHash(etHash: ErgoTreeHash): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxesByHash(etHash))

  def lookupUtxosByHash(etHash: ErgoTreeHash): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxosByHash(etHash))

  def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxesByT8Hash(etT8Hash))

  def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxosByT8Hash(etT8Hash))

  def lookupBoxes(boxes: Set[BoxId]): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxes(boxes))

  def lookupUtxos(boxes: Set[BoxId]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxos(boxes))

  def isEmpty: ZIO[BoxRepo, Throwable, Boolean] =
    ZIO.serviceWithZIO[BoxRepo](_.isEmpty)
