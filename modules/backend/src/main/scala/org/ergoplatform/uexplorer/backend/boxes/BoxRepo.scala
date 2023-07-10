package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.{BlockId, BoxId}
import org.ergoplatform.uexplorer.db.*
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

  def lookupBoxes(boxes: Iterable[BoxId]): Task[List[Box]]

  def lookupUtxos(boxes: Iterable[BoxId]): Task[List[Utxo]]

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

  def lookupBoxes(boxes: Iterable[BoxId]): ZIO[BoxRepo, Throwable, List[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxes(boxes))

  def lookupUtxos(boxes: Iterable[BoxId]): ZIO[BoxRepo, Throwable, List[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxos(boxes))

  def isEmpty: ZIO[BoxRepo, Throwable, Boolean] =
    ZIO.serviceWithZIO[BoxRepo](_.isEmpty)
