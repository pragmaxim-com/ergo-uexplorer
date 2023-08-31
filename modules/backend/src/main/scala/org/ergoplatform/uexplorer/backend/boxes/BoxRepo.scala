package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash, TokenId}
import zio.*

trait BoxRepo:

  def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    assetsToBox: Iterable[Asset2Box],
    assets: Iterable[Asset],
    utxos: Iterable[Utxo]
  ): Task[Iterable[BoxId]]

  def deleteUtxo(boxId: BoxId): Task[Long]

  def deleteUtxos(boxId: Iterable[BoxId]): Task[Long]

  def lookupUnspentAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset2Box]]

  def lookupAnyAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset2Box]]

  def lookupBox(boxId: BoxId): Task[Option[Box]]

  def lookupUtxo(boxId: BoxId): Task[Option[Utxo]]

  def lookupBoxes(boxes: Set[BoxId]): Task[List[Box]]

  def lookupUtxos(boxes: Set[BoxId]): Task[List[Utxo]]

  def lookupUtxosByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]]

  def lookupBoxesByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]]

  def lookupUtxoIdsByTokenId(tokenId: TokenId): Task[Set[BoxId]]

  def lookupBoxesByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]]

  def lookupUtxosByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]]

  def lookupUtxoIdsByHash(etHash: ErgoTreeHash): Task[Set[BoxId]]

  def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]]

  def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]]

  def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Set[BoxId]]

  def isEmpty: Task[Boolean]

object BoxRepo:
  def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    assetsToBox: Iterable[Asset2Box],
    assets: Iterable[Asset],
    utxos: Iterable[Utxo]
  ): ZIO[BoxRepo, Throwable, Iterable[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.insertUtxos(ergoTrees, ergoTreeT8s, assetsToBox, assets, utxos))

  def deleteUtxo(boxId: BoxId): ZIO[BoxRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BoxRepo](_.deleteUtxo(boxId))

  def deleteUtxos(boxIds: Iterable[BoxId]): ZIO[BoxRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BoxRepo](_.deleteUtxos(boxIds))

  def lookupBox(boxId: BoxId): ZIO[BoxRepo, Throwable, Option[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBox(boxId))

  def lookupUtxo(boxId: BoxId): ZIO[BoxRepo, Throwable, Option[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxo(boxId))

  def lookupUtxosByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxosByTokenId(tokenId, columns, filter))

  def lookupBoxesByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxesByTokenId(tokenId, columns, filter))

  def lookupBoxesByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxesByHash(etHash, columns, filter))

  def lookupUtxosByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxosByHash(etHash, columns, filter))

  def lookupUtxoIdsByHash(etHash: ErgoTreeHash): ZIO[BoxRepo, Throwable, Set[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxoIdsByHash(etHash))

  def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxesByT8Hash(etT8Hash, columns, filter))

  def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxosByT8Hash(etT8Hash, columns, filter))

  def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash): ZIO[BoxRepo, Throwable, Set[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxoIdsByT8Hash(etT8Hash))

  def lookupBoxes(boxes: Set[BoxId]): ZIO[BoxRepo, Throwable, Iterable[Box]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupBoxes(boxes))

  def lookupUtxos(boxes: Set[BoxId]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxos(boxes))

  def isEmpty: ZIO[BoxRepo, Throwable, Boolean] =
    ZIO.serviceWithZIO[BoxRepo](_.isEmpty)
