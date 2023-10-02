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
  
  def joinUtxoWithErgoTreeAndBlock(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[((Utxo, ErgoTree), Block)]]
  
  def joinBoxWithErgoTreeAndBlock(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[((Box, ErgoTree), Block)]]
  
  def deleteUtxos(boxId: Iterable[BoxId]): Task[Long]
  
  def lookupUtxo(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Option[Utxo]]
  
  def lookupUtxos(boxIds: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]]
  
  def lookupBoxIdsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
  def lookupUtxoIdsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
  def lookupBoxIdsByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
  def lookupUtxoIdsByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
  def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
  def lookupBoxIdsByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]]
  
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

  def deleteUtxos(boxIds: Iterable[BoxId]): ZIO[BoxRepo, Throwable, Long] =
    ZIO.serviceWithZIO[BoxRepo](_.deleteUtxos(boxIds))

  def lookupUtxo(boxId: BoxId, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Option[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxo(boxId, columns, filter))

  def lookupUtxoIdsByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Set[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxoIdsByHash(etHash, columns, filter))

  def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Set[BoxId]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxoIdsByT8Hash(etT8Hash, columns, filter))

  def lookupUtxos(boxes: Set[BoxId], columns: List[String], filter: Map[String, Any]): ZIO[BoxRepo, Throwable, Iterable[Utxo]] =
    ZIO.serviceWithZIO[BoxRepo](_.lookupUtxos(boxes, columns, filter))

  def isEmpty: ZIO[BoxRepo, Throwable, Boolean] =
    ZIO.serviceWithZIO[BoxRepo](_.isEmpty)
