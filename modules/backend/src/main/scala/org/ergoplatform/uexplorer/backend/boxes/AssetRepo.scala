package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash, TokenId}
import zio.*

trait AssetRepo:

  def lookupUnspentAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupAnyAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupAnyAssetsByHash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupAnyAssetsByT8Hash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupUnspentAssetsByHash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupUnspentAssetsByBoxId(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupAnyAssetsByBoxId(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupAnyAssetsByBoxIds(boxId: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupUnspentAssetsByBoxIds(boxId: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]

  def lookupUnspentAssetsByT8Hash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]]
