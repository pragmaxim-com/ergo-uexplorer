package org.ergoplatform.uexplorer.backend.boxes

import io.getquill.*
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.*
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash, TokenId}
import zio.*

import java.sql.SQLException
import javax.sql.DataSource

case class PersistentAssetRepo(ds: DataSource) extends AssetRepo with Codecs:
  val ctx = new H2ZioJdbcContext(Literal)
  import ctx.*

  private val dsLayer = ZLayer.succeed(ds)

  def lookupUnspentAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((_, a) => a.map(_.tokenId).contains(lift(tokenId)))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((_, a) => a.map(_.tokenId).contains(lift(tokenId)))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByHash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, _) => b.ergoTreeHash == lift(hash))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByT8Hash(t8hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => b.ergoTreeT8Hash.contains(lift(t8hash)))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupUnspentAssetsByHash(hash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => b.ergoTreeHash == lift(hash))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupUnspentAssetsByBoxId(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => b.boxId == lift(boxId))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByBoxId(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => b.boxId == lift(boxId))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByBoxIds(boxIds: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => liftQuery(boxIds).contains(b.boxId))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupUnspentAssetsByBoxIds(boxIds: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => liftQuery(boxIds).contains(b.boxId))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

  def lookupUnspentAssetsByT8Hash(t8hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[(BoxId, Option[Asset2Box])]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .leftJoin(query[Asset2Box])
            .on((b, a) => a.boxId == b.boxId)
            .filter((b, a) => b.ergoTreeT8Hash.contains(lift(t8hash)))
            .map((b, a) => b.boxId -> a)
        }
      }
      .provide(dsLayer)

object PersistentAssetRepo:
  def layer: ZLayer[DataSource, Nothing, PersistentAssetRepo] =
    ZLayer.fromFunction(PersistentAssetRepo(_))
