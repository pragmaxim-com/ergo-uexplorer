package org.ergoplatform.uexplorer.backend.boxes

import io.getquill.*
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash, TokenId}
import org.ergoplatform.uexplorer.backend.Codecs
import org.ergoplatform.uexplorer.db.*
import zio.*

import java.sql.SQLException
import javax.sql.DataSource

case class PersistentBoxRepo(ds: DataSource) extends BoxRepo with Codecs:
  val ctx = new H2ZioJdbcContext(Literal)
  import ctx.*

  private val dsLayer = ZLayer.succeed(ds)

  // TODO https://github.com/zio/zio-protoquill/issues/298
  inline def onConflictIgnoreForBatchQueries[T](inline entity: Insert[T]): Insert[T] =
    sql"$entity ON CONFLICT DO NOTHING".as[Insert[T]]

  private def insertUtxosQuery(utxos: Iterable[Utxo]) =
    quote {
      liftQuery(utxos).foreach(utxo => query[Utxo].insertValue(utxo))
    }
  private def insertBoxesQuery(boxes: Iterable[Box]) =
    quote {
      liftQuery(boxes).foreach(box => query[Box].insertValue(box))
    }
  private def insertErgoTreesQuery(ergoTrees: Iterable[ErgoTree]) =
    quote {
      liftQuery(ergoTrees).foreach(ergoTree => onConflictIgnoreForBatchQueries(query[ErgoTree].insertValue(ergoTree)))
    }
  private def insertErgoTreeT8sQuery(ergoTreeT8s: Iterable[ErgoTreeT8]) =
    quote {
      liftQuery(ergoTreeT8s).foreach(ergoTreeT8 => onConflictIgnoreForBatchQueries(query[ErgoTreeT8].insertValue(ergoTreeT8)))
    }
  private def insertAssetsToBox(assetsToBox: Iterable[Asset2Box]) =
    quote {
      liftQuery(assetsToBox).foreach(assetToBox => query[Asset2Box].insertValue(assetToBox))
    }
  private def insertAssets(assets: Iterable[Asset]) =
    quote {
      liftQuery(assets).foreach(asset => onConflictIgnoreForBatchQueries(query[Asset].insertValue(asset)))
    }

  override def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    assetsToBox: Iterable[Asset2Box],
    assets: Iterable[Asset],
    utxos: Iterable[Utxo]
  ): Task[Iterable[BoxId]] =
    (for {
      _ <- ZIO.when(ergoTrees.nonEmpty)(ctx.run(insertErgoTreesQuery(ergoTrees)))
      _ <- ZIO.when(ergoTreeT8s.nonEmpty)(ctx.run(insertErgoTreeT8sQuery(ergoTreeT8s)))
      _ <- ZIO.when(assets.nonEmpty)(ctx.run(insertAssets(assets)))
      _ <- ZIO.when(utxos.nonEmpty)(ctx.run(insertBoxesQuery(utxos.map(_.toBox))))
      _ <- ZIO.when(assetsToBox.nonEmpty)(ctx.run(insertAssetsToBox(assetsToBox)))
      _ <- ZIO.when(utxos.nonEmpty)(ctx.run(insertUtxosQuery(utxos)))
    } yield utxos.map(_.boxId)).provide(dsLayer)

  override def deleteUtxos(boxIds: Iterable[BoxId]): Task[Long] =
    ctx
      .run {
        quote {
          query[Utxo].filter(p => liftQuery(boxIds).contains(p.boxId)).delete
        }
      }
      .provide(dsLayer)

  def joinUtxoWithErgoTreeAndBlock(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[((Utxo, ErgoTree), Block)]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filterByKeys(filter)
            .filterColumns(columns)
            .join(query[ErgoTree])
            .on { case (b, et) => b.ergoTreeHash == et.hash }
            .join(query[Block])
            .on { case ((b, _), block) => b.blockId == block.blockId }
            .filter { case ((b, _), _) => b.boxId == lift(boxId) }
        }
      }
      .provide(dsLayer)

  def joinBoxWithErgoTreeAndBlock(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Iterable[((Box, ErgoTree), Block)]] =
    ctx
      .run {
        quote {
          query[Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .join(query[ErgoTree])
            .on { case (b, et) => b.ergoTreeHash == et.hash }
            .join(query[Block])
            .on { case ((b, _), block) => b.blockId == block.blockId }
            .filter { case ((b, _), _) => b.boxId == lift(boxId) }
        }
      }
      .provide(dsLayer)

  override def lookupUtxo(boxId: BoxId, columns: List[String], filter: Map[String, Any]): Task[Option[Utxo]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(p => p.boxId == lift(boxId))
        }
      }
      .provide(dsLayer)
      .map(_.headOption)

  override def lookupUtxoIdsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Asset2Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .join(query[Utxo])
            .on((a, utxo) => utxo.boxId == a.boxId)
            .filter((a, _) => a.tokenId == lift(tokenId))
            .map((_, utxo) => utxo.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupBoxIdsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Asset2Box]
            .filterByKeys(filter)
            .filterColumns(columns)
            .join(query[Box])
            .on((a, utxo) => utxo.boxId == a.boxId)
            .filter((a, _) => a.tokenId == lift(tokenId))
            .map((_, utxo) => utxo.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  def lookupBoxIdsByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Box]
            .filter(b => b.ergoTreeHash == lift(etHash))
            .filterByKeys(filter)
            .filterColumns(columns)
            .map(_.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupUtxos(boxIds: Set[BoxId], columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(b => liftQuery(boxIds).contains(b.boxId))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxoIdsByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(b => b.ergoTreeHash == lift(etHash))
            .filterByKeys(filter)
            .filterColumns(columns)
            .map(_.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(_.ergoTreeT8Hash.contains(lift(etT8Hash)))
            .map(_.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupBoxIdsByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Box]
            .filter(_.ergoTreeT8Hash.contains(lift(etT8Hash)))
            .map(_.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def isEmpty: Task[Boolean] =
    ctx
      .run {
        quote {
          query[Utxo].take(1).isEmpty
        }
      }
      .provide(dsLayer)

object PersistentBoxRepo:
  def layer: ZLayer[DataSource, Nothing, PersistentBoxRepo] =
    ZLayer.fromFunction(PersistentBoxRepo(_))
