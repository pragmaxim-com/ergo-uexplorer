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
      _ <- ZIO.collectAllParDiscard(
             List(
               ctx.run(insertErgoTreesQuery(ergoTrees)),
               ctx.run(insertErgoTreeT8sQuery(ergoTreeT8s)),
               ctx.run(insertAssets(assets))
             )
           )
      _ <- ctx.run(insertBoxesQuery(utxos.map(_.toBox)))
      _ <- ZIO.collectAllParDiscard(
             List(
               ctx.run(insertAssetsToBox(assetsToBox)),
               ctx.run(insertUtxosQuery(utxos))
             )
           )
    } yield utxos.map(_.boxId)).provide(dsLayer)

  override def deleteUtxo(boxId: BoxId): Task[Long] =
    ctx
      .run {
        quote {
          query[Utxo].filter(p => p.boxId == lift(boxId)).delete
        }
      }
      .provide(dsLayer)

  override def deleteUtxos(boxIds: Iterable[BoxId]): Task[Long] =
    ctx
      .run {
        quote {
          query[Utxo].filter(p => liftQuery(boxIds).contains(p.boxId)).delete
        }
      }
      .provide(dsLayer)

  def lookupUnspentAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset2Box]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .join(query[Asset2Box])
            .on((a, utxo) => a.boxId == utxo.boxId)
            .filter((_, a) => a.tokenId == lift(tokenId))
            .map((_, a) => Asset2Box(a.tokenId, a.boxId, a.amount))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  def lookupAnyAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset2Box]] =
    ctx
      .run {
        quote {
          query[Box]
            .join(query[Asset2Box])
            .on((a, box) => a.boxId == box.boxId)
            .filter((_, a) => a.tokenId == lift(tokenId))
            .map((_, a) => Asset2Box(a.tokenId, a.boxId, a.amount))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupBox(boxId: BoxId): Task[Option[Box]] =
    ctx
      .run {
        quote {
          query[Box]
            .filter(p => p.boxId == lift(boxId))
        }
      }
      .provide(dsLayer)
      .map(_.headOption)

  override def lookupUtxo(boxId: BoxId): Task[Option[Utxo]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(p => p.boxId == lift(boxId))
        }
      }
      .provide(dsLayer)
      .map(_.headOption)

  override def lookupBoxes(ids: Set[BoxId]): Task[List[Box]] =
    ctx
      .run {
        quote {
          query[Box]
            .filter(p => liftQuery(ids).contains(p.boxId))
        }
      }
      .provide(dsLayer)

  override def lookupUtxos(ids: Set[BoxId]): Task[List[Utxo]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(p => liftQuery(ids).contains(p.boxId))
        }
      }
      .provide(dsLayer)

  override def lookupUtxosByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    ctx
      .run {
        quote {
          query[Asset2Box]
            .join(query[Utxo])
            .on((a, utxo) => utxo.boxId == a.boxId)
            .filter((a, _) => a.tokenId == lift(tokenId))
            .map((_, b) => Utxo(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupBoxesByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    ctx
      .run {
        quote {
          query[Asset2Box]
            .join(query[Box])
            .on((a, b) => b.boxId == a.boxId)
            .filter((a, _) => a.tokenId == lift(tokenId))
            .map((_, b) => Box(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxoIdsByTokenId(tokenId: TokenId): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Asset2Box]
            .join(query[Utxo])
            .on((a, b) => b.boxId == a.boxId)
            .filter((a, _) => a.tokenId == lift(tokenId))
            .map((_, b) => b.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupBoxesByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    ctx
      .run {
        quote {
          query[ErgoTree]
            .join(query[Box])
            .on((et, box) => et.hash == box.ergoTreeHash)
            .filter((et, _) => et.hash == lift(etHash))
            .map((_, b) => Box(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxosByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    ctx
      .run {
        quote {
          query[ErgoTree]
            .join(query[Utxo])
            .on((et, utxo) => et.hash == utxo.ergoTreeHash)
            .filter((et, _) => et.hash == lift(etHash))
            .map((_, b) => Utxo(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxoIdsByHash(etHash: ErgoTreeHash): Task[Set[BoxId]] =
    ctx
      .run {
        quote {
          query[Utxo]
            .filter(_.ergoTreeHash == lift(etHash))
            .map(_.boxId)
        }
      }
      .map(_.toSet)
      .provide(dsLayer)

  override def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    ctx
      .run {
        quote {
          query[ErgoTreeT8]
            .join(query[Box])
            .on((et, box) => box.ergoTreeT8Hash.contains(et.hash))
            .filter((et, _) => et.hash == lift(etT8Hash))
            .map((_, b) => Box(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    ctx
      .run {
        quote {
          query[ErgoTreeT8]
            .join(query[Utxo])
            .on((et, box) => box.ergoTreeT8Hash.contains(et.hash))
            .filter((et, _) => et.hash == lift(etT8Hash))
            .map((_, b) => Utxo(b.boxId, b.txId, b.ergoTreeHash, b.ergoTreeT8Hash, b.ergValue, b.r4, b.r5, b.r6, b.r7, b.r8, b.r9))
            .filterByKeys(filter)
            .filterColumns(columns)
        }
      }
      .provide(dsLayer)

  override def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Set[BoxId]] =
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
