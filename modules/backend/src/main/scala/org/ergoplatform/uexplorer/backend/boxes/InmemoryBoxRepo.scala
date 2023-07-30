package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.boxes.InmemoryBoxRepo.matchCaseClassWith
import org.ergoplatform.uexplorer.db.{Asset, Box, ErgoTree, ErgoTreeT8, Utxo}
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash, TokenId}
import zio.*

case class BoxRepoState(
  utxos: Map[BoxId, Utxo],
  boxes: Map[BoxId, Box],
  ets: Map[ErgoTreeHash, ErgoTree],
  etT8s: Map[ErgoTreeT8Hash, ErgoTreeT8],
  assets: List[Asset]
)

object BoxRepoState {
  def empty: BoxRepoState = BoxRepoState(Map.empty, Map.empty, Map.empty, Map.empty, List.empty)
}

case class InmemoryBoxRepo(state: Ref[BoxRepoState]) extends BoxRepo:
  override def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    assets: List[Asset],
    utxos: Iterable[Utxo]
  ): Task[Iterable[BoxId]] =
    state.modify { state =>
      utxos.map(_.boxId) -> state.copy(
        utxos  = state.utxos ++ utxos.map(b => b.boxId -> b),
        boxes  = state.boxes ++ utxos.map(b => b.boxId -> b.toBox),
        ets    = state.ets ++ ergoTrees.map(et => et.hash -> et),
        etT8s  = state.etT8s ++ ergoTreeT8s.map(etT8 => etT8.hash -> etT8),
        assets = state.assets ++ assets
      )
    }

  override def deleteUtxo(boxId: BoxId): Task[Long] = state.modify(s => 0L -> s.copy(s.utxos.removed(boxId)))

  override def deleteUtxos(boxIds: Iterable[BoxId]): Task[Long] =
    state.modify(s => 0L -> s.copy(s.utxos.removedAll(boxIds)))

  override def lookupUnspentAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset]] =
    state.get.map { state =>
      val utxos = state.utxos.toSet
      state.assets.filter { asset =>
        val matchingUtxos = utxos.filter(_._2.boxId == asset.boxId)
        matchingUtxos.nonEmpty && matchCaseClassWith(matchingUtxos.asInstanceOf[Set[AnyRef]] + asset, columns, filter)
      }
    }

  override def lookupAnyAssetsByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Asset]] =
    state.get.map { state =>
      val boxes = state.boxes.toSet
      state.assets.filter { asset =>
        val matchingBoxes = boxes.filter(_._2.boxId == asset.boxId)
        matchingBoxes.nonEmpty && matchCaseClassWith(matchingBoxes.asInstanceOf[Set[AnyRef]] + asset, columns, filter)
      }
    }

  override def lookupUtxosByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    state.get.map { state =>
      val assets = state.assets.toSet
      state.utxos.filter { case (_, utxo) =>
        val matchingAssets = assets.filter(_.boxId == utxo.boxId)
        matchingAssets.nonEmpty && matchCaseClassWith(matchingAssets.asInstanceOf[Set[AnyRef]] + utxo, columns, filter)
      }.values
    }

  override def lookupBoxesByTokenId(tokenId: TokenId, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    state.get.map(state =>
      val assets = state.assets.toSet
      state.boxes.filter { case (_, box) =>
        val matchingAssets = assets.filter(_.boxId == box.boxId)
        matchingAssets.nonEmpty && matchCaseClassWith(matchingAssets.asInstanceOf[Set[AnyRef]] + box, columns, filter)
      }.values
    )

  def lookupUtxoIdsByTokenId(tokenId: TokenId): Task[Set[BoxId]] =
    state.get.map { state =>
      state.assets.collect {
        case asset if state.utxos.contains(asset.boxId) && asset.tokenId == tokenId =>
          asset.boxId
      }.toSet
    }

  override def lookupBox(boxId: BoxId): Task[Option[Box]] = state.get.map(_.boxes.get(boxId))

  override def lookupUtxo(boxId: BoxId): Task[Option[Utxo]] = state.get.map(_.utxos.get(boxId))

  override def lookupBoxes(boxes: Set[BoxId]): Task[List[Box]] =
    state.get.map(_.boxes.filter(t => boxes.contains(t._1)).valuesIterator.toList)

  override def lookupUtxos(utxos: Set[BoxId]): Task[List[Utxo]] =
    state.get.map(_.utxos.filter(t => utxos.contains(t._1)).valuesIterator.toList)

  override def lookupBoxesByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    state.get.map(state =>
      state.boxes.collect {
        case (_, box) if box.ergoTreeHash == etHash && matchCaseClassWith(List(box), columns, filter) =>
          box
      }
    )

  override def lookupUtxosByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    state.get.map(state =>
      state.utxos.collect {
        case (_, utxo) if utxo.ergoTreeHash == etHash && matchCaseClassWith(List(utxo), columns, filter) =>
          utxo
      }
    )

  override def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    state.get.map(state =>
      state.boxes.collect {
        case (_, box) if box.ergoTreeT8Hash.contains(etT8Hash) && matchCaseClassWith(List(box), columns, filter) =>
          box
      }
    )

  override def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    state.get.map(state =>
      state.utxos.collect {
        case (_, utxo) if utxo.ergoTreeT8Hash.contains(etT8Hash) => utxo
      }
    )

  override def lookupUtxoIdsByHash(etHash: ErgoTreeHash): Task[Set[BoxId]] =
    state.get.map(state =>
      state.utxos.collect {
        case (_, utxo) if utxo.ergoTreeHash == etHash => utxo.boxId
      }.toSet
    )

  override def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Set[BoxId]] =
    state.get.map(state =>
      state.utxos.collect {
        case (_, utxo) if utxo.ergoTreeT8Hash.contains(etT8Hash) => utxo.boxId
      }.toSet
    )

  override def isEmpty: Task[Boolean] =
    state.get.map(s => s.boxes.isEmpty && s.utxos.isEmpty && s.ets.isEmpty && s.etT8s.isEmpty)

object InmemoryBoxRepo {

  def matchCaseClassWith(caseClasses: Iterable[AnyRef], columns: List[String], filter: Map[String, Any]): Boolean = {
    val ccFields: Map[String, Any] =
      caseClasses
        .foldLeft(Map.empty[String, Any]) { case (acc, cc) =>
          acc ++ cc.getClass.getDeclaredFields
            .tapEach(_.setAccessible(true))
            .foldLeft(Map.empty[String, Any])((a, f) => a + (f.getName -> f.get(cc)))
        }
    filter.removedAll(columns).forall { case (k, v) =>
      ccFields.get(k).contains(v)
    }
  }

  def layer: ZLayer[Any, Nothing, InmemoryBoxRepo] =
    ZLayer.fromZIO(
      Ref.make(BoxRepoState.empty).map(new InmemoryBoxRepo(_))
    )

}
