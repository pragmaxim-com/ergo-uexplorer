package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.backend.boxes.InmemoryBoxRepo.matchCaseClassWith
import org.ergoplatform.uexplorer.db.{Box, ErgoTree, ErgoTreeT8, Utxo}
import org.ergoplatform.uexplorer.{BoxId, ErgoTreeHash, ErgoTreeT8Hash}
import zio.*

case class BoxRepoState(
  unspent: Map[BoxId, Utxo],
  boxes: Map[BoxId, Box],
  ets: Map[ErgoTreeHash, ErgoTree],
  etT8s: Map[ErgoTreeT8Hash, ErgoTreeT8]
)

object BoxRepoState {
  def empty = BoxRepoState(Map.empty, Map.empty, Map.empty, Map.empty)
}

case class InmemoryBoxRepo(state: Ref[BoxRepoState]) extends BoxRepo:
  override def insertUtxos(
    ergoTrees: Iterable[ErgoTree],
    ergoTreeT8s: Iterable[ErgoTreeT8],
    utxos: Iterable[Utxo]
  ): Task[Iterable[BoxId]] =
    state.modify { state =>
      utxos.map(_.boxId) -> state.copy(
        unspent = state.unspent ++ utxos.map(b => b.boxId -> b),
        boxes   = state.boxes ++ utxos.map(b => b.boxId -> b.toBox),
        ets     = state.ets ++ ergoTrees.map(et => et.hash -> et),
        etT8s   = state.etT8s ++ ergoTreeT8s.map(etT8 => etT8.hash -> etT8)
      )
    }

  override def deleteUtxo(boxId: BoxId): Task[Long] = state.modify(s => 0L -> s.copy(s.unspent.removed(boxId)))

  override def deleteUtxos(boxIds: Iterable[BoxId]): Task[Long] =
    state.modify(s => 0L -> s.copy(s.unspent.removedAll(boxIds)))

  override def lookupBox(boxId: BoxId): Task[Option[Box]] = state.get.map(_.boxes.get(boxId))

  override def lookupUtxo(boxId: BoxId): Task[Option[Utxo]] = state.get.map(_.unspent.get(boxId))

  override def lookupBoxes(boxes: Set[BoxId]): Task[List[Box]] =
    state.get.map(_.boxes.filter(t => boxes.contains(t._1)).valuesIterator.toList)

  override def lookupUtxos(utxos: Set[BoxId]): Task[List[Utxo]] =
    state.get.map(_.unspent.filter(t => utxos.contains(t._1)).valuesIterator.toList)

  override def lookupBoxesByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    state.get.map(state =>
      state.boxes.collect {
        case (_, box) if box.ergoTreeHash == etHash && matchCaseClassWith(box, columns, filter) =>
          box
      }
    )

  override def lookupUtxosByHash(etHash: ErgoTreeHash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    state.get.map(state =>
      state.unspent.collect {
        case (_, utxo) if utxo.ergoTreeHash == etHash && matchCaseClassWith(utxo, columns, filter) =>
          utxo
      }
    )

  override def lookupBoxesByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Box]] =
    state.get.map(state =>
      state.boxes.collect {
        case (_, box) if box.ergoTreeT8Hash.contains(etT8Hash) && matchCaseClassWith(box, columns, filter) =>
          box
      }
    )

  override def lookupUtxosByT8Hash(etT8Hash: ErgoTreeT8Hash, columns: List[String], filter: Map[String, Any]): Task[Iterable[Utxo]] =
    state.get.map(state =>
      state.unspent.collect {
        case (_, utxo) if utxo.ergoTreeT8Hash.contains(etT8Hash) => utxo
      }
    )

  override def lookupUtxoIdsByHash(etHash: ErgoTreeHash): Task[Set[BoxId]] =
    state.get.map(state =>
      state.unspent.collect {
        case (_, utxo) if utxo.ergoTreeHash == etHash => utxo.boxId
      }.toSet
    )

  override def lookupUtxoIdsByT8Hash(etT8Hash: ErgoTreeT8Hash): Task[Set[BoxId]] =
    state.get.map(state =>
      state.unspent.collect {
        case (_, utxo) if utxo.ergoTreeT8Hash.contains(etT8Hash) => utxo.boxId
      }.toSet
    )

  override def isEmpty: Task[Boolean] =
    state.get.map(s => s.boxes.isEmpty && s.unspent.isEmpty && s.ets.isEmpty && s.etT8s.isEmpty)

object InmemoryBoxRepo {

  def matchCaseClassWith(cc: AnyRef, columns: List[String], filter: Map[String, Any]): Boolean = {
    val ccFields: Map[String, Any] =
      cc.getClass.getDeclaredFields
        .tapEach(_.setAccessible(true))
        .foldLeft(Map.empty)((a, f) => a + (f.getName -> f.get(cc)))
    filter.removedAll(columns).forall { case (k, v) =>
      ccFields.get(k).contains(v)
    }
  }

  def layer: ZLayer[Any, Nothing, InmemoryBoxRepo] =
    ZLayer.fromZIO(
      Ref.make(BoxRepoState.empty).map(new InmemoryBoxRepo(_))
    )

}
