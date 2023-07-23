package org.ergoplatform.uexplorer.backend.boxes

import org.ergoplatform.uexplorer.{BlockId, BoxId, ErgoTreeHash, ErgoTreeT8Hash}
import org.ergoplatform.uexplorer.Const.Protocol
import org.ergoplatform.uexplorer.db.{Block, Box, ErgoTree, ErgoTreeT8, Utxo}
import zio.*

import scala.collection.mutable

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

  override def isEmpty: Task[Boolean] =
    state.get.map(s => s.boxes.isEmpty && s.unspent.isEmpty && s.ets.isEmpty && s.etT8s.isEmpty)

object InmemoryBoxRepo {
  def layer: ZLayer[Any, Nothing, InmemoryBoxRepo] =
    ZLayer.fromZIO(
      Ref.make(BoxRepoState.empty).map(new InmemoryBoxRepo(_))
    )

}
