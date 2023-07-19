package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore.*
import org.h2.mvstore.MVMap.DecisionMaker
import org.h2.mvstore.{MVMap, MVStore}
import zio.Task

import java.nio.file.Path
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

case class MultiMvSet[K, C[_], V](
  id: MultiColId
)(implicit
  store: MVStore,
  c: MultiSetCodec[C, V],
  sc: SuperNodeSetCodec[C, V],
  vc: ValueCodec[SuperNodeCounter],
  kc: HotKeyCodec[K]
) extends MultiSetLike[K, C, V] {

  private val commonMap: MapLike[K, C[V]]           = new MvMap[K, C[V]](id)
  private val superNodeMap: SuperNodeMvSet[K, C, V] = SuperNodeMvSet[K, C, V](id)

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def size: MultiColSize = MultiColSize(superNodeMap.size, superNodeMap.totalSize, commonMap.size)

  def clearEmptySuperNodes: Task[Unit] =
    superNodeMap.clearEmptySuperNodes

  def getReport: (Path, Vector[(String, SuperNodeCounter)]) =
    ergoHomeDir.resolve(s"hot-keys-$id-$randomNumberPerRun.csv") -> superNodeMap.getReport

  def removeSubsetOrFail(k: K, values: IterableOnce[V], size: Int)(f: C[V] => Option[C[V]]): Try[Unit] =
    superNodeMap.removeAllOrFail(k, values, size).fold(commonMap.removeOrUpdateOrFail(k)(f))(identity)

  def adjustAndForget(k: K, values: IterableOnce[V], size: Int): Try[_] =
    superNodeMap.putAllNewOrFail(k, values, size).getOrElse {
      val (appended, _) = commonMap.adjustCollection(k)(c.append(values))
      if (appended)
        Success(())
      else
        Failure(new AssertionError(s"All inserted values under key $k should be appended"))
    }

}
