package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.HotKey
import org.h2.mvstore.{MVMap, MVStore}
import zio.Task

import java.nio.file.Path
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

case class MultiMvSet[K, C[_], V](
                                   id: MultiColId,
                                   hotKeyDir: Path
)(implicit
  store: MVStore,
  c: MultiSetCodec[C, V],
  sc: SuperNodeSetCodec[C, V],
  vc: ValueCodec[SuperNodeCounter],
  kc: HotKeyCodec[K]
) extends MultiSetLike[K, C, V] {
  private val commonMap: MapLike[K, C[V]]           = new MvMap[K, C[V]](id)
  private val superNodeMap: SuperNodeMvSet[K, C, V] = SuperNodeMvSet[K, C, V](id, hotKeyDir)

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def get(k: K): Option[C[V]] =
    superNodeMap
      .get(k)
      .orElse(commonMap.get(k))

  def contains(k: K): Boolean = superNodeMap.contains(k) || commonMap.containsKey(k)

  def size: MultiColSize = MultiColSize(superNodeMap.size, superNodeMap.totalSize, commonMap.size)

  def clearEmptySuperNodes: Task[Unit] =
    superNodeMap.clearEmptySuperNodes

  def getReport: (Path, Vector[HotKey]) =
    superNodeMap.getReport

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
