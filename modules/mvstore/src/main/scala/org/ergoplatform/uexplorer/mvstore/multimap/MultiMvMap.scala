package org.ergoplatform.uexplorer.mvstore.multimap

import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.HotKey
import org.h2.mvstore.{MVMap, MVStore}
import zio.Task

import java.nio.file.Path
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

case class MultiMvMap[PK, C[_, _], K, V](
                                          id: MultiColId,
                                          hotKeyDir: Path
)(implicit
  store: MVStore,
  c: MultiMapCodec[C, K, V],
  sc: SuperNodeMapCodec[C, K, V],
  vc: ValueCodec[SuperNodeCounter],
  kc: HotKeyCodec[PK]
) extends MultiMapLike[PK, C, K, V] {
  private val commonMap: MapLike[PK, C[K, V]]           = new MvMap[PK, C[K, V]](id)
  private val superNodeMap: SuperNodeMvMap[PK, C, K, V] = SuperNodeMvMap[PK, C, K, V](id, hotKeyDir)

  def clearEmptySuperNodes: Task[Unit] = superNodeMap.clearEmptySuperNodes()

  def getReport: (Path, Vector[HotKey]) =
    superNodeMap.getReport

  def get(pk: PK, sk: K): Option[V] =
    superNodeMap
      .get(pk, sk)
      .orElse(commonMap.get(pk).flatMap(v => c.readOne(sk, v)))

  def getPartially(pk: PK, sk: IterableOnce[K]): Option[C[K, V]] =
    superNodeMap
      .getPartially(pk, sk)
      .orElse(commonMap.getWithOp(pk)(c.readPartially(sk)))

  def getAll(pk: PK): Option[C[K, V]] =
    superNodeMap.getAll(pk).orElse(commonMap.get(pk))

  def remove(pk: PK): Removed =
    superNodeMap.remove(pk).isDefined || commonMap.removeAndForget(pk)

  def removeOrFail(pk: PK): Try[C[K, V]] =
    superNodeMap
      .remove(pk)
      .fold(commonMap.removeOrFail(pk))(Success(_))

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def size: MultiColSize = MultiColSize(superNodeMap.size, superNodeMap.totalSize, commonMap.size)

  def removeAllOrFail(pk: PK, secondaryKeys: IterableOnce[K], size: Int)(f: C[K, V] => Option[C[K, V]]): Try[Unit] =
    superNodeMap.removeAllOrFail(pk, secondaryKeys, size).fold(commonMap.removeOrUpdateOrFail(pk)(f))(identity)

  def adjustAndForget(pk: PK, entries: IterableOnce[(K, V)], size: Int): Try[_] =
    superNodeMap.putAllNewOrFail(pk, entries, size).getOrElse {
      val (appended, _) = commonMap.adjustCollection(pk)(c.append(entries))
      if (appended)
        Success(())
      else
        Failure(new AssertionError(s"All inserted values under key $pk should be appended"))
    }

}
