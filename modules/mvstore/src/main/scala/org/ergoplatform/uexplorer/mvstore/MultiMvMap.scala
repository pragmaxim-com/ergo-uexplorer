package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.mvstore.MultiMapLike.MultiMapSize
import org.h2.mvstore.MVMap.DecisionMaker
import org.h2.mvstore.{MVMap, MVStore}

import scala.collection.concurrent
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

//TODO make SuperNodePCol
class MultiMvMap[PK, C[_, _], K, V](
  commonMap: MapLike[PK, C[K, V]],
  superNodeMap: SuperNodeMapLike[PK, C, K, V]
)(implicit c: MultiMapCodec[C, K, V])
  extends MultiMapLike[PK, C, K, V] {

  def getFinalReport: Try[String] = superNodeMap.getFinalReport

  def get(pk: PK, sk: K): Option[V] =
    superNodeMap
      .get(pk, sk)
      .orElse(commonMap.get(pk).flatMap(v => c.readOne(sk, v)))

  def getAll(pk: PK): Option[C[K, V]] =
    superNodeMap.getAll(pk).orElse(commonMap.get(pk))

  def remove(pk: PK): Removed =
    superNodeMap.remove(pk).isDefined || commonMap.removeAndForget(pk)

  def removeOrFail(pk: PK): Try[C[K, V]] =
    superNodeMap
      .remove(pk)
      .fold(commonMap.removeOrFail(pk))(Success(_))

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def size: MultiMapSize = MultiMapSize(superNodeMap.size, superNodeMap.totalSize, commonMap.size)

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
