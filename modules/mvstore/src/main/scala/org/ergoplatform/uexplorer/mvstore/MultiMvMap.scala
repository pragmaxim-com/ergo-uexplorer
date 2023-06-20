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

  def get(key: PK, secondaryKey: K): Option[V] =
    superNodeMap
      .get(key, secondaryKey)
      .orElse(commonMap.get(key).flatMap(v => c.readOne(secondaryKey, v)))

  def getAll(key: PK): Option[C[K, V]] =
    superNodeMap.getAll(key).orElse(commonMap.get(key))

  def remove(key: PK): Removed =
    superNodeMap.remove(key).isDefined || commonMap.removeAndForget(key)

  def removeOrFail(key: PK): Try[C[K, V]] =
    superNodeMap
      .remove(key)
      .fold(commonMap.removeOrFail(key))(Success(_))

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def size: MultiMapSize = MultiMapSize(superNodeMap.size, superNodeMap.totalSize, commonMap.size)

  def removeAllOrFail(k: PK, secondaryKeys: IterableOnce[K], size: Int)(f: C[K, V] => Option[C[K, V]]): Try[Unit] =
    superNodeMap.removeAllOrFail(k, secondaryKeys, size).fold(commonMap.removeOrUpdateOrFail(k)(f))(identity)

  def adjustAndForget(key: PK, entries: IterableOnce[(K, V)], size: Int): Try[_] =
    superNodeMap.putAllNewOrFail(key, entries, size).getOrElse {
      val (appended, _) = commonMap.adjustCollection(key)(c.append(entries))
      if (appended)
        Success(())
      else
        Failure(new AssertionError(s"All inserted values under key $key should be appended"))
    }

}
