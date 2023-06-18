package org.ergoplatform.uexplorer.mvstore

import org.ergoplatform.uexplorer.Address
import org.h2.mvstore.MVMap.DecisionMaker
import org.h2.mvstore.{MVMap, MVStore}

import scala.collection.concurrent
import java.util.Map.Entry
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}
import org.ergoplatform.uexplorer.mvstore.kryo.KryoSerialization.Implicits.*

//TODO make SuperNodePCol
class MultiMvMap[PK, C[_, _], K, V](
  commonMap: MapLike[PK, C[K, V]],
  superNodeMap: SuperNodeMapLike[PK, C, K, V]
) extends MultiMapLike[PK, C, K, V] {

  def get(key: PK, secondaryKey: K)(implicit c: MultiMapCodec[C, K, V]): Option[V] =
    superNodeMap
      .get(key, secondaryKey)
      .orElse(commonMap.get(key).flatMap(v => c.read(secondaryKey, v)))

  def getAll(key: PK): Option[C[K, V]] =
    superNodeMap.getAll(key).orElse(commonMap.get(key))

  def remove(key: PK): Boolean =
    superNodeMap.remove(key).isDefined || commonMap.removeAndForget(key)

  def removeOrFail(key: PK): Try[C[K, V]] =
    superNodeMap
      .remove(key)
      .fold(commonMap.removeOrFail(key))(Success(_))

  def isEmpty: Boolean = superNodeMap.isEmpty && commonMap.isEmpty

  def size: Int = superNodeMap.size + commonMap.size

  def removeAllOrFail(k: PK, secondaryKeys: IterableOnce[K])(f: C[K, V] => Option[C[K, V]]): Try[Unit] =
    superNodeMap.removeAllOrFail(k, secondaryKeys).fold(commonMap.removeOrUpdateOrFail(k)(f))(identity)

  def adjustAndForget(key: PK, entries: IterableOnce[(K, V)])(f: Option[C[K, V]] => C[K, V]): Try[_] =
    superNodeMap.putAllNewOrFail(key, entries).getOrElse(Try(commonMap.adjustAndForget(key)(f)))

}
