package org.ergoplatform.uexplorer.mvstore

import com.typesafe.scalalogging.LazyLogging
import org.h2.mvstore.{MVMap, MVStore}

import java.util.Map.Entry
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.util.{Failure, Success, Try}

class SuperNodeMvMap[SK, SV[_, _], K, V](
  superNodeNameByKey: Map[SK, String],
  store: MVStore
)(implicit codec: SuperNodeCodec[SV, K, V])
  extends SuperNodeMapLike[SK, SV, K, V]
  with LazyLogging {

  private val supernodeByKey: concurrent.Map[SK, MVMap[K, V]] = new ConcurrentHashMap().asScala

  def get(sk: SK, secondaryKey: K): Option[V] =
    supernodeByKey.get(sk).flatMap(m => Option(m.get(secondaryKey)))

  def getAll(sk: SK): Option[SV[K, V]] =
    supernodeByKey.get(sk).map(codec.readAll)

  def putOnlyNew(sk: SK, k: K, v: V): Option[Boolean] =
    superNodeNameByKey
      .get(sk)
      .map { superNodeName =>
        supernodeByKey.get(sk) match {
          case None =>
            logger.info(s"Creating new supernode map $superNodeName")
            codec.write(store.openMap[K, V](superNodeName), k, v)
          case Some(m) =>
            codec.write(m, k, v)
        }
      }

  def putAllNewOrFail(sk: SK, entries: IterableOnce[(K, V)]): Option[Try[Unit]] =
    superNodeNameByKey
      .get(sk)
      .map { superNodeName =>
        val replacedValueOpt =
          supernodeByKey.get(sk) match {
            case None =>
              logger.info(s"Creating new supernode map $superNodeName")
              val newSuperNodeMap = store.openMap[K, V](superNodeName)
              supernodeByKey.putIfAbsent(sk, newSuperNodeMap)
              codec.writeAll(newSuperNodeMap, entries)
            case Some(m) =>
              codec.writeAll(m, entries)
          }
        replacedValueOpt
          .map(e => Failure(new AssertionError(s"Key ${e._1} was already present!")))
          .getOrElse(Success(()))
      }

  def remove(sk: SK): Option[SV[K, V]] =
    superNodeNameByKey
      .get(sk)
      .flatMap { superNodeName =>
        supernodeByKey.remove(sk).map { mvMapToRemove =>
          logger.info(s"Removing supernode map $superNodeName")
          val result = codec.readAll(mvMapToRemove)
          store.removeMap(superNodeName)
          result
        }
      }

  def removeAllOrFail(sk: SK, values: IterableOnce[K]): Option[Try[Unit]] =
    superNodeNameByKey
      .get(sk)
      .flatMap { _ =>
        supernodeByKey.get(sk).map { mvMap =>
          values.iterator.find(v => mvMap.remove(v) == null).fold(Success(())) { key =>
            Failure(new AssertionError(s"Removing non-existing key $key"))
          }
        }
      }

  def isEmpty: Boolean = supernodeByKey.forall(_._2.isEmpty)

  def size: Int = supernodeByKey.iterator.map(_._2.size()).sum

}
