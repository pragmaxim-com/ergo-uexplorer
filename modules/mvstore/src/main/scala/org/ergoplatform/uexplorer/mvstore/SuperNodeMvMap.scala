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
  registeredSuperNodeNamesByKey: Map[SK, String],
  store: MVStore
)(implicit codec: SuperNodeCodec[SV, K, V])
  extends SuperNodeMapLike[SK, SV, K, V]
  with LazyLogging {

  private val existingSupernodeMapsByKey: concurrent.Map[SK, MVMap[K, V]] = {
    val existingMapNames = store.getMapNames.asScala.toSet
    val nonEmptySuperNodeMapsByKey =
      registeredSuperNodeNamesByKey.collect {
        case (sk, name) if existingMapNames.contains(name) =>
          sk -> store.openMap[K, V](name)
      }
    logger.info(
      s"Opened ${nonEmptySuperNodeMapsByKey.size} SuperNodeMaps from ${registeredSuperNodeNamesByKey.size} registered"
    )
    nonEmptySuperNodeMapsByKey.foreach { case (sk, map) =>
      if (map.isEmpty) {
        logger.warn(s"Supernode map $sk is not empty however we add new on")
      }
    }
    new ConcurrentHashMap[SK, MVMap[K, V]]().asScala.addAll(
      nonEmptySuperNodeMapsByKey
    )
  }

  def get(sk: SK, secondaryKey: K): Option[V] =
    existingSupernodeMapsByKey.get(sk).flatMap(m => Option(m.get(secondaryKey)))

  def getAll(sk: SK): Option[SV[K, V]] =
    existingSupernodeMapsByKey.get(sk).map(codec.readAll)

  def putOnlyNew(sk: SK, k: K, v: V): Option[Boolean] =
    registeredSuperNodeNamesByKey
      .get(sk)
      .map { superNodeName =>
        existingSupernodeMapsByKey.get(sk) match {
          case None =>
            logger.info(s"Creating new supernode map for $superNodeName")
            codec.write(store.openMap[K, V](superNodeName), k, v)
          case Some(m) =>
            codec.write(m, k, v)
        }
      }

  def putAllNewOrFail(sk: SK, entries: IterableOnce[(K, V)]): Option[Try[Unit]] =
    registeredSuperNodeNamesByKey
      .get(sk)
      .map { superNodeName =>
        val replacedValueOpt =
          existingSupernodeMapsByKey.get(sk) match {
            case None =>
              logger.info(s"Creating new supernode map for $superNodeName")
              val newSuperNodeMap = store.openMap[K, V](superNodeName)
              existingSupernodeMapsByKey.putIfAbsent(sk, newSuperNodeMap)
              codec.writeAll(newSuperNodeMap, entries)
            case Some(m) =>
              codec.writeAll(m, entries)
          }
        replacedValueOpt
          .map(e => Failure(new AssertionError(s"Key ${e._1} was already present in supernode $sk!")))
          .getOrElse(Success(()))
      }

  def remove(sk: SK): Option[SV[K, V]] =
    registeredSuperNodeNamesByKey
      .get(sk)
      .flatMap { superNodeName =>
        existingSupernodeMapsByKey.remove(sk).map { mvMapToRemove =>
          logger.info(s"Removing supernode map for $superNodeName")
          val result = codec.readAll(mvMapToRemove)
          store.removeMap(superNodeName)
          result
        }
      }

  def removeAllOrFail(sk: SK, keys: IterableOnce[K]): Option[Try[Unit]] =
    registeredSuperNodeNamesByKey
      .get(sk)
      .flatMap { superNodeName =>
        existingSupernodeMapsByKey.get(sk).map { mvMap =>
          keys.iterator
            .find(k => mvMap.remove(k) == null)
            .fold(Success(())) { key =>
              Failure(new AssertionError(s"Removing non-existing key $key from superNode $sk"))
            } // we don't remove supernode map when it gets empty as common map as  on/off/on/off is expensive
        /*
            .flatMap { _ =>
              if (mvMap.isEmpty) {
                logger.info(s"Removing supernode map for $superNodeName as it was emptied")
                existingSupernodeMapsByKey.remove(sk).fold(Try(store.removeMap(superNodeName))) { m =>
                  Try(store.removeMap(m))
                }
              } else
                Success(())
            }
         */
        }
      }

  def isEmpty: Boolean = existingSupernodeMapsByKey.forall(_._2.isEmpty)

  def size: Int = existingSupernodeMapsByKey.iterator.map(_._2.size()).sum

}
