package org.ergoplatform.uexplorer.mvstore

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.h2.mvstore.{MVMap, MVStore}

import java.io.File
import java.nio.file.Path
import java.util.Map.Entry
import scala.jdk.CollectionConverters.*
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.util.{Failure, Success, Try}

class SuperNodeMvMap[SK, SV[_, _], K, V](
  store: MVStore,
  superNodeCollector: SuperNodeCollector[SK]
)(implicit codec: SuperNodeCodec[SV, K, V], vc: ValueCodec[Counter])
  extends SuperNodeMapLike[SK, SV, K, V]
  with LazyLogging {

  private lazy val existingSupernodeMapsByKey: concurrent.Map[SK, MVMap[K, V]] =
    new ConcurrentHashMap[SK, MVMap[K, V]]().asScala.addAll(
      superNodeCollector
        .getExistingSuperNodeKeysWithName(store.getMapNames.asScala.toSet)
        .view
        .mapValues(store.openMap[K, V])
        .toMap
    )

  private lazy val counterBySuperNode = new MvMap[SK, Counter]("counterBySuperNode", store)

  private def getSuperNodeKey(k: SK): Counter =
    counterBySuperNode.adjust(k)(_.fold(Counter(1, 1, 0, 0)) { case Counter(writeOps, readOps, added, removed) =>
      Counter(writeOps + 1, readOps + 1, added, removed)
    })

  private def insertSuperNodeKey(k: SK, size: Int): Counter =
    counterBySuperNode.adjust(k)(_.fold(Counter(1, 0, size, 0)) { case Counter(writeOps, readOps, added, removed) =>
      Counter(writeOps + 1, readOps, added + size, removed)
    })

  private def removeSuperNodeKey(k: SK, size: Int): Option[Counter] =
    counterBySuperNode.removeOrUpdate(k) { case Counter(writeOps, readOps, added, removed) =>
      Some(Counter(writeOps + 1, readOps, added, removed + size))
    }

  def getFinalReport: Option[String] =
    superNodeCollector
      .report(counterBySuperNode.iterator(None, None, false))

  def get(sk: SK, secondaryKey: K): Option[V] =
    existingSupernodeMapsByKey
      .get(sk)
      .flatMap(m => Option(m.get(secondaryKey)))
      .orElse {
        getSuperNodeKey(sk)
        None
      }

  def getAll(sk: SK): Option[SV[K, V]] =
    existingSupernodeMapsByKey
      .get(sk)
      .map(codec.readAll)
      .orElse {
        getSuperNodeKey(sk)
        None
      }

  def putOnlyNew(sk: SK, k: K, v: V): Option[Appended] =
    superNodeCollector
      .getSuperNodeNameByKey(sk)
      .map { superNodeName =>
        existingSupernodeMapsByKey.get(sk) match {
          case None =>
            logger.info(s"Creating new supernode map for $superNodeName")
            codec.write(store.openMap[K, V](superNodeName), k, v)
          case Some(m) =>
            codec.write(m, k, v)
        }
      }
      .orElse {
        insertSuperNodeKey(sk, 1)
        None
      }

  def putAllNewOrFail(sk: SK, entries: IterableOnce[(K, V)], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getSuperNodeNameByKey(sk)
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
      .orElse {
        insertSuperNodeKey(sk, size)
        None
      }

  def remove(sk: SK): Option[SV[K, V]] =
    superNodeCollector
      .getSuperNodeNameByKey(sk)
      .flatMap { superNodeName =>
        existingSupernodeMapsByKey.remove(sk).map { mvMapToRemove =>
          logger.info(s"Removing supernode map for $superNodeName")
          val result = codec.readAll(mvMapToRemove)
          store.removeMap(superNodeName)
          result
        }
      }
      .orElse {
        removeSuperNodeKey(sk, 1)
        None
      }

  def removeAllOrFail(sk: SK, keys: IterableOnce[K], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getSuperNodeNameByKey(sk)
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
      .orElse {
        removeSuperNodeKey(sk, size)
        None
      }

  def isEmpty: Boolean = existingSupernodeMapsByKey.forall(_._2.isEmpty)

  def size: Int = existingSupernodeMapsByKey.iterator.map(_._2.size()).sum

}

object SuperNodeMvMap {
  def apply[SK: KeyCodec, SV[_, _], K, V](store: MVStore, superNodeFile: File)(implicit
    sc: SuperNodeCodec[SV, K, V],
    vc: ValueCodec[Counter]
  ): SuperNodeMapLike[SK, SV, K, V] =
    new SuperNodeMvMap[SK, SV, K, V](store, new SuperNodeCollector[SK](superNodeFile))
}
