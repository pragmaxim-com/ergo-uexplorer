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

class SuperNodeMvMap[HK, C[_, _], K, V](
  store: MVStore,
  superNodeCollector: SuperNodeCollector[HK]
)(implicit codec: SuperNodeCodec[C, K, V], vc: ValueCodec[Counter])
  extends SuperNodeMapLike[HK, C, K, V]
  with LazyLogging {

  private lazy val existingMapsByHotKey: concurrent.Map[HK, MVMap[K, V]] =
    new ConcurrentHashMap[HK, MVMap[K, V]]().asScala.addAll(
      superNodeCollector
        .getExistingStringifiedHotKeys(store.getMapNames.asScala.toSet)
        .view
        .mapValues(store.openMap[K, V])
        .toMap
    )

  private lazy val counterByHotKey = new MvMap[HK, Counter]("counterByHotKey", store)

  private def collectReadHotKey(k: HK): Counter =
    counterByHotKey.adjust(k)(_.fold(Counter(1, 1, 0, 0)) { case Counter(writeOps, readOps, added, removed) =>
      Counter(writeOps + 1, readOps + 1, added, removed)
    })

  private def collectInsertedHotKey(k: HK, size: Int): Counter =
    counterByHotKey.adjust(k)(_.fold(Counter(1, 0, size, 0)) { case Counter(writeOps, readOps, added, removed) =>
      Counter(writeOps + 1, readOps, added + size, removed)
    })

  private def collectRemovedHotKey(k: HK, size: Int): Option[Counter] =
    counterByHotKey.removeOrUpdate(k) { case Counter(writeOps, readOps, added, removed) =>
      Some(Counter(writeOps + 1, readOps, added, removed + size))
    }

  def clear(): Try[Unit] = Try {
    val emptyMaps =
      existingMapsByHotKey
        .foldLeft(Set.newBuilder[HK]) {
          case (acc, (hotKey, map)) if map.isEmpty =>
            acc.addOne(hotKey)
          case (acc, _) =>
            acc
        }
        .result()
    logger.info(s"Going to remove ${emptyMaps.size} empty supernode maps")
    emptyMaps
      .foreach { hk =>
        existingMapsByHotKey
          .remove(hk)
          .foreach(store.removeMap)
      }
  }

  def writeReport: Try[_] =
    superNodeCollector
      .writeReport(counterByHotKey.iterator(None, None, false))

  def get(hotKey: HK, sk: K): Option[V] =
    existingMapsByHotKey
      .get(hotKey)
      .flatMap(m => Option(m.get(sk)))
      .orElse {
        collectReadHotKey(hotKey)
        None
      }

  def getPartially(hotKey: HK, sk: IterableOnce[K]): Option[C[K, V]] =
    existingMapsByHotKey
      .get(hotKey)
      .map(m => codec.readPartially(m, sk))
      .orElse {
        collectReadHotKey(hotKey)
        None
      }

  def getAll(hotKey: HK): Option[C[K, V]] =
    existingMapsByHotKey
      .get(hotKey)
      .map(codec.readAll)
      .orElse {
        collectReadHotKey(hotKey)
        None
      }

  def putOnlyNew(hotKey: HK, sk: K, v: V): Option[Appended] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .map { superNodeName =>
        existingMapsByHotKey.get(hotKey) match {
          case None =>
            codec.write(store.openMap[K, V](superNodeName), sk, v)
          case Some(m) =>
            codec.write(m, sk, v)
        }
      }
      .orElse {
        collectInsertedHotKey(hotKey, 1)
        None
      }

  def putAllNewOrFail(hotKey: HK, entries: IterableOnce[(K, V)], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .map { superNodeName =>
        val replacedValueOpt =
          existingMapsByHotKey.get(hotKey) match {
            case None =>
              val newSuperNodeMap = store.openMap[K, V](superNodeName)
              existingMapsByHotKey.putIfAbsent(hotKey, newSuperNodeMap)
              codec.writeAll(newSuperNodeMap, entries)
            case Some(m) =>
              codec.writeAll(m, entries)
          }
        replacedValueOpt
          .map(e => Failure(new AssertionError(s"Key ${e._1} was already present in supernode $hotKey!")))
          .getOrElse(Success(()))
      }
      .orElse {
        collectInsertedHotKey(hotKey, size)
        None
      }

  def remove(hotKey: HK): Option[C[K, V]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .flatMap { superNodeName =>
        existingMapsByHotKey.remove(hotKey).map { mvMapToRemove =>
          logger.info(s"Removing supernode map for $superNodeName")
          val result = codec.readAll(mvMapToRemove)
          store.removeMap(superNodeName)
          result
        }
      }
      .orElse {
        collectRemovedHotKey(hotKey, 1)
        None
      }

  def removeAllOrFail(hotKey: HK, secondaryKeys: IterableOnce[K], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .flatMap { superNodeName =>
        existingMapsByHotKey.get(hotKey).map { mvMap =>
          secondaryKeys.iterator
            .find(k => mvMap.remove(k) == null)
            .fold(Success(())) { sk =>
              Failure(new AssertionError(s"Removing non-existing secondary key $sk from superNode $superNodeName"))
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
        collectRemovedHotKey(hotKey, size)
        None
      }

  def isEmpty: Boolean = existingMapsByHotKey.forall(_._2.isEmpty)

  def size: Int = existingMapsByHotKey.size

  def totalSize: Int = existingMapsByHotKey.iterator.map(_._2.size()).sum

}

object SuperNodeMvMap {
  def apply[HK: HotKeyCodec, C[_, _], K, V](store: MVStore, superNodeFile: File)(implicit
    sc: SuperNodeCodec[C, K, V],
    vc: ValueCodec[Counter]
  ): SuperNodeMvMap[HK, C, K, V] =
    new SuperNodeMvMap[HK, C, K, V](store, new SuperNodeCollector[HK](superNodeFile))
}
