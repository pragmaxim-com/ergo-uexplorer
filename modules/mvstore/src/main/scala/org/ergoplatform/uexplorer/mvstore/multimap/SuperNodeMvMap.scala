package org.ergoplatform.uexplorer.mvstore.multimap

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.mvstore.*
import org.h2.mvstore.{MVMap, MVStore}

import java.io.File
import java.nio.file.Path
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class SuperNodeMvMap[HK, C[_, _], K, V](
  id: String,
  superNodeCollector: SuperNodeCollector[HK]
)(implicit store: MVStore, codec: SuperNodeMapCodec[C, K, V], vc: ValueCodec[SuperNodeCounter])
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

  private lazy val counterByHotKey = new MvMap[HK, SuperNodeCounter](s"$id-counter")

  private def collectReadHotKey(k: HK): SuperNodeCounter =
    counterByHotKey.adjust(k)(_.fold(SuperNodeCounter(1, 1, 0, 0)) {
      case SuperNodeCounter(writeOps, readOps, added, removed) =>
        SuperNodeCounter(writeOps + 1, readOps + 1, added, removed)
    })

  private def collectInsertedHotKey(k: HK, size: Int): SuperNodeCounter =
    counterByHotKey.adjust(k)(_.fold(SuperNodeCounter(1, 0, size, 0)) {
      case SuperNodeCounter(writeOps, readOps, added, removed) =>
        SuperNodeCounter(writeOps + 1, readOps, added + size, removed)
    })

  private def collectRemovedHotKey(k: HK, size: Int): Option[SuperNodeCounter] =
    counterByHotKey.removeOrUpdate(k) { case SuperNodeCounter(writeOps, readOps, added, removed) =>
      Some(SuperNodeCounter(writeOps + 1, readOps, added, removed + size))
    }

  def clearEmptySuperNodes(): Try[Unit] = Try {
    val emptyMaps =
      existingMapsByHotKey
        .foldLeft(Set.newBuilder[HK]) {
          case (acc, (hotKey, map)) if map.isEmpty =>
            acc.addOne(hotKey)
          case (acc, _) =>
            acc
        }
        .result()
    logger.info(s"Going to remove ${emptyMaps.size} empty $id supernode maps")
    emptyMaps
      .foreach { hk =>
        existingMapsByHotKey
          .remove(hk)
          .foreach(store.removeMap)
      }
  }

  def getReport: Vector[(String, SuperNodeCounter)] =
    superNodeCollector
      .filterAndSortHotKeys(counterByHotKey.iterator(None, None, false))

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
          .map(e => Failure(new AssertionError(s"In $id, secondary-key ${e._1} was already present under hotkey $hotKey!")))
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
          logger.info(s"In $id, removing supernode map for $superNodeName")
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
              Failure(new AssertionError(s"In $id, removing non-existing secondary key $sk from superNode $superNodeName"))
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
  def apply[HK: HotKeyCodec, C[_, _], K, V](id: String)(implicit
    store: MVStore,
    sc: SuperNodeMapCodec[C, K, V],
    vc: ValueCodec[SuperNodeCounter]
  ): SuperNodeMvMap[HK, C, K, V] =
    new SuperNodeMvMap[HK, C, K, V](id, new SuperNodeCollector[HK](id))
}
