package org.ergoplatform.uexplorer.mvstore.multiset

import com.typesafe.scalalogging.LazyLogging
import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCollector.Counter
import org.h2.mvstore.db.NullValueDataType
import org.h2.mvstore.{MVMap, MVStore}
import org.h2.value.Value

import java.io.File
import java.nio.file.Path
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class SuperNodeMvSet[HK, C[_], V](
  id: String,
  superNodeCollector: SuperNodeCollector[HK]
)(implicit store: MVStore, codec: SuperNodeSetCodec[C, V], vc: ValueCodec[Counter])
  extends SuperNodeSetLike[HK, C, V]
  with LazyLogging {

  private lazy val existingMapsByHotKey: concurrent.Map[HK, MVMap[V, Value]] =
    new ConcurrentHashMap[HK, MVMap[V, Value]]().asScala.addAll(
      superNodeCollector
        .getExistingStringifiedHotKeys(store.getMapNames.asScala.toSet)
        .view
        .mapValues { name =>
          store.openMap(
            name,
            MVMap.Builder[V, Value].valueType(NullValueDataType.INSTANCE)
          )
        }
        .toMap
    )

  private lazy val counterByHotKey = new MvMap[HK, Counter](s"$id-counter")

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

  def getReport: Vector[(String, Counter)] =
    superNodeCollector
      .filterAndSortHotKeys(counterByHotKey.iterator(None, None, false))

  def putAllNewOrFail(hotKey: HK, values: IterableOnce[V], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .map { superNodeName =>
        val replacedValueOpt =
          existingMapsByHotKey.get(hotKey) match {
            case None =>
              val newSuperNodeMap: MVMap[V, Value] =
                store.openMap(
                  superNodeName,
                  MVMap.Builder[V, Value].valueType(NullValueDataType.INSTANCE)
                )
              existingMapsByHotKey.putIfAbsent(hotKey, newSuperNodeMap)
              codec.writeAll(newSuperNodeMap, values)
            case Some(m) =>
              codec.writeAll(m, values)
          }
        replacedValueOpt
          .map(v => Failure(new AssertionError(s"In $id, secondary-key $v was already present under hotkey $hotKey!")))
          .getOrElse(Success(()))
      }
      .orElse {
        collectInsertedHotKey(hotKey, size)
        None
      }

  def removeAllOrFail(hotKey: HK, values: IterableOnce[V], size: Int): Option[Try[Unit]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .flatMap { superNodeName =>
        existingMapsByHotKey.get(hotKey).map { mvMap =>
          values.iterator
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

object SuperNodeMvSet {
  def apply[HK: HotKeyCodec, C[_], V](id: String)(implicit
    store: MVStore,
    sc: SuperNodeSetCodec[C, V],
    vc: ValueCodec[Counter]
  ): SuperNodeMvSet[HK, C, V] =
    new SuperNodeMvSet[HK, C, V](id, new SuperNodeCollector[HK](id))
}
