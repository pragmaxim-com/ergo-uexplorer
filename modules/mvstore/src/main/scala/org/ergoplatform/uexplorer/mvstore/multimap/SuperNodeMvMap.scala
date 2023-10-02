package org.ergoplatform.uexplorer.mvstore.multimap

import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.HotKey
import org.h2.mvstore.{MVMap, MVStore}
import org.scalameta.logger
import zio.{Task, ZIO}

import java.io.File
import java.nio.file.Path
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import scala.collection.concurrent
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class SuperNodeMvMap[HK, C[A, B] <: java.util.Map[A, B], K, V](
  id: String,
  superNodeCollector: SuperNodeCollector[HK],
  existingMapsByHotKey: concurrent.Map[HK, MVMap[K, V]],
  counterByHotKey: MvMap[HK, SuperNodeCounter],
  hotKeyPath: Path
)(implicit store: MVStore, codec: SuperNodeMapCodec[C, K, V])
  extends SuperNodeMapLike[HK, C, K, V] {

  private def collectReadHotKey(k: HK): SuperNodeCounter =
    counterByHotKey.adjust(k)(_.fold(SuperNodeCounter(1, 1, 0, 0)) { case SuperNodeCounter(writeOps, readOps, added, removed) =>
      SuperNodeCounter(writeOps + 1, readOps + 1, added, removed)
    })

  private def collectInsertedHotKey(k: HK, size: Int): SuperNodeCounter =
    counterByHotKey.adjust(k)(_.fold(SuperNodeCounter(1, 0, size, 0)) { case SuperNodeCounter(writeOps, readOps, added, removed) =>
      SuperNodeCounter(writeOps + 1, readOps, added + size, removed)
    })

  private def collectRemovedHotKey(k: HK, size: Int): Option[SuperNodeCounter] =
    counterByHotKey.removeOrUpdate(k) { case SuperNodeCounter(writeOps, readOps, added, removed) =>
      Some(SuperNodeCounter(writeOps + 1, readOps, added, removed + size))
    }

  def keysWithSize: Iterator[(HK, CacheSize)] = 
    existingMapsByHotKey.iterator.map { case (k, map) => k -> map.size() }

  def iterator[E](fn: MVMap[K, V] => E): Iterator[(HK, E)] = 
    existingMapsByHotKey.iterator.map { case (k, map) => k -> fn(map) }

  def clearEmptyOrClosedSuperNodes(): Task[Unit] = {
    val emptyMaps =
      existingMapsByHotKey
        .foldLeft(Set.newBuilder[HK]) {
          case (acc, (hotKey, map)) if map.isEmpty =>
            acc.addOne(hotKey)
          case (acc, _) =>
            acc
        }
        .result()
    val closedMaps =
      existingMapsByHotKey
        .foldLeft(Set.newBuilder[HK]) {
          case (acc, (hotKey, map)) if map.isClosed =>
            acc.addOne(hotKey)
          case (acc, _) =>
            acc
        }
        .result()
    for
      _ <- ZIO.log(s"$id contains ${existingMapsByHotKey.size} supernode maps")
      _ <- ZIO.when(emptyMaps.nonEmpty)(ZIO.log(s"Going to remove and close ${emptyMaps.size} empty $id supernode maps"))
      _ <- ZIO.when(closedMaps.nonEmpty)(ZIO.log(s"Going to remove ${closedMaps.size} closed $id supernode maps"))
      _ <- ZIO.attempt(closedMaps.foreach(existingMapsByHotKey.remove))
    /*      _ <- ZIO.attempt(
             emptyMaps
               .foreach { hk =>
                 existingMapsByHotKey
                   .remove(hk)
                   .foreach(store.removeMap)
               }
           )
     */
    yield ()
  }

  def getReport: (Path, Vector[HotKey]) =
    hotKeyPath -> superNodeCollector
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
      .flatMap { hotKeyString =>
        existingMapsByHotKey.get(hotKey).map { mvMap =>
          secondaryKeys.iterator
            .find(k => mvMap.remove(k) == null)
            .fold(Success(())) { sk =>
              Failure(new AssertionError(s"In $id, removing non-existing secondary key $sk from superNode $hotKeyString"))
            } // we don't remove supernode map when it gets empty as common map as  on/off/on/off is expensive
        /*
            .flatMap { _ =>
              if (mvMap.isEmpty) {
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

  def count: Int = existingMapsByHotKey.iterator.count(m => !m._2.isEmpty)

  def totalSize: Int = existingMapsByHotKey.iterator.map(_._2.size()).sum

  def mergeCommonMap(implicit vc: ValueCodec[C[K, V]]): Task[MapLike[HK, C[K, V]]] =
    MvMap[HK, C[K, V]](id).tap { commonMap =>
      ZIO.log(s"Checking if ${existingMapsByHotKey.size} hotkeys needs merging at $id") *> ZIO
        .attempt {
          superNodeCollector.getStringifiedHotKeys.flatMap { case (hotKey, hotKeyString) =>
            commonMap.get(hotKey).map { values =>
              existingMapsByHotKey.get(hotKey) match {
                case None =>
                  val sMap = store.openMap[K, V](hotKeyString)
                  existingMapsByHotKey.putIfAbsent(hotKey, sMap)
                  codec.writeAll(sMap, values.entrySet().asScala.map(e => e.getKey -> e.getValue))
                  commonMap.remove(hotKey)
                  hotKey
                case Some(sMap) =>
                  codec.writeAll(sMap, values.entrySet().asScala.map(e => e.getKey -> e.getValue))
                  commonMap.remove(hotKey)
                  hotKey
              }
            }
          }
        }
        .tap(keys => ZIO.when(keys.nonEmpty)(ZIO.log(s"Migrated ${keys.size} $id keys to ${keys.size} newly created super maps ...")))
    }
}

object SuperNodeMvMap {
  def apply[HK: HotKeyCodec, C[A, B] <: java.util.Map[A, B], K, V](id: String, hotKeyDir: Path)(implicit
    store: MVStore,
    sc: SuperNodeMapCodec[C, K, V],
    vc: ValueCodec[SuperNodeCounter]
  ): Task[SuperNodeMvMap[HK, C, K, V]] = {
    val hotKeyPath = hotKeyDir.resolve(SuperNodeCounter.hotKeyFileName(id))
    for
      superNodeCollector <- SuperNodeCollector[HK](hotKeyPath)
      counterByHotKey    <- MvMap[HK, SuperNodeCounter](s"$id-counter")
    yield {
      val existingMapsByHotKey: concurrent.Map[HK, MVMap[K, V]] =
        new ConcurrentHashMap[HK, MVMap[K, V]]().asScala.addAll(
          superNodeCollector
            .getExistingStringifiedHotKeys(store.getMapNames.asScala.toSet)
            .view
            .mapValues(store.openMap[K, V])
            .toMap
        )
      new SuperNodeMvMap[HK, C, K, V](id, superNodeCollector, existingMapsByHotKey, counterByHotKey, hotKeyPath)
    }
  }
}
