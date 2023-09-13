package org.ergoplatform.uexplorer.mvstore.multiset

import org.ergoplatform.uexplorer.mvstore.*
import org.ergoplatform.uexplorer.mvstore.SuperNodeCounter.HotKey
import org.h2.mvstore.db.NullValueDataType
import org.h2.mvstore.{MVMap, MVStore}
import org.h2.value.Value
import zio.{Task, ZIO}

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

class SuperNodeMvSet[HK, C[A] <: java.util.Collection[A], V](
  id: String,
  superNodeCollector: SuperNodeCollector[HK],
  existingMapsByHotKey: concurrent.Map[HK, MVMap[V, Value]],
  counterByHotKey: MvMap[HK, SuperNodeCounter],
  hotKeyPath: Path
)(implicit store: MVStore, codec: SuperNodeSetCodec[C, V])
  extends SuperNodeSetLike[HK, C, V] {

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
      _ <- ZIO.log(s"$id contains ${existingMapsByHotKey.size} supernode sets")
      _ <- ZIO.when(emptyMaps.nonEmpty)(ZIO.log(s"Going to remove and close ${emptyMaps.size} empty $id supernode sets"))
      _ <- ZIO.when(closedMaps.nonEmpty)(ZIO.log(s"Going to remove ${closedMaps.size} closed $id supernode sets"))
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

  def get(hotKey: HK): Option[C[V]] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .flatMap { _ =>
        existingMapsByHotKey.get(hotKey)
      }
      .map(codec.readAll)

  def contains(hotKey: HK): Boolean =
    superNodeCollector
      .getHotKeyString(hotKey)
      .exists { _ =>
        existingMapsByHotKey.contains(hotKey)
      }

  def contains(hotKey: HK, v: V): Option[Boolean] =
    superNodeCollector
      .getHotKeyString(hotKey)
      .flatMap { _ =>
        existingMapsByHotKey.get(hotKey)
      }
      .map(codec.contains(v, _))

  def size: Int = existingMapsByHotKey.size

  def totalSize: Int = existingMapsByHotKey.iterator.map(_._2.size()).sum

  def mergeCommonMap(implicit vc: ValueCodec[C[V]]): Task[MapLike[HK, C[V]]] =
    MvMap[HK, C[V]](id).tap { commonMap =>
      ZIO
        .attempt {
          superNodeCollector.getStringifiedHotKeys.flatMap { case (hotKey, hotKeyString) =>
            commonMap.get(hotKey).map { values =>
              existingMapsByHotKey.get(hotKey) match {
                case None =>
                  val sMap: MVMap[V, Value] =
                    store.openMap(
                      hotKeyString,
                      MVMap.Builder[V, Value].valueType(NullValueDataType.INSTANCE)
                    )
                  existingMapsByHotKey.putIfAbsent(hotKey, sMap)
                  codec.writeAll(sMap, values.iterator().asScala)
                  commonMap.remove(hotKey)
                  hotKey
                case Some(sMap) =>
                  codec.writeAll(sMap, values.iterator().asScala)
                  commonMap.remove(hotKey)
                  hotKey
              }
            }
          }
        }
        .tap(keys => ZIO.when(keys.nonEmpty)(ZIO.log(s"Migrated ${keys.size} $id keys to ${keys.size} newly created super maps ...")))
    }
}

object SuperNodeMvSet {
  def apply[HK: HotKeyCodec, C[A] <: java.util.Collection[A], V](id: String, hotKeyDir: Path)(implicit
    store: MVStore,
    sc: SuperNodeSetCodec[C, V],
    vc: ValueCodec[SuperNodeCounter]
  ): Task[SuperNodeMvSet[HK, C, V]] = {
    val hotKeyPath = hotKeyDir.resolve(SuperNodeCounter.hotKeyFileName(id))
    for
      superNodeCollector <- SuperNodeCollector[HK](hotKeyPath)
      counterByHotKey    <- MvMap[HK, SuperNodeCounter](s"$id-counter")
    yield {
      val existingMapsByHotKey: concurrent.Map[HK, MVMap[V, Value]] =
        new ConcurrentHashMap[HK, MVMap[V, Value]]().asScala.addAll(
          superNodeCollector
            .getExistingStringifiedHotKeys(store.getMapNames.asScala.toSet)
            .view
            .mapValues(name => store.openMap(name, MVMap.Builder[V, Value].valueType(NullValueDataType.INSTANCE)))
            .toMap
        )

      new SuperNodeMvSet[HK, C, V](id, superNodeCollector, existingMapsByHotKey, counterByHotKey, hotKeyPath)
    }
  }
}
