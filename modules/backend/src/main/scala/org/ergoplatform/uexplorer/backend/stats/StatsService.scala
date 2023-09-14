package org.ergoplatform.uexplorer.backend.stats

import org.ergoplatform.uexplorer.Const
import org.ergoplatform.uexplorer.storage.MvStorage
import zio.{Task, ZIO, ZLayer}

import scala.jdk.CollectionConverters.*
import scala.math.Ordering

case class StatsService(storage: MvStorage):

  def getTopAddressesByUtxoCount(limit: Int, maxLines: Int): Task[Seq[String]] = {
    val validLimit = Math.min(Math.max(limit, 1000), 10000000)
    ZIO.attempt(
      storage.utxosByErgoTreeHex.superNodeMap.keysWithSize
        .filter(_._2 >= validLimit)
        .toVector
        .sortBy(_._2)(Ordering[Int].reverse)
        .take(maxLines)
        .map { case (k, count) => s"$k $count" }
    )
  }

  def getTopAddressesByValue(limit: Int, maxLines: Int): Task[Seq[String]] = {
    val validLimit = Math.min(Math.max(limit, 1000), 10000000) * Const.CoinsInOneErgo
    ZIO.attempt(
      storage.utxosByErgoTreeHex.superNodeMap
        .iterator(_.values().asScala.sum)
        .filter(_._2 >= validLimit)
        .toVector
        .sortBy(_._2)(Ordering[Long].reverse)
        .take(maxLines)
        .map { case (k, sum) => s"$k $sum" }
    )
  }

object StatsService:
  def layer: ZLayer[MvStorage, Nothing, StatsService] =
    ZLayer.fromFunction(StatsService.apply _)

  def getTopAddressesByUtxoCount(limit: Int, maxLines: Int): ZIO[StatsService, Throwable, Seq[String]] =
    ZIO.serviceWithZIO[StatsService](_.getTopAddressesByUtxoCount(limit, maxLines))

  def getTopAddressesByValue(limit: Int, maxLines: Int): ZIO[StatsService, Throwable, Seq[String]] =
    ZIO.serviceWithZIO[StatsService](_.getTopAddressesByValue(limit, maxLines))
