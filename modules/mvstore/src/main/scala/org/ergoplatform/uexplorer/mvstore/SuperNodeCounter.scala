package org.ergoplatform.uexplorer.mvstore

import zio.{Task, ZIO}
import zio.stream.{ZPipeline, ZSink, ZStream}

import java.nio.file.Path

case class SuperNodeCounter(writeOps: Long, readOps: Long, boxesAdded: Int, boxesRemoved: Int) {
  import SuperNodeCounter.hotLimit
  def this() = this(0, 0, 0, 0)

  def isHot: Boolean =
    writeOps > hotLimit || readOps > hotLimit || boxesAdded > hotLimit || boxesRemoved > hotLimit
}

object SuperNodeCounter {
  def hotKeyFileName(id: String) = s"hot-keys-$id.csv.gz"

  private val hotLimit = 500

  sealed trait HotKey {
    def key: String
  }

  case class NewHotKey(key: String, counter: SuperNodeCounter) extends HotKey

  case class ExistingHotKey(key: String) extends HotKey

  def writeReport(lines: IndexedSeq[String], targetPath: Path): Task[Unit] =
    ZIO.attempt(targetPath.toFile.delete()) *>
      ZStream
        .fromIterable(lines)
        .mapConcat(line => (line + java.lang.System.lineSeparator()).getBytes)
        .via(ZPipeline.gzip())
        .run(ZSink.fromPath(targetPath))
        .unit

}
