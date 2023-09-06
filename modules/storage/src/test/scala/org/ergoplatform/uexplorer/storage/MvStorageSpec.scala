package org.ergoplatform.uexplorer.storage

import org.ergoplatform.uexplorer.mvstore.multimap.MultiMvMap
import org.ergoplatform.uexplorer.mvstore.multiset.MultiMvSet
import org.ergoplatform.uexplorer.mvstore.{tempDir, HotKeyCodec}
import org.ergoplatform.uexplorer.{randomNumberPerJvmRun, BoxId}
import org.ergoplatform.uexplorer.storage.Implicits.*
import org.h2.mvstore.MVStore
import zio.test.{assertTrue, ZIOSpecDefault}
import zio.{Task, ZIO}

import java.io.*
import java.nio.file.Path
import java.util
import java.util.zip.*

object MvStorageSpec extends ZIOSpecDefault:

  def writeHotKeys(testDir: Path): Task[Unit] = ZIO.attempt {
    testDir.toFile.getParentFile.mkdirs()
    val hotKeyPath = testDir.toFile
    val fos        = new FileOutputStream(hotKeyPath)
    val gzos       = new GZIPOutputStream(fos)
    val w          = new PrintWriter(gzos)
    for (line <- (1 to 10).map(_.toString))
      w.write(line + "\n")
    w.close()
    gzos.close()
    fos.close()
  }

  implicit val hotBoxCodec: HotKeyCodec[String] = new HotKeyCodec[String] {
    def serialize(key: String): String   = key
    def deserialize(key: String): String = key
  }

  def spec =
    suite("mvStorage")(
      test("merge common map with super map") {
        for {
          testDir       <- ZIO.attempt(tempDir.resolve(s"ergo-$randomNumberPerJvmRun-test-maps"))
          mvStoreConf   <- MvStoreConf.configIO
          given MVStore <- MvStorage.buildMvStore(testDir.resolve(s"mv-store-$randomNumberPerJvmRun.db").toFile, mvStoreConf)
          mm            <- MultiMvMap[BoxId, util.Map, BoxId, Long]("test-maps", testDir)
          _ <- ZIO.attempt((1 to 10).map(x => BoxId(x.toString)).foreach(i => mm.adjustAndForget(i, (1 to 1000).map(i => BoxId(i.toString) -> i.toLong), 1000)))
          _ <- ZIO.attempt(implicitly[MVStore].commit())
          _ <- writeHotKeys(testDir.resolve("hot-keys-test-maps.csv.gz"))
          mm2 <- MultiMvMap[BoxId, util.Map, BoxId, Long]("test-maps", testDir)
        } yield assertTrue(mm2.size.commonSize == 0, mm2.size.superNodeSum == 10000)
      },
      test("merge common map with super set") {
        for {
          testDir       <- ZIO.attempt(tempDir.resolve(s"ergo-$randomNumberPerJvmRun-test-sets"))
          mvStoreConf   <- MvStoreConf.configIO
          given MVStore <- MvStorage.buildMvStore(testDir.resolve(s"mv-store-$randomNumberPerJvmRun.db").toFile, mvStoreConf)
          mm            <- MultiMvSet[BoxId, util.Set, BoxId]("test-sets", testDir)
          _             <- ZIO.attempt((1 to 10).map(x => BoxId(x.toString)).foreach(i => mm.adjustAndForget(i, (1 to 1000).map(i => BoxId(i.toString)), 1000)))
          _             <- ZIO.attempt(implicitly[MVStore].commit())
          _             <- writeHotKeys(testDir.resolve("hot-keys-test-sets.csv.gz"))
          mm2           <- MultiMvSet[BoxId, util.Set, BoxId]("test-sets", testDir)
        } yield assertTrue(mm2.size.commonSize == 0, mm2.size.superNodeSum == 10000)
      }
    )
