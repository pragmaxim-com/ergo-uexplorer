package org.ergoplatform.uexplorer.tool

import zio.{Task, ZIO}

import java.io.{File, FileWriter}
import java.nio.file.Path
import scala.util.{Success, Try}

object FileUtils {

  def writeReport(lines: IndexedSeq[String], targetPath: Path): Task[Unit] = {
    val targetFile = targetPath.toFile
    if (targetFile.exists()) {
      ZIO.log(s"Writing to a file ${targetFile.getAbsolutePath} that already exists")
    } else {
      ZIO.log(s"Writing ${lines.size} lines to ${targetFile.getAbsolutePath}") *> ZIO.attempt {
        val report     = lines.mkString("", "\n", "\n")
        val fileWriter = new FileWriter(targetFile)
        try fileWriter.write(report)
        finally fileWriter.close()
        report
      }.unit
    }
  }

}
