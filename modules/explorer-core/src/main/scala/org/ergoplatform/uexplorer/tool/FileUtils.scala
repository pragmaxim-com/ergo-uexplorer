package org.ergoplatform.uexplorer.tool

import com.typesafe.scalalogging.LazyLogging

import java.io.{File, FileWriter}
import java.nio.file.Path
import scala.util.{Success, Try}

object FileUtils extends LazyLogging {

  def writeReport(lines: IndexedSeq[String], targetPath: Path): Try[_] = {
    val targetFile = targetPath.toFile
    if (targetFile.exists()) {
      logger.info(s"Writing to a file ${targetFile.getAbsolutePath} that already exists")
      Success(())
    } else {
      Try {
        logger.info(s"Writing ${lines.size} lines to ${targetFile.getAbsolutePath}")
        val report     = lines.mkString("", "\n", "\n")
        val fileWriter = new FileWriter(targetFile)
        try fileWriter.write(report)
        finally fileWriter.close()
        report
      }
    }
  }

}
