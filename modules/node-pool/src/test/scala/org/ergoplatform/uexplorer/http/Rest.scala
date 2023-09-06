package org.ergoplatform.uexplorer.http

import io.circe.parser.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.{BlockId, CoreConf, Height, HexString}
import org.ergoplatform.uexplorer.chain.{BlockProcessor, ChainLinker, ChainTip}
import org.ergoplatform.uexplorer.db.LinkedBlock
import org.ergoplatform.uexplorer.node.ApiFullBlock
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Task, ZIO}

import java.io.BufferedInputStream
import java.util.zip.GZIPInputStream
import scala.collection.immutable.{SortedMap, TreeMap}
import scala.io.Source

object Rest {

  object info {
    val minNodeHeight            = 4000
    val sync                     = "sync.json"
    val poll                     = "poll.json"
    val woRestApiAndFullHeight   = "wo-rest-api-and-full-height.json"
    val woRestApiAndWoFullHeight = "wo-rest-api-and-wo-full-height.json"
  }

  case class Blocks(blockIdsFile: String, blocksFile: String) extends Codecs {
    lazy val idsByHeight: SortedMap[Height, BlockId] =
      Blocks.loadCacheFromFile(blockIdsFile).map { case (height, id) => height -> BlockId.castUnsafe(id) }

    lazy val byHeight: SortedMap[Height, String] = Blocks.loadCacheFromFile(blocksFile)

    lazy val byId: SortedMap[BlockId, String] = byHeight.map { case (height, block) =>
      idsByHeight(height) -> block
    }(HexString.given_Ordering_HexString)

    def forHeights(heights: Iterable[Height])(implicit ps: CoreConf): ZIO[Any, Throwable, Chunk[LinkedBlock]] =
      BlockProcessor
        .processingFlow(ChainLinker(getById, ChainTip.empty))
        .map(_.head)
        .apply(stream(heights))
        .run(ZSink.collectAll)

    def forOffset(offset: Int, limit: Int): Vector[BlockId] =
      idsByHeight.iteratorFrom(offset).take(limit).map(_._2).toVector

    def getByHeight(height: Int): ApiFullBlock =
      parse(byHeight(height)).flatMap(_.as[ApiFullBlock]).toOption.get

    def getById(blockId: BlockId): Task[ApiFullBlock] =
      ZIO.attempt(parse(byId(blockId)).flatMap(_.as[ApiFullBlock]).toOption.get)

    def stream(heights: Iterable[Height]): ZStream[Any, Nothing, ApiFullBlock] =
      ZStream.fromIterable(heights).map(getByHeight)
  }

  object Blocks {

    def loadCacheFromFile(fileName: String): SortedMap[Height, String] = {
      val source =
        if (fileName.endsWith(".gz"))
          Source
            .fromInputStream(
              new GZIPInputStream(
                new BufferedInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName))
              )
            )
        else
          Source.fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName))

      source
        .getLines()
        .filterNot(_.trim.isEmpty)
        .foldLeft(1 -> TreeMap.empty[Height, String]) { case ((height, cache), blockStr) =>
          height + 1 -> cache.updated(height, blockStr)
        }
        ._2
    }

    def regular: Blocks     = Blocks("blocks/block_ids.gz", "blocks/blocks.gz")
    def forkShorter: Blocks = Blocks("forks/forkIds_shorter.txt", "forks/forks_shorter.txt")
    def forkLoner: Blocks   = Blocks("forks/forkIds_longer.txt", "forks/forks_longer.txt")
  }

}
