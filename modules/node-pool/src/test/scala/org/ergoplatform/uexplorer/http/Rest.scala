package org.ergoplatform.uexplorer.http

import io.circe.parser.*
import org.ergoplatform.ErgoAddressEncoder
import org.ergoplatform.uexplorer.{BlockId, Height, HexString, ProtocolSettings}
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

  private def loadCacheFromFile(fileName: String) =
    Source
      .fromInputStream(
        new GZIPInputStream(
          new BufferedInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(fileName))
        )
      )
      .getLines()
      .filterNot(_.trim.isEmpty)
      .foldLeft(1 -> TreeMap.empty[Height, String]) { case ((height, cache), blockStr) =>
        height + 1 -> cache.updated(height, blockStr)
      }
      ._2

  object info {
    val minNodeHeight            = 4000
    val sync                     = "sync.json"
    val poll                     = "poll.json"
    val woRestApiAndFullHeight   = "wo-rest-api-and-full-height.json"
    val woRestApiAndWoFullHeight = "wo-rest-api-and-wo-full-height.json"
  }

  object blockIds {
    lazy val byHeight: SortedMap[Height, BlockId] =
      loadCacheFromFile("blocks/block_ids.gz").map { case (height, id) => height -> BlockId.castUnsafe(id) }
  }

  object blocks extends Codecs {
    lazy val byHeight: SortedMap[Height, String] = loadCacheFromFile("blocks/blocks.gz")
    lazy val byId: SortedMap[BlockId, String] = byHeight.map { case (height, block) =>
      blockIds.byHeight(height) -> block
    }(HexString.given_Ordering_HexString)

    def forOffset(offset: Int, limit: Int): Vector[BlockId] =
      blockIds.byHeight.iteratorFrom(offset).take(limit).map(_._2).toVector

    def getByHeight(height: Int): ApiFullBlock =
      parse(byHeight(height)).flatMap(_.as[ApiFullBlock]).toOption.get

    def getById(blockId: BlockId): Task[ApiFullBlock] =
      ZIO.attempt(parse(byId(blockId)).flatMap(_.as[ApiFullBlock]).toOption.get)

    def stream(heights: Iterable[Height]): ZStream[Any, Nothing, ApiFullBlock] =
      ZStream.fromIterable(heights).map(getByHeight)
  }

  object chain {
    def forHeights(heights: Iterable[Height])(implicit ps: ProtocolSettings): ZIO[Any, Throwable, Chunk[LinkedBlock]] =
      BlockProcessor
        .processingFlow(ChainLinker(blocks.getById, ChainTip.empty))
        .map(_.head)
        .apply(blocks.stream(heights))
        .run(ZSink.collectAll)
  }
}
