package org.ergoplatform.uexplorer.indexer

import eu.timepit.refined._
import eu.timepit.refined.string.HexStringSpec
import org.ergoplatform.explorer._
import org.scalacheck.Gen
import scorex.crypto.hash.Blake2b256
import scorex.util.Random
import scorex.util.encode.Base16

import scala.io
import scala.util.Try

trait TestSupport {

  val MainNetMinerPk: HexString = HexString
    .fromString[Try](
      "0377d854c54490abc6c565d8e548d5fc92a6a6c2f4415ed96f0c340ece92e1ed2f"
    )
    .get

  def hexStringGen: Gen[String] =
    Gen
      .nonEmptyListOf(Gen.alphaNumChar)
      .map(_ => Base16.encode(Blake2b256.hash(Random.randomBytes().mkString)))

  def hexStringRGen: Gen[HexString] =
    hexStringGen
      .map(x => refineV[HexStringSpec](x).right.get)
      .map(HexString.apply)

  def idGen: Gen[BlockId] =
    hexStringGen.map(x => BlockId.fromString[Try](x).get)

  def getPeerInfo(fileName: String): String =
    io.Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(s"info/$fileName"))
      .mkString

  def getConnectedPeers: String =
    io.Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("peers/connected.json"))
      .mkString

}
