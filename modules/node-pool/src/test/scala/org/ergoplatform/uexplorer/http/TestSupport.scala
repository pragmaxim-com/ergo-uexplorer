package org.ergoplatform.uexplorer.http

import eu.timepit.refined.*
import eu.timepit.refined.string.HexStringSpec
import org.ergoplatform.uexplorer.{BlockId, HexString}
import org.scalacheck.Gen
import scorex.crypto.hash.Blake2b256
import scorex.util.Random
import scorex.util.encode.Base16

trait TestSupport {

  val MainNetMinerPk: HexString = HexString
    .fromStringUnsafe(
      "0377d854c54490abc6c565d8e548d5fc92a6a6c2f4415ed96f0c340ece92e1ed2f"
    )

  def hexStringGen: Gen[String] =
    Gen
      .nonEmptyListOf(Gen.alphaNumChar)
      .map(_ => Base16.encode(Blake2b256.hash(Random.randomBytes().mkString)))

  def hexStringRGen: Gen[HexString] =
    hexStringGen
      .map(HexString.fromStringUnsafe)

  def idGen: Gen[BlockId] =
    hexStringGen.map(x => BlockId.fromStringUnsafe(x))

  def getPeerInfo(fileName: String): String =
    scala.io.Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream(s"info/$fileName"))
      .mkString

  def getConnectedPeers: String =
    scala.io.Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("peers/connected.json"))
      .mkString

  def getUnconfirmedTxs: String =
    scala.io.Source
      .fromInputStream(Thread.currentThread().getContextClassLoader.getResourceAsStream("transactions/unconfirmed.json"))
      .mkString

}
