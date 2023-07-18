package org.ergoplatform.uexplorer.http

trait TestSupport {

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
