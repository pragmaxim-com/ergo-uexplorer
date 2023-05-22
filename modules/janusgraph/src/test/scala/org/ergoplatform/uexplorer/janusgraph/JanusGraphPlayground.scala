package org.ergoplatform.uexplorer.indexer

import com.google.common.collect.{BoundType, MinMaxPriorityQueue, TreeMultiset}
import org.apache.commons.codec.digest.MurmurHash2
import org.apache.commons.configuration2.BaseConfiguration
import org.apache.tinkerpop.gremlin.process.traversal.{AnonymousTraversalSource, Traversal}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__ as _g
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.ergoplatform.uexplorer.Address
import org.janusgraph.core.Multiplicity
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util
import java.util.Comparator

class JanusGraphPlayground extends AsyncFreeSpec with Matchers {
  import akka.actor.testkit.typed.scaladsl.ActorTestKit
  import akka.actor.typed.ActorSystem
  import org.apache.tinkerpop.gremlin.driver.Cluster
  import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0
  import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper
  import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
  import org.ergoplatform.uexplorer.{BoxId, Const}
  import org.janusgraph.core.JanusGraphFactory
  import org.apache.tinkerpop.gremlin.structure.{T, Vertex}
  import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry
  import org.janusgraph.graphdb.types.vertices.PropertyKeyVertex
  import scala.jdk.CollectionConverters.*
  import java.util.UUID
  import scala.util.Random

  "blabla" in {
    /*
    implicit val graph = JanusGraphFactory.build
      .set("storage.backend", "cql")
      .set("storage.hostname", "localhost")
      .set("storage.port", "9044")
      .set("storage.cql.keyspace", "janusgraph")
      .set("graph.set-vertex-id", true)
      .set("gremlin.graph", "org.janusgraph.core.JanusGraphFactory")
      .open.asInstanceOf[StandardJanusGraph]

    try {
        val vid = Utils.vertexHash("9fzRcctiWfzoJyqGtPWqoXPuxSmFw6zpnjtsQ1B6jSN514XqH4q", graph)
        val result =
          graph
            .traversal()
            .V(vid)
            .repeat(_g.in("to").out("from").simplePath())
            .times(2)
            .fold()
            .next()
            .asScala
            .map {
              case v if v.label() == "address" =>
                v.property("address")
              case v if v.label() == "txId" =>
                throw new IllegalStateException(s"Unexpected ${v.property("txId")}")
              case v =>
                throw new IllegalStateException(s"Unexpected vertex $v")
            }
            .toSet.size

        println(s"$vid $result")

    scala.io.Source.fromFile(new File("/home/lisak/.ergo-uexplorer/snapshots/894/utxosByAddress"))
      .getLines()
      .map(line => line.takeWhile(_ != ' ').trim)
      .foreach { address =>
      }
    } finally graph.close
     */
    0 shouldBe 0
  }

}

/*
val addresses = (1 to 10).map(_ => UUID.randomUUID.toString.repeat(20)).toArray
val nodeCount = 1 * 1000


val mgmt = graph.openManagement()
val address = mgmt.getOrCreatePropertyKey("address")
mgmt.buildIndex("byAddress", classOf[PropertyKeyVertex]).addKey(address).buildCompositeIndex()
mgmt.makeEdgeLabel("spentBy").multiplicity(Multiplicity.SIMPLE).make()
mgmt.commit()

(1 to nodeCount).foldLeft(0 -> Vector.empty[Vertex]) {
  case ((counter, boxesToSpend), _) if boxesToSpend.isEmpty =>
    val newBox1 = graph.addVertex((counter + 1).toString)
    newBox1.property("address", addresses(Random.nextInt(addresses.length)))
    val newBox2 = graph.addVertex((counter + 2).toString)
    newBox2.property("address", addresses(Random.nextInt(addresses.length)))
    val newBox3 = graph.addVertex((counter + 3).toString)
    newBox3.property("address", addresses(Random.nextInt(addresses.length)))

    val root = graph.addVertex("root")
    root.addEdge("spentBy", newBox1)
    root.addEdge("spentBy", newBox2)
    root.addEdge("spentBy", newBox3)

    (counter + 3, Vector(newBox1, newBox2, newBox3))
  case ((counter, boxesToSpend), _) =>
    val newBox1 = graph.addVertex((counter + 1).toString)
    newBox1.property("address", addresses(Random.nextInt(addresses.length)))
    val newBox2 = graph.addVertex((counter + 2).toString)
    newBox2.property("address", addresses(Random.nextInt(addresses.length)))
    val newBox3 = graph.addVertex((counter + 3).toString)
    newBox3.property("address", addresses(Random.nextInt(addresses.length)))

    val boxToSpend1 = boxesToSpend.head
    val boxToSpend2 = boxesToSpend(1)
    val boxToSpend3 = boxesToSpend(2)
    boxToSpend1.addEdge("spentBy", newBox1)
    boxToSpend1.addEdge("spentBy", newBox2)
    boxToSpend1.addEdge("spentBy", newBox3)
    boxToSpend2.addEdge("spentBy", newBox1)
    boxToSpend2.addEdge("spentBy", newBox2)
    boxToSpend2.addEdge("spentBy", newBox3)
    boxToSpend3.addEdge("spentBy", newBox1)
    boxToSpend3.addEdge("spentBy", newBox2)
    boxToSpend3.addEdge("spentBy", newBox3)

    (counter + 3, Vector(newBox1, newBox2, newBox3))
}
graph.tx.commit()
println("all done " + graph.vertices().asScala.toVector.size)
 */
