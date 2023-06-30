package org.ergoplatform.uexplorer.cassandra

import akka.NotUsed
import akka.stream.scaladsl.*
import akka.stream.*
import akka.stream.ActorAttributes.Dispatcher

import scala.concurrent.Future
import scala.concurrent.duration.*

trait AkkaStreamSupport {

  def heavyBalanceFlow[In, Out](
    worker: Flow[In, Out, Any],
    parallelism: Int,
    workerAttributes: Attributes
  ): Flow[In, Out, NotUsed] = {
    import akka.stream.scaladsl.GraphDSL.Implicits.*

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer = b.add(Balance[In](parallelism, waitForAllDownstreams = true))
      val merge    = b.add(Merge[Out](parallelism))

      for (_ <- 1 to parallelism)
        balancer ~> worker.withAttributes(workerAttributes) ~> merge

      FlowShape(balancer.in, merge.out)
    })
  }

  def broadcastTo3Workers[In, Out](
    w1: Flow[In, Out, _],
    w2: Flow[In, Out, _],
    w3: Flow[In, Out, _]
  ): Flow[In, Out, _] = {
    import akka.stream.scaladsl.GraphDSL.Implicits.*

    Flow.fromGraph(
      GraphDSL
        .create() { implicit b =>
          val zipWith =
            ZipWith[Out, Out, Out, Out] { (in1, in2, in3) =>
              assert(Set(in1, in2, in3).size == 1)
              in1
            }
          val broadcast = b.add(Broadcast[In](3, eagerCancel = true))
          val zip       = b.add(zipWith)
          broadcast.out(0) ~> w1 ~> zip.in0
          broadcast.out(1) ~> w2 ~> zip.in1
          broadcast.out(2) ~> w3 ~> zip.in2

          FlowShape[In, Out](broadcast.in, zip.out)
        }
        .withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
    )
  }

  def broadcastTo4Workers[In, Out](
    w1: Flow[In, Out, _],
    w2: Flow[In, Out, _],
    w3: Flow[In, Out, _],
    w4: Flow[In, Out, _]
  ): Flow[In, Out, _] = {
    import akka.stream.scaladsl.GraphDSL.Implicits.*

    Flow.fromGraph(
      GraphDSL
        .create() { implicit b =>
          val zipWith =
            ZipWith[Out, Out, Out, Out, Out] { (in1, in2, in3, in4) =>
              assert(Set(in1, in2, in3, in4).size == 1)
              in1
            }
          val broadcast = b.add(Broadcast[In](4, eagerCancel = true))
          val zip       = b.add(zipWith)
          broadcast.out(0) ~> w1 ~> zip.in0
          broadcast.out(1) ~> w2 ~> zip.in1
          broadcast.out(2) ~> w3 ~> zip.in2
          broadcast.out(3) ~> w4 ~> zip.in3

          FlowShape[In, Out](broadcast.in, zip.out)
        }
        .withAttributes(Attributes.inputBuffer(initial = 1, max = 1))
    )
  }

  // maxParallelism corresponds to 'parallelism-factor = 0.5' from configuration
  def cpuHeavyBalanceFlow[In, Out](
    worker: Flow[In, Out, Any],
    maxParallelism: Int    = Runtime.getRuntime.availableProcessors() / 2,
    dispatcher: Dispatcher = ActorAttributes.IODispatcher
  ): Flow[In, Out, NotUsed] =
    heavyBalanceFlow(
      worker,
      parallelism = Math.max(maxParallelism, Runtime.getRuntime.availableProcessors()),
      Attributes.asyncBoundary
        .and(Attributes.inputBuffer(1, 32))
        .and(dispatcher)
    )

}
