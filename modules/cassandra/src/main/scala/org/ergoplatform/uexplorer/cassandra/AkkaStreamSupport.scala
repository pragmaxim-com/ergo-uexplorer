package org.ergoplatform.uexplorer.cassandra

import akka.NotUsed
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RestartSource, Source}
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

  def heavyBroadcastFlow[In, Out](
    workers: Seq[Flow[In, Out, Any]],
    workerAttributes: Attributes
  ): Flow[In, Out, NotUsed] = {
    import akka.stream.scaladsl.GraphDSL.Implicits.*

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val broadcast = b.add(Broadcast[In](workers.size, eagerCancel = true))
      val merge     = b.add(Merge[Out](workers.size))

      workers.foreach { worker =>
        broadcast ~> worker.withAttributes(workerAttributes) ~> merge
      }

      FlowShape(broadcast.in, merge.out)
    })
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

  // maxParallelism corresponds to 'parallelism-factor = 0.5' from configuration
  def cpuHeavyBroadcastFlow[In, Out](
    workers: Seq[Flow[In, Out, Any]],
    dispatcher: Dispatcher = ActorAttributes.IODispatcher
  ): Flow[In, Out, NotUsed] =
    heavyBroadcastFlow(
      workers,
      Attributes.asyncBoundary
        .and(Attributes.inputBuffer(1, 32))
        .and(dispatcher)
    )

}
