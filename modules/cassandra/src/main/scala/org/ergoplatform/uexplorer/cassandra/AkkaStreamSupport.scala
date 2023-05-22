package org.ergoplatform.uexplorer

import akka.NotUsed
import akka.stream.scaladsl.{Balance, Flow, GraphDSL, Merge, RestartSource, Source}
import akka.stream.*

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

  // maxParallelism corresponds to 'parallelism-factor = 0.5' from configuration
  def cpuHeavyBalanceFlow[In, Out](
    worker: Flow[In, Out, Any],
    maxParallelism: Int = Runtime.getRuntime.availableProcessors() / 2
  ): Flow[In, Out, NotUsed] =
    heavyBalanceFlow(
      worker,
      parallelism = Math.max(maxParallelism, Runtime.getRuntime.availableProcessors()),
      Attributes.asyncBoundary
        .and(Attributes.inputBuffer(8, 32))
        .and(ActorAttributes.IODispatcher)
    )

}
