package org.ergoplatform.uexplorer

import java.util.ArrayDeque

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object ExeContext {

  object Implicits {
    implicit def trampoline: ExecutionContextExecutor = ExeContext.trampoline
  }
  object trampoline extends ExecutionContextExecutor {
    private val local = new ThreadLocal[AnyRef]

    private object Empty

    def execute(runnable: Runnable): Unit =
      local.get match {
        case null =>
          try {
            local.set(Empty)
            runnable.run()
            executeScheduled()
          } finally local.set(null)
        case Empty =>
          local.set(runnable)
        case next: Runnable =>
          val runnables = new ArrayDeque[Runnable](4)
          runnables.addLast(next)
          runnables.addLast(runnable)
          local.set(runnables)
        case arrayDeque: ArrayDeque[_] =>
          val runnables = arrayDeque.asInstanceOf[ArrayDeque[Runnable]]
          runnables.addLast(runnable)
        case illegal =>
          throw new IllegalStateException(s"Unsupported trampoline ThreadLocal value: $illegal")
      }

    @tailrec
    private def executeScheduled(): Unit =
      local.get match {
        case Empty =>
          ()
        case next: Runnable =>
          local.set(Empty)
          next.run()
          executeScheduled()
        case arrayDeque: ArrayDeque[_] =>
          val runnables = arrayDeque.asInstanceOf[ArrayDeque[Runnable]]
          while (!runnables.isEmpty) {
            val runnable = runnables.removeFirst()
            runnable.run()
          }
        case illegal =>
          throw new IllegalStateException(s"Unsupported trampoline ThreadLocal value: $illegal")
      }

    def reportFailure(t: Throwable): Unit = t.printStackTrace()
  }

}
