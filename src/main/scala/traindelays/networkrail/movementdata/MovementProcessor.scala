package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import fs2.{Pipe, Sink}
import traindelays.networkrail.db.{MovementLogTable, SubscriberTable}
import traindelays.networkrail.subscribers.SubscriberHandler

import scala.concurrent.ExecutionContext

trait MovementProcessor {

  def stream: fs2.Stream[IO, Unit]
}

object MovementProcessor {
  def apply(movementMessageQueue: Queue[IO, MovementRecord],
            movementLogTable: MovementLogTable,
            subscriberFetcher: SubscriberHandler)(implicit executionContext: ExecutionContext) =
    new MovementProcessor {

      private val recordsToLogPipe: Pipe[IO, MovementRecord, Option[MovementLog]] =
        (in: fs2.Stream[IO, MovementRecord]) => in.map(_.toMovementLog)

      override def stream: fs2.Stream[IO, Unit] =
        movementMessageQueue.dequeue
          .through(recordsToLogPipe)
          .collect[MovementLog] { case Some(ml) => ml }
//          .filter(x => x.toc == "88")
          .observe1(x => IO(println("RECEIVED: " + x)))
          .observe(subscriberFetcher.notifySubscribersSink)
          .to(movementLogTable.dbWriter)

    }
}
