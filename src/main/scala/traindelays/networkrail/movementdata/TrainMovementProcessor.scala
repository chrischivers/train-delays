package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.Pipe
import fs2.async.mutable.Queue
import traindelays.networkrail.db.MovementLogTable
import traindelays.networkrail.subscribers.SubscriberHandler

import scala.concurrent.ExecutionContext

trait TrainMovementProcessor {

  def stream: fs2.Stream[IO, Unit]
}

object TrainMovementProcessor {
  def apply(movementMessageQueue: Queue[IO, TrainMovementRecord],
            movementLogTable: MovementLogTable,
            subscriberFetcher: SubscriberHandler)(implicit executionContext: ExecutionContext) =
    new TrainMovementProcessor {

      private val recordsToLogPipe: Pipe[IO, TrainMovementRecord, Option[MovementLog]] =
        (in: fs2.Stream[IO, TrainMovementRecord]) => in.map(_.toMovementLog)

      override def stream: fs2.Stream[IO, Unit] =
        movementMessageQueue.dequeue
          .through(recordsToLogPipe)
          .collect[MovementLog] { case Some(ml) => ml }
//          .filter(x => x.trainId.value == "G74349")
//          .observe(subscriberFetcher.notifySubscribersSink)
          .observe1(x => IO(println(x)))
          .to(movementLogTable.dbWriter)

    }
}
