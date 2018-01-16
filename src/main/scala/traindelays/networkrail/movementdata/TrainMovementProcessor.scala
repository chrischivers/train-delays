package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.Pipe
import fs2.async.mutable.Queue
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db.MovementLogTable
import traindelays.networkrail.subscribers.SubscriberHandler

import scala.concurrent.ExecutionContext

object TrainMovementProcessor {
  def apply(movementMessageQueue: Queue[IO, TrainMovementRecord],
            movementLogTable: MovementLogTable,
            subscriberHandler: SubscriberHandler,
            trainActivationCache: TrainActivationCache)(implicit executionContext: ExecutionContext) =
    new MovementProcessor {

      private val recordsToLogPipe: Pipe[IO, TrainMovementRecord, Option[MovementLog]] =
        (in: fs2.Stream[IO, TrainMovementRecord]) =>
          in.flatMap(x => fs2.Stream.eval(x.asMovementLog(trainActivationCache)))

      override def stream: fs2.Stream[IO, Unit] =
        movementMessageQueue.dequeue
          .through(recordsToLogPipe)
          .collect[MovementLog] { case Some(ml) => ml }
          .observe(subscriberHandler.movementNotifier)
          .observe1(x => IO(println(x)))
          .to(movementLogTable.dbWriter)

    }
}
