package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.Pipe
import fs2.async.mutable.Queue
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db.CancellationLogTable
import traindelays.networkrail.subscribers.SubscriberHandler

import scala.concurrent.ExecutionContext

object TrainCancellationProcessor {
  def apply(cancellationMessageQueue: Queue[IO, TrainCancellationRecord],
            subscriberHandler: SubscriberHandler,
            cancellationLogTable: CancellationLogTable,
            activationCache: TrainActivationCache,
            metricsIncrementAction: => Unit)(implicit executionContext: ExecutionContext) =
    new MovementProcessor {

      private val recordsToLogPipe: Pipe[IO, TrainCancellationRecord, Option[CancellationLog]] =
        (in: fs2.Stream[IO, TrainCancellationRecord]) =>
          in.flatMap(x => fs2.Stream.eval(x.toCancellationLog(activationCache)))

      override def stream: fs2.Stream[IO, Unit] =
        cancellationMessageQueue.dequeue
          .observe1(_ => IO(metricsIncrementAction))
          .through(recordsToLogPipe)
          .collect[CancellationLog] { case Some(cl) => cl }
          .observe(subscriberHandler.cancellationNotifier)
          .to(cancellationLogTable.dbWriter)

    }
}
