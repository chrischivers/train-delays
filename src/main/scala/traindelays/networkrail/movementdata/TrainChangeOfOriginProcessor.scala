package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.Pipe
import fs2.async.mutable.Queue
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db.ChangeOfOriginLogTable
import traindelays.networkrail.subscribers.SubscriberHandler

import scala.concurrent.ExecutionContext

object TrainChangeOfOriginProcessor {
  def apply(changeOfOriginMessageQueue: Queue[IO, TrainChangeOfOriginRecord],
            subscriberHandler: SubscriberHandler,
            changeOfOriginLogTable: ChangeOfOriginLogTable,
            activationCache: TrainActivationCache,
            metricsIncrementAction: => Unit)(implicit executionContext: ExecutionContext) =
    new MovementProcessor {

      private val recordsToLogPipe: Pipe[IO, TrainChangeOfOriginRecord, Option[ChangeOfOriginLog]] =
        (in: fs2.Stream[IO, TrainChangeOfOriginRecord]) =>
          in.flatMap(x => fs2.Stream.eval(x.toChangeOfOriginLog(activationCache)))

      override def stream: fs2.Stream[IO, Unit] =
        changeOfOriginMessageQueue.dequeue
          .observe1(_ => IO(metricsIncrementAction))
          .through(recordsToLogPipe)
          .collect[ChangeOfOriginLog] { case Some(cl) => cl }
          .observe(subscriberHandler.changeOfOriginNotifier)
          .to(changeOfOriginLogTable.dbWriter)

    }
}
