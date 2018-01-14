package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import traindelays.networkrail.cache.TrainActivationCache

trait TrainActivationProcessor {

  def stream: fs2.Stream[IO, Unit]
}

object TrainActivationProcessor {
  def apply(activationMessageQueue: Queue[IO, TrainActivationRecord], trainActivationCache: TrainActivationCache) =
    new TrainMovementProcessor {

      val cacheWriter: fs2.Sink[IO, TrainActivationRecord] = fs2.Sink { record =>
        trainActivationCache.addToCache(record.trainId, record.scheduleTrainId).map(_ => ())
      }

      override def stream: fs2.Stream[IO, Unit] =
        activationMessageQueue.dequeue
          .observe1(x => IO(println(x)))
          .to(cacheWriter)

    }
}
