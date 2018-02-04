package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async.mutable.Queue
import traindelays.networkrail.cache.TrainActivationCache

object TrainActivationProcessor {
  def apply(activationMessageQueue: Queue[IO, TrainActivationRecord], trainActivationCache: TrainActivationCache) =
    new MovementProcessor {

      private val cacheWriter: fs2.Sink[IO, TrainActivationRecord] = fs2.Sink { record =>
        //TODO what do we do with service code. Is it redundant?
        trainActivationCache.addToCache(record).map(_ => ())
      }

      override def stream: fs2.Stream[IO, Unit] =
        activationMessageQueue.dequeue
//          .observe1(x => IO(println(x)))
          .to(cacheWriter)

    }
}
