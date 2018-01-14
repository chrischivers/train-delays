package traindelays.networkrail.cache

import cats.Eval
import cats.effect.IO
import redis.RedisClient
import traindelays.networkrail.movementdata.TrainId
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait TrainActivationCache {

  def addToCache(trainId: TrainId, scheduleTrainId: ScheduleTrainId): IO[Boolean]
  def getFromCache(trainId: TrainId): IO[Option[ScheduleTrainId]]
}

object TrainActivationCache {
  def apply(redisClient: RedisClient, expiry: FiniteDuration)(implicit executionContext: ExecutionContext) =
    new TrainActivationCache {

      override def addToCache(trainId: TrainId, scheduleTrainId: ScheduleTrainId): IO[Boolean] =
        IO.fromFuture(
          Eval.later(redisClient.set(trainId.value, scheduleTrainId.value, pxMilliseconds = Some(expiry.toMillis))))

      override def getFromCache(trainId: TrainId): IO[Option[ScheduleTrainId]] =
        IO.fromFuture(Eval.later(redisClient.get[String](trainId.value).map(_.map(ScheduleTrainId(_)))))

    }
}
