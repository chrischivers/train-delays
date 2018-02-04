package traindelays.networkrail.cache

import akka.util.ByteString
import cats.Eval
import cats.effect.IO
import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}
import traindelays.networkrail.movementdata.{TrainActivationRecord, TrainId}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait TrainActivationCache {

  def addToCache(trainActivationRecord: TrainActivationRecord): IO[Boolean]
  def getFromCache(trainId: TrainId): IO[Option[TrainActivationRecord]]
}

object TrainActivationCache {

  implicit val byteStringSerializer = new ByteStringSerializer[TrainActivationRecord] {
    override def serialize(data: TrainActivationRecord): ByteString = ByteString(data.asJson.noSpaces)
  }

  implicit val byteStringDeserializer = new ByteStringDeserializer[TrainActivationRecord] {
    override def deserialize(bs: ByteString): TrainActivationRecord =
      decode[TrainActivationRecord](bs.utf8String)
        .fold(err => throw err, identity)
  }

  def apply(redisClient: RedisClient, expiry: FiniteDuration)(implicit executionContext: ExecutionContext) =
    new TrainActivationCache {

      override def addToCache(trainActivationRecord: TrainActivationRecord): IO[Boolean] =
        IO.fromFuture(
          Eval.later(redisClient
            .set(trainActivationRecord.trainId.value, trainActivationRecord, pxMilliseconds = Some(expiry.toMillis))))

      override def getFromCache(trainId: TrainId): IO[Option[TrainActivationRecord]] =
        IO.fromFuture(Eval.later(redisClient.get[TrainActivationRecord](trainId.value)))

    }
}
