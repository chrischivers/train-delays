package traindelays.networkrail.cache

import akka.util.ByteString
import cats.Eval
import cats.effect.IO
import io.circe.{Decoder, Encoder}
import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}
import traindelays.networkrail.movementdata.{TrainActivationRecord, TrainId}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import io.circe.generic.semiauto._
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

  private val trainActivationRecordSimpleDecoder: Decoder[TrainActivationRecord] = deriveDecoder[TrainActivationRecord]
  private val trainActivationRecordSimpleEncoder: Encoder[TrainActivationRecord] = deriveEncoder[TrainActivationRecord]

  implicit val byteStringSerializer = new ByteStringSerializer[TrainActivationRecord] {
    override def serialize(data: TrainActivationRecord): ByteString =
      ByteString(data.asJson(trainActivationRecordSimpleEncoder).noSpaces)
  }

  implicit val byteStringDeserializer = new ByteStringDeserializer[TrainActivationRecord] {
    override def deserialize(bs: ByteString): TrainActivationRecord =
      (for {
        json    <- parse(bs.utf8String)
        decoded <- trainActivationRecordSimpleDecoder.decodeJson(json)
      } yield decoded).fold(err => throw err, identity)

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
