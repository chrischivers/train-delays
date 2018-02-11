package traindelays.networkrail.cache

import akka.util.ByteString
import cats.Eval
import cats.effect.IO
import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}
import redis.{ByteStringDeserializer, ByteStringSerializer, RedisClient}
import traindelays.networkrail.movementdata.{TrainActivationRecord, TrainId}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import io.circe.generic.semiauto._
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser._
import traindelays.networkrail.{ServiceCode, StanoxCode}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait TrainActivationCache {

  def addToCache(trainActivationRecord: TrainActivationRecord): IO[Boolean]
  def getFromCache(trainId: TrainId): IO[Option[TrainActivationRecord]]
}

object TrainActivationCache {

  private val trainActivationRecordCacheDecoder: Decoder[TrainActivationRecord] = (c: HCursor) =>
    for {
      scheduleTrainId          <- c.downField("scheduleTrainId").as[String].map(ScheduleTrainId(_))
      trainServiceCode         <- c.downField("trainServiceCode").as[String].map(ServiceCode(_))
      trainId                  <- c.downField("trainId").as[String].map(TrainId(_))
      originStanox             <- c.downField("originStanox").as[String].map(StanoxCode(_))
      originDepartureTimestamp <- c.downField("originDepartureTimestamp").as[Long]
    } yield {
      new TrainActivationRecord(scheduleTrainId, trainServiceCode, trainId, originStanox, originDepartureTimestamp)
  }
  private val trainActivationRecordCacheEncoder: Encoder[TrainActivationRecord] = (a: TrainActivationRecord) =>
    Json.obj(
      ("scheduleTrainId", Json.fromString(a.scheduleTrainId.value)),
      ("trainServiceCode", Json.fromString(a.trainServiceCode.value)),
      ("trainId", Json.fromString(a.trainId.value)),
      ("originStanox", Json.fromString(a.originStanox.value)),
      ("originDepartureTimestamp", Json.fromLong(a.originDepartureTimestamp))
  )

  implicit val byteStringSerializer = new ByteStringSerializer[TrainActivationRecord] {
    override def serialize(data: TrainActivationRecord): ByteString =
      ByteString(data.asJson(trainActivationRecordCacheEncoder).noSpaces)
  }

  implicit val byteStringDeserializer = new ByteStringDeserializer[TrainActivationRecord] {
    override def deserialize(bs: ByteString): TrainActivationRecord =
      (for {
        json    <- parse(bs.utf8String)
        decoded <- trainActivationRecordCacheDecoder.decodeJson(json)
      } yield decoded).fold(err => throw err, identity)

  }

  def apply(redisClient: RedisClient, expiry: FiniteDuration)(implicit executionContext: ExecutionContext) =
    new TrainActivationCache {

      override def addToCache(trainActivationRecord: TrainActivationRecord): IO[Boolean] =
        IO.fromFuture(
          Eval.always(redisClient
            .set(trainActivationRecord.trainId.value, trainActivationRecord, pxMilliseconds = Some(expiry.toMillis))))

      override def getFromCache(trainId: TrainId): IO[Option[TrainActivationRecord]] =
        IO.fromFuture(Eval.always(redisClient.get[TrainActivationRecord](trainId.value)))

    }
}
