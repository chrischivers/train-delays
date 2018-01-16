package traindelays.networkrail

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.Decoder.Result
import io.circe.{Decoder, DecodingFailure, HCursor}
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.scheduledata.ScheduleTrainId

package object movementdata {

  sealed trait EventType {
    val string: String
  }
  case object Departure extends EventType {
    override val string: String = "DEPARTURE"
  }
  case object Arrival extends EventType {
    override val string: String = "ARRIVAL"
  }
  object EventType {

    def fromString(str: String): EventType =
      str match {
        case Departure.string => Departure
        case Arrival.string   => Arrival
      }
    implicit val decoder: Decoder[EventType] = Decoder.decodeString.map(fromString)

    implicit val meta: Meta[EventType] =
      Meta[String].xmap(EventType.fromString, _.string)
  }

  sealed trait VariationStatus {
    val string: String
  }
  case object OnTime extends VariationStatus {
    override val string: String = "ON TIME"
  }
  case object Early extends VariationStatus {
    override val string: String = "EARLY"
  }
  case object Late extends VariationStatus {
    override val string: String = "LATE"
  }

  case object OffRoute extends VariationStatus {
    override val string: String = "OFF ROUTE"
  }
  object VariationStatus {

    def fromString(str: String): VariationStatus =
      str match {
        case OnTime.string   => OnTime
        case Early.string    => Early
        case Late.string     => Late
        case OffRoute.string => OffRoute
      }
    implicit val decoder: Decoder[VariationStatus] = Decoder.decodeString.map(fromString)

    implicit val meta: Meta[VariationStatus] =
      Meta[String].xmap(VariationStatus.fromString, _.string)
  }

  case class TrainId(value: String)
  object TrainId {
    implicit val decoder: Decoder[TrainId] = Decoder.decodeString.map(TrainId(_))
    implicit val meta: Meta[TrainId] =
      Meta[String].xmap(TrainId(_), _.value)
  }

  sealed trait TrainMovements

  object TrainMovements {

    implicit val trainMovement: Decoder[TrainMovements] = (c: HCursor) =>
      for {
        messageType <- c.downField("header").downField("msg_type").as[String]
        decoded <- messageType match {
          case "0001"  => c.as[TrainActivationRecord]
          case "0003"  => c.as[TrainMovementRecord]
          case unknown => Right(UnhandledTrainRecord(unknown))
        }
      } yield decoded
  }

  case class UnhandledTrainRecord(unhandledType: String) extends TrainMovements

  case class TrainActivationRecord(scheduleTrainId: ScheduleTrainId, trainServiceCode: ServiceCode, trainId: TrainId)
      extends TrainMovements

  object TrainActivationRecord {
    implicit val trainActivationDecoder: Decoder[TrainActivationRecord] {
      def apply(c: HCursor): Result[TrainActivationRecord]
    } = new Decoder[TrainActivationRecord] {

      override def apply(c: HCursor): Result[TrainActivationRecord] = {
        val bodyObject = c.downField("body")
        for {
          trainId          <- bodyObject.downField("train_id").as[TrainId]
          trainServiceCode <- bodyObject.downField("train_service_code").as[ServiceCode]
          scheduleTrainId  <- bodyObject.downField("train_uid").as[ScheduleTrainId]

        } yield {
          TrainActivationRecord(scheduleTrainId, trainServiceCode, trainId)
        }
      }
    }
  }

  case class TrainMovementRecord(trainId: TrainId,
                                 trainServiceCode: ServiceCode,
                                 eventType: EventType,
                                 toc: TOC,
                                 actualTimestamp: Long,
                                 plannedTimestamp: Long,
                                 plannedPassengerTimestamp: Option[Long],
                                 stanox: Option[Stanox],
                                 variationStatus: Option[VariationStatus])
      extends TrainMovements {

    def asMovementLog(trainActivationCache: TrainActivationCache): IO[Option[MovementLog]] =
      TrainMovementRecord.movementRecordToMovementLog(this, trainActivationCache)
  }

  object TrainMovementRecord {

    implicit val movementRecordDecoder: Decoder[TrainMovementRecord] {
      def apply(c: HCursor): Result[TrainMovementRecord]
    } = new Decoder[TrainMovementRecord] {

      override def apply(c: HCursor): Result[TrainMovementRecord] = {
        val bodyObject = c.downField("body")
        for {
          trainId          <- bodyObject.downField("train_id").as[TrainId]
          trainServiceCode <- bodyObject.downField("train_service_code").as[ServiceCode]
          eventType        <- bodyObject.downField("event_type").as[EventType]
          toc              <- bodyObject.downField("toc_id").as[TOC]
          actualTimestamp <- bodyObject
            .downField("actual_timestamp")
            .as[String]
            .map(_.toLong)
          plannedTimestamp <- bodyObject
            .downField("planned_timestamp")
            .as[String]
            .map(_.toLong)
          plannedPassengerTimestamp <- bodyObject
            .downField("gbtt_timestamp")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(_.toLong))
          stanox <- bodyObject
            .downField("loc_stanox")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(Stanox(_)))
          variationStatus <- bodyObject.downField("variation_status").as[Option[VariationStatus]]

        } yield {
          TrainMovementRecord(trainId,
                              trainServiceCode,
                              eventType,
                              toc,
                              actualTimestamp,
                              plannedTimestamp,
                              plannedPassengerTimestamp,
                              stanox,
                              variationStatus)
        }
      }
    }

    private def emptyStringToNone[A](in: String)(f: String => A): Option[A] =
      if (in == "") None else Some(f(in))

    private def emptyStringOptionToNone[A](in: Option[String])(f: String => A): Option[A] =
      if (in.contains("")) None else in.map(f)

    private def movementRecordToMovementLog(movementRec: TrainMovementRecord,
                                            cache: TrainActivationCache): IO[Option[MovementLog]] =
      cache.getFromCache(movementRec.trainId).map { scheduleTrainIdOpt =>
        for {
          stanox                    <- movementRec.stanox
          plannedPassengerTimestamp <- movementRec.plannedPassengerTimestamp
          variationStatus           <- movementRec.variationStatus
          scheduleTrainId           <- scheduleTrainIdOpt
        } yield {
          MovementLog(
            None,
            movementRec.trainId,
            scheduleTrainId,
            movementRec.trainServiceCode,
            movementRec.eventType,
            movementRec.toc,
            stanox,
            plannedPassengerTimestamp,
            movementRec.actualTimestamp,
            movementRec.actualTimestamp - plannedPassengerTimestamp,
            variationStatus
          )
        }
      }
  }

  case class MovementLog(id: Option[Int],
                         trainId: TrainId,
                         scheduleTrainId: ScheduleTrainId,
                         serviceCode: ServiceCode,
                         eventType: EventType,
                         toc: TOC,
                         stanox: Stanox,
                         plannedPassengerTimestamp: Long,
                         actualTimestamp: Long,
                         difference: Long,
                         variationStatus: VariationStatus)
}
