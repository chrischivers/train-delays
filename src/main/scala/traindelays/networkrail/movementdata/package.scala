package traindelays.networkrail

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}
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

  sealed trait CancellationType {
    val string: String
  }

  case object OnCall extends CancellationType {
    override val string: String = "ON CALL"
  }
  case object AtOrigin extends CancellationType {
    override val string: String = "AT ORIGIN"
  }
  case object EnRoute extends CancellationType {
    override val string: String = "EN ROUTE"
  }
  case object OutOfPlan extends CancellationType {
    override val string: String = "OUT OF PLAN"
  }

  object CancellationType {

    def fromString(str: String): CancellationType =
      str match {
        case OnCall.string    => OnCall
        case AtOrigin.string  => AtOrigin
        case EnRoute.string   => EnRoute
        case OutOfPlan.string => OutOfPlan
      }
    implicit val decoder: Decoder[CancellationType] = Decoder.decodeString.map(fromString)

    implicit val meta: Meta[CancellationType] =
      Meta[String].xmap(CancellationType.fromString, _.string)
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
          case "0002"  => c.as[TrainCancellationRecord]
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

  case class TrainCancellationRecord(trainId: TrainId,
                                     trainServiceCode: ServiceCode,
                                     toc: TOC,
                                     stanoxCode: StanoxCode,
                                     cancellationType: CancellationType,
                                     cancellationReasonCode: String)
      extends TrainMovements {
    def toCancellationLog(trainActivationCache: TrainActivationCache): IO[Option[CancellationLog]] =
      TrainCancellationRecord.cancellationRecordToCancellationLog(this, trainActivationCache)
  }

  object TrainCancellationRecord {
    implicit val trainCancellationDecoder: Decoder[TrainCancellationRecord] {
      def apply(c: HCursor): Result[TrainCancellationRecord]
    } = new Decoder[TrainCancellationRecord] {

      override def apply(c: HCursor): Result[TrainCancellationRecord] = {
        val bodyObject = c.downField("body")
        for {
          trainId                <- bodyObject.downField("train_id").as[TrainId]
          trainServiceCode       <- bodyObject.downField("train_service_code").as[ServiceCode]
          toc                    <- bodyObject.downField("toc_id").as[TOC]
          stanoxCode             <- bodyObject.downField("loc_stanox").as[StanoxCode]
          cancellationType       <- bodyObject.downField("canx_type").as[CancellationType]
          cancellationReasonCode <- bodyObject.downField("canx_reason_code").as[String]

        } yield {
          TrainCancellationRecord(trainId, trainServiceCode, toc, stanoxCode, cancellationType, cancellationReasonCode)
        }
      }
    }
    def cancellationRecordToCancellationLog(cancellationRec: TrainCancellationRecord, cache: TrainActivationCache) =
      cache.getFromCache(cancellationRec.trainId).map { scheduleTrainIdOpt =>
        for {
          scheduleTrainId <- scheduleTrainIdOpt
        } yield {
          CancellationLog(
            None,
            cancellationRec.trainId,
            scheduleTrainId,
            cancellationRec.trainServiceCode,
            cancellationRec.toc,
            cancellationRec.stanoxCode,
            cancellationRec.cancellationType,
            cancellationRec.cancellationReasonCode
          )
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
                                 stanoxCode: Option[StanoxCode],
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
          stanoxCode <- bodyObject
            .downField("loc_stanox")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(StanoxCode(_)))
          variationStatus <- bodyObject.downField("variation_status").as[Option[VariationStatus]]

        } yield {
          TrainMovementRecord(trainId,
                              trainServiceCode,
                              eventType,
                              toc,
                              actualTimestamp,
                              plannedTimestamp,
                              plannedPassengerTimestamp,
                              stanoxCode,
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
          stanoxCode                <- movementRec.stanoxCode
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
            stanoxCode,
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
                         stanoxCode: StanoxCode,
                         plannedPassengerTimestamp: Long,
                         actualTimestamp: Long,
                         difference: Long,
                         variationStatus: VariationStatus)

  case class CancellationLog(id: Option[Int],
                             trainId: TrainId,
                             scheduleTrainId: ScheduleTrainId,
                             serviceCode: ServiceCode,
                             toc: TOC,
                             stanoxCode: StanoxCode,
                             cancellationType: CancellationType,
                             cancellationReasonCode: String)

  trait MovementProcessor {
    def stream: fs2.Stream[IO, Unit]
  }
}
