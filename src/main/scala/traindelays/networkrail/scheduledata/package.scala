package traindelays.networkrail

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import doobie.util.meta.Meta
import fs2.Pipe
import io.circe.Decoder.Result
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}

package object scheduledata {

  val timeFormatter = DateTimeFormatter.ofPattern("HHmm")

  sealed trait JsonFilter[A] {
    implicit val jsonFilter: (Json => Boolean)
  }

  sealed trait Transformer[A] {
    implicit val transform: fs2.Pipe[IO, A, A]
  }

  case class ScheduleTrainId(value: String)
  object ScheduleTrainId {
    implicit val decoder: Decoder[ScheduleTrainId] = Decoder.decodeString.map(ScheduleTrainId(_))

    implicit val meta: Meta[ScheduleTrainId] =
      Meta[String].xmap(ScheduleTrainId(_), _.value)
  }

  case class AtocCode(value: String)
  object AtocCode {
    implicit val decoder: Decoder[AtocCode] = Decoder.decodeString.map(AtocCode(_))

    implicit val meta: Meta[AtocCode] =
      Meta[String].xmap(AtocCode(_), _.value)
  }

  case class ScheduleRecord(scheduleTrainId: ScheduleTrainId,
                            trainServiceCode: ServiceCode,
                            atocCode: AtocCode,
                            daysRun: DaysRun,
                            scheduleStartDate: LocalDate,
                            scheduleEndDate: LocalDate,
                            locationRecords: List[ScheduleLocationRecord])

  object ScheduleRecord {

    implicit case object JsonFilter extends JsonFilter[ScheduleRecord] {
      override implicit val jsonFilter
        : Json => Boolean = _.hcursor.downField("JsonScheduleV1").downField("train_status").as[String] == Right("P")
    }

    implicit case object ScheduleRecordTransformer extends Transformer[ScheduleRecord] {
      override implicit val transform: Pipe[IO, ScheduleRecord, ScheduleRecord] =
        (in: fs2.Stream[IO, ScheduleRecord]) =>
          in.map(rec =>
            rec.copy(locationRecords = rec.locationRecords.filterNot(locRec =>
              locRec.departureTime.isEmpty && locRec.arrivalTime.isEmpty)))
    }

    private def daysRunFrom(daysRun: String): Either[DecodingFailure, DaysRun] =
      if (daysRun.length == 7 && daysRun.forall(char => char == '1' || char == '0')) {
        val boolVec = daysRun.foldLeft(Vector[Boolean]())((vec, char) => vec :+ (if (char == '1') true else false))
        Right(DaysRun(boolVec(0), boolVec(1), boolVec(2), boolVec(3), boolVec(4), boolVec(5), boolVec(6)))
      } else Left(DecodingFailure.apply(s"Unable to decode DaysRun string $daysRun", List.empty))

    implicit val localDateDecoder: Decoder[LocalDate] {
      def apply(c: HCursor): Result[LocalDate]
    } = new Decoder[LocalDate] {
      override def apply(c: HCursor): Result[LocalDate] = c.as[String].map(LocalDate.parse(_))
    }

    implicit val scheduleRecordDecoder: Decoder[ScheduleRecord] {
      def apply(c: HCursor): Result[ScheduleRecord]
    } = new Decoder[ScheduleRecord] {

      override def apply(c: HCursor): Result[ScheduleRecord] = {
        val scheduleObject = c.downField("JsonScheduleV1")
        for {
          daysRun           <- scheduleObject.downField("schedule_days_runs").as[String]
          daysRunDecoded    <- daysRunFrom(daysRun)
          atocCode          <- scheduleObject.downField("atoc_code").as[AtocCode]
          scheduleStartDate <- scheduleObject.downField("schedule_start_date").as[LocalDate]
          scheduleEndDate   <- scheduleObject.downField("schedule_end_date").as[LocalDate]
          scheduleTrainUid  <- scheduleObject.downField("CIF_train_uid").as[ScheduleTrainId]
          scheduleSegment = scheduleObject.downField("schedule_segment")
          serviceCode         <- scheduleSegment.downField("CIF_train_service_code").as[ServiceCode]
          locationRecordArray <- scheduleSegment.downField("schedule_location").as[List[ScheduleLocationRecord]]
        } yield {
          ScheduleRecord(scheduleTrainUid,
                         serviceCode,
                         atocCode,
                         daysRunDecoded,
                         scheduleStartDate,
                         scheduleEndDate,
                         locationRecordArray)
        }
      }
    }

    case class ScheduleLocationRecord(locationType: LocationType,
                                      tiplocCode: TipLocCode,
                                      arrivalTime: Option[LocalTime],
                                      departureTime: Option[LocalTime])

    object ScheduleLocationRecord {

      case class TipLocCode(value: String)
      object TipLocCode {
        implicit val decoder: Decoder[TipLocCode] = Decoder.decodeString.map(TipLocCode(_))

        implicit val meta: Meta[TipLocCode] =
          Meta[String].xmap(TipLocCode(_), _.value)
      }

      sealed trait LocationType {
        val string: String
      }
      case object OriginatingLocation extends LocationType {
        override val string: String = "LO"
      }
      case object TerminatingLocation extends LocationType {
        override val string: String = "LT"
      }
      case object IntermediateLocation extends LocationType {
        override val string: String = "LI"
      }
      object LocationType {

        def fromString(str: String): LocationType =
          str match {
            case OriginatingLocation.string  => OriginatingLocation
            case TerminatingLocation.string  => TerminatingLocation
            case IntermediateLocation.string => IntermediateLocation
          }
        implicit val decoder: Decoder[LocationType] = Decoder.decodeString.map(fromString)

        implicit val meta: Meta[LocationType] =
          Meta[String].xmap(LocationType.fromString, _.string)
      }

      import io.circe.java8.time.decodeLocalTime
      implicit final val localTimeDecoder: Decoder[LocalTime] =
        decodeLocalTime(timeFormatter)

      implicit val scheduleLocationRecordDecoder: Decoder[ScheduleLocationRecord] {
        def apply(c: HCursor): Result[ScheduleLocationRecord]
      } = new Decoder[ScheduleLocationRecord] {
        override def apply(c: HCursor): Result[ScheduleLocationRecord] =
          for {
            locationType  <- c.downField("location_type").as[LocationType]
            tiplocCode    <- c.downField("tiploc_code").as[TipLocCode]
            departureTime <- c.downField("public_departure").as[Option[LocalTime]]
            arrivalTime   <- c.downField("public_arrival").as[Option[LocalTime]]
          } yield {
            ScheduleLocationRecord(locationType, tiplocCode, arrivalTime, departureTime)
          }
      }
    }

    case class DaysRun(monday: Boolean,
                       tuesday: Boolean,
                       wednesday: Boolean,
                       thursday: Boolean,
                       friday: Boolean,
                       saturday: Boolean,
                       sunday: Boolean)

  }

  case class TipLocRecord(tipLocCode: TipLocCode, stanox: Stanox, description: Option[String])

  object TipLocRecord {

    implicit case object JsonFilter extends JsonFilter[TipLocRecord] {
      override implicit val jsonFilter: Json => Boolean =
        _.hcursor.downField("TiplocV1").downField("stanox").as[String].isRight
    }

    implicit case object TipLocTransformer extends Transformer[TipLocRecord] {
      override implicit val transform: Pipe[IO, TipLocRecord, TipLocRecord] = identity
    }

    implicit val tipLocDecoder: Decoder[TipLocRecord] {
      def apply(c: HCursor): Result[TipLocRecord]
    } = new Decoder[TipLocRecord] {

      override def apply(c: HCursor): Result[TipLocRecord] = {
        val tipLocObject = c.downField("TiplocV1")
        for {
          tipLocCode  <- tipLocObject.downField("tiploc_code").as[TipLocCode]
          stanox      <- tipLocObject.downField("stanox").as[Stanox]
          description <- tipLocObject.downField("tps_description").as[Option[String]]
        } yield {
          TipLocRecord(tipLocCode, stanox, description)
        }
      }
    }
  }
}
