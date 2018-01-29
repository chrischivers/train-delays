package traindelays.networkrail

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import doobie.util.meta.Meta
import fs2.Pipe
import io.circe.Decoder.Result
import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType
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
    import io.circe.generic.semiauto._
    implicit val decoder: Decoder[ScheduleTrainId] = Decoder.decodeString.map(ScheduleTrainId(_))
    implicit val encoder: Encoder[ScheduleTrainId] = deriveEncoder[ScheduleTrainId]

    implicit val meta: Meta[ScheduleTrainId] =
      Meta[String].xmap(ScheduleTrainId(_), _.value)
  }

  case class AtocCode(value: String)
  object AtocCode {
    import io.circe.generic.semiauto._
    implicit val decoder: Decoder[AtocCode] = Decoder.decodeString.map(AtocCode(_))
    implicit val encoder: Encoder[AtocCode] = deriveEncoder[AtocCode]

    implicit val meta: Meta[AtocCode] =
      Meta[String].xmap(AtocCode(_), _.value)
  }

  case class ScheduleRecord(scheduleTrainId: ScheduleTrainId,
                            trainServiceCode: ServiceCode,
                            atocCode: AtocCode,
                            daysRun: DaysRun,
                            scheduleStartDate: LocalDate,
                            scheduleEndDate: LocalDate,
                            locationRecords: List[ScheduleLocationRecord]) {

    def toScheduleLogs(stanoxRecords: List[StanoxRecord]): List[ScheduleLog] =
      ScheduleRecord.scheduleRecordToScheduleLogs(this, stanoxRecords)
  }

  object ScheduleRecord {

    import cats.instances.list._
    import cats.syntax.traverse._

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

    def scheduleRecordToScheduleLogs(scheduleRecord: ScheduleRecord,
                                     stanoxRecords: List[StanoxRecord]): List[ScheduleLog] = {
      val locationRecordsWithIndex = scheduleRecord.locationRecords.zipWithIndex
      locationRecordsWithIndex.flatMap {
        case (locationRecord, index) =>
          stanoxRecords
            .find(rec => rec.tipLocCode == locationRecord.tipLocCode)
            .flatMap { stanoxRecord =>
              scheduleRecord.daysRun.toDaysRunPattern.map { daysRunPattern =>
                {
                  val (subsequentStanoxCodes, subsequentArrivalTimes) =
                    subsequentStanoxCodesAndArrivalTimes(locationRecordsWithIndex, index, stanoxRecords).unzip
                  createScheduleLogFrom(scheduleRecord,
                                        index,
                                        locationRecord,
                                        stanoxRecord,
                                        subsequentStanoxCodes,
                                        subsequentArrivalTimes,
                                        daysRunPattern)
                }

              }
            }
      }
    }

    private def subsequentStanoxCodesAndArrivalTimes(
        locationRecordsWithIndex: List[(ScheduleLocationRecord, Int)],
        index: Int,
        existingStanoxRecords: List[StanoxRecord]): List[(StanoxCode, LocalTime)] =
      locationRecordsWithIndex
        .dropWhile(_._2 < index + 1)
        .map {
          case (scheduleLocationRecord, _) =>
            val stanoxRecord = existingStanoxRecords.find(_.tipLocCode == scheduleLocationRecord.tipLocCode)
            stanoxRecord
              .getOrElse(throw new IllegalStateException(
                s"Unable to find stanox record for tiplocCode ${scheduleLocationRecord.tipLocCode}")) //TODO do this in a better way
              .stanoxCode -> scheduleLocationRecord.arrivalTime.getOrElse(throw new IllegalStateException(
              "Arrival time optional for subsequent stops")) //TODO do this in a better way
        }

    private def createScheduleLogFrom(scheduleRecord: ScheduleRecord,
                                      index: Int,
                                      locationRecord: ScheduleLocationRecord,
                                      stanoxRecord: StanoxRecord,
                                      subsequentStanoxCodes: List[StanoxCode],
                                      subsequentArrivalTimes: List[LocalTime],
                                      daysRunPattern: DaysRunPattern) =
      ScheduleLog(
        None,
        scheduleRecord.scheduleTrainId,
        scheduleRecord.trainServiceCode,
        scheduleRecord.atocCode,
        index + 1,
        stanoxRecord.stanoxCode,
        subsequentStanoxCodes,
        subsequentArrivalTimes,
        scheduleRecord.daysRun.monday,
        scheduleRecord.daysRun.tuesday,
        scheduleRecord.daysRun.wednesday,
        scheduleRecord.daysRun.thursday,
        scheduleRecord.daysRun.friday,
        scheduleRecord.daysRun.saturday,
        scheduleRecord.daysRun.sunday,
        daysRunPattern,
        scheduleRecord.scheduleStartDate,
        scheduleRecord.scheduleEndDate,
        locationRecord.locationType,
        locationRecord.arrivalTime,
        locationRecord.departureTime
      )

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
          daysRunDecoded    <- DaysRun.daysRunFrom(daysRun)
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
                                      tipLocCode: TipLocCode,
                                      arrivalTime: Option[LocalTime],
                                      departureTime: Option[LocalTime])

    object ScheduleLocationRecord {

      sealed trait LocationType {
        val string: String
      }

      object LocationType {

        case object OriginatingLocation extends LocationType {
          override val string: String = "LO"
        }
        case object TerminatingLocation extends LocationType {
          override val string: String = "LT"
        }
        case object IntermediateLocation extends LocationType {
          override val string: String = "LI"
        }

        def fromString(str: String): LocationType =
          str match {
            case OriginatingLocation.string  => OriginatingLocation
            case TerminatingLocation.string  => TerminatingLocation
            case IntermediateLocation.string => IntermediateLocation
          }
        implicit val decoder: Decoder[LocationType] = Decoder.decodeString.map(fromString)
        implicit val encoder: Encoder[LocationType] = (a: LocationType) => Json.fromString(a.string)

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
                       sunday: Boolean) {
      def toDaysRunPattern: Option[DaysRunPattern] =
        if (monday || tuesday || wednesday || thursday || friday) Some(DaysRunPattern.Weekdays)
        else if (saturday) Some(DaysRunPattern.Saturdays)
        else if (sunday) Some(DaysRunPattern.Sundays)
        else None
    }

    object DaysRun {
      def daysRunFrom(daysRun: String): Either[DecodingFailure, DaysRun] =
        if (daysRun.length == 7 && daysRun.forall(char => char == '1' || char == '0')) {
          val boolVec = daysRun.foldLeft(Vector[Boolean]())((vec, char) => vec :+ (if (char == '1') true else false))
          Right(DaysRun(boolVec(0), boolVec(1), boolVec(2), boolVec(3), boolVec(4), boolVec(5), boolVec(6)))
        } else Left(DecodingFailure.apply(s"Unable to decode DaysRun string $daysRun", List.empty))
    }
  }

  case class StanoxRecord(stanoxCode: StanoxCode, tipLocCode: TipLocCode, crs: Option[CRS], description: Option[String])

  object StanoxRecord {

    implicit case object JsonFilter extends JsonFilter[StanoxRecord] {
      override implicit val jsonFilter: Json => Boolean = json =>
        json.hcursor.downField("TiplocV1").downField("stanox").as[String].isRight &&
          json.hcursor.downField("TiplocV1").downField("tiploc_code").as[String].isRight
    }

    implicit case object StanoxRecordTransformer extends Transformer[StanoxRecord] {
      override implicit val transform: Pipe[IO, StanoxRecord, StanoxRecord] = identity
    }

    import io.circe.generic.semiauto._
    implicit val stanoxRecordEncoder: Encoder[StanoxRecord] = deriveEncoder[StanoxRecord]

    implicit val stanoxRecordDecoder: Decoder[StanoxRecord] {
      def apply(c: HCursor): Result[StanoxRecord]
    } = new Decoder[StanoxRecord] {

      override def apply(c: HCursor): Result[StanoxRecord] = {
        val tipLocObject = c.downField("TiplocV1")
        for {
          stanoxCode  <- tipLocObject.downField("stanox").as[StanoxCode]
          tipLocCode  <- tipLocObject.downField("tiploc_code").as[TipLocCode]
          crs         <- tipLocObject.downField("crs_code").as[Option[CRS]]
          description <- tipLocObject.downField("tps_description").as[Option[String]]
        } yield {
          StanoxRecord(stanoxCode, tipLocCode, crs, description)
        }
      }
    }
  }

}
