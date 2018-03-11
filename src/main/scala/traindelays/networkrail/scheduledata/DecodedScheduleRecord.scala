package traindelays.networkrail.scheduledata

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import doobie.util.meta.Meta
import io.circe.Decoder.Result
import io.circe._
import traindelays.networkrail._
import traindelays.networkrail.db.ScheduleTable.ScheduleRecordPrimary
import traindelays.networkrail.db.StanoxTable
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType

trait DecodedScheduleRecord extends DecodedRecord {
  val scheduleTrainId: ScheduleTrainId
  val scheduleStartDate: LocalDate
  val stpIndicator: StpIndicator
}

object DecodedScheduleRecord extends StrictLogging {

  case class Create(scheduleTrainId: ScheduleTrainId,
                    trainServiceCode: ServiceCode,
                    trainCategory: Option[TrainCategory],
                    trainStatus: Option[TrainStatus],
                    atocCode: Option[AtocCode],
                    daysRun: DaysRun,
                    scheduleStartDate: LocalDate,
                    scheduleEndDate: LocalDate,
                    stpIndicator: StpIndicator,
                    locationRecords: List[ScheduleLocationRecord])
      extends DecodedScheduleRecord {

    def toScheduleLogs(stanoxTable: StanoxTable): IO[List[ScheduleRecordPrimary]] =
      for {
        existingStanoxRecords <- stanoxTable.retrieveAllRecords()
        existingStanoxRecordsMap = StanoxRecord.stanoxRecordsToMap(existingStanoxRecords)
      } yield decodedScheduleRecordToScheduleLogs(this, existingStanoxRecordsMap)

  }

  case class Delete(scheduleTrainId: ScheduleTrainId, scheduleStartDate: LocalDate, stpIndicator: StpIndicator)
      extends DecodedScheduleRecord

  def decodedScheduleRecordToScheduleLogs(scheduleRecordCreate: DecodedScheduleRecord.Create,
                                          stanoxRecords: Map[TipLocCode, StanoxCode]): List[ScheduleRecordPrimary] = {
    val locationRecordsWithIndex = scheduleRecordCreate.locationRecords
      .filterNot(rec => rec.arrivalTime.isEmpty && rec.departureTime.isEmpty)
      .filter(locRec => stanoxRecords.get(locRec.tipLocCode).isDefined)
      .zipWithIndex
    locationRecordsWithIndex.flatMap {
      case (locationRecord, index) =>
        stanoxRecords
          .get(locationRecord.tipLocCode)
          .flatMap { stanoxRecord =>
            scheduleRecordCreate.daysRun.toDaysRunPattern.map { daysRunPattern =>
              {
                val (subsequentStanoxCodes, subsequentArrivalTimes) =
                  subsequentStanoxCodesAndArrivalTimes(locationRecordsWithIndex, index, stanoxRecords).unzip
                createScheduleRecordFrom(scheduleRecordCreate,
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
      existingStanoxRecords: Map[TipLocCode, StanoxCode]): List[(StanoxCode, LocalTime)] =
    locationRecordsWithIndex
      .dropWhile(_._2 < index + 1)
      .map {
        case (scheduleLocationRecord, _) =>
          existingStanoxRecords.getOrElse(
            scheduleLocationRecord.tipLocCode,
            throw new IllegalStateException(
              s"Unable to find stanox record for tiplocCode ${scheduleLocationRecord.tipLocCode}")) -> scheduleLocationRecord.arrivalTime
            .orElse(scheduleLocationRecord.departureTime)
            .getOrElse(throw new IllegalStateException(
              s"Arrival time and departure time not included for a subsequent stop [$scheduleLocationRecord]")) //TODO do this in a better way
      }

  private def createScheduleRecordFrom(scheduleRecordCreate: DecodedScheduleRecord.Create,
                                       index: Int,
                                       locationRecord: ScheduleLocationRecord,
                                       stanoxCode: StanoxCode,
                                       subsequentStanoxCodes: List[StanoxCode],
                                       subsequentArrivalTimes: List[LocalTime],
                                       daysRunPattern: DaysRunPattern) =
    ScheduleRecordPrimary(
      None,
      scheduleRecordCreate.scheduleTrainId,
      scheduleRecordCreate.trainServiceCode,
      scheduleRecordCreate.stpIndicator,
      scheduleRecordCreate.trainCategory,
      scheduleRecordCreate.trainStatus,
      scheduleRecordCreate.atocCode,
      index + 1,
      stanoxCode,
      subsequentStanoxCodes,
      subsequentArrivalTimes,
      scheduleRecordCreate.daysRun.monday,
      scheduleRecordCreate.daysRun.tuesday,
      scheduleRecordCreate.daysRun.wednesday,
      scheduleRecordCreate.daysRun.thursday,
      scheduleRecordCreate.daysRun.friday,
      scheduleRecordCreate.daysRun.saturday,
      scheduleRecordCreate.daysRun.sunday,
      daysRunPattern,
      scheduleRecordCreate.scheduleStartDate,
      scheduleRecordCreate.scheduleEndDate,
      locationRecord.locationType,
      locationRecord.arrivalTime,
      locationRecord.departureTime
    )

  implicit private val localDateDecoder: Decoder[LocalDate] {
    def apply(c: HCursor): Result[LocalDate]
  } = new Decoder[LocalDate] {
    override def apply(c: HCursor): Result[LocalDate] = c.as[String].map(LocalDate.parse(_))
  }

  val scheduleRecordDecoder: Decoder[DecodedScheduleRecord] {
    def apply(c: HCursor): Result[DecodedScheduleRecord]
  } = new Decoder[DecodedScheduleRecord] {

    override def apply(c: HCursor): Result[DecodedScheduleRecord] = {
      val cursor = c.downField("JsonScheduleV1")
      cursor.downField("transaction_type").as[TransactionType].flatMap {
        case TransactionType.Create => logDecodingErrors(c, decodeScheduleCreateRecord(cursor))
        case TransactionType.Delete => logDecodingErrors(c, decodeScheduleDeleteRecord(cursor))
        case TransactionType.Update =>
          Left(DecodingFailure(s"Update for JsonScheduleV1 not handled ${c.value}", c.history))
      }
    }
  }

  private def decodeScheduleCreateRecord(scheduleObject: ACursor) =
    for {
      daysRun           <- scheduleObject.downField("schedule_days_runs").as[String]
      daysRunDecoded    <- DaysRun.daysRunFrom(daysRun)
      atocCode          <- scheduleObject.downField("atoc_code").as[Option[AtocCode]]
      scheduleStartDate <- scheduleObject.downField("schedule_start_date").as[LocalDate]
      scheduleEndDate   <- scheduleObject.downField("schedule_end_date").as[LocalDate]
      stpIndicator      <- scheduleObject.downField("CIF_stp_indicator").as[StpIndicator]
      scheduleTrainUid  <- scheduleObject.downField("CIF_train_uid").as[ScheduleTrainId]
      trainStatus       <- scheduleObject.downField("train_status").as[Option[TrainStatus]]
      scheduleSegment = scheduleObject.downField("schedule_segment")
      serviceCode         <- scheduleSegment.downField("CIF_train_service_code").as[ServiceCode]
      trainCategory       <- scheduleSegment.downField("CIF_train_category").as[Option[TrainCategory]]
      locationRecordArray <- scheduleSegment.downField("schedule_location").as[Option[List[ScheduleLocationRecord]]]
    } yield {
      DecodedScheduleRecord.Create(
        scheduleTrainUid,
        serviceCode,
        trainCategory,
        trainStatus,
        atocCode,
        daysRunDecoded,
        scheduleStartDate,
        scheduleEndDate,
        stpIndicator,
        locationRecordArray.getOrElse(Nil)
      )
    }

  private def decodeScheduleDeleteRecord(scheduleObject: ACursor) =
    for {
      scheduleTrainUid  <- scheduleObject.downField("CIF_train_uid").as[ScheduleTrainId]
      scheduleStartDate <- scheduleObject.downField("schedule_start_date").as[LocalDate]
      stpIndicator      <- scheduleObject.downField("CIF_stp_indicator").as[StpIndicator]
    } yield {
      DecodedScheduleRecord.Delete(scheduleTrainUid, scheduleStartDate, stpIndicator)
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
