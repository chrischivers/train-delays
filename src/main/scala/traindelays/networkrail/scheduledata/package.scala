package traindelays.networkrail

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}

package object scheduledata {

  val timeFormatter = DateTimeFormatter.ofPattern("HHmm")

  sealed trait JsonFilter[A] {
    implicit val filter: (Json => Boolean)
  }

  case class ScheduleRecord(trainUid: String,
                            trainServiceCode: String,
                            daysRun: DaysRun,
                            scheduleStartDate: LocalDate,
                            scheduleEndDate: LocalDate,
                            locationRecords: List[ScheduleLocationRecord])

  object ScheduleRecord {

    implicit case object JsonFilter extends JsonFilter[ScheduleRecord] {
      override implicit val filter
        : Json => Boolean = _.hcursor.downField("JsonScheduleV1").downField("train_status").as[String] == Right("P")
    }

    private def daysRunFrom(daysRun: String): DaysRun = {
      //TODO handle error
      assert(daysRun.length == 7 && daysRun.forall(char => char == '1' || char == '0'))
      val boolVec = daysRun.foldLeft(Vector[Boolean]())((vec, char) => vec :+ (if (char == '1') true else false))
      DaysRun(boolVec(0), boolVec(1), boolVec(2), boolVec(3), boolVec(4), boolVec(5), boolVec(6))
    }

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
          scheduleStartDate <- scheduleObject.downField("schedule_start_date").as[LocalDate]
          scheduleEndDate   <- scheduleObject.downField("schedule_end_date").as[LocalDate]
          trainUid          <- scheduleObject.downField("CIF_train_uid").as[String]
          scheduleSegment = scheduleObject.downField("schedule_segment")
          serviceCode         <- scheduleSegment.downField("CIF_train_service_code").as[String]
          locationRecordArray <- scheduleSegment.downField("schedule_location").as[List[ScheduleLocationRecord]]
        } yield {
          ScheduleRecord(trainUid,
                         serviceCode,
                         daysRunFrom(daysRun),
                         scheduleStartDate,
                         scheduleEndDate,
                         locationRecordArray)
        }
      }
    }

    case class ScheduleLocationRecord(locationType: String,
                                      tiplocCode: String,
                                      arrivalTime: Option[LocalTime],
                                      departureTime: Option[LocalTime])

    object ScheduleLocationRecord {

      import io.circe.java8.time.decodeLocalTime

      implicit final val localTimeDecoder: Decoder[LocalTime] =
        decodeLocalTime(timeFormatter)

      implicit val scheduleLocationRecordDecoder: Decoder[ScheduleLocationRecord] {
        def apply(c: HCursor): Result[ScheduleLocationRecord]
      } = new Decoder[ScheduleLocationRecord] {
        override def apply(c: HCursor): Result[ScheduleLocationRecord] =
          for {
            locationType  <- c.downField("location_type").as[String]
            tiplocCode    <- c.downField("tiploc_code").as[String]
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

  case class TipLocRecord(tipLocCode: String, stanox: String, description: String)

  object TipLocRecord {

    implicit case object JsonFilter extends JsonFilter[TipLocRecord] {
      override implicit val filter: Json => Boolean = _.hcursor.downField("TiplocV1").succeeded
    }

    implicit val tipLocDecoder: Decoder[TipLocRecord] {
      def apply(c: HCursor): Result[TipLocRecord]
    } = new Decoder[TipLocRecord] {

      override def apply(c: HCursor): Result[TipLocRecord] = {
        val tipLocObject = c.downField("TiplocV1")
        for {
          tipLocCode  <- tipLocObject.downField("tiploc_code").as[String]
          stanox      <- tipLocObject.downField("stanox").as[String]
          description <- tipLocObject.downField("tps_description").as[String]
        } yield {
          TipLocRecord(tipLocCode, stanox, description)
        }
      }
    }
  }
}
