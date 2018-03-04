package traindelays.networkrail.scheduledata

import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import com.typesafe.scalalogging.StrictLogging
import io.circe.Decoder.Result
import io.circe._
import traindelays.networkrail._
import traindelays.networkrail.db.AssociationTable.AssociationRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.DaysRun

trait DecodedAssociationRecord extends DecodedRecord {
  val mainScheduleTrainId: ScheduleTrainId
  val associatedScheduleTrainId: ScheduleTrainId
  val associationStartDate: LocalDate
  val stpIndicator: StpIndicator
  val location: TipLocCode
}

object DecodedAssociationRecord extends StrictLogging {

  case class Create(mainScheduleTrainId: ScheduleTrainId,
                    associatedScheduleTrainId: ScheduleTrainId,
                    associationStartDate: LocalDate,
                    stpIndicator: StpIndicator,
                    location: TipLocCode,
                    daysRun: DaysRun,
                    associationEndDate: LocalDate,
                    associationCategory: Option[AssociationCategory])
      extends DecodedAssociationRecord {
    def toAssociationRecord: Option[AssociationRecord] =
      daysRun.toDaysRunPattern.map(
        daysRunPattern =>
          AssociationRecord(
            None,
            mainScheduleTrainId,
            associatedScheduleTrainId,
            associationStartDate,
            associationEndDate,
            stpIndicator,
            location,
            daysRun.monday,
            daysRun.tuesday,
            daysRun.wednesday,
            daysRun.thursday,
            daysRun.friday,
            daysRun.saturday,
            daysRun.sunday,
            daysRunPattern,
            associationCategory
        ))
  }

  case class Delete(mainScheduleTrainId: ScheduleTrainId,
                    associatedScheduleTrainId: ScheduleTrainId,
                    associationStartDate: LocalDate,
                    stpIndicator: StpIndicator,
                    location: TipLocCode)
      extends DecodedAssociationRecord

  implicit private val localDateDecoder: Decoder[LocalDate] {
    def apply(c: HCursor): Result[LocalDate]
  } = new Decoder[LocalDate] {
    val zoneId = ZoneId.of("UTC")
    override def apply(c: HCursor): Result[LocalDate] =
      c.as[String].map(str => LocalDateTime.ofInstant(Instant.parse(str), zoneId).toLocalDate)
  }

  val associationRecordDecoder: Decoder[DecodedAssociationRecord] {
    def apply(c: HCursor): Result[DecodedAssociationRecord]
  } = new Decoder[DecodedAssociationRecord] {

    override def apply(c: HCursor): Result[DecodedAssociationRecord] = {
      val cursor = c.downField("JsonAssociationV1")
      cursor.downField("transaction_type").as[TransactionType].flatMap {
        case TransactionType.Create => logDecodingErrors(c, decodeAssociationCreateRecord(cursor))
        case TransactionType.Delete => logDecodingErrors(c, decodeAssociationDeleteRecord(cursor))
        case TransactionType.Update =>
          Left(DecodingFailure(s"Update for JsonAssociationV1 not handled ${c.value}", c.history))
      }
    }
  }

  private def decodeAssociationCreateRecord(scheduleObject: ACursor) =
    for {
      mainTrainId       <- scheduleObject.downField("main_train_uid").as[ScheduleTrainId]
      associatedTrainId <- scheduleObject.downField("assoc_train_uid").as[ScheduleTrainId]
      startDate         <- scheduleObject.downField("assoc_start_date").as[LocalDate]
      endDate           <- scheduleObject.downField("assoc_end_date").as[LocalDate]
      daysRun           <- scheduleObject.downField("assoc_days").as[String]
      daysRunDecoded    <- DaysRun.daysRunFrom(daysRun)
      associationCategory <- scheduleObject
        .downField("category")
        .as[Option[String]]
        .map(_.flatMap(str => AssociationCategory.fromString(str.trim)))
      location     <- scheduleObject.downField("location").as[TipLocCode]
      stpIndicator <- scheduleObject.downField("CIF_stp_indicator").as[StpIndicator]
    } yield {
      DecodedAssociationRecord.Create(
        mainTrainId,
        associatedTrainId,
        startDate,
        stpIndicator,
        location,
        daysRunDecoded,
        endDate,
        associationCategory
      )
    }

  private def decodeAssociationDeleteRecord(scheduleObject: ACursor) =
    for {
      mainTrainId       <- scheduleObject.downField("main_train_uid").as[ScheduleTrainId]
      associatedTrainId <- scheduleObject.downField("assoc_train_uid").as[ScheduleTrainId]
      startDate         <- scheduleObject.downField("assoc_start_date").as[LocalDate]
      location          <- scheduleObject.downField("location").as[TipLocCode]
      stpIndicator      <- scheduleObject.downField("CIF_stp_indicator").as[StpIndicator]
    } yield {
      DecodedAssociationRecord.Delete(mainTrainId, associatedTrainId, startDate, stpIndicator, location)
    }
}
