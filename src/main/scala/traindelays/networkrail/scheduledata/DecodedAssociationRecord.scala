package traindelays.networkrail.scheduledata

import java.time.LocalDate

import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail._
import traindelays.networkrail.db.AssociationTable.AssociationRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.DaysRun

trait DecodedAssociationRecord {
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
                    associationCategory: AssociationCategory)
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

}
