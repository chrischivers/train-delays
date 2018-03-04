package traindelays.networkrail.db

import java.time.LocalDate

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail.db.AssociationTable.AssociationRecord
import traindelays.networkrail.TipLocCode
import traindelays.networkrail.scheduledata._

import scala.concurrent.duration.FiniteDuration

trait AssociationTable extends MemoizedTable[AssociationRecord] {

  def deleteAllRecords(): IO[Unit]

  def deleteRecord(mainScheduleTrainId: ScheduleTrainId,
                   associatedScheduleTrainId: ScheduleTrainId,
                   associationStartDate: LocalDate,
                   stpIndicator: StpIndicator,
                   location: TipLocCode): IO[Unit]
}

object AssociationTable extends StrictLogging {

  import doobie._
  import doobie.implicits._

  case class AssociationRecord(id: Option[Int],
                               mainScheduleTrainId: ScheduleTrainId,
                               associatedScheduleTrainId: ScheduleTrainId,
                               associatedStart: LocalDate,
                               associatedEnd: LocalDate,
                               stpIndicator: StpIndicator,
                               location: TipLocCode,
                               monday: Boolean,
                               tuesday: Boolean,
                               wednesday: Boolean,
                               thursday: Boolean,
                               friday: Boolean,
                               saturday: Boolean,
                               sunday: Boolean,
                               daysRunPattern: DaysRunPattern,
                               associationCategory: Option[AssociationCategory])

  def addAssociationRecord(log: AssociationRecord): Update0 =
    sql"""
      INSERT INTO association
      (main_schedule_train_id, associated_schedule_train_id, associated_start, associated_end, stp_indicator, location, 
      monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, association_category)
      VALUES(${log.mainScheduleTrainId}, ${log.associatedScheduleTrainId}, ${log.associatedStart}, ${log.associatedEnd}, ${log.stpIndicator}, 
      ${log.location}, ${log.monday}, ${log.tuesday}, ${log.wednesday},
      ${log.thursday}, ${log.friday}, ${log.saturday}, ${log.sunday}, ${log.daysRunPattern}, ${log.associationCategory})
      ON CONFLICT DO NOTHING
     """.update

  def allAssociationRecordsRecords(): Query0[AssociationRecord] =
    sql"""
      SELECT id, main_schedule_train_id, associated_schedule_train_id, associated_start, associated_end, stp_indicator, location, 
               monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, association_category
      FROM association
      """.query[AssociationRecord]

  def deleteAllAssociationRecords(): Update0 =
    sql"""DELETE FROM association""".update

  def deleteRecord(mainScheduleTrainId: ScheduleTrainId,
                   associatedScheduleTrainId: ScheduleTrainId,
                   associationStartDate: LocalDate,
                   stpIndicator: StpIndicator,
                   location: TipLocCode): Update0 =
    sql"""DELETE FROM association
          WHERE main_schedule_train_id = ${mainScheduleTrainId}
          AND associated_schedule_train_id = ${associatedScheduleTrainId}
          AND associated_start = ${associationStartDate}
          AND stp_indicator = ${stpIndicator}
          AND location = ${location}
       """.update

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): AssociationTable =
    new AssociationTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def deleteAllRecords(): IO[Unit] =
        AssociationTable.deleteAllAssociationRecords().run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[AssociationRecord]] =
        AssociationTable
          .allAssociationRecordsRecords()
          .to[List]
          .transact(db)

      override def deleteRecord(mainScheduleTrainId: ScheduleTrainId,
                                associatedScheduleTrainId: ScheduleTrainId,
                                associationStartDate: LocalDate,
                                stpIndicator: StpIndicator,
                                location: TipLocCode): IO[Unit] =
        AssociationTable
          .deleteRecord(mainScheduleTrainId, associatedScheduleTrainId, associationStartDate, stpIndicator, location)
          .run
          .transact(db)
          .map(_ => ())

      override def addRecord(record: AssociationRecord): IO[Unit] =
        AssociationTable
          .addAssociationRecord(record)
          .run
          .transact(db)
          .map(_ => ())
    }

}
