package traindelays.networkrail.db

import java.time.LocalDate

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail.db.AssociationTable.AssociationRecord
import traindelays.networkrail.TipLocCode
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordSecondary, ScheduleRecordPrimary}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.scheduledata._

import scala.concurrent.duration.FiniteDuration

trait AssociationTable extends MemoizedTable[AssociationRecord] {

  def deleteAllRecords(): IO[Unit]

  def retrieveRecordFor(mainScheduleTrainId: ScheduleTrainId,
                        associatedScheduleTrainId: ScheduleTrainId,
                        associationStartDate: LocalDate,
                        stpIndicator: StpIndicator,
                        location: TipLocCode): IO[Option[AssociationRecord]]

  def deleteRecordBy(id: Int): IO[Unit]

  def retrieveJoinOrDivideRecordsNotInSecondaryTable(): IO[List[AssociationRecord]]
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
                               associationCategory: Option[AssociationCategory]) {

    def toSecondaryScheduleRecords(
        scheduleRecordsForMainId: List[ScheduleRecordPrimary],
        scheduleRecordsForAssociatedId: List[ScheduleRecordPrimary],
        stanoxRecordForAssocationLocation: List[StanoxRecord]): Option[List[ScheduleRecordSecondary]] =
      AssociationRecord.toAssociationScheduleRecords(this,
                                                     scheduleRecordsForMainId,
                                                     scheduleRecordsForAssociatedId,
                                                     stanoxRecordForAssocationLocation)

  }

  object AssociationRecord {
    def toAssociationScheduleRecords(
        associationRecord: AssociationRecord,
        scheduleRecordsForMainId: List[ScheduleRecordPrimary],
        scheduleRecordsForAssociatedId: List[ScheduleRecordPrimary],
        stanoxRecordForAssocationLocation: List[StanoxRecord]): Option[List[ScheduleRecordSecondary]] = {

      val relevantScheduleRecordsForMainId = relevantScheduleRecords(
        associationRecord.mainScheduleTrainId,
        associationRecord.daysRunPattern,
        associationRecord.associatedStart,
        associationRecord.associatedEnd,
        scheduleRecordsForMainId
      )
      val relevantScheduleRecordsForAssociatedId =
        relevantScheduleRecords(
          associationRecord.associatedScheduleTrainId,
          associationRecord.daysRunPattern,
          associationRecord.associatedStart,
          associationRecord.associatedEnd,
          scheduleRecordsForAssociatedId
        )

      val assembledScheduleRecords =
        associationRecord.associationCategory.flatMap {
          case AssociationCategory.Join =>
            relevantScheduleRecordsForAssociatedId.flatMap(
              associatedRecs =>
                relevantScheduleRecordsForMainId.map(
                  mainRecs =>
                    associatedRecs ++ mainRecs
                      .dropWhile(x => !stanoxRecordForAssocationLocation.exists(_.stanoxCode.contains(x.stanoxCode)))
                      .map(_.copy(scheduleTrainId = associationRecord.associatedScheduleTrainId))))
          case AssociationCategory.Divide =>
            relevantScheduleRecordsForAssociatedId.flatMap(
              associatedRecs =>
                relevantScheduleRecordsForMainId.map(
                  mainRecs =>
                    mainRecs
                      .takeWhile(x => !stanoxRecordForAssocationLocation.exists(_.stanoxCode.contains(x.stanoxCode)))
                      .map(_.copy(scheduleTrainId = associationRecord.associatedScheduleTrainId)) ++ associatedRecs))
          case _ => throw new RuntimeException("Not implementing other association cases")
        }

      associationRecord.id.flatMap { id =>
        assembledScheduleRecords.map(records => {
          records.zipWithIndex.map {
            case (record, index) => {
              val subsequentRecords = records.dropWhile(_.stanoxCode != record.stanoxCode).drop(1)
              ScheduleRecordSecondary(
                None,
                record.scheduleTrainId,
                record.serviceCode,
                associationRecord.stpIndicator,
                record.trainCategory,
                record.trainStatus,
                record.atocCode,
                index,
                record.stanoxCode,
                subsequentRecords.map(_.stanoxCode),
                subsequentRecords.map(
                  subsRec =>
                    subsRec.arrivalTime.getOrElse(subsRec.departureTime.getOrElse(throw new RuntimeException(
                      "Not found arrival or departure time for subsequent stop")))), //todo make this better
                associationRecord.monday,
                associationRecord.tuesday,
                associationRecord.wednesday,
                associationRecord.thursday,
                associationRecord.friday,
                associationRecord.saturday,
                associationRecord.sunday,
                associationRecord.daysRunPattern,
                associationRecord.associatedStart,
                associationRecord.associatedEnd,
                record.locationType,
                record.arrivalTime,
                record.departureTime,
                id
              )
            }
          }
        })
      }
    }

    private def relevantScheduleRecords(scheduleTrainId: ScheduleTrainId,
                                        daysRunPattern: DaysRunPattern,
                                        associationStartDate: LocalDate,
                                        associationEndDate: LocalDate,
                                        scheduleRecords: List[ScheduleRecordPrimary]) =
      scheduleRecords
        .filter(rec => rec.scheduleTrainId == scheduleTrainId)
        .filter(rec => dateOverlaps(rec, associationStartDate, associationEndDate))
        .filter(rec => rec.daysRunPattern == daysRunPattern)
        .groupBy(_.scheduleStart)
        .values
        .headOption
        .map(_.sortBy(_.stopSequence))
    private def dateOverlaps(scheduleRecord: ScheduleRecordPrimary,
                             associationStartDate: LocalDate,
                             associationEndDate: LocalDate) =
      scheduleRecord.scheduleStart.isBefore(associationEndDate) &&
        scheduleRecord.scheduleEnd.isAfter(associationStartDate)

  }

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

  def allAssociationRecords(): Query0[AssociationRecord] =
    sql"""
      SELECT id, main_schedule_train_id, associated_schedule_train_id, associated_start, associated_end, stp_indicator, location, 
               monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, association_category
      FROM association
      """.query[AssociationRecord]

  def joinDivideRecordsNotInSecondarySchedule(): Query0[AssociationRecord] =
    sql"""
      SELECT id, main_schedule_train_id, associated_schedule_train_id, associated_start, associated_end, stp_indicator, location,
               monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, association_category
      FROM association
      WHERE association_category != 'NP'
      AND association_category IS NOT NULL
      AND id NOT IN (SELECT DISTINCT association_id FROM schedule_secondary)
      """.query[AssociationRecord]

  def deleteAllAssociationRecords(): Update0 =
    sql"""DELETE FROM association""".update

  def deleteAssociationRecord(id: Int): Update0 =
    sql"DELETE FROM association WHERE id = ${id}".update

  def retrieveRecordFor(mainScheduleTrainId: ScheduleTrainId,
                        associatedScheduleTrainId: ScheduleTrainId,
                        associationStartDate: LocalDate,
                        stpIndicator: StpIndicator,
                        location: TipLocCode): Query0[AssociationRecord] =
    sql"""SELECT id, main_schedule_train_id, associated_schedule_train_id, associated_start, associated_end, stp_indicator, location,
           monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern, association_category
          FROM association
          WHERE main_schedule_train_id = ${mainScheduleTrainId}
          AND associated_schedule_train_id = ${associatedScheduleTrainId}
          AND associated_start = ${associationStartDate}
          AND stp_indicator = ${stpIndicator}
          AND location = ${location}
       """.query[AssociationRecord]

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): AssociationTable =
    new AssociationTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def deleteAllRecords(): IO[Unit] =
        AssociationTable.deleteAllAssociationRecords().run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[AssociationRecord]] =
        AssociationTable
          .allAssociationRecords()
          .to[List]
          .transact(db)

      override def retrieveRecordFor(mainScheduleTrainId: ScheduleTrainId,
                                     associatedScheduleTrainId: ScheduleTrainId,
                                     associationStartDate: LocalDate,
                                     stpIndicator: StpIndicator,
                                     location: TipLocCode): IO[Option[AssociationRecord]] =
        AssociationTable
          .retrieveRecordFor(mainScheduleTrainId,
                             associatedScheduleTrainId,
                             associationStartDate,
                             stpIndicator,
                             location)
          .option
          .transact(db)

      override def addRecord(record: AssociationRecord): IO[Unit] =
        AssociationTable
          .addAssociationRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveJoinOrDivideRecordsNotInSecondaryTable(): IO[List[AssociationRecord]] =
        AssociationTable.joinDivideRecordsNotInSecondarySchedule().to[List].transact(db)

      override def deleteRecordBy(id: Int): IO[Unit] = deleteAssociationRecord(id).run.transact(db).map(_ => ())
    }
}
