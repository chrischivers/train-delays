package traindelays.networkrail.db

import java.time.LocalDate

import cats.effect.IO
import doobie.implicits._
import doobie.util.transactor.Transactor
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.scheduledata.{DaysRunPattern, ScheduleTrainId, StpIndicator}

import scala.concurrent.duration.FiniteDuration

object SchedulePrimaryTable {

  import ScheduleTable._

  val tableName: doobie.Fragment = fr"schedule"

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): ScheduleTable[ScheduleRecordPrimary] =
    new ScheduleTable[ScheduleRecordPrimary] {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(log: ScheduleRecordPrimary): IO[Unit] =
        ScheduleTable
          .addScheduleLogRecord(log)
          .run
          .transact(db)
          .map(_ => ())

      override def deleteAllRecords(): IO[Unit] =
        deleteAllScheduleRecords(tableName).run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[ScheduleRecordPrimary]] =
        allScheduleRecordsFragment(tableName, ScheduleRecordPrimary.selectFieldsFragment)
          .query[ScheduleRecordPrimary]
          .to[List]
          .transact(db)

      override def retrieveScheduleRecordsFor(from: StanoxCode,
                                              to: StanoxCode,
                                              pattern: DaysRunPattern,
                                              stpIndicator: StpIndicator): IO[List[ScheduleRecordPrimary]] =
        scheduleRecordsForFragment(from, to, pattern, stpIndicator)(tableName,
                                                                    ScheduleRecordPrimary.selectFieldsFragment)
          .query[ScheduleRecordPrimary]
          .to[List]
          .transact(db)

      override def retrieveRecordBy(id: Int): IO[Option[ScheduleRecordPrimary]] =
        scheduleRecordForFragment(id)(tableName, ScheduleRecordPrimary.selectFieldsFragment)
          .query[ScheduleRecordPrimary]
          .option
          .transact(db)

      override def retrieveAllDistinctStanoxCodes: IO[List[StanoxCode]] =
        distinctStanoxCodes(tableName).to[List].transact(db)

      override def retrieveScheduleRecordsFor(trainId: ScheduleTrainId,
                                              from: StanoxCode): IO[List[ScheduleRecordPrimary]] =
        scheduleRecordsForFragment(trainId, from)(tableName, ScheduleRecordPrimary.selectFieldsFragment)
          .query[ScheduleRecordPrimary]
          .to[List]
          .transact(db)

      override def deleteRecord(scheduleTrainId: ScheduleTrainId,
                                scheduleStartDate: LocalDate,
                                stpIndicator: StpIndicator): IO[Unit] =
        ScheduleTable
          .deleteRecord(scheduleTrainId, scheduleStartDate, stpIndicator)(tableName)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveScheduleRecordsFor(trainId: ScheduleTrainId): IO[List[ScheduleRecordPrimary]] =
        scheduleRecordsForFragment(trainId)(tableName, ScheduleRecordPrimary.selectFieldsFragment)
          .query[ScheduleRecordPrimary]
          .to[List]
          .transact(db)
    }
}
