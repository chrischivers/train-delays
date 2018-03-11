package traindelays.networkrail.db

import java.time.LocalDate

import cats.effect.IO
import doobie.implicits._
import doobie.util.transactor.Transactor
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.scheduledata.{DaysRunPattern, ScheduleTrainId, StpIndicator}

import scala.concurrent.duration.FiniteDuration

object ScheduleSecondaryTable {

  import ScheduleTable._

  val tableName: doobie.Fragment = fr"schedule_secondary"

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): ScheduleTable[ScheduleRecordSecondary] =
    new ScheduleTable[ScheduleRecordSecondary] {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(log: ScheduleRecordSecondary): IO[Unit] =
        ScheduleTable
          .addScheduleLogRecord(log)
          .run
          .transact(db)
          .map(_ => ())

      override def deleteAllRecords(): IO[Unit] =
        deleteAllScheduleRecords(tableName).run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[ScheduleRecordSecondary]] =
        allScheduleRecordsFragment(tableName, ScheduleRecordSecondary.selectFieldsFragment)
          .query[ScheduleRecordSecondary]
          .to[List]
          .transact(db)

      override def retrieveScheduleRecordsFor(from: StanoxCode,
                                              to: StanoxCode,
                                              pattern: DaysRunPattern,
                                              stpIndicator: StpIndicator): IO[List[ScheduleRecordSecondary]] =
        scheduleRecordsForFragment(from, to, pattern, stpIndicator)(
          tableName,
          ScheduleRecordSecondary.selectFieldsFragment).query[ScheduleRecordSecondary].to[List].transact(db)

      override def retrieveRecordBy(id: Int): IO[Option[ScheduleRecordSecondary]] =
        scheduleRecordForFragment(id)(tableName, ScheduleRecordSecondary.selectFieldsFragment)
          .query[ScheduleRecordSecondary]
          .option
          .transact(db)

      override def retrieveAllDistinctStanoxCodes: IO[List[StanoxCode]] =
        distinctStanoxCodes(tableName).to[List].transact(db)

      override def retrieveScheduleRecordsFor(trainId: ScheduleTrainId,
                                              from: StanoxCode): IO[List[ScheduleRecordSecondary]] =
        scheduleRecordsForFragment(trainId, from)(tableName, ScheduleRecordSecondary.selectFieldsFragment)
          .query[ScheduleRecordSecondary]
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

      override def retrieveScheduleRecordsFor(trainId: ScheduleTrainId): IO[List[ScheduleRecordSecondary]] =
        scheduleRecordsForFragment(trainId)(tableName, ScheduleRecordSecondary.selectFieldsFragment)
          .query[ScheduleRecordSecondary]
          .to[List]
          .transact(db)
    }
}
