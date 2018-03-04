package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata._
import traindelays.networkrail._

import scala.concurrent.duration.FiniteDuration

trait ScheduleTable extends MemoizedTable[ScheduleRecord] {

  def deleteAllRecords(): IO[Unit]

  def deleteRecord(scheduleTrainId: ScheduleTrainId, scheduleStartDate: LocalDate, stpIndicator: StpIndicator): IO[Unit]

  def addRecords(records: List[ScheduleRecord]): IO[Unit]

  def retrieveScheduleLogRecordsFor(from: StanoxCode,
                                    to: StanoxCode,
                                    pattern: DaysRunPattern,
                                    stpIndicator: StpIndicator): IO[List[ScheduleRecord]]

  def retrieveScheduleLogRecordsFor(trainId: ScheduleTrainId, stanoxCode: StanoxCode): IO[List[ScheduleRecord]]

  def retrieveRecordBy(id: Int): IO[Option[ScheduleRecord]]

  def retrieveAllDistinctStanoxCodes: IO[List[StanoxCode]]

}

object ScheduleTable extends StrictLogging {

  import cats.instances.list._
  import doobie._
  import doobie.implicits._
  import doobie.postgres.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[java.sql.Time]
    .xmap(t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute), lt => java.sql.Time.valueOf(lt))

  implicit val localTimeListMeta: Meta[List[LocalTime]] =
    Meta[List[String]].xmap(_.map(t => LocalTime.parse(t)), lt => lt.map(_.toString))

  case class ScheduleRecord(id: Option[Int],
                            scheduleTrainId: ScheduleTrainId,
                            serviceCode: ServiceCode,
                            stpIndicator: StpIndicator,
                            trainCategory: Option[TrainCategory],
                            trainStatus: Option[TrainStatus],
                            atocCode: Option[AtocCode],
                            stopSequence: Int,
                            stanoxCode: StanoxCode,
                            subsequentStanoxCodes: List[StanoxCode],
                            subsequentArrivalTimes: List[LocalTime],
                            monday: Boolean,
                            tuesday: Boolean,
                            wednesday: Boolean,
                            thursday: Boolean,
                            friday: Boolean,
                            saturday: Boolean,
                            sunday: Boolean,
                            daysRunPattern: DaysRunPattern,
                            scheduleStart: LocalDate,
                            scheduleEnd: LocalDate,
                            locationType: LocationType,
                            arrivalTime: Option[LocalTime],
                            departureTime: Option[LocalTime])

  def addScheduleLogRecord(log: ScheduleRecord): Update0 =
    sql"""
      INSERT INTO schedule
      (schedule_train_id, service_code, stp_indicator, train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
      subsequent_arrival_times, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      days_run_pattern, schedule_start, schedule_end, location_type, arrival_time, departure_time)
      VALUES(${log.scheduleTrainId}, ${log.serviceCode}, ${log.stpIndicator}, ${log.trainCategory}, ${log.trainStatus}, ${log.atocCode}, ${log.stopSequence}, ${log.stanoxCode},
      ${log.subsequentStanoxCodes}, ${log.subsequentArrivalTimes}, ${log.monday}, ${log.tuesday}, ${log.wednesday},
      ${log.thursday}, ${log.friday}, ${log.saturday}, ${log.sunday}, ${log.daysRunPattern}, ${log.scheduleStart},
      ${log.scheduleEnd}, ${log.locationType}, ${log.arrivalTime}, ${log.departureTime})
     """.update

  type ScheduleLogToBeInserted = ((ScheduleTrainId,
                                   ServiceCode,
                                   StpIndicator,
                                   Option[TrainCategory],
                                   Option[TrainStatus],
                                   Option[AtocCode],
                                   Int,
                                   StanoxCode,
                                   List[StanoxCode],
                                   List[LocalTime]),
                                  (Boolean,
                                   Boolean,
                                   Boolean,
                                   Boolean,
                                   Boolean,
                                   Boolean,
                                   Boolean,
                                   DaysRunPattern,
                                   LocalDate,
                                   LocalDate,
                                   LocationType,
                                   Option[LocalTime],
                                   Option[LocalTime]))

  def addScheduleLogRecords(logs: List[ScheduleRecord]): ConnectionIO[Int] = {

    val toBeInserted: List[ScheduleLogToBeInserted] = logs.map(
      log =>
        ((log.scheduleTrainId,
          log.serviceCode,
          log.stpIndicator,
          log.trainCategory,
          log.trainStatus,
          log.atocCode,
          log.stopSequence,
          log.stanoxCode,
          log.subsequentStanoxCodes,
          log.subsequentArrivalTimes),
         (log.monday,
          log.tuesday,
          log.wednesday,
          log.thursday,
          log.friday,
          log.saturday,
          log.sunday,
          log.daysRunPattern,
          log.scheduleStart,
          log.scheduleEnd,
          log.locationType,
          log.arrivalTime,
          log.departureTime)))

    val sql = s"""
       |   INSERT INTO schedule
       |      (schedule_train_id, service_code, stp_indicator, train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
       |      subsequent_arrival_times,
       |      monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
       |      schedule_start, schedule_end, location_type, arrival_time, departure_time)
       |      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.stripMargin

    Update[ScheduleLogToBeInserted](sql).updateMany(toBeInserted)
  }

  def allScheduleLogRecords(): Query0[ScheduleRecord] =
    sql"""
      SELECT id, schedule_train_id, service_code,  stp_indicator,train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
      subsequent_arrival_times, monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
      schedule_start, schedule_end, location_type, arrival_time, departure_time
      FROM schedule
      """.query[ScheduleRecord]

  def scheduleRecordsFor(fromStation: StanoxCode,
                         toStation: StanoxCode,
                         daysRunPattern: DaysRunPattern,
                         stpIndicator: StpIndicator): Query0[ScheduleRecord] =
    sql"""
         SELECT id, schedule_train_id, service_code, stp_indicator, train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
         subsequent_arrival_times, monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
         schedule_start, schedule_end, location_type, arrival_time, departure_time
         FROM schedule
         WHERE stanox_code = ${fromStation}
         AND days_run_pattern = ${daysRunPattern}
         AND ${toStation} = ANY(subsequent_stanox_codes)
         AND stp_indicator = ${stpIndicator}
          """.query[ScheduleRecord]

  def scheduleRecordsFor(trainId: ScheduleTrainId, fromStation: StanoxCode): Query0[ScheduleRecord] =
    sql"""
         SELECT id, schedule_train_id, service_code, stp_indicator, train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
         subsequent_arrival_times, monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
         schedule_start, schedule_end, location_type, arrival_time, departure_time
         FROM schedule
         WHERE schedule_train_id = ${trainId} AND stanox_code = ${fromStation}
          """.query[ScheduleRecord]

  def scheduleRecordFor(id: Int) =
    sql"""
         SELECT id, schedule_train_id, service_code, stp_indicator, train_category, train_status, atoc_code, stop_sequence, stanox_code, subsequent_stanox_codes,
                subsequent_arrival_times, monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
                schedule_start, schedule_end, location_type, arrival_time, departure_time
         FROM schedule
         WHERE id = ${id}
          """.query[ScheduleRecord]

  def distinctStanoxCodes =
    sql"""
         SELECT DISTINCT stanox_code
         FROM schedule
          """.query[StanoxCode]

  def deleteAllScheduleLogRecords(): Update0 =
    sql"""DELETE FROM schedule""".update

  def deleteRecord(scheduleTrainId: ScheduleTrainId,
                   scheduleStartDate: LocalDate,
                   stpIndicator: StpIndicator): Update0 =
    sql"""DELETE FROM schedule
          WHERE schedule_train_id = ${scheduleTrainId}
          AND schedule_start = ${scheduleStartDate}
          AND stp_indicator = ${stpIndicator}
       """.update

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): ScheduleTable =
    new ScheduleTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(log: ScheduleRecord): IO[Unit] =
        ScheduleTable
          .addScheduleLogRecord(log)
          .run
          .transact(db)
          .map(_ => ())

      override def deleteAllRecords(): IO[Unit] =
        ScheduleTable.deleteAllScheduleLogRecords().run.transact(db).map(_ => ())

      override def addRecords(records: List[ScheduleRecord]): IO[Unit] =
        ScheduleTable.addScheduleLogRecords(records).transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[ScheduleRecord]] =
        ScheduleTable
          .allScheduleLogRecords()
          .list
          .transact(db)

      override def retrieveScheduleLogRecordsFor(from: StanoxCode,
                                                 to: StanoxCode,
                                                 pattern: DaysRunPattern,
                                                 stpIndicator: StpIndicator): IO[List[ScheduleRecord]] =
        ScheduleTable.scheduleRecordsFor(from, to, pattern, stpIndicator).list.transact(db)

      override def retrieveRecordBy(id: Int): IO[Option[ScheduleRecord]] =
        ScheduleTable.scheduleRecordFor(id).option.transact(db)

      override def retrieveAllDistinctStanoxCodes: IO[List[StanoxCode]] =
        ScheduleTable.distinctStanoxCodes.list.transact(db)

      override def retrieveScheduleLogRecordsFor(trainId: ScheduleTrainId, from: StanoxCode): IO[List[ScheduleRecord]] =
        ScheduleTable.scheduleRecordsFor(trainId, from).list.transact(db)

      override def deleteRecord(scheduleTrainId: ScheduleTrainId,
                                scheduleStartDate: LocalDate,
                                stpIndicator: StpIndicator): IO[Unit] =
        ScheduleTable.deleteRecord(scheduleTrainId, scheduleStartDate, stpIndicator).run.transact(db).map(_ => ())
    }

}
