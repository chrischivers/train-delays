package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleRecord, ScheduleTrainId}
import traindelays.networkrail.{ServiceCode, Stanox}

trait ScheduleTable extends Table[ScheduleLog] {

  def deleteAllRecords(): IO[Unit]

  def retrieveAllScheduleRecords(): IO[List[ScheduleRecord]]

  def addRecords(records: List[ScheduleLog]): IO[Unit]

  val dbWriterMultiple: fs2.Sink[IO, List[ScheduleLog]] = fs2.Sink { records =>
    addRecords(records)
  }
}

object ScheduleTable {

  import cats.instances.list._
  import doobie._
  import doobie.implicits._

  implicit val LocalTimeMeta: Meta[LocalTime] = Meta[java.sql.Time].xmap(
    t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute),
    lt => new java.sql.Time(lt.getHour, lt.getMinute, lt.getSecond))

  case class ScheduleLog(id: Option[Int],
                         scheduleTrainId: ScheduleTrainId,
                         serviceCode: ServiceCode,
                         atocCode: AtocCode,
                         stopSequence: Int,
                         tiplocCode: TipLocCode,
                         stanox: Stanox,
                         monday: Boolean,
                         tuesday: Boolean,
                         wednesday: Boolean,
                         thursday: Boolean,
                         friday: Boolean,
                         saturday: Boolean,
                         sunday: Boolean,
                         scheduleStart: LocalDate,
                         scheduleEnd: LocalDate,
                         locationType: LocationType,
                         arrivalTime: Option[LocalTime],
                         departureTime: Option[LocalTime]) {

    def matchesKeyFields(that: ScheduleLog): Boolean =
      that.scheduleTrainId == scheduleTrainId &&
        that.serviceCode == serviceCode &&
        that.tiplocCode == tiplocCode &&
        that.stopSequence == stopSequence &&
        that.scheduleStart == scheduleStart &&
        that.scheduleEnd == scheduleEnd
  }

  def addScheduleLogRecord(log: ScheduleLog): Update0 =
    sql"""
      INSERT INTO schedule
      (schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, stanox,
      monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, arrival_time, departure_time)
      VALUES(${log.scheduleTrainId}, ${log.serviceCode}, ${log.atocCode}, ${log.stopSequence}, ${log.tiplocCode},
      ${log.stanox}, ${log.monday}, ${log.tuesday}, ${log.wednesday}, ${log.thursday}, ${log.friday}, ${log.saturday},
        ${log.sunday}, ${log.scheduleStart}, ${log.scheduleEnd}, ${log.locationType},
        ${log.arrivalTime}, ${log.departureTime})
     """.update

  type ScheduleLogToBeInserted = (ScheduleTrainId,
                                  ServiceCode,
                                  AtocCode,
                                  Int,
                                  TipLocCode,
                                  Stanox,
                                  Boolean,
                                  Boolean,
                                  Boolean,
                                  Boolean,
                                  Boolean,
                                  Boolean,
                                  Boolean,
                                  LocalDate,
                                  LocalDate,
                                  LocationType,
                                  Option[LocalTime],
                                  Option[LocalTime])

  def addScheduleLogRecords(logs: List[ScheduleLog]): ConnectionIO[Int] = {

    val toBeInserted: List[ScheduleLogToBeInserted] = logs.map(
      log =>
        (log.scheduleTrainId,
         log.serviceCode,
         log.atocCode,
         log.stopSequence,
         log.tiplocCode,
         log.stanox,
         log.monday,
         log.tuesday,
         log.wednesday,
         log.thursday,
         log.friday,
         log.saturday,
         log.sunday,
         log.scheduleStart,
         log.scheduleEnd,
         log.locationType,
         log.arrivalTime,
         log.departureTime))

    val sql = s"""
       |   INSERT INTO schedule
       |      (schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, stanox,
       |      monday, tuesday, wednesday, thursday, friday, saturday, sunday,
       |      schedule_start, schedule_end, location_type, arrival_time, departure_time)
       |      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.stripMargin

    Update[ScheduleLogToBeInserted](sql).updateMany(toBeInserted)
  }

  def allScheduleLogRecords(): Query0[ScheduleLog] =
    sql"""
      SELECT id, schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, stanox,
      monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, arrival_time, departure_time
      from schedule
      """.query[ScheduleLog]

  def deleteAllScheduleLogRecords(): Update0 =
    sql"""DELETE FROM schedule""".update

  private def toScheduleRecords(retrieved: List[ScheduleLog]): List[ScheduleRecord] =
    retrieved
      .groupBy(
        r =>
          (r.scheduleTrainId,
           r.serviceCode,
           r.atocCode,
           r.scheduleStart,
           r.scheduleEnd,
           r.monday,
           r.tuesday,
           r.wednesday,
           r.thursday,
           r.friday,
           r.saturday,
           r.sunday))
      .map {
        case ((scheduleTrainId, serviceCode, atocCode, scheduleStart, scheduleEnd, mon, tue, wed, thu, fri, sat, sun),
              recs) =>
          ScheduleRecord(
            scheduleTrainId,
            serviceCode,
            atocCode,
            DaysRun(mon, tue, wed, thu, fri, sat, sun),
            scheduleStart,
            scheduleEnd,
            recs
              .sortBy(_.stopSequence)
              .map(rec => ScheduleLocationRecord(rec.locationType, rec.tiplocCode, rec.arrivalTime, rec.departureTime))
          )
      }
      .toList

  def apply(db: Transactor[IO]): ScheduleTable =
    new ScheduleTable {
      override def addRecord(log: ScheduleLog): IO[Unit] =
        ScheduleTable
          .addScheduleLogRecord(log)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveAllRecords(): IO[List[ScheduleLog]] =
        ScheduleTable
          .allScheduleLogRecords()
          .list
          .transact(db)

      override def retrieveAllScheduleRecords(): IO[List[ScheduleRecord]] =
        retrieveAllRecords().map(retrieved => toScheduleRecords(retrieved))

      override def deleteAllRecords(): IO[Unit] =
        ScheduleTable.deleteAllScheduleLogRecords().run.transact(db).map(_ => ())

      override def addRecords(records: List[ScheduleLog]): IO[Unit] =
        ScheduleTable.addScheduleLogRecords(records).transact(db).map(_ => ())
    }

}
