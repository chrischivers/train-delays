package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.scheduledata.ScheduleRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}

trait ScheduleTable {

  def addRecord(record: ScheduleRecord): IO[Unit]
  def retrieveAllRecords(): IO[List[ScheduleRecord]]
}

object ScheduleTable {

  import cats.instances.list._
  import cats.syntax.traverse._
  import doobie._
  import doobie.implicits._

  implicit val LocalTimeMeta: Meta[LocalTime] = Meta[java.sql.Time].xmap(
    t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute),
    lt => new java.sql.Time(lt.getHour, lt.getMinute, lt.getSecond))

  case class RetrievedScheduleRecord(trainId: String,
                                     serviceCode: String,
                                     monday: Boolean,
                                     tuesday: Boolean,
                                     wednesday: Boolean,
                                     thursday: Boolean,
                                     friday: Boolean,
                                     saturday: Boolean,
                                     sunday: Boolean,
                                     scheduleStart: LocalDate,
                                     scheduleEnd: LocalDate,
                                     locationType: String,
                                     tiplocCode: String,
                                     arrivalTime: Option[LocalTime],
                                     departureTime: Option[LocalTime])

  def addScheduleRecords(record: ScheduleRecord): List[Update0] =
    record.locationRecords.map { locationRecord =>
      sql"""
      INSERT INTO schedule
      (train_id, service_code, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, tiploc_code, arrival_time, departure_time)
      VALUES(${record.trainUid}, ${record.trainServiceCode}, ${record.daysRun.monday}, ${record.daysRun.tuesday},
        ${record.daysRun.wednesday}, ${record.daysRun.thursday}, ${record.daysRun.friday}, ${record.daysRun.saturday},
        ${record.daysRun.sunday}, ${record.scheduleStartDate}, ${record.scheduleEndDate}, ${locationRecord.locationType},
        ${locationRecord.tiplocCode}, ${locationRecord.arrivalTime}, ${locationRecord.departureTime})
     """.update
    }

  def allScheduleRecords(): Query0[RetrievedScheduleRecord] =
    sql"""
      SELECT train_id, service_code, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, tiploc_code, arrival_time, departure_time
      from schedule
      """.query[RetrievedScheduleRecord]

  private def toScheduleRecords(retrieved: List[RetrievedScheduleRecord]): List[ScheduleRecord] =
    retrieved
      .groupBy(
        r =>
          (r.trainId,
           r.serviceCode,
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
        case ((trainId, serviceCode, scheduleStart, scheduleEnd, mon, tue, wed, thu, fri, sat, sun), recs) =>
          ScheduleRecord(
            trainId,
            serviceCode,
            DaysRun(mon, tue, wed, thu, fri, sat, sun),
            scheduleStart,
            scheduleEnd,
            recs.map(rec =>
              ScheduleLocationRecord(rec.locationType, rec.tiplocCode, rec.arrivalTime, rec.departureTime))
          )
      }
      .toList

  def apply(db: Transactor[IO]): ScheduleTable =
    new ScheduleTable {
      override def addRecord(record: ScheduleRecord): IO[Unit] =
        ScheduleTable
          .addScheduleRecords(record)
          .map(update => update.run.transact(db))
          .sequence[IO, Int]
          .map(_ => ())

      override def retrieveAllRecords(): IO[List[ScheduleRecord]] =
        ScheduleTable
          .allScheduleRecords()
          .list
          .transact(db)
          .map(retrieved => toScheduleRecords(retrieved))
    }
}
