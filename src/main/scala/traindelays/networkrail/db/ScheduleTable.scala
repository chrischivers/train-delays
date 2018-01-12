package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.ServiceCode
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleRecord, ScheduleTrainId}

trait ScheduleTable extends Table[ScheduleRecord] {

  def deleteAllRecords(): IO[Unit]
}

object ScheduleTable {

  import cats.instances.list._
  import cats.syntax.traverse._
  import doobie._
  import doobie.implicits._

  implicit val LocalTimeMeta: Meta[LocalTime] = Meta[java.sql.Time].xmap(
    t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute),
    lt => new java.sql.Time(lt.getHour, lt.getMinute, lt.getSecond))

  case class RetrievedScheduleRecord(scheduleTrainId: ScheduleTrainId,
                                     serviceCode: ServiceCode,
                                     atocCode: AtocCode,
                                     stopSequence: Int,
                                     tiplocCode: TipLocCode,
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
                                     departureTime: Option[LocalTime])

  def addScheduleRecords(record: ScheduleRecord): List[Update0] =
    record.locationRecords.zipWithIndex.map {
      case (locationRecord, index) =>
        sql"""
      INSERT INTO schedule
      (schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, arrival_time, departure_time)
      VALUES(${record.scheduleTrainId}, ${record.trainServiceCode}, ${record.atocCode}, ${index + 1}, ${locationRecord.tiplocCode}, ${record.daysRun.monday},
        ${record.daysRun.tuesday}, ${record.daysRun.wednesday}, ${record.daysRun.thursday}, ${record.daysRun.friday}, ${record.daysRun.saturday},
        ${record.daysRun.sunday}, ${record.scheduleStartDate}, ${record.scheduleEndDate}, ${locationRecord.locationType},
        ${locationRecord.arrivalTime}, ${locationRecord.departureTime})
     """.update
    }

  def allScheduleRecords(): Query0[RetrievedScheduleRecord] =
    sql"""
      SELECT schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      schedule_start, schedule_end, location_type, arrival_time, departure_time
      from schedule
      """.query[RetrievedScheduleRecord]

  def deleteAllScheduleRecords(): Update0 =
    sql"""DELETE FROM schedule""".update

  private def toScheduleRecords(retrieved: List[RetrievedScheduleRecord]): List[ScheduleRecord] =
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

      override def deleteAllRecords(): IO[Unit] = ScheduleTable.deleteAllScheduleRecords().run.transact(db).map(_ => ())
    }

}
