package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.{Decoder, Encoder, Json}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.{LocationType, TipLocCode}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleRecord, ScheduleTrainId}
import traindelays.networkrail.{ServiceCode, Stanox}

import scala.concurrent.duration.FiniteDuration

trait ScheduleTable extends MemoizedTable[ScheduleLog] {

  def deleteAllRecords(): IO[Unit]

  def retrieveAllScheduleRecords(): IO[List[ScheduleRecord]]

  def addRecords(records: List[ScheduleLog]): IO[Unit]

  def retrieveScheduleLogRecordsFor(from: TipLocCode, to: TipLocCode, pattern: DaysRunPattern): IO[List[ScheduleLog]]

  val dbWriterMultiple: fs2.Sink[IO, List[ScheduleLog]] = fs2.Sink { records =>
    addRecords(records)
  }
}

object ScheduleTable extends StrictLogging {

  import cats.instances.list._
  import doobie._
  import doobie.implicits._
  import doobie.postgres._, doobie.postgres.implicits._
  import io.circe.java8.time._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[java.sql.Time]
    .xmap(t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute),
          lt => new java.sql.Time(lt.getHour, lt.getMinute, lt.getSecond))

  implicit val localTimeListMeta: Meta[List[LocalTime]] =
    Meta[List[String]].xmap(_.map(t => LocalTime.parse(t)), lt => lt.map(_.toString))

  case class ScheduleLog(id: Option[Int],
                         scheduleTrainId: ScheduleTrainId,
                         serviceCode: ServiceCode,
                         atocCode: AtocCode,
                         stopSequence: Int,
                         tiplocCode: TipLocCode,
                         subsequentTipLocCodes: List[TipLocCode],
                         subsequentArrivalTimes: List[LocalTime],
                         stanox: Stanox,
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
                         departureTime: Option[LocalTime]) {

    def matchesKeyFields(that: ScheduleLog): Boolean =
      that.scheduleTrainId == scheduleTrainId &&
        that.serviceCode == serviceCode &&
        that.tiplocCode == tiplocCode &&
        that.stopSequence == stopSequence &&
        that.scheduleStart == scheduleStart &&
        that.scheduleEnd == scheduleEnd
  }
  object ScheduleLog {
    import io.circe.generic.semiauto._
    import io.circe.java8.time._

//    implicit val encoder: Encoder[ScheduleLog] = deriveEncoder[ScheduleLog]

    sealed trait DaysRunPattern {
      val string: String
    }

    object DaysRunPattern {

      case object Weekdays extends DaysRunPattern {
        override val string: String = "weekdays"
      }
      case object Saturdays extends DaysRunPattern {
        override val string: String = "saturdays"
      }
      case object Sundays extends DaysRunPattern {
        override val string: String = "sundays"
      }

      import doobie.util.meta.Meta

      def fromString(str: String): Option[DaysRunPattern] =
        str.toLowerCase match {
          case Weekdays.string  => Some(Weekdays)
          case Saturdays.string => Some(Saturdays)
          case Sundays.string   => Some(Sundays)
          case _                => None
        }
      implicit val decoder: Decoder[DaysRunPattern] = Decoder.decodeString.map(str =>
        fromString(str).getOrElse {
          logger.error(s"Unknown days run pattern [$str]. Defaulting to 'weekdays'")
          Weekdays
      })

      implicit val encoder: Encoder[DaysRunPattern] = (a: DaysRunPattern) => Json.fromString(a.string)

      implicit val meta: Meta[DaysRunPattern] =
        Meta[String].xmap(str => DaysRunPattern.fromString(str).getOrElse(Weekdays), _.string)
    }
  }

  def addScheduleLogRecord(log: ScheduleLog): Update0 =
    sql"""
      INSERT INTO schedule
      (schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, subsequent_tip_loc_codes, 
      subsequent_arrival_times, stanox, monday, tuesday, wednesday, thursday, friday, saturday, sunday, 
      days_run_pattern, schedule_start, schedule_end, location_type, arrival_time, departure_time)
      VALUES(${log.scheduleTrainId}, ${log.serviceCode}, ${log.atocCode}, ${log.stopSequence}, ${log.tiplocCode},
      ${log.subsequentTipLocCodes}, ${log.subsequentArrivalTimes},
      ${log.stanox}, ${log.monday}, ${log.tuesday}, ${log.wednesday}, ${log.thursday}, ${log.friday}, ${log.saturday},
        ${log.sunday}, ${log.daysRunPattern}, ${log.scheduleStart}, ${log.scheduleEnd}, ${log.locationType},
        ${log.arrivalTime}, ${log.departureTime})
     """.update

  type ScheduleLogToBeInserted = (ScheduleTrainId,
                                  ServiceCode,
                                  AtocCode,
                                  Int,
                                  TipLocCode,
                                  List[TipLocCode],
                                  List[LocalTime],
                                  Stanox,
                                  Boolean,
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
                                  Option[LocalTime])

  def addScheduleLogRecords(logs: List[ScheduleLog]): ConnectionIO[Int] = {

    val toBeInserted: List[ScheduleLogToBeInserted] = logs.map(
      log =>
        (log.scheduleTrainId,
         log.serviceCode,
         log.atocCode,
         log.stopSequence,
         log.tiplocCode,
         log.subsequentTipLocCodes,
         log.subsequentArrivalTimes,
         log.stanox,
         log.monday,
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
         log.departureTime))

    val sql = s"""
       |   INSERT INTO schedule
       |      (schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, subsequent_tip_loc_codes,
       |      subsequent_arrival_times, stanox,
       |      monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
       |      schedule_start, schedule_end, location_type, arrival_time, departure_time)
       |      VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """.stripMargin

    Update[ScheduleLogToBeInserted](sql).updateMany(toBeInserted)
  }

  def allScheduleLogRecords(): Query0[ScheduleLog] =
    sql"""
      SELECT id, schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, subsequent_tip_loc_codes,
      subsequent_arrival_times, stanox,
      monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
      schedule_start, schedule_end, location_type, arrival_time, departure_time
      FROM schedule
      """.query[ScheduleLog]

  def scheduleRecordsFor(fromStation: TipLocCode,
                         toStation: TipLocCode,
                         daysRunPattern: DaysRunPattern): Query0[ScheduleLog] =
    //TODO something with dates (only main valid dates)
    sql"""  
         SELECT id, schedule_train_id, service_code, atoc_code, stop_sequence, tiploc_code, subsequent_tip_loc_codes,
         subsequent_arrival_times, stanox,
         monday, tuesday, wednesday, thursday, friday, saturday, sunday, days_run_pattern,
         schedule_start, schedule_end, location_type, arrival_time, departure_time
         FROM schedule
         WHERE days_run_pattern = ${daysRunPattern} AND tiploc_code = ${fromStation} AND ${toStation} = ANY(subsequent_tip_loc_codes)
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

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): ScheduleTable =
    new ScheduleTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(log: ScheduleLog): IO[Unit] =
        ScheduleTable
          .addScheduleLogRecord(log)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveAllScheduleRecords(): IO[List[ScheduleRecord]] =
        retrieveAllRecords().map(retrieved => toScheduleRecords(retrieved))

      override def deleteAllRecords(): IO[Unit] =
        ScheduleTable.deleteAllScheduleLogRecords().run.transact(db).map(_ => ())

      override def addRecords(records: List[ScheduleLog]): IO[Unit] =
        ScheduleTable.addScheduleLogRecords(records).transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[ScheduleLog]] =
        ScheduleTable
          .allScheduleLogRecords()
          .list
          .transact(db)

      override def retrieveScheduleLogRecordsFor(from: TipLocCode,
                                                 to: TipLocCode,
                                                 pattern: DaysRunPattern): IO[List[ScheduleLog]] =
        ScheduleTable.scheduleRecordsFor(from, to, pattern).list.transact(db)
    }

}
