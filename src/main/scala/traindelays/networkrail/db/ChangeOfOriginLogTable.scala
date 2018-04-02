package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.movementdata.ChangeOfOriginLog
import traindelays.networkrail.scheduledata.ScheduleTrainId

trait ChangeOfOriginLogTable extends NonMemoizedTable[ChangeOfOriginLog] {
  def retrieveRecordsFor(scheduleTrainId: ScheduleTrainId,
                         fromTimestamp: Option[Long],
                         toTimestamp: Option[Long]): IO[List[ChangeOfOriginLog]]

  val dbWriter: fs2.Sink[IO, ChangeOfOriginLog] = fs2.Sink { record =>
    safeAddRecord(record)
  }
}

object ChangeOfOriginLogTable {

  import doobie._
  import doobie.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[String]
    .xmap(LocalTime.parse(_), _.toString)

  implicit val localDateMeta: doobie.Meta[LocalDate] = doobie
    .Meta[String]
    .xmap(LocalDate.parse(_), _.toString)

  def add(record: ChangeOfOriginLog): Update0 =
    sql"""
      INSERT INTO change_of_origin_log
      (train_id, schedule_train_id, service_code, toc, new_stanox_code, origin_stanox_code, origin_departure_timestamp, origin_departure_date, origin_departure_time, reason_code)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.toc}, ${record.stanoxCode}, ${record.originStanoxCode}, ${record.originDepartureTimestamp},
      ${record.originDepartureDate}, ${record.originDepartureTime}, ${record.reasonCode})
     """.update

  def allChangeOfOriginLogRecords(): Query0[ChangeOfOriginLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, toc, new_stanox_code, origin_stanox_code,
      origin_departure_timestamp, origin_departure_date, origin_departure_time, reason_code
      FROM change_of_origin_log
      """.query[ChangeOfOriginLog]

  def changeOfOriginLogsFor(scheduleTrainId: ScheduleTrainId,
                            fromTimestamp: Long,
                            toTimestamp: Long): Query0[ChangeOfOriginLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, toc, new_stanox_code, origin_stanox_code,
      origin_departure_timestamp, origin_departure_date, origin_departure_time, reason_code
      FROM change_of_origin_log
      WHERE schedule_train_id = ${scheduleTrainId}
      AND origin_departure_timestamp BETWEEN ${fromTimestamp} AND ${toTimestamp}
      """.query[ChangeOfOriginLog]

  def apply(db: Transactor[IO]): ChangeOfOriginLogTable =
    new ChangeOfOriginLogTable {
      override def addRecord(record: ChangeOfOriginLog): IO[Unit] =
        ChangeOfOriginLogTable
          .add(record)
          .run
          .transact(db)
          .map(_ => ())

      override protected def retrieveAll(): IO[List[ChangeOfOriginLog]] =
        ChangeOfOriginLogTable
          .allChangeOfOriginLogRecords()
          .to[List]
          .transact(db)

      override def retrieveRecordsFor(scheduleTrainId: ScheduleTrainId,
                                      fromTimestamp: Option[Long],
                                      toTimestamp: Option[Long]): IO[List[ChangeOfOriginLog]] =
        ChangeOfOriginLogTable
          .changeOfOriginLogsFor(scheduleTrainId, fromTimestamp.getOrElse(0), toTimestamp.getOrElse(Long.MaxValue))
          .to[List]
          .transact(db)
    }
}
