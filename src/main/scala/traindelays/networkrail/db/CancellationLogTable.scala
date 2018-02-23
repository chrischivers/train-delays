package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.movementdata.CancellationLog

trait CancellationLogTable extends NonMemoizedTable[CancellationLog]

object CancellationLogTable {

  import doobie._
  import doobie.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[String]
    .xmap(LocalTime.parse(_), _.toString)

  implicit val localDateMeta: doobie.Meta[LocalDate] = doobie
    .Meta[String]
    .xmap(LocalDate.parse(_), _.toString)

  def addCancellationLogRecord(record: CancellationLog): Update0 =
    sql"""
      INSERT INTO cancellation_log
      (train_id, schedule_train_id, service_code, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, origin_departure_date, origin_departure_time, cancellation_type, cancellation_reason_code)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.toc}, ${record.stanoxCode}, ${record.originStanoxCode}, ${record.originDepartureTimestamp},
      ${record.originDepartureDate}, ${record.originDepartureTime}, ${record.cancellationType}, ${record.cancellationReasonCode})
     """.update

  def allCancellationLogRecords(): Query0[CancellationLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, toc, stanox_code, origin_stanox_code, origin_departure_timestamp,
      origin_departure_date, origin_departure_time, cancellation_type, cancellation_reason_code
      from cancellation_log
      """.query[CancellationLog]

  def apply(db: Transactor[IO]): CancellationLogTable =
    new CancellationLogTable {
      override def addRecord(record: CancellationLog): IO[Unit] =
        CancellationLogTable
          .addCancellationLogRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override protected def retrieveAll(): IO[List[CancellationLog]] =
        CancellationLogTable
          .allCancellationLogRecords()
          .list
          .transact(db)
    }
}
