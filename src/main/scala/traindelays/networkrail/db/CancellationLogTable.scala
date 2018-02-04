package traindelays.networkrail.db

import java.sql.Timestamp
import java.time.Instant

import cats.effect.IO
import doobie.enum.JdbcType.Timestamp
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog}

trait CancellationLogTable extends NonMemoizedTable[CancellationLog]

object CancellationLogTable {

  import doobie._
  import doobie.implicits._

  def addCancellationLogRecord(record: CancellationLog): Update0 =
    sql"""
      INSERT INTO cancellation_log
      (train_id, schedule_train_id, service_code, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, cancellation_type, cancellation_reason_code)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.toc}, ${record.stanoxCode}, ${record.originStanoxCode}, ${record.originDepartureTimestamp},
      ${record.cancellationType}, ${record.cancellationReasonCode})
     """.update

  def allCancellationLogRecords(): Query0[CancellationLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, cancellation_type, cancellation_reason_code
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
