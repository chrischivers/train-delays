package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog}

trait CancellationLogTable extends Table[CancellationLog]

object CancellationLogTable {

  import doobie._
  import doobie.implicits._

  def addCancellationLogRecord(record: CancellationLog): Update0 =
    sql"""
      INSERT INTO cancellation_log
      (train_id, schedule_train_id, service_code, toc, stanox, cancellation_type, cancellation_reason_code)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.toc}, ${record.stanox},
      ${record.cancellationType}, ${record.cancellationReasonCode})
     """.update

  def allCancellationLogRecords(): Query0[CancellationLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, toc, stanox, cancellation_type, cancellation_reason_code
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

      override def retrieveAllRecords(): IO[List[CancellationLog]] =
        CancellationLogTable
          .allCancellationLogRecords()
          .list
          .transact(db)
    }
}
