package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.movementdata.MovementLog

trait MovementLogTable extends NonMemoizedTable[MovementLog]

object MovementLogTable {

  import doobie._
  import doobie.implicits._

  def addMovementLogRecord(record: MovementLog): Update0 =
    sql"""
      INSERT INTO movement_log
      (train_id, schedule_train_id, service_code, event_type, toc, stanox_code, planned_passenger_timestamp, actual_timestamp, difference, variation_status)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.eventType}, ${record.toc}, ${record.stanoxCode},
      ${record.plannedPassengerTimestamp}, ${record.actualTimestamp}, ${record.difference}, ${record.variationStatus})
     """.update

  def allMovementLogRecords(): Query0[MovementLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, event_type, toc, stanox_code, planned_passenger_timestamp, actual_timestamp, difference, variation_status
      from movement_log
      """.query[MovementLog]

  def apply(db: Transactor[IO]): MovementLogTable =
    new MovementLogTable {
      override def addRecord(record: MovementLog): IO[Unit] =
        MovementLogTable
          .addMovementLogRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override protected def retrieveAll(): IO[List[MovementLog]] =
        MovementLogTable
          .allMovementLogRecords()
          .list
          .transact(db)
    }
}
