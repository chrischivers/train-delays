package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.movementdata.{MovementLog, MovementRecord}
import traindelays.networkrail.scheduledata.TipLocRecord

trait MovementLogTable {

  def addRecord(record: MovementLog): IO[Unit]
  def retrieveAllRecords(): IO[List[MovementLog]]
}

/*
 id SERIAL PRIMARY KEY,
  train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  event_type VARCHAR(15) NOT NULL,
  stanox VARCHAR(10) NOT NULL,
  planned_passenger_timestamp TIMESTAMP NOT NULL,
  actual_timestamp TIMESTAMP NOT NULL,
  difference LONG NOT NULL

  case class MovementLog(id: Option[String],
                         trainId: String,
                         serviceCode: String,
                         eventType: String,
                         stanox: String,
                         plannedPassengerTimestamp: Long,
                         actualTimestamp: Long,
                         difference: Long)
 */
object MovementLogTable {

  import doobie._
  import doobie.implicits._

  def addMovementLogRecord(record: MovementLog): Update0 =
    sql"""
      INSERT INTO movement_log
      (train_id, service_code, event_type, stanox, planned_passenger_timestamp, actual_timestamp, difference)
      VALUES(${record.trainId}, ${record.serviceCode}, ${record.eventType}, ${record.stanox},
      ${record.plannedPassengerTimestamp}, ${record.actualTimestamp}, ${record.difference})
     """.update

  def allMovementLogRecords(): Query0[MovementLog] =
    sql"""
      SELECT id, train_id, service_code, event_type, stanox, planned_passenger_timestamp, actual_timestamp, difference
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

      override def retrieveAllRecords(): IO[List[MovementLog]] =
        MovementLogTable
          .allMovementLogRecords()
          .list
          .transact(db)
    }
}
