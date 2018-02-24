package traindelays.networkrail.db

import java.time.{LocalDate, LocalTime}

import cats.effect.IO
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.movementdata.MovementLog
import traindelays.networkrail.scheduledata.ScheduleTrainId

trait MovementLogTable extends NonMemoizedTable[MovementLog] {
  def retrieveRecordsFor(scheduleTrainId: ScheduleTrainId,
                         stanoxCodesAffected: Option[List[StanoxCode]],
                         fromTimestamp: Option[Long],
                         toTimestamp: Option[Long]): IO[List[MovementLog]]
}

object MovementLogTable {

  import doobie._
  import doobie.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[String]
    .xmap(LocalTime.parse(_), _.toString)

  implicit val localDateMeta: doobie.Meta[LocalDate] = doobie
    .Meta[String]
    .xmap(LocalDate.parse(_), _.toString)

  def addMovementLogRecord(record: MovementLog): Update0 =
    sql"""
      INSERT INTO movement_log
      (train_id, schedule_train_id, service_code, event_type, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, origin_departure_date, origin_departure_time, planned_passenger_timestamp, planned_passenger_time, actual_timestamp, actual_time, difference, variation_status)
      VALUES(${record.trainId}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.eventType}, ${record.toc}, ${record.stanoxCode}, ${record.originStanoxCode}, ${record.originDepartureTimestamp}, ${record.originDepartureDate}, ${record.originDepartureTime},
      ${record.plannedPassengerTimestamp}, ${record.plannedPassengerTime}, ${record.actualTimestamp}, ${record.actualTime}, ${record.difference}, ${record.variationStatus})
     """.update

  def allMovementLogRecords(): Query0[MovementLog] =
    sql"""
      SELECT id, train_id, schedule_train_id, service_code, event_type, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, origin_departure_date, origin_departure_time,
      planned_passenger_timestamp, planned_passenger_time, actual_timestamp, actual_time, difference, variation_status
      FROM movement_log
      """.query[MovementLog]

  def movementLogsFor(scheduleTrainId: ScheduleTrainId,
                      stanoxCodesAffected: Option[List[StanoxCode]],
                      fromTimestamp: Long,
                      toTimestamp: Long): Query0[MovementLog] = {

    import cats.data.NonEmptyList
    import cats.syntax.list._

    val selectFrom                   = fr"""
      SELECT id, train_id, schedule_train_id, service_code, event_type, toc, stanox_code, origin_stanox_code, origin_departure_timestamp, origin_departure_date, origin_departure_time,
      planned_passenger_timestamp, planned_passenger_time, actual_timestamp, actual_time, difference, variation_status
      FROM movement_log
      """
    val scheduleTrainIdCond          = fr"WHERE schedule_train_id = ${scheduleTrainId}"
    val originDepartureTimestampCond = fr"AND origin_departure_timestamp BETWEEN ${fromTimestamp} AND ${toTimestamp}"
    def stanoxCodesAffectedCond(stanoxCodes: NonEmptyList[StanoxCode]) =
      fr"AND " ++ Fragments.in(fr"stanox_code", stanoxCodes)

    val stanoxCodesNel = stanoxCodesAffected.flatMap(_.toNel)
    (selectFrom ++ scheduleTrainIdCond ++ originDepartureTimestampCond ++ stanoxCodesNel.fold(Fragment.empty)(nel =>
      stanoxCodesAffectedCond(nel))).query[MovementLog]

  }

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

      //TODO test this
      override def retrieveRecordsFor(scheduleTrainId: ScheduleTrainId,
                                      stanoxCodesAffected: Option[List[StanoxCode]],
                                      fromTimestamp: Option[Long],
                                      toTimestamp: Option[Long]): IO[List[MovementLog]] =
        MovementLogTable
          .movementLogsFor(scheduleTrainId,
                           stanoxCodesAffected,
                           fromTimestamp.getOrElse(0),
                           toTimestamp.getOrElse(Long.MaxValue))
          .list
          .transact(db)
    }
}
