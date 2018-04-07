package traindelays.networkrail.db

import java.time.LocalTime

import cats.effect.IO
import traindelays.networkrail.ServiceCode
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.{SubscriberRecord, UserId}

import scala.concurrent.duration.FiniteDuration

trait SubscriberTable extends MemoizedTable[SubscriberRecord] {
  def subscriberRecordsFor(scheduleTrainId: ScheduleTrainId, serviceCode: ServiceCode): IO[List[SubscriberRecord]]

  def subscriberRecordsFor(userId: UserId): IO[List[SubscriberRecord]]

  def subscriberRecordFor(id: Int): IO[Option[SubscriberRecord]]

  def deleteRecord(id: Int): IO[Unit]

  def deleteAllRecords(): IO[Unit]
}

object SubscriberTable {

  import doobie._
  import doobie.implicits._

  implicit val localTimeMeta: doobie.Meta[LocalTime] = doobie
    .Meta[java.sql.Time]
    .xmap(t => LocalTime.of(t.toLocalTime.getHour, t.toLocalTime.getMinute), lt => java.sql.Time.valueOf(lt))

  def addSubscriberRecord(record: SubscriberRecord): Update0 =
    sql"""
      INSERT INTO subscribers
      (user_id, email, email_verified, name, first_name, family_name, locale, schedule_train_id, service_code, from_stanox_code, from_crs, departure_time, to_stanox_code, to_crs, arrival_time, days_run_pattern, subscribe_timestamp)
      VALUES(${record.userId}, ${record.emailAddress}, ${record.emailVerified}, ${record.name}, ${record.firstName}, ${record.familyName},
      ${record.locale}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.fromStanoxCode},  ${record.fromCRS}, ${record.departureTime},
      ${record.toStanoxCode}, ${record.toCRS}, ${record.arrivalTime}, ${record.daysRunPattern}, now())
     """.update

  protected def allSubscriberRecords(): Query0[SubscriberRecord] =
    sql"""
      SELECT id, user_id, email, email_verified, name, first_name, family_name, locale, schedule_train_id,
      service_code, from_stanox_code, from_crs, departure_time, to_stanox_code, to_crs, arrival_time, days_run_pattern
      FROM subscribers
      """.query[SubscriberRecord]

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): SubscriberTable =
    new SubscriberTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(record: SubscriberRecord): IO[Unit] =
        SubscriberTable
          .addSubscriberRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def subscriberRecordsFor(scheduleTrainId: ScheduleTrainId,
                                        serviceCode: ServiceCode): IO[List[SubscriberRecord]] =
        sql"""
          SELECT id, user_id, email, email_verified, name, first_name, family_name, locale, schedule_train_id,
          service_code, from_stanox_code, from_crs, departure_time, to_stanox_code, to_crs, arrival_time, days_run_pattern
          FROM subscribers
          WHERE schedule_train_id = ${scheduleTrainId} AND service_code = ${serviceCode}
      """.query[SubscriberRecord].to[List].transact(db)

      override def deleteAllRecords(): IO[Unit] =
        deleteAllSubscriberRecords().run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[SubscriberRecord]] =
        SubscriberTable
          .allSubscriberRecords()
          .to[List]
          .transact(db)

      override def subscriberRecordsFor(userId: UserId): IO[List[SubscriberRecord]] =
        sql"""
          SELECT id, user_id, email, email_verified, name, first_name, family_name, locale, schedule_train_id,
          service_code, from_stanox_code, from_crs, departure_time, to_stanox_code, to_crs, arrival_time, days_run_pattern
          FROM subscribers
          WHERE user_id = ${userId}
      """.query[SubscriberRecord].to[List].transact(db)

      override def deleteRecord(id: Int): IO[Unit] =
        sql"""DELETE FROM subscribers WHERE id = ${id}""".update.run.transact(db).map(_ => ())

      def deleteAllSubscriberRecords(): Update0 =
        sql"""DELETE FROM subscribers""".update

      override def subscriberRecordFor(id: Int): IO[Option[SubscriberRecord]] =
        sql"""
             SELECT id, user_id, email, email_verified, name, first_name, family_name, locale, schedule_train_id,
             service_code, from_stanox_code, from_crs, departure_time, to_stanox_code, to_crs, arrival_time, days_run_pattern
             FROM subscribers
             WHERE id = ${id}
           """.query[SubscriberRecord].option.transact(db)
    }
}
