package traindelays.networkrail.db

import cats.effect.IO
import traindelays.SubscribersConfig
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.{ServiceCode, Stanox}

import scala.concurrent.duration.FiniteDuration
import scalacache.memoization._

trait SubscriberTable extends MemoizedTable[SubscriberRecord] {
  def subscriberRecordsFor(scheduleTrainId: ScheduleTrainId,
                           serviceCode: ServiceCode,
                           stanox: Stanox): IO[List[SubscriberRecord]]
  def deleteAllRecords(): IO[Unit]
}

object SubscriberTable {

  import doobie._
  import doobie.implicits._

  def addSubscriberRecord(record: SubscriberRecord): Update0 =
    sql"""
      INSERT INTO subscribers
      (user_id, email, schedule_train_id, service_code, stanox)
      VALUES(${record.userId}, ${record.email}, ${record.scheduleTrainId}, ${record.serviceCode}, ${record.stanox})
     """.update

  protected def allSubscriberRecords(): Query0[SubscriberRecord] =
    sql"""
      SELECT id, user_id, email, schedule_train_id, service_code, stanox
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
                                        serviceCode: ServiceCode,
                                        stanox: Stanox): IO[List[SubscriberRecord]] =
        sql"""
          SELECT id, user_id, email, schedule_train_id, service_code, stanox
          FROM subscribers
          WHERE schedule_train_id = ${scheduleTrainId} AND service_code = ${serviceCode} AND stanox = ${stanox}
      """.query[SubscriberRecord].list.transact(db)

      override def deleteAllRecords(): IO[Unit] =
        SubscriberTable.deleteAllSubscriberRecords().run.transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[SubscriberRecord]] =
        SubscriberTable
          .allSubscriberRecords()
          .list
          .transact(db)
    }

  def deleteAllSubscriberRecords(): Update0 =
    sql"""DELETE FROM subscribers""".update
}
