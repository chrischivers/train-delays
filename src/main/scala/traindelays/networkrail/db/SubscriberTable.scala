package traindelays.networkrail.db

import cats.effect.IO
import traindelays.SubscribersConfig
import traindelays.networkrail.subscribers.SubscriberRecord

import scalacache.memoization._

trait SubscriberTable extends Table[SubscriberRecord] {
  def subscriberRecordsFor(trainId: String, serviceCode: String, stanox: String): IO[List[SubscriberRecord]]
  def deleteAllRecords(): IO[Unit]
}

object SubscriberTable {

  import doobie._
  import doobie.implicits._

  def addSubscriberRecord(record: SubscriberRecord): Update0 =
    sql"""
      INSERT INTO subscribers
      (user_id, email, train_id, service_code, stanox)
      VALUES(${record.userId}, ${record.email}, ${record.trainId}, ${record.serviceCode}, ${record.stanox})
     """.update

  protected def allSubscriberRecords(): Query0[SubscriberRecord] =
    sql"""
      SELECT id, user_id, email, train_id, service_code, stanox
      FROM subscribers
      """.query[SubscriberRecord]

  def apply(db: Transactor[IO]): SubscriberTable =
    new SubscriberTable {
      override def addRecord(record: SubscriberRecord): IO[Unit] =
        SubscriberTable
          .addSubscriberRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveAllRecords(): IO[List[SubscriberRecord]] =
        SubscriberTable
          .allSubscriberRecords()
          .list
          .transact(db)

      override def subscriberRecordsFor(trainId: String,
                                        serviceCode: String,
                                        stanox: String): IO[List[SubscriberRecord]] =
        sql"""
          SELECT id, user_id, email, train_id, service_code, stanox
          FROM subscribers
          WHERE train_id = ${trainId} AND service_code = ${serviceCode} AND stanox = ${stanox}
      """.query[SubscriberRecord].list.transact(db)

      override def deleteAllRecords(): IO[Unit] =
        SubscriberTable.deleteAllSubscriberRecords().run.transact(db).map(_ => ())
    }

  def deleteAllSubscriberRecords(): Update0 =
    sql"""DELETE FROM subscribers""".update
}

object MemoizedSubscriberTable {
  import doobie._
  import scalacache.CatsEffect.modes._
  import scalacache._
  import scalacache.guava._

  def apply(db: Transactor[IO], subscribersConfig: SubscribersConfig) = new SubscriberTable {

    val subscriberTable                                                = SubscriberTable(db)
    implicit val subscriberRecordsCache: Cache[List[SubscriberRecord]] = GuavaCache[List[SubscriberRecord]]

    override def subscriberRecordsFor(trainId: String,
                                      serviceCode: String,
                                      stanox: String): IO[List[SubscriberRecord]] =
      subscriberTable.subscriberRecordsFor(trainId, serviceCode, stanox)

    override def addRecord(record: SubscriberRecord): IO[Unit] = subscriberTable.addRecord(record)

    override def retrieveAllRecords(): IO[List[SubscriberRecord]] =
      memoizeF(Some(subscribersConfig.memoizeFor)) {
        subscriberTable.retrieveAllRecords()
      }

    override def deleteAllRecords(): IO[Unit] = subscriberTable.deleteAllRecords()
  }
}
