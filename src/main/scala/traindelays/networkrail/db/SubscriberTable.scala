package traindelays.networkrail.db

import cats.effect.IO
import doobie.Transactor
import traindelays.networkrail.subscribers.SubscriberRecord

trait SubscriberTable extends Table[SubscriberRecord] {
  def subscriberRecordsFor(trainId: String, serviceCode: String, stanox: String): IO[List[SubscriberRecord]]
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

  def allSubscriberRecords(): Query0[SubscriberRecord] =
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
    }
}

object MemoizedSubscriberTable {
  def apply(db: Transactor[IO]) = new SubscriberTable {

    val subscriberTable = SubscriberTable(db)

    override def subscriberRecordsFor(trainId: String,
                                      serviceCode: String,
                                      stanox: String): IO[List[SubscriberRecord]] = ???

    override def addRecord(record: SubscriberRecord): IO[Unit] = ???

    override def retrieveAllRecords(): IO[List[SubscriberRecord]] = ???
  }
}
