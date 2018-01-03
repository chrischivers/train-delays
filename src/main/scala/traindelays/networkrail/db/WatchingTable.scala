package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.watching.WatchingRecord

trait WatchingTable extends Table[WatchingRecord]

object WatchingTable {

  import doobie._
  import doobie.implicits._

  def addWatchingRecord(record: WatchingRecord): Update0 =
    sql"""
      INSERT INTO watching
      (user_id, train_id, service_code, stanox)
      VALUES(${record.userId}, ${record.trainId}, ${record.serviceCode}, ${record.stanox})
     """.update

  def allWatchingRecords(): Query0[WatchingRecord] =
    sql"""
      SELECT id, user_id, train_id, service_code, stanox
      from watching
      """.query[WatchingRecord]

  def apply(db: Transactor[IO]): WatchingTable =
    new WatchingTable {
      override def addRecord(record: WatchingRecord): IO[Unit] =
        WatchingTable
          .addWatchingRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveAllRecords(): IO[List[WatchingRecord]] =
        WatchingTable
          .allWatchingRecords()
          .list
          .transact(db)
    }
}
