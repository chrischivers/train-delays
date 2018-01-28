package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.scheduledata.StanoxRecord

import scala.concurrent.duration.FiniteDuration

trait StanoxTable extends MemoizedTable[StanoxRecord] {
  def stanoxRecordFor(stanoxCode: StanoxCode): IO[Option[StanoxRecord]]

  def addStanoxRecords(records: List[StanoxRecord]): IO[Unit]

  def deleteAllRecords(): IO[Unit]

}

object StanoxTable {

  import cats.instances.list._
  import doobie._
  import doobie.implicits._

  def addStanoxRecord(record: StanoxRecord): Update0 =
    sql"""
      INSERT INTO stanox
      (stanox_code, tiploc_code, crs, description)
      VALUES(${record.stanoxCode}, ${record.tipLocCode}, ${record.crs}, ${record.description})
     """.update

  def addStanoxRecords(records: List[StanoxRecord]) = {
    val sql =
      s"""
         |    INSERT INTO stanox
         |      (stanox_code, tiploc_code, crs, description)
         |      VALUES(?, ?, ?, ?)
  """.stripMargin

    Update[StanoxRecord](sql).updateMany(records)
  }

  def allStanoxRecords(): Query0[StanoxRecord] =
    sql"""
      SELECT stanox_code, tiploc_code, crs, description
      FROM stanox
      """.query[StanoxRecord]

  def stanoxRecordFor(stanoxCode: StanoxCode) =
    sql"""
    SELECT stanox_code, tiploc_code, crs, description
    FROM stanox
    WHERE stanox_code = ${stanoxCode.value}
      """.query[StanoxRecord]

  def deleteAllStanoxRecords(): Update0 =
    sql"""DELETE FROM stanox""".update

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): StanoxTable =
    new StanoxTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(record: StanoxRecord): IO[Unit] =
        StanoxTable
          .addStanoxRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def stanoxRecordFor(stanoxCode: StanoxCode): IO[Option[StanoxRecord]] =
        StanoxTable
          .stanoxRecordFor(stanoxCode)
          .option
          .transact(db)

      override def addStanoxRecords(records: List[StanoxRecord]): IO[Unit] =
        StanoxTable.addStanoxRecords(records).transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[StanoxRecord]] =
        StanoxTable
          .allStanoxRecords()
          .list
          .transact(db)

      override def deleteAllRecords(): IO[Unit] =
        StanoxTable.deleteAllStanoxRecords().run.transact(db).map(_ => ())

    }
}
