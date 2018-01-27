package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import traindelays.networkrail.scheduledata.TipLocRecord

import scala.concurrent.duration.FiniteDuration

trait TipLocTable extends MemoizedTable[TipLocRecord] {
  def tipLocRecordFor(tipLocCode: TipLocCode): IO[Option[TipLocRecord]]

  def addTipLocRecords(records: List[TipLocRecord]): IO[Unit]

  def deleteAllRecords(): IO[Unit]

}

object TipLocTable {

  import doobie._
  import doobie.implicits._
  import cats.instances.list._

  def addTiplocRecord(record: TipLocRecord): Update0 =
    sql"""
      INSERT INTO tiploc
      (tiploc_code, stanox, crs, description)
      VALUES(${record.tipLocCode}, ${record.stanox}, ${record.crs}, ${record.description})
     """.update

  def addTiplocRecords(records: List[TipLocRecord]) = {
    val sql =
      s"""
         |    INSERT INTO tiploc
         |      (tiploc_code, stanox, crs, description)
         |      VALUES(?, ?, ?, ?)
  """.stripMargin

    Update[TipLocRecord](sql).updateMany(records)
  }

  def allTiplocRecords(): Query0[TipLocRecord] =
    sql"""
      SELECT tiploc_code, stanox, crs, description
      FROM tiploc
      """.query[TipLocRecord]

  def tipLocRecordFor(tipLocCode: TipLocCode) =
    sql"""SELECT tiploc_code, stanox, crs, description
    FROM tiploc
    WHERE tiploc_code = ${tipLocCode.value}
      """.query[TipLocRecord]

  def deleteAllTiplocRecords(): Update0 =
    sql"""DELETE FROM tiploc""".update

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): TipLocTable =
    new TipLocTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(record: TipLocRecord): IO[Unit] =
        TipLocTable
          .addTiplocRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def tipLocRecordFor(tipLocCode: TipLocCode): IO[Option[TipLocRecord]] =
        TipLocTable
          .tipLocRecordFor(tipLocCode)
          .option
          .transact(db)

      override def addTipLocRecords(records: List[TipLocRecord]): IO[Unit] =
        TipLocTable.addTiplocRecords(records).transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[TipLocRecord]] =
        TipLocTable
          .allTiplocRecords()
          .list
          .transact(db)

      override def deleteAllRecords(): IO[Unit] =
        TipLocTable.deleteAllTiplocRecords().run.transact(db).map(_ => ())

    }
}
