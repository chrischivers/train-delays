package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.Stanox
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import traindelays.networkrail.scheduledata.TipLocRecord

trait TipLocTable extends Table[TipLocRecord] {
  def tipLocRecordFor(tipLocCode: TipLocCode): IO[Option[TipLocRecord]]

}

object TipLocTable {

  import doobie._
  import doobie.implicits._

  def addTiplocRecord(record: TipLocRecord): Update0 =
    sql"""
      INSERT INTO tiploc
      (tiploc_code, stanox, description)
      VALUES(${record.tipLocCode}, ${record.stanox}, ${record.description})
     """.update

  def allTiplocRecords(): Query0[TipLocRecord] =
    sql"""
      SELECT tiploc_code, stanox, description
      FROM tiploc
      """.query[TipLocRecord]

  def tipLocRecordFor(tipLocCode: TipLocCode) =
    sql"""SELECT tiploc_code, stanox, description
    FROM tiploc
    WHERE tiploc_code = ${tipLocCode.value}
      """.query[TipLocRecord]

  def apply(db: Transactor[IO]): TipLocTable =
    new TipLocTable {
      override def addRecord(record: TipLocRecord): IO[Unit] =
        TipLocTable
          .addTiplocRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def retrieveAllRecords(): IO[List[TipLocRecord]] =
        TipLocTable
          .allTiplocRecords()
          .list
          .transact(db)

      override def tipLocRecordFor(tipLocCode: TipLocCode): IO[Option[TipLocRecord]] =
        TipLocTable
          .tipLocRecordFor(tipLocCode)
          .option
          .transact(db)

    }
}
