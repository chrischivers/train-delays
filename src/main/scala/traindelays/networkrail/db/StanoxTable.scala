package traindelays.networkrail.db

import cats.effect.IO
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.scheduledata.DecodedStanoxRecord
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}

import scala.concurrent.duration.FiniteDuration
import scalacache.{Cache, Flags}
import scalacache.guava.GuavaCache
import scalacache.memoization.memoizeF

trait StanoxTable extends MemoizedTable[StanoxRecord] {
  def stanoxRecordsFor(stanoxCode: StanoxCode): IO[List[StanoxRecord]]

  def updateRecord(record: StanoxRecord): IO[Unit]

  def addRecords(records: List[StanoxRecord]): IO[Unit]

  def deleteAllRecords(): IO[Unit]

  def deleteRecord(tipLocCode: TipLocCode): IO[Unit]

  def retrieveAllNonEmptyRecords(forceRefesh: Boolean = false): IO[List[StanoxRecord]]

  val dbUpdater: fs2.Sink[IO, DecodedStanoxRecord] = fs2.Sink {
    case rec @ DecodedStanoxRecord.Create(_, _, _, _) => addRecord(rec.toStanoxRecord)
    case rec @ DecodedStanoxRecord.Update(_, _, _, _) => updateRecord(rec.toStanoxRecord)
    case rec @ DecodedStanoxRecord.Delete(_)          => deleteRecord(rec.tipLocCode)
  }

}

object StanoxTable {

  import cats.instances.list._
  import doobie._
  import doobie.implicits._

  case class StanoxRecord(tipLocCode: TipLocCode,
                          stanoxCode: Option[StanoxCode],
                          crs: Option[CRS],
                          description: Option[String],
                          primary: Option[Boolean] = None)

  object StanoxRecord {
    def stanoxRecordsToMap(stanoxRecords: List[StanoxRecord]): Map[TipLocCode, StanoxCode] =
      stanoxRecords
        .flatMap(rec => rec.stanoxCode.map(stanoxCode => rec.tipLocCode -> stanoxCode))
        .toMap
  }

  def addStanoxRecord(record: StanoxRecord): Update0 =
    sql"""
      INSERT INTO stanox
      (tiploc_code, stanox_code, crs, description, primary_entry)
      VALUES(${record.tipLocCode}, ${record.stanoxCode}, ${record.crs}, ${record.description}, ${record.primary})
     """.update

  def updateStanoxRecord(record: StanoxRecord): Update0 =
    sql"""
      INSERT INTO stanox
      ( tiploc_code, stanox_code, crs, description, primary_entry)
      VALUES(${record.tipLocCode}, ${record.stanoxCode}, ${record.crs}, ${record.description}, ${record.primary})
      ON CONFLICT (stanox_code, tiploc_code)
      DO UPDATE SET crs = ${record.crs}, description = ${record.description};
     """.update

  def addStanoxRecords(records: List[StanoxRecord]) = {
    val sql =
      s"""
         |    INSERT INTO stanox
         |      (tiploc_code, stanox_code, crs, description, primary_entry)
         |      VALUES(?, ?, ?, ?, ?)
  """.stripMargin

    Update[StanoxRecord](sql).updateMany(records)
  }

  def allStanoxRecords(): Query0[StanoxRecord] =
    sql"""
      SELECT tiploc_code, stanox_code, crs, description, primary_entry
      FROM stanox
      """.query[StanoxRecord]

  def allNonEmptyStanoxRecords(): Query0[StanoxRecord] =
    sql"""
      SELECT tiploc_code, stanox_code, crs, description, primary_entry
      FROM stanox
      WHERE crs IS NOT NULL
      AND stanox_code IS NOT NULL
      AND tiploc_code IS NOT NULL
      """.query[StanoxRecord]

  def stanoxRecordFor(stanoxCode: StanoxCode) =
    sql"""
    SELECT tiploc_code, stanox_code, crs, description, primary_entry
    FROM stanox
    WHERE stanox_code = ${stanoxCode.value}
      """.query[StanoxRecord]

  def deleteAllStanoxRecords(): Update0 =
    sql"""DELETE FROM stanox""".update

  def deleteRecord(tipLocCode: TipLocCode) =
    sql"""DELETE FROM stanox
          WHERE tiploc_code = ${tipLocCode}
       """.update

  def apply(db: Transactor[IO], memoizeDuration: FiniteDuration): StanoxTable =
    new StanoxTable {

      override val memoizeFor: FiniteDuration = memoizeDuration

      override def addRecord(record: StanoxRecord): IO[Unit] =
        StanoxTable
          .addStanoxRecord(record)
          .run
          .transact(db)
          .map(_ => ())

      override def stanoxRecordsFor(stanoxCode: StanoxCode): IO[List[StanoxRecord]] =
        StanoxTable
          .stanoxRecordFor(stanoxCode)
          .list
          .transact(db)

      override def addRecords(records: List[StanoxRecord]): IO[Unit] =
        StanoxTable.addStanoxRecords(records).transact(db).map(_ => ())

      override protected def retrieveAll(): IO[List[StanoxRecord]] =
        StanoxTable
          .allStanoxRecords()
          .list
          .transact(db)

      override def deleteAllRecords(): IO[Unit] =
        StanoxTable.deleteAllStanoxRecords().run.transact(db).map(_ => ())

      private val memoizeCacheWithCRS: Cache[List[StanoxRecord]] = GuavaCache[List[StanoxRecord]]

      override def retrieveAllNonEmptyRecords(forceRefresh: Boolean = false): IO[List[StanoxRecord]] =
        if (forceRefresh) {
          StanoxTable
            .allNonEmptyStanoxRecords()
            .list
            .transact(db)
        } else {
          memoizeF(Some(memoizeFor))(
            StanoxTable
              .allNonEmptyStanoxRecords()
              .list
              .transact(db)
          )(memoizeCacheWithCRS, scalacache.CatsEffect.modes.io, Flags.defaultFlags)
        }

      override def updateRecord(record: StanoxRecord): IO[Unit] =
        StanoxTable.updateStanoxRecord(record).run.transact(db).map(_ => ())

      override def deleteRecord(tipLocCode: TipLocCode): IO[Unit] =
        StanoxTable.deleteRecord(tipLocCode).run.transact(db).map(_ => ())
    }
}
