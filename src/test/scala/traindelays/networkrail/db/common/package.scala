package traindelays.networkrail.db

import cats.effect.IO
import doobie.hikari.HikariTransactor
import fs2.Stream
import org.scalatest.Matchers.fail
import traindelays.DatabaseConfig
import traindelays.networkrail.db
import traindelays.networkrail.movementdata.MovementLog
import traindelays.networkrail.scheduledata.{ScheduleRecord, TipLocRecord}

import scala.concurrent.ExecutionContext
import scala.util.Random

package object common {

  import doobie._
  import doobie.implicits._

  implicit class DBExt(db: Transactor[IO]) {
    val scheduleTable = ScheduleTable(db)

    def clean: IO[Int] =
      sql"DELETE FROM schedule".update.run.transact(db)
  }

  def testDatabaseConfig() = {
    val databaseName = s"test-${System.currentTimeMillis()}-${Random.nextInt(99)}-${Random.nextInt(99)}"
    DatabaseConfig(
      "org.h2.Driver",
      "jdbc:h2:mem:" + databaseName + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
      "sa",
      "",
      migrationScripts = List("db/migration/common")
    )
  }

  val testTransactor = db.transactor(testDatabaseConfig())(_.clean())

  def withDatabase[A](databaseConfig: DatabaseConfig)(f: HikariTransactor[IO] => IO[A]): A =
    db.usingTransactor(databaseConfig)(_.clean)(x => Stream.eval(f(x)))
      .runLast
      .unsafeRunSync()
      .getOrElse(fail(s"Unable to perform the operation"))

  import cats.instances.list._
  import cats.syntax.traverse._

  def withScheduleTable[A](config: DatabaseConfig)(initState: ScheduleRecord*)(f: ScheduleTable => IO[A])(
      implicit executionContext: ExecutionContext): A =
    withDatabase(config) { db =>
      for {
        _ <- db.clean
        _ <- initState.toList
          .flatMap(record => {
            ScheduleTable
              .addScheduleRecords(record)
              .map(_.run.transact(db))
          })
          .sequence[IO, Int]
        result <- f(ScheduleTable(db))
      } yield result
    }

  def withTiplocTable[A](config: DatabaseConfig)(initState: TipLocRecord*)(f: TipLocTable => IO[A])(
      implicit executionContext: ExecutionContext): A =
    withDatabase(config) { db =>
      for {
        _ <- db.clean
        _ <- initState.toList
          .map(record => {
            TipLocTable
              .addTiplocRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]
        result <- f(TipLocTable(db))
      } yield result
    }

  def withMovementLogTable[A](config: DatabaseConfig)(initState: MovementLog*)(f: MovementLogTable => IO[A])(
      implicit executionContext: ExecutionContext): A =
    withDatabase(config) { db =>
      for {
        _ <- db.clean
        _ <- initState.toList
          .map(record => {
            MovementLogTable
              .addMovementLogRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]
        result <- f(MovementLogTable(db))
      } yield result
    }
}
