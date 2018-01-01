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

    def clean: IO[Int] =
      sql"DELETE FROM schedule".update.run.transact(db)
    sql"DELETE FROM tiploc".update.run.transact(db)
    sql"DELETE FROM movement_log".update.run.transact(db)
  }

  case class AppInitialState(scheduleRecords: List[ScheduleRecord] = List.empty,
                             tiplocRecords: List[TipLocRecord] = List.empty,
                             movementLogs: List[MovementLog] = List.empty)

  object AppInitialState {
    def empty = AppInitialState()
  }

  case class TrainDelaysTestFixture(scheduleTable: ScheduleTable,
                                    tipLocTable: TipLocTable,
                                    movementLogTable: MovementLogTable)

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

  def withInitialState[A](config: DatabaseConfig)(initState: AppInitialState = AppInitialState.empty)(
      f: TrainDelaysTestFixture => IO[A])(implicit executionContext: ExecutionContext): A =
    withDatabase(config) { db =>
      for {
        _ <- db.clean
        _ <- initState.scheduleRecords
          .flatMap(record => {
            ScheduleTable
              .addScheduleRecords(record)
              .map(_.run.transact(db))
          })
          .sequence[IO, Int]
        _ <- initState.tiplocRecords
          .map(record => {
            TipLocTable
              .addTiplocRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

        _ <- initState.movementLogs
          .map(record => {
            MovementLogTable
              .addMovementLogRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]
        result <- f(TrainDelaysTestFixture(ScheduleTable(db), TipLocTable(db), MovementLogTable(db)))
      } yield result
    }
}
