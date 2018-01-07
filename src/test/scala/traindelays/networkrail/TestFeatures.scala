package traindelays

import cats.effect.IO
import doobie.hikari.HikariTransactor
import fs2.Stream
import org.scalatest.Matchers.fail
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, TipLocTable, SubscriberTable}
import traindelays.networkrail.movementdata.{MovementLog, MovementRecord}
import traindelays.networkrail.scheduledata.{ScheduleRecord, TipLocRecord}
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.db._

import scala.concurrent.ExecutionContext
import scala.util.Random

trait TestFeatures {

  import doobie._
  import doobie.implicits._

  implicit class DBExt(db: Transactor[IO]) {

    def clean: IO[Int] =
      sql"DELETE FROM schedule".update.run.transact(db)
    sql"DELETE FROM tiploc".update.run.transact(db)
    sql"DELETE FROM movement_log".update.run.transact(db)
    sql"DELETE FROM watching".update.run.transact(db)
  }

  case class AppInitialState(scheduleRecords: List[ScheduleRecord] = List.empty,
                             tiplocRecords: List[TipLocRecord] = List.empty,
                             movementLogs: List[MovementLog] = List.empty,
                             subscriberRecords: List[SubscriberRecord] = List.empty)

  object AppInitialState {
    def empty = AppInitialState()
  }

  case class TrainDelaysTestFixture(scheduleTable: ScheduleTable,
                                    tipLocTable: TipLocTable,
                                    movementLogTable: MovementLogTable,
                                    subscriberTable: SubscriberTable)

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

  val testTransactor = transactor(testDatabaseConfig())(_.clean())

  def withDatabase[A](databaseConfig: DatabaseConfig)(f: HikariTransactor[IO] => IO[A]): A =
    usingTransactor(databaseConfig)(_.clean)(x => Stream.eval(f(x))).runLast
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
        _ <- initState.subscriberRecords
          .map(record => {
            SubscriberTable
              .addSubscriberRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]
        result <- f(
          TrainDelaysTestFixture(ScheduleTable(db), TipLocTable(db), MovementLogTable(db), SubscriberTable(db)))
      } yield result
    }

  def createMovementRecord(trainId: String = "12345",
                           trainServiceCode: String = "23456",
                           eventType: Option[String] = Some("ARRIVAL"),
                           toc: Option[String] = Some("SN"),
                           plannedEventType: Option[String] = Some("ARRIVAL"),
                           actualTimestamp: Option[Long] = Some(System.currentTimeMillis()),
                           plannedTimestamp: Option[Long] = Some(System.currentTimeMillis() - 60000),
                           plannedPassengerTimestamp: Option[Long] = Some(System.currentTimeMillis() - 60000),
                           stanox: Option[String] = Some("REDHILL"),
                           variationStatus: Option[String] = Some("LATE")) =
    MovementRecord(
      trainId,
      trainServiceCode,
      eventType,
      toc,
      plannedEventType,
      actualTimestamp,
      plannedTimestamp,
      plannedPassengerTimestamp,
      stanox,
      variationStatus
    )
}
