package traindelays

import akka.actor.ActorSystem
import cats.effect.IO
import doobie.hikari.HikariTransactor
import fs2.Stream
import org.scalatest.Matchers.fail
import traindelays.networkrail.cache.{MockRedisClient, TrainActivationCache}
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, SubscriberTable, TipLocTable, _}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.{ScheduleRecord, ScheduleTrainId, TipLocRecord}
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.{ServiceCode, Stanox, TOC}

import scala.concurrent.ExecutionContext
import scala.util.Random

trait TestFeatures {

  import doobie._
  import doobie.implicits._

  implicit val actorSystem: ActorSystem = ActorSystem()

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
                             subscriberRecords: List[SubscriberRecord] = List.empty,
                             cancellationLogs: List[CancellationLog] = List.empty)

  object AppInitialState {
    def empty = AppInitialState()
  }

  case class TrainDelaysTestFixture(scheduleTable: ScheduleTable,
                                    tipLocTable: TipLocTable,
                                    movementLogTable: MovementLogTable,
                                    cancellationLogTable: CancellationLogTable,
                                    subscriberTable: SubscriberTable,
                                    trainActivationCache: TrainActivationCache)

  def testDatabaseConfig() = {
    val databaseName = s"testdb-${System.currentTimeMillis()}-${Random.nextInt(Integer.MAX_VALUE)}"
    DatabaseConfig(
      "org.h2.Driver",
      "jdbc:h2:mem:" + databaseName + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
      "sa",
      ""
    )
  }

  val testTransactor = setUpTransactor(testDatabaseConfig())(_.clean())

  def withDatabase[A](databaseConfig: DatabaseConfig)(f: HikariTransactor[IO] => IO[A]): A =
    withTransactor(databaseConfig)(_.clean)(x => Stream.eval(f(x))).runLast
      .unsafeRunSync()
      .getOrElse(fail(s"Unable to perform the operation"))

  def withQueues = {
    import scala.concurrent.ExecutionContext.Implicits.global
    for {
      trainMovementQueue     <- fs2.async.unboundedQueue[IO, TrainMovementRecord]
      trainActivationQueue   <- fs2.async.unboundedQueue[IO, TrainActivationRecord]
      trainCancellationQueue <- fs2.async.unboundedQueue[IO, TrainCancellationRecord]
    } yield (trainMovementQueue, trainActivationQueue, trainCancellationQueue)
  }

  import cats.instances.list._
  import cats.syntax.traverse._

  import scala.concurrent.duration._

  def withInitialState[A](
      databaseConfig: DatabaseConfig,
      subscribersConfig: SubscribersConfig = SubscribersConfig(1 minute),
      redisCacheExpiry: FiniteDuration = 5 seconds)(initState: AppInitialState = AppInitialState.empty)(
      f: TrainDelaysTestFixture => IO[A])(implicit executionContext: ExecutionContext): A =
    withDatabase(databaseConfig) { db =>
      val redisClient          = MockRedisClient()
      val trainActivationCache = TrainActivationCache(redisClient, redisCacheExpiry)
      val tipLocTable          = TipLocTable(db)

      for {
        _ <- db.clean
        _ <- initState.tiplocRecords
          .map(record => {
            TipLocTable
              .addTiplocRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

        _ <- initState.scheduleRecords
          .map(record => {
            record.toScheduleLogs(tipLocTable).flatMap { scheduleLogs =>
              scheduleLogs
                .map { scheduleLog =>
                  ScheduleTable
                    .addScheduleLogRecord(scheduleLog)
                    .run
                    .transact(db)
                }
                .sequence[IO, Int]
            }
          })
          .sequence[IO, List[Int]]

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

        _ <- initState.cancellationLogs
          .map(record => {
            CancellationLogTable
              .addCancellationLogRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]
        result <- f(
          TrainDelaysTestFixture(ScheduleTable(db),
                                 TipLocTable(db),
                                 MovementLogTable(db),
                                 CancellationLogTable(db),
                                 MemoizedSubscriberTable(db, subscribersConfig),
                                 trainActivationCache))
      } yield result
    }

  def createMovementRecord(trainId: TrainId = TrainId("12345"),
                           trainServiceCode: ServiceCode = ServiceCode("23456"),
                           eventType: EventType = Arrival,
                           toc: TOC = TOC("SN"),
                           actualTimestamp: Long = System.currentTimeMillis(),
                           plannedTimestamp: Long = System.currentTimeMillis() - 60000,
                           plannedPassengerTimestamp: Option[Long] = Some(System.currentTimeMillis() - 60000),
                           stanox: Option[Stanox] = Some(Stanox("REDHILL")),
                           variationStatus: Option[VariationStatus] = Some(Late)) =
    TrainMovementRecord(
      trainId,
      trainServiceCode,
      eventType,
      toc,
      actualTimestamp,
      plannedTimestamp,
      plannedPassengerTimestamp,
      stanox,
      variationStatus
    )

  def createCancellationRecord(trainId: TrainId = TrainId("12345"),
                               trainServiceCode: ServiceCode = ServiceCode("23456"),
                               toc: TOC = TOC("SN"),
                               stanox: Stanox = Stanox("REDHILL"),
                               cancellationType: CancellationType = EnRoute,
                               cancellationReasonCode: String = "YI") =
    TrainCancellationRecord(
      trainId,
      trainServiceCode,
      toc,
      stanox,
      cancellationType,
      cancellationReasonCode
    )

  def createActivationRecord(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G123456"),
                             trainServiceCode: ServiceCode = ServiceCode("23456"),
                             trainId: TrainId = TrainId("12345"),
  ) =
    TrainActivationRecord(
      scheduleTrainId,
      trainServiceCode,
      trainId
    )

  def movementRecordToMovementLog(movementRecord: TrainMovementRecord,
                                  id: Option[Int],
                                  scheduleTrainId: ScheduleTrainId): MovementLog =
    MovementLog(
      id,
      movementRecord.trainId,
      scheduleTrainId,
      movementRecord.trainServiceCode,
      movementRecord.eventType,
      movementRecord.toc,
      movementRecord.stanox.get,
      movementRecord.plannedPassengerTimestamp.get,
      movementRecord.actualTimestamp,
      movementRecord.actualTimestamp - movementRecord.plannedPassengerTimestamp.get,
      movementRecord.variationStatus.get
    )

  def cancellationRecordToCancellationLog(cancellationRecord: TrainCancellationRecord,
                                          id: Option[Int],
                                          scheduleTrainId: ScheduleTrainId) =
    CancellationLog(
      id,
      cancellationRecord.trainId,
      scheduleTrainId,
      cancellationRecord.trainServiceCode,
      cancellationRecord.toc,
      cancellationRecord.stanox,
      cancellationRecord.cancellationType,
      cancellationRecord.cancellationReasonCode
    )
}
