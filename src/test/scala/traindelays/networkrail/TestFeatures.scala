package traindelays

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import akka.actor.ActorSystem
import cats.effect.IO
import doobie.hikari.HikariTransactor
import fs2.Stream
import org.http4s.Uri
import org.scalatest.Matchers.fail
import traindelays.networkrail.cache.{MockRedisClient, TrainActivationCache}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, SubscriberTable, _}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType.{
  OriginatingLocation,
  TerminatingLocation
}
import traindelays.networkrail.scheduledata.ScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata._
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.{ServiceCode, StanoxCode, TOC, TipLocCode}

import scala.concurrent.ExecutionContext
import scala.util.Random

trait TestFeatures {

  import doobie._
  import doobie.implicits._

  implicit val actorSystem: ActorSystem = ActorSystem()

  implicit class DBExt(db: Transactor[IO]) {

    def clean: IO[Unit] =
      for {
        _ <- sql"DELETE FROM schedule".update.run.transact(db)
        _ <- sql"DELETE FROM stanox".update.run.transact(db)
        _ <- sql"DELETE FROM movement_log".update.run.transact(db)
        _ <- sql"DELETE FROM subscribers".update.run.transact(db)
        _ <- sql"DELETE FROM cancellation_log".update.run.transact(db)
      } yield ()
  }

  case class AppInitialState(scheduleRecords: List[ScheduleRecord] = List.empty,
                             stanoxRecords: List[StanoxRecord] = List.empty,
                             movementLogs: List[MovementLog] = List.empty,
                             subscriberRecords: List[SubscriberRecord] = List.empty,
                             cancellationLogs: List[CancellationLog] = List.empty)

  object AppInitialState {
    def empty = AppInitialState()
  }

  case class TrainDelaysTestFixture(scheduleTable: ScheduleTable,
                                    stanoxTable: StanoxTable,
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
      scheduleDataConfig: ScheduleDataConfig =
        ScheduleDataConfig(Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 1 minute),
      redisCacheExpiry: FiniteDuration = 5 seconds)(initState: AppInitialState = AppInitialState.empty)(
      f: TrainDelaysTestFixture => IO[A])(implicit executionContext: ExecutionContext): A =
    withDatabase(databaseConfig) { db =>
      val redisClient          = MockRedisClient()
      val trainActivationCache = TrainActivationCache(redisClient, redisCacheExpiry)
      val stanoxTable          = StanoxTable(db, scheduleDataConfig.memoizeFor)

      for {
        _ <- db.clean
        _ <- initState.stanoxRecords
          .map(record => {
            StanoxTable
              .addStanoxRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

        _ <- initState.scheduleRecords
          .map(record => {
            for {
              existingStanoxRecords <- stanoxTable.retrieveAllRecords()
              x <- record
                .toScheduleLogs(existingStanoxRecords)
                .map { scheduleLog =>
                  ScheduleTable
                    .addScheduleLogRecord(scheduleLog)
                    .run
                    .transact(db)
                }
                .sequence[IO, Int]

            } yield x
          })
          .sequence[IO, List[Int]]

        z <- initState.movementLogs
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
          TrainDelaysTestFixture(
            ScheduleTable(db, scheduleDataConfig.memoizeFor),
            stanoxTable,
            MovementLogTable(db),
            CancellationLogTable(db),
            SubscriberTable(db, subscribersConfig.memoizeFor),
            trainActivationCache
          ))
      } yield result
    }

  def createMovementRecord(trainId: TrainId = TrainId("12345"),
                           trainServiceCode: ServiceCode = ServiceCode("23456"),
                           eventType: EventType = Arrival,
                           toc: TOC = TOC("SN"),
                           actualTimestamp: Long = System.currentTimeMillis(),
                           plannedTimestamp: Long = System.currentTimeMillis() - 60000,
                           plannedPassengerTimestamp: Option[Long] = Some(System.currentTimeMillis() - 60000),
                           stanoxCode: Option[StanoxCode] = Some(StanoxCode("REDHILL")),
                           variationStatus: Option[VariationStatus] = Some(Late)) =
    TrainMovementRecord(
      trainId,
      trainServiceCode,
      eventType,
      toc,
      actualTimestamp,
      plannedTimestamp,
      plannedPassengerTimestamp,
      stanoxCode,
      variationStatus
    )

  def createCancellationRecord(trainId: TrainId = TrainId("12345"),
                               trainServiceCode: ServiceCode = ServiceCode("23456"),
                               toc: TOC = TOC("SN"),
                               stanoxCode: StanoxCode = StanoxCode("REDHILL"),
                               cancellationType: CancellationType = EnRoute,
                               cancellationReasonCode: String = "YI") =
    TrainCancellationRecord(
      trainId,
      trainServiceCode,
      toc,
      stanoxCode,
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
      movementRecord.stanoxCode.get,
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
      cancellationRecord.stanoxCode,
      cancellationRecord.cancellationType,
      cancellationRecord.cancellationReasonCode
    )

  def createScheduleRecord(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
                           trainServiceCode: ServiceCode = ServiceCode("24745000"),
                           atocCode: AtocCode = AtocCode("SN"),
                           daysRun: DaysRun = DaysRun(monday = true,
                                                      tuesday = true,
                                                      wednesday = true,
                                                      thursday = true,
                                                      friday = true,
                                                      saturday = false,
                                                      sunday = false),
                           scheduleStartDate: LocalDate = LocalDate.parse("2017-12-11"),
                           scheduleEndDate: LocalDate = LocalDate.parse("2017-12-29"),
                           locationRecords: List[ScheduleLocationRecord] = List(
                             ScheduleLocationRecord(OriginatingLocation,
                                                    TipLocCode("REIGATE"),
                                                    None,
                                                    Some(LocalTime.parse("0649", timeFormatter))),
                             ScheduleLocationRecord(TerminatingLocation,
                                                    TipLocCode("REDHILL"),
                                                    Some(LocalTime.parse("0653", timeFormatter)),
                                                    None)
                           )) =
    ScheduleRecord(
      scheduleTrainId,
      trainServiceCode,
      atocCode,
      daysRun,
      scheduleStartDate,
      scheduleEndDate,
      locationRecords
    )

  def createScheduleLogRecord(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
                              trainServiceCode: ServiceCode = ServiceCode("24745000"),
                              atocCode: AtocCode = AtocCode("SN"),
                              monday: Boolean = true,
                              tuesday: Boolean = true,
                              wednesday: Boolean = true,
                              thursday: Boolean = true,
                              friday: Boolean = true,
                              saturday: Boolean = false,
                              sunday: Boolean = false,
                              daysRunPattern: DaysRunPattern = DaysRunPattern.Weekdays,
                              index: Int = 1,
                              stanoxCode: StanoxCode = StanoxCode("12345"),
                              subsequentStanoxCodes: List[StanoxCode] = List(StanoxCode("23456"), StanoxCode("34567")),
                              subsequentArrivalTimes: List[LocalTime] =
                                List(LocalTime.parse("0710", timeFormatter), LocalTime.parse("0725", timeFormatter)),
                              scheduleStartDate: LocalDate = LocalDate.parse("2017-12-11"),
                              scheduleEndDate: LocalDate = LocalDate.parse("2017-12-29"),
                              locationType: LocationType = OriginatingLocation,
                              arrivalTime: Option[LocalTime] = Some(LocalTime.parse("0649", timeFormatter)),
                              departureTime: Option[LocalTime] = Some(LocalTime.parse("0649", timeFormatter))) =
    ScheduleLog(
      None,
      scheduleTrainId,
      trainServiceCode,
      atocCode,
      index,
      stanoxCode,
      subsequentStanoxCodes,
      subsequentArrivalTimes,
      monday,
      tuesday,
      wednesday,
      thursday,
      friday,
      saturday,
      sunday,
      daysRunPattern,
      scheduleStartDate,
      scheduleEndDate,
      locationType,
      arrivalTime,
      departureTime
    )
}
