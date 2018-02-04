package traindelays

import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import akka.actor.ActorSystem
import cats.effect.IO
import doobie.hikari.HikariTransactor
import fs2.Stream
import fs2.async.mutable.Queue
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
import traindelays.networkrail.subscribers._
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

  case class AppInitialState(scheduleLogRecords: List[ScheduleLog] = List.empty,
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
                                    trainActivationCache: TrainActivationCache,
                                    emailer: StubEmailer,
                                    subscriberHandler: SubscriberHandler)

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
      f: TrainDelaysTestFixture => A)(implicit executionContext: ExecutionContext): A =
    withDatabase(databaseConfig) { db =>
      val redisClient          = MockRedisClient()
      val trainActivationCache = TrainActivationCache(redisClient, redisCacheExpiry)
      val stanoxTable          = StanoxTable(db, scheduleDataConfig.memoizeFor)
      val movementLogTable     = MovementLogTable(db)
      val subscriberTable      = SubscriberTable(db, subscribersConfig.memoizeFor)
      val scheduleTable        = ScheduleTable(db, scheduleDataConfig.memoizeFor)
      val emailer              = StubEmailer()
      val subscriberHandler    = SubscriberHandler(movementLogTable, subscriberTable, scheduleTable, emailer)

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

        _ <- initState.scheduleLogRecords
          .map { scheduleLog =>
            ScheduleTable
              .addScheduleLogRecord(scheduleLog)
              .run
              .transact(db)
          }
          .sequence[IO, Int]

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

      } yield
        f(
          TrainDelaysTestFixture(scheduleTable,
                                 stanoxTable,
                                 movementLogTable,
                                 CancellationLogTable(db),
                                 subscriberTable,
                                 trainActivationCache,
                                 emailer,
                                 subscriberHandler))
    }

  def createMovementRecord(trainId: TrainId = TrainId("12345"),
                           trainServiceCode: ServiceCode = ServiceCode("23456"),
                           eventType: EventType = Arrival,
                           toc: TOC = TOC("SN"),
                           actualTimestamp: Long = System.currentTimeMillis(),
                           plannedTimestamp: Long = System.currentTimeMillis() - 60000,
                           plannedPassengerTimestamp: Option[Long] = Some(System.currentTimeMillis() - 60000),
                           stanoxCode: Option[StanoxCode] = Some(StanoxCode("98765")),
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
                               stanoxCode: StanoxCode = StanoxCode("87654"),
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
                             originStanox: StanoxCode = StanoxCode("74895"),
                             originDepartureTimestamp: Long = System.currentTimeMillis() + 7200000) =
    TrainActivationRecord(
      scheduleTrainId,
      trainServiceCode,
      trainId,
      originStanox,
      originDepartureTimestamp
    )

  def movementRecordToMovementLog(movementRecord: TrainMovementRecord,
                                  id: Option[Int],
                                  scheduleTrainId: ScheduleTrainId,
                                  originStanoxCode: StanoxCode,
                                  originDepartureTimestamp: Long): MovementLog =
    MovementLog(
      id,
      movementRecord.trainId,
      scheduleTrainId,
      movementRecord.trainServiceCode,
      movementRecord.eventType,
      movementRecord.toc,
      movementRecord.stanoxCode.get,
      originStanoxCode,
      originDepartureTimestamp,
      movementRecord.plannedPassengerTimestamp.get,
      movementRecord.actualTimestamp,
      movementRecord.actualTimestamp - movementRecord.plannedPassengerTimestamp.get,
      movementRecord.variationStatus.get
    )

  def cancellationRecordToCancellationLog(cancellationRecord: TrainCancellationRecord,
                                          id: Option[Int],
                                          scheduleTrainId: ScheduleTrainId,
                                          originStanoxCode: StanoxCode,
                                          originDepartureTimestamp: Long) =
    CancellationLog(
      id,
      cancellationRecord.trainId,
      scheduleTrainId,
      cancellationRecord.trainServiceCode,
      cancellationRecord.toc,
      cancellationRecord.stanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
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

  def createScheduleLog(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
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

  def createCancellationLog(trainId: TrainId = TrainId("862F60MY30"),
                            scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G12345"),
                            serviceCode: ServiceCode = ServiceCode("24673605"),
                            toc: TOC = TOC("SN"),
                            stanoxCode: StanoxCode = StanoxCode("87214"),
                            originStanoxCode: StanoxCode = StanoxCode("46754"),
                            originDepartureTimestamp: Long = System.currentTimeMillis() + 7200000,
                            cancellationType: CancellationType = EnRoute,
                            cancellationReasonCode: String = "YI") =
    CancellationLog(
      None,
      trainId,
      scheduleTrainId,
      serviceCode,
      toc,
      stanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
      cancellationType,
      cancellationReasonCode
    )

  def createMovementLog(trainId: TrainId = TrainId("862F60MY30"),
                        scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G12345"),
                        serviceCode: ServiceCode = ServiceCode("24673605"),
                        eventType: EventType = Arrival,
                        toc: TOC = TOC("SN"),
                        stanoxCode: StanoxCode = StanoxCode("87214"),
                        originStanoxCode: StanoxCode = StanoxCode("47353"),
                        originDepartureTimestamp: Long = System.currentTimeMillis() + 7200000,
                        plannedPassengerTimestamp: Long = 1514663220000L,
                        actualTimestamp: Long = 1514663160000L,
                        variationStatus: VariationStatus = Early) =
    MovementLog(
      None,
      trainId,
      scheduleTrainId,
      serviceCode,
      eventType,
      toc,
      stanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
      plannedPassengerTimestamp,
      actualTimestamp,
      actualTimestamp - plannedPassengerTimestamp,
      variationStatus
    )

  def createSubscriberRecord(userId: UserId = UserId("1234567abc"),
                             emailAddress: String = "test@test.com",
                             emailVerified: Option[Boolean] = Some(true),
                             name: Option[String] = Some("joebloggs"),
                             firstName: Option[String] = Some("Joe"),
                             familyName: Option[String] = Some("Bloggs"),
                             locale: Option[String] = Some("GB"),
                             scheduleTrainId: ScheduleTrainId = ScheduleTrainId("GB1234"),
                             serviceCode: ServiceCode = ServiceCode("900002"),
                             fromStanoxCode: StanoxCode = StanoxCode("73940"),
                             toStanoxCode: StanoxCode = StanoxCode("29573")) =
    SubscriberRecord(None,
                     userId,
                     emailAddress,
                     emailVerified,
                     name,
                     firstName,
                     familyName,
                     locale,
                     scheduleTrainId,
                     serviceCode,
                     fromStanoxCode,
                     toStanoxCode)

  def stanoxRecordsToMap(stanoxRecords: List[StanoxRecord]): Map[TipLocCode, StanoxCode] =
    stanoxRecords.map(x => x.tipLocCode -> x.stanoxCode).toMap

  def randomGen = Random.nextInt(9999999).toString

  def runAllQueues(trainActivationQueue: Queue[IO, TrainActivationRecord],
                   trainMovementQueue: Queue[IO, TrainMovementRecord],
                   trainCancellationQueue: Queue[IO, TrainCancellationRecord],
                   fixture: TrainDelaysTestFixture)(implicit executionContext: ExecutionContext) = {

    TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run.unsafeRunTimed(1 second)
    TrainMovementProcessor(trainMovementQueue,
                           fixture.movementLogTable,
                           fixture.subscriberHandler,
                           fixture.trainActivationCache).stream.run.unsafeRunTimed(1 second)
    TrainCancellationProcessor(trainCancellationQueue,
                               fixture.subscriberHandler,
                               fixture.cancellationLogTable,
                               fixture.trainActivationCache).stream.run.unsafeRunTimed(1 second)

  }
}
