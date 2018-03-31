package traindelays.networkrail

import java.io.File
import java.nio.file.Paths
import java.time.{LocalDate, LocalTime}

import akka.actor.ActorSystem
import cats.effect.IO
import com.typesafe.config.ConfigFactory
import doobie.hikari.HikariTransactor
import fs2.Stream
import fs2.async.mutable.Queue
import org.http4s.{HttpService, Uri}
import org.scalatest.Matchers.fail
import redis.RedisClient
import traindelays._
import traindelays.networkrail.cache.TrainActivationCache
import traindelays.networkrail.db.AssociationTable.AssociationRecord
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordSecondary, ScheduleRecordPrimary}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, SubscriberTable, _}
import traindelays.networkrail.metrics.TestMetricsLogging
import traindelays.networkrail.movementdata.CancellationType.EnRoute
import traindelays.networkrail.movementdata.EventType.Arrival
import traindelays.networkrail.movementdata.VariationStatus.{Early, Late}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.DaysRunPattern.Weekdays
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.ScheduleLocationRecord.LocationType.{
  OriginatingLocation,
  TerminatingLocation
}
import traindelays.networkrail.scheduledata.DecodedScheduleRecord.{DaysRun, ScheduleLocationRecord}
import traindelays.networkrail.scheduledata._
import traindelays.networkrail.subscribers._
import traindelays.ui.{timeFormatter => _, _}

import scala.concurrent.ExecutionContext
import scala.language.postfixOps
import scala.util.Random

trait TestFeatures {

  import doobie._
  import doobie.implicits._

  implicit val actorSystem: ActorSystem = ActorSystem()

  case class Queues(trainMovementQueue: Queue[IO, TrainMovementRecord],
                    trainActivationQueue: Queue[IO, TrainActivationRecord],
                    trainCancellationQueue: Queue[IO, TrainCancellationRecord],
                    trainChangeOfOriginQueue: Queue[IO, TrainChangeOfOriginRecord])

  implicit class DBExt(db: Transactor[IO]) {

    def clean: IO[Unit] =
      for {
        _ <- sql"DROP TABLE IF EXISTS schedule".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS schedule_association".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS stanox".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS movement_log".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS subscribers".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS cancellation_log".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS change_of_origin_log".update.run.transact(db)
        _ <- sql"DROP TABLE IF EXISTS association".update.run.transact(db)
      } yield ()
  }

  case class AppInitialState(schedulePrimaryRecords: List[ScheduleRecordPrimary] = List.empty,
                             scheduleSecondaryRecords: List[ScheduleRecordSecondary] = List.empty,
                             associationRecords: List[AssociationRecord] = List.empty,
                             stanoxRecords: List[StanoxRecord] = List.empty,
                             movementLogs: List[MovementLog] = List.empty,
                             subscriberRecords: List[SubscriberRecord] = List.empty,
                             cancellationLogs: List[CancellationLog] = List.empty,
                             changeOfOriginLogs: List[ChangeOfOriginLog] = List.empty)

  object AppInitialState {
    def empty = AppInitialState()
  }

  case class TrainDelaysTestFixture(schedulePrimaryTable: ScheduleTable[ScheduleRecordPrimary],
                                    scheduleSecondaryTable: ScheduleTable[ScheduleRecordSecondary],
                                    stanoxTable: StanoxTable,
                                    associationTable: AssociationTable,
                                    movementLogTable: MovementLogTable,
                                    cancellationLogTable: CancellationLogTable,
                                    changeOfOriginLogTable: ChangeOfOriginLogTable,
                                    subscriberTable: SubscriberTable,
                                    trainActivationCache: TrainActivationCache,
                                    emailer: StubEmailer,
                                    subscriberHandler: SubscriberHandler,
                                    metricsLogging: TestMetricsLogging)

  val config             = TrainDelaysConfig(ConfigFactory.parseFile(new File(getClass.getResource("/application.conf").getPath)))
  val testDatabaseConfig = config.databaseConfig
  val testTransactor     = setUpTransactor(testDatabaseConfig)(_.clean())

  def withDatabase[A](databaseConfig: DatabaseConfig)(f: HikariTransactor[IO] => IO[A]): A =
    withTransactor(databaseConfig)(_.clean)(transactor => Stream.eval(f(transactor))).compile.last
      .unsafeRunSync()
      .getOrElse(fail(s"Unable to perform the operation"))

  def withQueues[A](f: Queues => A): A = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val queues = for {
      trainMovementQueue       <- fs2.async.unboundedQueue[IO, TrainMovementRecord]
      trainActivationQueue     <- fs2.async.unboundedQueue[IO, TrainActivationRecord]
      trainCancellationQueue   <- fs2.async.unboundedQueue[IO, TrainCancellationRecord]
      trainChangeOfOriginQueue <- fs2.async.unboundedQueue[IO, TrainChangeOfOriginRecord]
    } yield Queues(trainMovementQueue, trainActivationQueue, trainCancellationQueue, trainChangeOfOriginQueue)
    queues.map(q => f(q)).unsafeRunSync()
  }

  import cats.instances.list._
  import cats.syntax.traverse._

  import scala.concurrent.duration._

  def withInitialState[A](
      databaseConfig: DatabaseConfig,
      subscribersConfig: SubscribersConfig = SubscribersConfig(1 minute, 15 minutes),
      scheduleDataConfig: ScheduleDataConfig =
        ScheduleDataConfig(Uri.unsafeFromString(""), Uri.unsafeFromString(""), Paths.get(""), Paths.get(""), 1 minute),
      redisCacheExpiry: FiniteDuration = 5 seconds)(initState: AppInitialState = AppInitialState.empty)(
      f: TrainDelaysTestFixture => A)(implicit executionContext: ExecutionContext): A =
    withDatabase(databaseConfig) { db =>
      val redisClient            = RedisClient()
      val trainActivationCache   = TrainActivationCache(redisClient, redisCacheExpiry)
      val stanoxTable            = StanoxTable(db, scheduleDataConfig.memoizeFor)
      val movementLogTable       = MovementLogTable(db)
      val subscriberTable        = SubscriberTable(db, subscribersConfig.memoizeFor)
      val schedulePrimaryTable   = SchedulePrimaryTable(db, scheduleDataConfig.memoizeFor)
      val scheduleSecondaryTable = ScheduleSecondaryTable(db, scheduleDataConfig.memoizeFor)
      val associationTable       = AssociationTable(db, scheduleDataConfig.memoizeFor)
      val emailer                = StubEmailer()
      val subscriberHandler =
        SubscriberHandler(movementLogTable,
                          subscriberTable,
                          schedulePrimaryTable,
                          scheduleSecondaryTable,
                          stanoxTable,
                          emailer,
                          subscribersConfig)
      val metricsLogging = TestMetricsLogging(config.metricsConfig)

      for {
        _ <- IO.fromFuture(IO(redisClient.flushall()))
        _ <- initState.stanoxRecords
          .map(record => {
            StanoxTable
              .addStanoxRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

        _ <- initState.schedulePrimaryRecords
          .map { scheduleLog =>
            ScheduleTable
              .addScheduleLogRecord(scheduleLog)
              .run
              .transact(db)
          }
          .sequence[IO, Int]

        _ <- initState.scheduleSecondaryRecords
          .map { scheduleLog =>
            ScheduleTable
              .addScheduleLogRecord(scheduleLog)
              .run
              .transact(db)
          }
          .sequence[IO, Int]

        _ <- initState.associationRecords
          .map { associationRecord =>
            AssociationTable
              .addAssociationRecord(associationRecord)
              .run
              .transact(db)
          }
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

        _ <- initState.cancellationLogs
          .map(record => {
            CancellationLogTable
              .addCancellationLogRecord(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

        _ <- initState.changeOfOriginLogs
          .map(record => {
            ChangeOfOriginLogTable
              .add(record)
              .run
              .transact(db)
          })
          .sequence[IO, Int]

      } yield
        f(
          TrainDelaysTestFixture(
            schedulePrimaryTable,
            scheduleSecondaryTable,
            stanoxTable,
            associationTable,
            movementLogTable,
            CancellationLogTable(db),
            ChangeOfOriginLogTable(db),
            subscriberTable,
            trainActivationCache,
            emailer,
            subscriberHandler,
            metricsLogging
          ))
    }

  def createMovementRecord(
      trainId: TrainId = TrainId("12345"),
      trainServiceCode: ServiceCode = ServiceCode("23456"),
      eventType: EventType = Arrival,
      toc: TOC = TOC("SN"),
      actualTimestamp: Long = System.currentTimeMillis(),
      plannedTimestamp: Option[Long] = Some(System.currentTimeMillis() - (16 * 60 * 1000)),
      plannedPassengerTimestamp: Option[Long] = Some(System.currentTimeMillis() - (16 * 60 * 1000)),
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

  def createChangeOfOriginRecord(trainId: TrainId = TrainId("12345"),
                                 trainServiceCode: ServiceCode = ServiceCode("23456"),
                                 toc: TOC = TOC("SN"),
                                 newOriginstanoxCode: StanoxCode = StanoxCode("87654"),
                                 originStanoxCode: Option[StanoxCode] = Some(StanoxCode("34532")),
                                 originDepartureTimestamp: Option[Long] = Some(System.currentTimeMillis() + 7200000),
                                 reasonCode: Option[String] = Some("YI")) =
    TrainChangeOfOriginRecord(
      trainId,
      trainServiceCode,
      toc,
      newOriginstanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
      reasonCode
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
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      movementRecord.plannedPassengerTimestamp.get,
      timestampToLocalTime(movementRecord.plannedPassengerTimestamp.get),
      movementRecord.actualTimestamp,
      timestampToLocalTime(movementRecord.actualTimestamp),
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
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      cancellationRecord.cancellationType,
      cancellationRecord.cancellationReasonCode
    )

  def changeOfOriginRecordToChangeOfOriginLog(changeOfOriginRecord: TrainChangeOfOriginRecord,
                                              id: Option[Int],
                                              scheduleTrainId: ScheduleTrainId,
                                              originStanoxCode: StanoxCode,
                                              originDepartureTimestamp: Long) =
    ChangeOfOriginLog(
      id,
      changeOfOriginRecord.trainId,
      scheduleTrainId,
      changeOfOriginRecord.trainServiceCode,
      changeOfOriginRecord.toc,
      changeOfOriginRecord.newOriginStanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      changeOfOriginRecord.reasonCode
    )

  def createDecodedScheduleCreateRecord(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
                                        trainServiceCode: ServiceCode = ServiceCode("24745000"),
                                        trainCategory: Option[TrainCategory] = Some(TrainCategory("OO")),
                                        trainStatus: Option[TrainStatus] = Some(TrainStatus("B")),
                                        atocCode: Option[AtocCode] = Some(AtocCode("SN")),
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
    DecodedScheduleRecord.Create(
      scheduleTrainId,
      trainServiceCode,
      trainCategory,
      trainStatus,
      atocCode,
      daysRun,
      scheduleStartDate,
      scheduleEndDate,
      StpIndicator.P,
      locationRecords
    )

  def createScheduleRecordPrimary(
      scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
      trainServiceCode: ServiceCode = ServiceCode("24745000"),
      stpIndicator: StpIndicator = StpIndicator.P,
      trainCategory: Option[TrainCategory] = Some(TrainCategory("OO")),
      trainStatus: Option[TrainStatus] = Some(TrainStatus("B")),
      atocCode: Option[AtocCode] = Some(AtocCode("SN")),
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
    ScheduleRecordPrimary(
      None,
      scheduleTrainId,
      trainServiceCode,
      stpIndicator,
      trainCategory,
      trainStatus,
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

  def createScheduleRecordSecondary(
      scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G76481"),
      trainServiceCode: ServiceCode = ServiceCode("24745000"),
      stpIndicator: StpIndicator = StpIndicator.P,
      trainCategory: Option[TrainCategory] = Some(TrainCategory("OO")),
      trainStatus: Option[TrainStatus] = Some(TrainStatus("B")),
      atocCode: Option[AtocCode] = Some(AtocCode("SN")),
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
      departureTime: Option[LocalTime] = Some(LocalTime.parse("0649", timeFormatter)),
      associationId: Int = 1) =
    ScheduleRecordSecondary(
      None,
      scheduleTrainId,
      trainServiceCode,
      stpIndicator,
      trainCategory,
      trainStatus,
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
      departureTime,
      associationId
    )

  def createAssociationRecord(id: Option[Int] = Some(1),
                              mainScheduleTrainID: ScheduleTrainId = ScheduleTrainId("G76481"),
                              associatedScheduleTrainID: ScheduleTrainId = ScheduleTrainId("G12389"),
                              associatedStartDate: LocalDate = LocalDate.parse("2017-12-11"),
                              associatedEndDate: LocalDate = LocalDate.parse("2017-12-29"),
                              stpIndicator: StpIndicator = StpIndicator.P,
                              location: TipLocCode = TipLocCode("REDHILL"),
                              monday: Boolean = true,
                              tuesday: Boolean = true,
                              wednesday: Boolean = true,
                              thursday: Boolean = true,
                              friday: Boolean = true,
                              saturday: Boolean = false,
                              sunday: Boolean = false,
                              daysRunPattern: DaysRunPattern = DaysRunPattern.Weekdays,
                              associationCategory: Option[AssociationCategory] = Some(AssociationCategory.Join)) =
    AssociationRecord(
      id,
      mainScheduleTrainID,
      associatedScheduleTrainID,
      associatedStartDate,
      associatedEndDate,
      stpIndicator,
      location,
      monday,
      tuesday,
      wednesday,
      thursday,
      friday,
      saturday,
      sunday,
      daysRunPattern,
      associationCategory
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
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      cancellationType,
      cancellationReasonCode
    )

  def createChangeOfOriginLog(trainId: TrainId = TrainId("862F60MY30"),
                              scheduleTrainId: ScheduleTrainId = ScheduleTrainId("G12345"),
                              serviceCode: ServiceCode = ServiceCode("24673605"),
                              toc: TOC = TOC("SN"),
                              newStanoxCode: StanoxCode = StanoxCode("87214"),
                              originStanoxCode: StanoxCode = StanoxCode("46754"),
                              originDepartureTimestamp: Long = System.currentTimeMillis() + 7200000,
                              reasonCode: Option[String] = Some("YI")) =
    ChangeOfOriginLog(
      None,
      trainId,
      scheduleTrainId,
      serviceCode,
      toc,
      newStanoxCode,
      originStanoxCode,
      originDepartureTimestamp,
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      reasonCode
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
      timestampToLocalDate(originDepartureTimestamp),
      timestampToLocalTime(originDepartureTimestamp),
      plannedPassengerTimestamp,
      timestampToLocalTime(plannedPassengerTimestamp),
      actualTimestamp,
      timestampToLocalTime(actualTimestamp),
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
                             toStanoxCode: StanoxCode = StanoxCode("29573"),
                             daysRunPattern: DaysRunPattern = Weekdays) =
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
                     toStanoxCode,
                     daysRunPattern)

  def stanoxRecordsToMap(stanoxRecords: List[StanoxRecord]): Map[TipLocCode, StanoxCode] =
    stanoxRecords.flatMap(x => x.stanoxCode.map(stanox => x.tipLocCode -> stanox)).toMap

  def randomGen = Random.nextInt(9999999).toString

  def runAllQueues(queues: Queues, fixture: TrainDelaysTestFixture)(implicit executionContext: ExecutionContext) = {

    TrainActivationProcessor(queues.trainActivationQueue,
                             fixture.trainActivationCache,
                             fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
      .unsafeRunTimed(1 second)
    TrainMovementProcessor(
      queues.trainMovementQueue,
      fixture.movementLogTable,
      fixture.subscriberHandler,
      fixture.trainActivationCache,
      fixture.metricsLogging.incrMovementRecordsReceived
    ).stream.compile.drain.unsafeRunTimed(1 second)
    TrainCancellationProcessor(
      queues.trainCancellationQueue,
      fixture.subscriberHandler,
      fixture.cancellationLogTable,
      fixture.trainActivationCache,
      fixture.metricsLogging.incrCancellationRecordsReceived
    ).stream.compile.drain.unsafeRunTimed(1 second)
    TrainChangeOfOriginProcessor(
      queues.trainChangeOfOriginQueue,
      fixture.subscriberHandler,
      fixture.changeOfOriginLogTable,
      fixture.trainActivationCache,
      fixture.metricsLogging.incrCancellationRecordsReceived
    ).stream.compile.drain.unsafeRunTimed(1 second)

  }

  def createDefaultInitialState(scheduleTrainId: ScheduleTrainId = ScheduleTrainId("12345"),
                                serviceCode: ServiceCode = ServiceCode("799984")): AppInitialState = {

    val stanoxRecord1 =
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode(randomGen)), Some(CRS("REI")), Some("Reigate"), None)
    val stanoxRecord2 =
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode(randomGen)), Some(CRS("RDH")), Some("Redhill"), None)
    val stanoxRecord3 =
      StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode(randomGen)), Some(CRS("MER")), Some("Merstham"), None)
    val stanoxRecord4 =
      StanoxRecord(TipLocCode("EASTCRYD"), Some(StanoxCode(randomGen)), Some(CRS("ECR")), Some("East Croydon"), None)
    val stanoxRecord5 =
      StanoxRecord(TipLocCode("LONVIC"), Some(StanoxCode(randomGen)), Some(CRS("VIC")), Some("London Victoria"), None)
    val stanoxRecords = List(stanoxRecord1, stanoxRecord2, stanoxRecord3, stanoxRecord4, stanoxRecord5)

    val scheduleRecord = createDecodedScheduleCreateRecord(
      trainServiceCode = serviceCode,
      scheduleTrainId = scheduleTrainId,
      locationRecords = List(
        ScheduleLocationRecord(LocationType.OriginatingLocation,
                               stanoxRecord1.tipLocCode,
                               None,
                               Some(LocalTime.parse("12:10"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord2.tipLocCode,
                               Some(LocalTime.parse("12:14")),
                               Some(LocalTime.parse("12:15"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord3.tipLocCode,
                               Some(LocalTime.parse("12:24")),
                               Some(LocalTime.parse("12:25"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord4.tipLocCode,
                               Some(LocalTime.parse("12:35")),
                               Some(LocalTime.parse("12:36"))),
        ScheduleLocationRecord(LocationType.TerminatingLocation,
                               stanoxRecord5.tipLocCode,
                               Some(LocalTime.parse("12:45")),
                               None)
      )
    )

    AppInitialState(
      schedulePrimaryRecords = toScheduleLogs(scheduleRecord, StanoxRecord.stanoxRecordsToMap(stanoxRecords)),
      stanoxRecords = stanoxRecords
    )
  }

  def createDefaultInitialStateWithJoinAssociation(
      mainScheduleTrainId: ScheduleTrainId = ScheduleTrainId("12345"),
      mainServiceCode: ServiceCode = ServiceCode("799984"),
      associatedScheduleTrainId: ScheduleTrainId = ScheduleTrainId("98573"),
      associatedServiceCode: ServiceCode = ServiceCode("62882")): AppInitialState = {

    val stanoxRecord1 =
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode(randomGen)), Some(CRS("REI")), Some("Reigate"), None)
    val stanoxRecord2 =
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode(randomGen)), Some(CRS("RDH")), Some("Redhill"), None)
    val stanoxRecord3 =
      StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode(randomGen)), Some(CRS("MER")), Some("Merstham"), None)
    val stanoxRecord4 =
      StanoxRecord(TipLocCode("EASTCRYD"), Some(StanoxCode(randomGen)), Some(CRS("ECR")), Some("East Croydon"), None)
    val stanoxRecord5 =
      StanoxRecord(TipLocCode("LONVIC"), Some(StanoxCode(randomGen)), Some(CRS("VIC")), Some("London Victoria"), None)
    val stanoxRecord6 =
      StanoxRecord(TipLocCode("GATWICK"), Some(StanoxCode(randomGen)), Some(CRS("GTW")), Some("Gatwick Airport"), None)

    val stanoxRecords = List(stanoxRecord1, stanoxRecord2, stanoxRecord3, stanoxRecord4, stanoxRecord5, stanoxRecord6)

    val decodedScheduleCreateRecord1 = createDecodedScheduleCreateRecord(
      trainServiceCode = mainServiceCode,
      scheduleTrainId = mainScheduleTrainId,
      locationRecords = List(
        ScheduleLocationRecord(LocationType.OriginatingLocation,
                               stanoxRecord1.tipLocCode,
                               None,
                               Some(LocalTime.parse("12:10"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord2.tipLocCode,
                               Some(LocalTime.parse("12:14")),
                               Some(LocalTime.parse("12:15"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord3.tipLocCode,
                               Some(LocalTime.parse("12:24")),
                               Some(LocalTime.parse("12:25"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord4.tipLocCode,
                               Some(LocalTime.parse("12:35")),
                               Some(LocalTime.parse("12:36"))),
        ScheduleLocationRecord(LocationType.TerminatingLocation,
                               stanoxRecord5.tipLocCode,
                               Some(LocalTime.parse("12:45")),
                               None)
      )
    )

    val decodedScheduleCreateRecord2 = createDecodedScheduleCreateRecord(
      trainServiceCode = associatedServiceCode,
      scheduleTrainId = associatedScheduleTrainId,
      locationRecords = List(
        ScheduleLocationRecord(LocationType.OriginatingLocation,
                               stanoxRecord6.tipLocCode,
                               None,
                               Some(LocalTime.parse("12:00"))),
        ScheduleLocationRecord(LocationType.TerminatingLocation,
                               stanoxRecord1.tipLocCode,
                               Some(LocalTime.parse("12:08")),
                               None)
      )
    )

    AppInitialState(
      schedulePrimaryRecords = toScheduleLogs(decodedScheduleCreateRecord1,
                                              StanoxRecord.stanoxRecordsToMap(stanoxRecords)) ++ toScheduleLogs(
        decodedScheduleCreateRecord2,
        StanoxRecord.stanoxRecordsToMap(stanoxRecords)),
      stanoxRecords = stanoxRecords
    )
  }

  def createDefaultInitialStateWithDivideAssociation(
      mainScheduleTrainId: ScheduleTrainId = ScheduleTrainId("12345"),
      mainServiceCode: ServiceCode = ServiceCode("799984"),
      associatedScheduleTrainId: ScheduleTrainId = ScheduleTrainId("98573"),
      associatedServiceCode: ServiceCode = ServiceCode("62882")): AppInitialState = {

    val stanoxRecord1 =
      StanoxRecord(TipLocCode("REIGATE"), Some(StanoxCode(randomGen)), Some(CRS("REI")), Some("Reigate"), None)
    val stanoxRecord2 =
      StanoxRecord(TipLocCode("REDHILL"), Some(StanoxCode(randomGen)), Some(CRS("RDH")), Some("Redhill"), None)
    val stanoxRecord3 =
      StanoxRecord(TipLocCode("MERSTHAM"), Some(StanoxCode(randomGen)), Some(CRS("MER")), Some("Merstham"), None)
    val stanoxRecord4 =
      StanoxRecord(TipLocCode("EASTCRYD"), Some(StanoxCode(randomGen)), Some(CRS("ECR")), Some("East Croydon"), None)
    val stanoxRecord5 =
      StanoxRecord(TipLocCode("LONVIC"), Some(StanoxCode(randomGen)), Some(CRS("VIC")), Some("London Victoria"), None)
    val stanoxRecord6 =
      StanoxRecord(TipLocCode("LDNBRG"), Some(StanoxCode(randomGen)), Some(CRS("LBG")), Some("London Bridge"), None)

    val stanoxRecords = List(stanoxRecord1, stanoxRecord2, stanoxRecord3, stanoxRecord4, stanoxRecord5, stanoxRecord6)

    val decodedScheduleCreateRecord1 = createDecodedScheduleCreateRecord(
      trainServiceCode = mainServiceCode,
      scheduleTrainId = mainScheduleTrainId,
      locationRecords = List(
        ScheduleLocationRecord(LocationType.OriginatingLocation,
                               stanoxRecord1.tipLocCode,
                               None,
                               Some(LocalTime.parse("12:10"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord2.tipLocCode,
                               Some(LocalTime.parse("12:14")),
                               Some(LocalTime.parse("12:15"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord3.tipLocCode,
                               Some(LocalTime.parse("12:24")),
                               Some(LocalTime.parse("12:25"))),
        ScheduleLocationRecord(LocationType.IntermediateLocation,
                               stanoxRecord4.tipLocCode,
                               Some(LocalTime.parse("12:35")),
                               Some(LocalTime.parse("12:36"))),
        ScheduleLocationRecord(LocationType.TerminatingLocation,
                               stanoxRecord5.tipLocCode,
                               Some(LocalTime.parse("12:45")),
                               None)
      )
    )

    val decodedScheduleCreateRecord2 = createDecodedScheduleCreateRecord(
      trainServiceCode = associatedServiceCode,
      scheduleTrainId = associatedScheduleTrainId,
      locationRecords = List(
        ScheduleLocationRecord(LocationType.OriginatingLocation,
                               stanoxRecord4.tipLocCode,
                               None,
                               Some(LocalTime.parse("12:38"))),
        ScheduleLocationRecord(LocationType.TerminatingLocation,
                               stanoxRecord6.tipLocCode,
                               Some(LocalTime.parse("12:50")),
                               None)
      )
    )

    AppInitialState(
      schedulePrimaryRecords = toScheduleLogs(decodedScheduleCreateRecord1,
                                              StanoxRecord.stanoxRecordsToMap(stanoxRecords)) ++ toScheduleLogs(
        decodedScheduleCreateRecord2,
        StanoxRecord.stanoxRecordsToMap(stanoxRecords)),
      stanoxRecords = stanoxRecords
    )
  }

  def createAuthenticatedDetails(userId: UserId = UserId(Random.nextInt(Integer.MAX_VALUE).toString),
                                 emailAddress: String = "test@test.com",
                                 emailVerified: Option[Boolean] = Some(true),
                                 name: Option[String] = Some("joebloggs"),
                                 firstName: Option[String] = Some("Joe"),
                                 familyName: Option[String] = Some("Bloggs"),
                                 locale: Option[String] = Some("GB")) =
    AuthenticatedDetails(userId, emailAddress, emailVerified, name, firstName, familyName, locale)

  def serviceFrom(fixture: TrainDelaysTestFixture,
                  uIConfig: UIConfig,
                  authenticatedDetails: AuthenticatedDetails): HttpService[IO] = {

    val googleAuthenticator = MockGoogleAuthenticator(authenticatedDetails)
    Service(
      HistoryService(fixture.movementLogTable,
                     fixture.cancellationLogTable,
                     fixture.stanoxTable,
                     fixture.schedulePrimaryTable,
                     fixture.scheduleSecondaryTable),
      ScheduleService(fixture.stanoxTable,
                      fixture.subscriberTable,
                      fixture.schedulePrimaryTable,
                      fixture.scheduleSecondaryTable,
                      googleAuthenticator,
                      uIConfig),
      fixture.schedulePrimaryTable,
      fixture.scheduleSecondaryTable,
      fixture.stanoxTable,
      fixture.subscriberTable,
      uIConfig,
      googleAuthenticator
    )
  }

  def getFlushedRedisClient(implicit executionContext: ExecutionContext): IO[RedisClient] = {
    val redisClient = RedisClient()
    IO.fromFuture(IO(redisClient.flushall())).map(_ => redisClient)
  }

  def toScheduleLogs(scheduleRecordCreate: DecodedScheduleRecord.Create,
                     existingStanoxRecordsMap: Map[TipLocCode, StanoxCode]): List[ScheduleRecordPrimary] =
    DecodedScheduleRecord.decodedScheduleRecordToScheduleLogs(scheduleRecordCreate, existingStanoxRecordsMap)
}
