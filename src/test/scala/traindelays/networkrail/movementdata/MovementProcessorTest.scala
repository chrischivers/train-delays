package traindelays.networkrail.movementdata

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.{ConfigLoader, DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MovementProcessorTest extends FlatSpec with Eventually with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10 seconds), interval = scaled(1 seconds))

  it should "persist Movement records in DB where all relevant fields exist" in {

    val activationRecord = createActivationRecord()
    val movementRecord   = createMovementRecord()
    withInitialState(config)() { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()
            val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
            val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)
            TrainMovementProcessor(trainMovementQueue,
                                   fixture.movementLogTable,
                                   subscriberHandler,
                                   fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            fixture.movementLogTable
              .retrieveAllRecords()
              .map { retrievedRecords =>
                retrievedRecords should have size 1
                retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord,
                                                                           Some(1),
                                                                           activationRecord.scheduleTrainId)
              }
              .unsafeRunSync()
        }
    }
  }

  it should "not process Movement records where one or more relevant fields do not exist" in {

    val activationRecord1 =
      createActivationRecord(trainId = TrainId("ABCDE"), scheduleTrainId = ScheduleTrainId("98765"))
    val activationRecord2 =
      createActivationRecord(trainId = TrainId("FGHIJ"), scheduleTrainId = ScheduleTrainId("54321"))
    val movementRecord1 = createMovementRecord(stanox = None, trainId = activationRecord1.trainId)
    val movementRecord2 = createMovementRecord(trainId = activationRecord2.trainId)
    withInitialState(config)() { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue) =>
            trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
            trainActivationQueue.enqueue1(activationRecord2).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()
            val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
            val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            TrainMovementProcessor(trainMovementQueue,
                                   fixture.movementLogTable,
                                   subscriberHandler,
                                   fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            fixture.movementLogTable
              .retrieveAllRecords()
              .map { retrievedRecords =>
                retrievedRecords should have size 1
                retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord2,
                                                                           Some(1),
                                                                           activationRecord2.scheduleTrainId)
              }
              .unsafeRunSync()
        }
    }
  }

  it should "expire activation records after period of time elapses" in {

    val activationRecord1 =
      createActivationRecord(scheduleTrainId = ScheduleTrainId("G99876"), trainId = TrainId("123456789"))
    val activationRecord2 =
      createActivationRecord(scheduleTrainId = ScheduleTrainId("G76489"), trainId = TrainId("98765432"))
    val movementRecord1 = createMovementRecord(trainId = activationRecord1.trainId)
    val movementRecord2 = createMovementRecord(trainId = activationRecord2.trainId)
    withInitialState(config, redisCacheExpiry = 3 seconds)() { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue) =>
            trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
            trainActivationQueue.enqueue1(activationRecord2).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()

            val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
            val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            val trainMovementProcessor = TrainMovementProcessor(trainMovementQueue,
                                                                fixture.movementLogTable,
                                                                subscriberHandler,
                                                                fixture.trainActivationCache)

            trainMovementProcessor.stream.run
              .unsafeRunTimed(1 second)

            fixture.movementLogTable
              .retrieveAllRecords()
              .map { retrievedRecords =>
                retrievedRecords should have size 1
                retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord1,
                                                                           Some(1),
                                                                           activationRecord1.scheduleTrainId)
              }
              .unsafeRunSync()

            Thread.sleep(3000)
            trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

            trainMovementProcessor.stream.run
              .unsafeRunTimed(1 second)

            fixture.movementLogTable
              .retrieveAllRecords()
              .map { retrievedRecords =>
                retrievedRecords should have size 1
              }
              .unsafeRunSync()
        }
    }
  }

  it should "only process movement records where TrainID to ScheduleTrainID mapping is found in cache" in {
    val activationRecord1 =
      createActivationRecord(scheduleTrainId = ScheduleTrainId("G99876"), trainId = TrainId("123456789"))
    val movementRecord1 = createMovementRecord()
    val movementRecord2 = createMovementRecord(trainId = activationRecord1.trainId)
    withInitialState(config)() { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue) =>
            trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

            val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
            val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            TrainMovementProcessor(trainMovementQueue,
                                   fixture.movementLogTable,
                                   subscriberHandler,
                                   fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)

            fixture.movementLogTable
              .retrieveAllRecords()
              .map { retrievedRecords =>
                retrievedRecords should have size 1
                retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord2,
                                                                           Some(1),
                                                                           activationRecord1.scheduleTrainId)
              }
              .unsafeRunSync()
        }
    }
  }

}
