package traindelays.networkrail.movementdata

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import traindelays.networkrail.TestFeatures
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

class MovementProcessorTest extends FlatSpec with Eventually with TestFeatures {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10 seconds), interval = scaled(1 seconds))

  it should "persist Movement records in DB where all relevant fields exist" in {

    val activationRecord = createActivationRecord()
    val movementRecord   = createMovementRecord()
    withInitialState(testDatabaseConfig)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()
        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)
        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)
        TrainMovementProcessor(queues.trainMovementQueue,
                               fixture.movementLogTable,
                               subscriberHandler,
                               fixture.trainActivationCache,
                               fixture.metricsLogging.incrMovementRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.movementLogTable
          .retrieveAllRecords()
          .map { retrievedRecords =>
            retrievedRecords should have size 1
            retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord,
                                                                       Some(1),
                                                                       activationRecord.scheduleTrainId,
                                                                       activationRecord.originStanox,
                                                                       activationRecord.originDepartureTimestamp)
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
    val movementRecord1 = createMovementRecord(stanoxCode = None, trainId = activationRecord1.trainId)
    val movementRecord2 = createMovementRecord(trainId = activationRecord2.trainId)
    withInitialState(testDatabaseConfig)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
        queues.trainActivationQueue.enqueue1(activationRecord2).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()
        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)

        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        TrainMovementProcessor(queues.trainMovementQueue,
                               fixture.movementLogTable,
                               subscriberHandler,
                               fixture.trainActivationCache,
                               fixture.metricsLogging.incrMovementRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.movementLogTable
          .retrieveAllRecords()
          .map { retrievedRecords =>
            retrievedRecords should have size 1
            retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord2,
                                                                       Some(1),
                                                                       activationRecord2.scheduleTrainId,
                                                                       activationRecord2.originStanox,
                                                                       activationRecord2.originDepartureTimestamp)
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
    withInitialState(testDatabaseConfig, redisCacheExpiry = 3 seconds)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
        queues.trainActivationQueue.enqueue1(activationRecord2).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()

        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)

        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        val trainMovementProcessor = TrainMovementProcessor(queues.trainMovementQueue,
                                                            fixture.movementLogTable,
                                                            subscriberHandler,
                                                            fixture.trainActivationCache,
                                                            fixture.metricsLogging.incrMovementRecordsReceived)

        trainMovementProcessor.stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.movementLogTable
          .retrieveAllRecords()
          .map { retrievedRecords =>
            retrievedRecords should have size 1
            retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord1,
                                                                       Some(1),
                                                                       activationRecord1.scheduleTrainId,
                                                                       activationRecord1.originStanox,
                                                                       activationRecord1.originDepartureTimestamp)
          }
          .unsafeRunSync()

        Thread.sleep(3000)
        queues.trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

        trainMovementProcessor.stream.compile.drain
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
    withInitialState(testDatabaseConfig)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord1).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)

        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        TrainMovementProcessor(queues.trainMovementQueue,
                               fixture.movementLogTable,
                               subscriberHandler,
                               fixture.trainActivationCache,
                               fixture.metricsLogging.incrMovementRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.movementLogTable
          .retrieveAllRecords()
          .map { retrievedRecords =>
            retrievedRecords should have size 1
            retrievedRecords.head shouldBe movementRecordToMovementLog(movementRecord2,
                                                                       Some(1),
                                                                       activationRecord1.scheduleTrainId,
                                                                       activationRecord1.originStanox,
                                                                       activationRecord1.originDepartureTimestamp)
          }
          .unsafeRunSync()
      }
    }
  }

  "Train Cancellation Processor" should "persist Cancellation records in DB where all relevant fields exist" in {

    val activationRecord   = createActivationRecord()
    val cancellationRecord = createCancellationRecord()
    withInitialState(testDatabaseConfig)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()
        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)
        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)
        TrainCancellationProcessor(
          queues.trainCancellationQueue,
          subscriberHandler,
          fixture.cancellationLogTable,
          fixture.trainActivationCache,
          fixture.metricsLogging.incrCancellationRecordsReceived
        ).stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.cancellationLogTable
          .retrieveAllRecords()
          .map { retrievedRecords =>
            retrievedRecords should have size 1
            retrievedRecords.head shouldBe cancellationRecordToCancellationLog(
              cancellationRecord,
              Some(1),
              activationRecord.scheduleTrainId,
              activationRecord.originStanox,
              activationRecord.originDepartureTimestamp)
          }
          .unsafeRunSync()
      }
    }
  }

  "Metrics Logger" should "increment metrics when records received" in {

    val activationRecord   = createActivationRecord()
    val movementRecord     = createMovementRecord()
    val cancellationRecord = createCancellationRecord()
    withInitialState(testDatabaseConfig)() { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()
        val emailer = Emailer(config.emailerConfig, fixture.metricsLogging)
        val subscriberHandler =
          SubscriberHandler(fixture.movementLogTable,
                            fixture.subscriberTable,
                            fixture.scheduleTable,
                            fixture.stanoxTable,
                            emailer)
        TrainActivationProcessor(queues.trainActivationQueue,
                                 fixture.trainActivationCache,
                                 fixture.metricsLogging.incrActivationRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)
        TrainMovementProcessor(queues.trainMovementQueue,
                               fixture.movementLogTable,
                               subscriberHandler,
                               fixture.trainActivationCache,
                               fixture.metricsLogging.incrMovementRecordsReceived).stream.compile.drain
          .unsafeRunTimed(1 second)

        TrainCancellationProcessor(
          queues.trainCancellationQueue,
          subscriberHandler,
          fixture.cancellationLogTable,
          fixture.trainActivationCache,
          fixture.metricsLogging.incrCancellationRecordsReceived
        ).stream.compile.drain
          .unsafeRunTimed(1 second)

        fixture.metricsLogging.getActivationRecordsCount shouldBe 1
        fixture.metricsLogging.getMovementRecordsCount shouldBe 1
        fixture.metricsLogging.getCancellationRecordsCount shouldBe 1
      }
    }
  }

}
