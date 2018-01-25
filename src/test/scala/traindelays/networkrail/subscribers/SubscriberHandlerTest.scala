package traindelays.networkrail.subscribers

import java.util.UUID

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.{ConfigLoader, DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SubscriberHandlerTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "produce report for a single subscribed route and single movement record" in {

    val movementRecord   = createMovementRecord()
    val userID           = UserId(UUID.randomUUID().toString)
    val scheduleTrainId  = ScheduleTrainId("AXgGH")
    val email            = "test@test.com"
    val serviceCode      = movementRecord.trainServiceCode
    val stanox           = movementRecord.stanox.get
    val subscriberRecord = SubscriberRecord(None, userID, email, scheduleTrainId, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(subscriberRecords = List(subscriberRecord),
                      movementLogs = List(movementRecordToMovementLog(movementRecord, None, scheduleTrainId)))) {
      fixture =>
        val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
        val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
        subscriberHandler.generateSubscriberReports.map { retrieved =>
          retrieved should have size 1
          retrieved.head.subscriberRecord shouldBe subscriberRecord.copy(id = Some(1))
          retrieved.head.movementLogs should have size 1
          retrieved.head.movementLogs.head shouldBe movementRecordToMovementLog(movementRecord,
                                                                                Some(1),
                                                                                scheduleTrainId)

        }
    }
  }

  it should "produce report for a single subscribed route and multiple movement records" in {

    val movementRecord1  = createMovementRecord()
    val movementRecord2  = createMovementRecord(eventType = Departure)
    val movementRecord3  = createMovementRecord(trainId = TrainId("ANOTHER_ID"))
    val userID           = UserId(UUID.randomUUID().toString)
    val scheduleTrainId  = ScheduleTrainId("AXgGH")
    val email            = "test@test.com"
    val trainID          = movementRecord1.trainId
    val serviceCode      = movementRecord1.trainServiceCode
    val stanox           = movementRecord1.stanox.get
    val subscriberRecord = SubscriberRecord(None, userID, email, scheduleTrainId, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(
        subscriberRecords = List(subscriberRecord),
        movementLogs = List(
          movementRecordToMovementLog(movementRecord1, None, scheduleTrainId),
          movementRecordToMovementLog(movementRecord3, None, ScheduleTrainId("Another_ID")),
          movementRecordToMovementLog(movementRecord2, None, scheduleTrainId)
        )
      )) { fixture =>
      val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
      subscriberHandler.generateSubscriberReports.map { retrieved =>
        retrieved should have size 1
        retrieved.head.subscriberRecord shouldBe subscriberRecord.copy(id = Some(1))
        retrieved.head.movementLogs should have size 2
        retrieved.head.movementLogs.head shouldBe movementRecordToMovementLog(movementRecord1, Some(1), scheduleTrainId)
        retrieved.head.movementLogs(1) shouldBe movementRecordToMovementLog(movementRecord2, Some(3), scheduleTrainId)

      }
    }
  }

  it should "email relevant subscribers when movement log received for which they are subscribed" in {

    val activationRecord = createActivationRecord()
    val movementRecord1  = createMovementRecord()

    val userID1          = UserId(UUID.randomUUID().toString)
    val email1           = "test1@test.com"
    val scheduleTrainId1 = activationRecord.scheduleTrainId
    val serviceCode1     = activationRecord.trainServiceCode
    val stanox1          = movementRecord1.stanox.get

    val userID2          = UserId(UUID.randomUUID().toString)
    val email2           = "test2@test.com"
    val scheduleTrainId2 = ScheduleTrainId("ID123")
    val serviceCode2     = activationRecord.trainServiceCode
    val stanox2          = movementRecord1.stanox.get

    val userID3          = UserId(UUID.randomUUID().toString)
    val email3           = "test3@test.com"
    val scheduleTrainId3 = activationRecord.scheduleTrainId
    val serviceCode3     = activationRecord.trainServiceCode
    val stanox3          = movementRecord1.stanox.get

    val subscriberRecord1 = SubscriberRecord(None, userID1, email1, scheduleTrainId1, serviceCode1, stanox1)
    val subscriberRecord2 = SubscriberRecord(None, userID2, email2, scheduleTrainId2, serviceCode2, stanox2)
    val subscriberRecord3 = SubscriberRecord(None, userID3, email3, scheduleTrainId3, serviceCode3, stanox3)

    val emailer = StubEmailer()

    withInitialState(config)(
      AppInitialState(
        subscriberRecords = List(subscriberRecord1, subscriberRecord2, subscriberRecord3)
      )) { fixture =>
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()

            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)
            TrainMovementProcessor(trainMovementQueue,
                                   fixture.movementLogTable,
                                   subscriberHandler,
                                   fixture.trainActivationCache).stream.run.unsafeRunTimed(1 second)
        }
    }

    emailer.emailsSent should have size 2
    emailer.emailsSent.head.to shouldBe email1
    emailer.emailsSent(1).to shouldBe email3

  }

  it should "email relevant subscribers when cancellation log received for which they are subscribed" in {
    val activationRecord   = createActivationRecord()
    val cancellationRecord = createCancellationRecord()

    val userID1          = UserId(UUID.randomUUID().toString)
    val email1           = "test1@test.com"
    val scheduleTrainId1 = activationRecord.scheduleTrainId
    val serviceCode1     = activationRecord.trainServiceCode
    val stanox1          = cancellationRecord.stanox

    val userID2          = UserId(UUID.randomUUID().toString)
    val email2           = "test2@test.com"
    val scheduleTrainId2 = ScheduleTrainId("ID123")
    val serviceCode2     = activationRecord.trainServiceCode
    val stanox2          = cancellationRecord.stanox

    val userID3          = UserId(UUID.randomUUID().toString)
    val email3           = "test3@test.com"
    val scheduleTrainId3 = activationRecord.scheduleTrainId
    val serviceCode3     = activationRecord.trainServiceCode
    val stanox3          = cancellationRecord.stanox

    val subscriberRecord1 = SubscriberRecord(None, userID1, email1, scheduleTrainId1, serviceCode1, stanox1)
    val subscriberRecord2 = SubscriberRecord(None, userID2, email2, scheduleTrainId2, serviceCode2, stanox2)
    val subscriberRecord3 = SubscriberRecord(None, userID3, email3, scheduleTrainId3, serviceCode3, stanox3)

    val emailer = StubEmailer()

    withInitialState(config)(
      AppInitialState(
        subscriberRecords = List(subscriberRecord1, subscriberRecord2, subscriberRecord3)
      )) { fixture =>
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()

            TrainActivationProcessor(trainActivationQueue, fixture.trainActivationCache).stream.run
              .unsafeRunTimed(1 second)
            TrainCancellationProcessor(trainCancellationQueue,
                                       subscriberHandler,
                                       fixture.cancellationLogTable,
                                       fixture.trainActivationCache).stream.run.unsafeRunTimed(1 second)
        }
    }

    emailer.emailsSent should have size 2
    emailer.emailsSent.head.to shouldBe email1
    emailer.emailsSent(1).to shouldBe email3

  }

}
