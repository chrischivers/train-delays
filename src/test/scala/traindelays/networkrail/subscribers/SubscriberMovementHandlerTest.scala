package traindelays.networkrail.subscribers

import java.time.{Instant, LocalTime, ZoneId, ZonedDateTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.LocationType
import traindelays.networkrail.scheduledata.{ScheduleTrainId, StanoxRecord}
import traindelays.networkrail.{CRS, ServiceCode, StanoxCode, TipLocCode}
import traindelays.{DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

class SubscriberMovementHandlerTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "email subscriber when movement log received relating to subscriber's FROM STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(fromStanoxCode))

    withInitialState(config)(
      initialState.copy(
        subscriberRecords = List(subscriberRecord)
      )) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("TRAIN MOVEMENT UPDATE")
      validateEmailBody(email.body, movementRecord, activationRecord, initialState.stanoxRecords)

    }
  }

  it should "email subscriber when movement log received relating to subscriber's TO STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(toStanoxCode))

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("TRAIN MOVEMENT UPDATE")
      validateEmailBody(email.body, movementRecord, activationRecord, initialState.stanoxRecords)
    }
  }

  it should "email subscriber when movement log received relating to a stanox between subscriber's FROM and TO STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.last.stanoxCode

    val midPointStanoxCode = Random.shuffle(initialState.scheduleLogRecords.drop(1).dropRight(1)).head.stanoxCode
    assert(midPointStanoxCode != fromStanoxCode && midPointStanoxCode != toStanoxCode)

    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(midPointStanoxCode))

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("TRAIN MOVEMENT UPDATE")
    }
  }

  it should "NOT email subscriber when movement log received relating to a stanox outside of subscriber's TO and FROM STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.drop(1).head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.dropRight(1).last.stanoxCode

    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord1 =
      createMovementRecord(trainId = trainId,
                           trainServiceCode = serviceCode,
                           stanoxCode = Some(initialState.scheduleLogRecords.head.stanoxCode))

    val movementRecord2 =
      createMovementRecord(trainId = trainId,
                           trainServiceCode = serviceCode,
                           stanoxCode = Some(initialState.scheduleLogRecords.last.stanoxCode))

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 0
    }
  }

  it should "NOT email subscriber when movement log received relates to correct stanox but different train ID" in {
    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = ScheduleTrainId(randomGen),
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(fromStanoxCode))

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 0
    }
  }

  it should "email multiple subscriber when movement log received relating to a stanox code within their stanox range" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.scheduleLogRecords.drop(1).head.stanoxCode
    val toStanoxCode   = initialState.scheduleLogRecords.dropRight(1).last.stanoxCode

    val subscriberRecord1 = createSubscriberRecord(
      userId = UserId(randomGen),
      emailAddress = "test1@gmail.com",
      scheduleTrainId = scheduleTrainId,
      serviceCode = serviceCode,
      fromStanoxCode = fromStanoxCode,
      toStanoxCode = toStanoxCode
    )

    val subscriberRecord2 = createSubscriberRecord(
      userId = UserId(randomGen),
      emailAddress = "test2@gmail.com",
      scheduleTrainId = scheduleTrainId,
      serviceCode = serviceCode,
      fromStanoxCode = fromStanoxCode,
      toStanoxCode = toStanoxCode
    )

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId,
                           trainServiceCode = serviceCode,
                           stanoxCode = Some(initialState.scheduleLogRecords(3).stanoxCode))

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord1, subscriberRecord2))) {
      fixture =>
        withQueues
          .map {
            case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
              trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
              trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

              runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
          }
          .unsafeRunSync()
        fixture.emailer.emailsSent should have size 2
        fixture.emailer.emailsSent.map(_.to) should contain theSameElementsAs List(subscriberRecord1.emailAddress,
                                                                                   subscriberRecord2.emailAddress)
        fixture.emailer.emailsSent.foreach { email =>
          validateEmailBody(email.body, movementRecord, activationRecord, initialState.stanoxRecords)
        }
    }
  }

  private def validateEmailBody(body: String,
                                movementRecord: TrainMovementRecord,
                                activationRecord: TrainActivationRecord,
                                stanoxRecords: List[StanoxRecord]) = {
    body should include(s"Train ID: ${activationRecord.scheduleTrainId.value}")
    val originStanox = stanoxRecords.find(_.stanoxCode == activationRecord.originStanox).get
    body should include(s"Train originated from: [${originStanox.crs.get.value}] ${originStanox.description.get}")

    val stanoxAffected = stanoxRecords.find(_.stanoxCode == movementRecord.stanoxCode.get).get
    body should include(s"Station affected: [${stanoxAffected.crs.get.value}] ${stanoxAffected.description.get}")
    body should include(s"Operator: ${movementRecord.toc.value}")
    body should include(s"Event type: ${movementRecord.eventType.string}")
    body should include(
      s"Expected time: ${SubscriberHandler.timestampToFormattedDateTime(movementRecord.plannedPassengerTimestamp.get)}")
    body should include(
      s"Actual time: ${SubscriberHandler.timestampToFormattedDateTime(movementRecord.actualTimestamp)}")
    body should include(
      s"Status: ${SubscriberHandler.statusTextFrom(movementRecord.variationStatus.get, movementRecord.plannedPassengerTimestamp.get, movementRecord.actualTimestamp)}")
  }
}
