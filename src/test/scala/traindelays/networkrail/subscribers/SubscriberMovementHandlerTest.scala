package traindelays.networkrail.subscribers

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.{ServiceCode, TestFeatures}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Random, Try}

class SubscriberMovementHandlerTest extends FlatSpec with TestFeatures {

  it should "email subscriber when movement log received relating to subscriber's FROM STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(fromStanoxCode))

    withInitialState(testDatabaseConfig)(
      initialState.copy(
        subscriberRecords = List(subscriberRecord)
      )) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("Train Delay Helper: Delay Update")
      validateEmailBody(email.body, movementRecord, activationRecord, initialState.stanoxRecords)

    }
  }

  it should "NOT email subscriber when train is on time or early" in {

    List(VariationStatus.Early, VariationStatus.OnTime).foreach { status =>
      val scheduleTrainId = ScheduleTrainId(randomGen)
      val serviceCode     = ServiceCode(randomGen)
      val trainId         = TrainId(randomGen)

      val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
      val fromStanoxCode = initialState.schedulePrimaryRecords.head.stanoxCode
      val toStanoxCode   = initialState.schedulePrimaryRecords.last.stanoxCode
      val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                    serviceCode = serviceCode,
                                                    fromStanoxCode = fromStanoxCode,
                                                    toStanoxCode = toStanoxCode)

      val activationRecord =
        createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
      val movementRecord =
        createMovementRecord(trainId = trainId,
                             trainServiceCode = serviceCode,
                             stanoxCode = Some(fromStanoxCode),
                             variationStatus = Some(status))

      withInitialState(testDatabaseConfig)(
        initialState.copy(
          subscriberRecords = List(subscriberRecord)
        )) { fixture =>
        withQueues { queues =>
          queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
          queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

          runAllQueues(queues, fixture)
        }
        fixture.emailer.emailsSent should have size 0
      }
    }
  }

  it should "email subscriber when movement log received relating to subscriber's TO STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(toStanoxCode))

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("Train Delay Helper: Delay Update")
      validateEmailBody(email.body, movementRecord, activationRecord, initialState.stanoxRecords)
    }
  }

  it should "email subscriber when movement log received relating to a stanox between subscriber's FROM and TO STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.last.stanoxCode

    val midPointStanoxCode = Random.shuffle(initialState.schedulePrimaryRecords.drop(1).dropRight(1)).head.stanoxCode
    assert(midPointStanoxCode != fromStanoxCode && midPointStanoxCode != toStanoxCode)

    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(midPointStanoxCode))

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }

      fixture.emailer.emailsSent should have size 1
      val email = fixture.emailer.emailsSent.head
      email.to shouldBe subscriberRecord.emailAddress
      email.subject should include("Train Delay Helper: Delay Update")
    }
  }

  it should "NOT email subscriber when movement log received relating to a stanox outside of subscriber's TO and FROM STANOX" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.drop(1).head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.dropRight(1).last.stanoxCode

    val subscriberRecord = createSubscriberRecord(scheduleTrainId = scheduleTrainId,
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord1 =
      createMovementRecord(trainId = trainId,
                           trainServiceCode = serviceCode,
                           stanoxCode = Some(initialState.schedulePrimaryRecords.head.stanoxCode))

    val movementRecord2 =
      createMovementRecord(trainId = trainId,
                           trainServiceCode = serviceCode,
                           stanoxCode = Some(initialState.schedulePrimaryRecords.last.stanoxCode))

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord1).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord2).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 0
    }
  }

  it should "NOT email subscriber when movement log received relates to correct stanox but different train ID" in {
    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.last.stanoxCode
    val subscriberRecord = createSubscriberRecord(scheduleTrainId = ScheduleTrainId(randomGen),
                                                  serviceCode = serviceCode,
                                                  fromStanoxCode = fromStanoxCode,
                                                  toStanoxCode = toStanoxCode)

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId, originStanox = fromStanoxCode)
    val movementRecord =
      createMovementRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = Some(fromStanoxCode))

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 0
    }
  }

  it should "email multiple subscriber when movement log received relating to a stanox code within their stanox range" in {

    val scheduleTrainId = ScheduleTrainId(randomGen)
    val serviceCode     = ServiceCode(randomGen)
    val trainId         = TrainId(randomGen)

    val initialState   = createDefaultInitialState(scheduleTrainId, serviceCode)
    val fromStanoxCode = initialState.schedulePrimaryRecords.drop(1).head.stanoxCode
    val toStanoxCode   = initialState.schedulePrimaryRecords.dropRight(1).last.stanoxCode

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
                           stanoxCode = Some(initialState.schedulePrimaryRecords(3).stanoxCode))

    withInitialState(testDatabaseConfig)(
      initialState.copy(subscriberRecords = List(subscriberRecord1, subscriberRecord2))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainMovementQueue.enqueue1(movementRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
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
    body should include(s"Date: ${timestampToFormattedDate(activationRecord.originDepartureTimestamp)}")
    val originStanox = stanoxRecords.find(_.stanoxCode.get == activationRecord.originStanox).get
    body should include(s"Train originated from: [${originStanox.crs.get.value}] ${originStanox.description.get}")

    val stanoxAffected = stanoxRecords.find(_.stanoxCode.get == movementRecord.stanoxCode.get).get
    body should include(s"Station affected: [${stanoxAffected.crs.get.value}] ${stanoxAffected.description.get}")
    body should include(s"Operator: ${movementRecord.toc.value}")
    body should include(s"Event type: ${movementRecord.eventType.string}")
    body should include(s"Expected time: ${timestampToFormattedTime(movementRecord.plannedPassengerTimestamp.get)}")
    body should include(s"Actual time: ${timestampToFormattedTime(movementRecord.actualTimestamp)}")
    body should include(
      s"Status: ${SubscriberHandler.statusTextFrom(movementRecord.variationStatus.get, movementRecord.plannedPassengerTimestamp.get, movementRecord.actualTimestamp)}")
  }

  private def timestampToFormattedTime(timestamp: Long): String = {
    val timeZone = ZoneId.of("Europe/London")
    Try {
      val time = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone).toLocalTime
      SubscriberHandler.timeFormatter(time)
    }.getOrElse("N/A")
  }

  private def timestampToFormattedDate(timestamp: Long): String = {
    val timeZone = ZoneId.of("Europe/London")
    Try {
      val date = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timeZone).toLocalDate
      SubscriberHandler.dateFormatter(date)
    }.getOrElse("N/A")
  }

}
