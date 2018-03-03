package traindelays.networkrail.subscribers

import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.{ServiceCode, TestFeatures}
import traindelays.networkrail.movementdata._
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.ExecutionContext.Implicits.global

class SubscriberCancellationHandlerTest extends FlatSpec with TestFeatures {

  it should "email subscriber when cancellation log received relating to subscriber's ROUTE" in {

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
    val cancellationRecord =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = fromStanoxCode)

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 1
      fixture.emailer.emailsSent.head.to shouldBe subscriberRecord.emailAddress
      fixture.emailer.emailsSent.head.subject should include("Train Delay Helper: Cancel Update")
    }
  }

  it should "still email subscriber when cancellation log received relating to subscriber's route despite being outside of subscriber's TO and FROM STANOX" in {

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

    val activationRecord = createActivationRecord(scheduleTrainId,
                                                  serviceCode,
                                                  trainId,
                                                  originStanox = initialState.scheduleLogRecords.head.stanoxCode)
    val cancellationRecord1 =
      createCancellationRecord(trainId = trainId,
                               trainServiceCode = serviceCode,
                               stanoxCode = initialState.scheduleLogRecords.head.stanoxCode)

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationRecord1).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 1
      fixture.emailer.emailsSent.head.to shouldBe subscriberRecord.emailAddress
      fixture.emailer.emailsSent.head.subject should include("Train Delay Helper: Cancel Update")
    }
  }

  it should "NOT email subscriber when cancellation log received relates to different train ID" in {
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
    val cancellationLog =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = fromStanoxCode)

    withInitialState(testDatabaseConfig)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationLog).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 0
    }
  }

  it should "email multiple subscriber when cancellation log received relating to route" in {

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
      serviceCode = serviceCode
    )

    val subscriberRecord2 = createSubscriberRecord(
      userId = UserId(randomGen),
      emailAddress = "test2@gmail.com",
      scheduleTrainId = scheduleTrainId,
      serviceCode = serviceCode
    )

    val activationRecord = createActivationRecord(scheduleTrainId,
                                                  serviceCode,
                                                  trainId,
                                                  originStanox = initialState.scheduleLogRecords.head.stanoxCode)
    val cancellationRecord =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = fromStanoxCode)

    withInitialState(testDatabaseConfig)(
      initialState.copy(subscriberRecords = List(subscriberRecord1, subscriberRecord2))) { fixture =>
      withQueues { queues =>
        queues.trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
        queues.trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()

        runAllQueues(queues, fixture)
      }
      fixture.emailer.emailsSent should have size 2
      fixture.emailer.emailsSent.map(_.to) should contain theSameElementsAs List(subscriberRecord1.emailAddress,
                                                                                 subscriberRecord2.emailAddress)
    }
  }
}
