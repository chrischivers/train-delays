package traindelays.networkrail.subscribers

import java.time.LocalTime

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

class SubscriberCancellationHandlerTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

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

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId)
    val cancellationRecord =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = fromStanoxCode)

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 1
      fixture.emailer.emailsSent.head.to shouldBe subscriberRecord.emailAddress
      fixture.emailer.emailsSent.head.subject should include("TRAIN CANCELLATION UPDATE")
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

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId)
    val cancellationRecord1 =
      createCancellationRecord(trainId = trainId,
                               trainServiceCode = serviceCode,
                               stanoxCode = initialState.scheduleLogRecords.head.stanoxCode)

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainCancellationQueue.enqueue1(cancellationRecord1).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
      fixture.emailer.emailsSent should have size 1
      fixture.emailer.emailsSent.head.to shouldBe subscriberRecord.emailAddress
      fixture.emailer.emailsSent.head.subject should include("TRAIN CANCELLATION UPDATE")
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

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId)
    val cancellationLog =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode, stanoxCode = fromStanoxCode)

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord))) { fixture =>
      withQueues
        .map {
          case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
            trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
            trainCancellationQueue.enqueue1(cancellationLog).unsafeRunSync()

            runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
        }
        .unsafeRunSync()
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

    val activationRecord = createActivationRecord(scheduleTrainId, serviceCode, trainId)
    val cancellationRecord =
      createCancellationRecord(trainId = trainId, trainServiceCode = serviceCode)

    withInitialState(config)(initialState.copy(subscriberRecords = List(subscriberRecord1, subscriberRecord2))) {
      fixture =>
        withQueues
          .map {
            case (trainMovementQueue, trainActivationQueue, trainCancellationQueue) =>
              trainActivationQueue.enqueue1(activationRecord).unsafeRunSync()
              trainCancellationQueue.enqueue1(cancellationRecord).unsafeRunSync()

              runAllQueues(trainActivationQueue, trainMovementQueue, trainCancellationQueue, fixture)
          }
          .unsafeRunSync()
        fixture.emailer.emailsSent should have size 2
        fixture.emailer.emailsSent.map(_.to) should contain theSameElementsAs List(subscriberRecord1.emailAddress,
                                                                                   subscriberRecord2.emailAddress)
    }
  }

  def createDefaultInitialState(scheduleTrainId: ScheduleTrainId, serviceCode: ServiceCode): AppInitialState = {

    val stanoxRecord1 = StanoxRecord(StanoxCode(randomGen), TipLocCode("REIGATE"), Some(CRS("REI")), None, None)
    val stanoxRecord2 = StanoxRecord(StanoxCode(randomGen), TipLocCode("REDHILL"), Some(CRS("RDH")), None, None)
    val stanoxRecord3 = StanoxRecord(StanoxCode(randomGen), TipLocCode("MERSTHAM"), Some(CRS("MER")), None, None)
    val stanoxRecord4 = StanoxRecord(StanoxCode(randomGen), TipLocCode("EASTCRYD"), Some(CRS("ECR")), None, None)
    val stanoxRecord5 = StanoxRecord(StanoxCode(randomGen), TipLocCode("LONVIC"), Some(CRS("VIC")), None, None)
    val stanoxRecords = List(stanoxRecord1, stanoxRecord2, stanoxRecord3, stanoxRecord4, stanoxRecord5)

    val scheduleRecord = createScheduleRecord(
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
      scheduleLogRecords = scheduleRecord.toScheduleLogs(stanoxRecordsToMap(stanoxRecords)),
      stanoxRecords = stanoxRecords
    )
  }

}
