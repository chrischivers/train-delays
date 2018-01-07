package traindelays.networkrail.subscribers

import java.util.UUID

import cats.effect.IO
import fs2.async
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import traindelays.networkrail.movementdata.{MovementProcessor, MovementRecord}
import traindelays.{ConfigLoader, DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SubscriberHandlerTest extends FlatSpec with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  it should "produce report for a single subscribed route and single movement record" in {

    val movementRecord   = createMovementRecord()
    val userID           = UUID.randomUUID().toString
    val email            = "test@test.com"
    val trainID          = movementRecord.trainId
    val serviceCode      = movementRecord.trainServiceCode
    val stanox           = movementRecord.stanox.get
    val subscriberRecord = SubscriberRecord(None, userID, email, trainID, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(subscriberRecords = List(subscriberRecord),
                      movementLogs = List(movementRecord.toMovementLog.get))) { fixture =>
      val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
      subscriberHandler.generateSubscriberReports.map { retrieved =>
        retrieved should have size 1
        retrieved.head.subscriberRecord shouldBe subscriberRecord.copy(id = Some(1))
        retrieved.head.movementLogs should have size 1
        retrieved.head.movementLogs.head shouldBe movementRecord.toMovementLog.get.copy(id = Some(1))

      }
    }
  }

  it should "produce report for a single subscribed route and multiple movement records" in {

    val movementRecord1  = createMovementRecord()
    val movementRecord2  = createMovementRecord(eventType = Some("DEPARTURE"))
    val movementRecord3  = createMovementRecord(trainId = "ANOTHER_ID")
    val userID           = UUID.randomUUID().toString
    val email            = "test@test.com"
    val trainID          = movementRecord1.trainId
    val serviceCode      = movementRecord1.trainServiceCode
    val stanox           = movementRecord1.stanox.get
    val subscriberRecord = SubscriberRecord(None, userID, email, trainID, serviceCode, stanox)

    withInitialState(config)(
      AppInitialState(
        subscriberRecords = List(subscriberRecord),
        movementLogs =
          List(movementRecord1.toMovementLog.get, movementRecord3.toMovementLog.get, movementRecord2.toMovementLog.get)
      )) { fixture =>
      val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
      subscriberHandler.generateSubscriberReports.map { retrieved =>
        retrieved should have size 1
        retrieved.head.subscriberRecord shouldBe subscriberRecord.copy(id = Some(1))
        retrieved.head.movementLogs should have size 2
        retrieved.head.movementLogs.head shouldBe movementRecord1.toMovementLog.get.copy(id = Some(1))
        retrieved.head.movementLogs(1) shouldBe movementRecord2.toMovementLog.get.copy(id = Some(3))

      }
    }
  }

  it should "email relevant subscribers when movement log received for which they are subscribed" in {

    val movementRecord1  = createMovementRecord()
    val userID           = UUID.randomUUID().toString
    val email            = "test@test.com"
    val trainID          = movementRecord1.trainId
    val serviceCode      = movementRecord1.trainServiceCode
    val stanox           = movementRecord1.stanox.get
    val subscriberRecord = SubscriberRecord(None, userID, email, trainID, serviceCode, stanox)

    val emailer = StubEmailer()

    withInitialState(config)(
      AppInitialState(
        subscriberRecords = List(subscriberRecord)
      )) { fixture =>
      val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)

      async
        .unboundedQueue[IO, MovementRecord]
        .map { queue =>
          queue.enqueue1(movementRecord1).unsafeRunSync()
          MovementProcessor(queue, fixture.movementLogTable, subscriberHandler).stream.run.unsafeRunTimed(3 seconds)
        }
    }

    emailer.emailsSent should have size 1
    emailer.emailsSent.head.to shouldBe email

  }
}
