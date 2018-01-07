package traindelays.networkrail.movementdata

import cats.effect.IO
import fs2.async
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import traindelays.networkrail.subscribers.{Emailer, SubscriberHandler}
import traindelays.{ConfigLoader, DatabaseConfig, TestFeatures}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MovementLoggerTest extends FlatSpec with Eventually with TestFeatures {

  protected def config: DatabaseConfig = testDatabaseConfig()

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10 seconds), interval = scaled(1 seconds))

  it should "process Movement records where all relevant fields exist" in {

    val movementRecord = createMovementRecord()
    withInitialState(config)() { fixture =>
      async
        .unboundedQueue[IO, MovementRecord]
        .map { queue =>
          queue.enqueue1(movementRecord).unsafeRunSync()
          val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
          val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
          MovementProcessor(queue, fixture.movementLogTable, subscriberHandler).stream.run.unsafeRunTimed(3 seconds)

          fixture.movementLogTable
            .retrieveAllRecords()
            .map { retrievedRecords =>
              retrievedRecords should have size 1
              retrievedRecords.head shouldBe movementRecord.toMovementLog.get.copy(id = Some(1))
            }
            .unsafeRunSync()
        }
    }
  }

  it should "not process Movement records where one or more relevant fields do not exist" in {

    val movementRecord1 = createMovementRecord(actualTimestamp = None)
    val movementRecord2 = createMovementRecord()
    withInitialState(config)() { fixture =>
      async
        .unboundedQueue[IO, MovementRecord]
        .map { queue =>
          queue.enqueue1(movementRecord1).unsafeRunSync()
          queue.enqueue1(movementRecord2).unsafeRunSync()
          val emailer           = Emailer(ConfigLoader.defaultConfig.emailerConfig)
          val subscriberHandler = SubscriberHandler(fixture.movementLogTable, fixture.subscriberTable, emailer)
          MovementProcessor(queue, fixture.movementLogTable, subscriberHandler).stream.run.unsafeRunTimed(3 seconds)

          fixture.movementLogTable
            .retrieveAllRecords()
            .map { retrievedRecords =>
              retrievedRecords should have size 1
              retrievedRecords.head shouldBe movementRecord2.toMovementLog.get.copy(id = Some(1))
            }
            .unsafeRunSync()
        }
    }
  }

}
