package traindelays.networkrail

import java.nio.file.Paths

import cats.effect.IO
import fs2.async
import io.circe.parser._
import org.scalactic.TripleEqualsSupport
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import traindelays.networkrail.db.common.{AppInitialState, withInitialState}
import traindelays.networkrail.movementdata.{MovementHandlerWatcher, MovementProcessor, MovementRecord}
import traindelays.stomp.StompClient

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class MovementsListenerIntegrationTest
    extends FlatSpec
    with IntegrationTest
    with TripleEqualsSupport
    with BeforeAndAfterEach
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(30 seconds), interval = scaled(2 seconds))

  it should "subscribe to a topic and receive updates" in {

    fs2.async
      .unboundedQueue[IO, MovementRecord]
      .map { queue =>
        val movementWatcher = new MovementHandlerWatcher(queue)
        val stompClient     = StompClient(testconfig.networkRailConfig)
        stompClient
          .subscribe(testconfig.networkRailConfig.movements.topic, movementWatcher)
          .unsafeRunSync()

        eventually {
          movementWatcher.rawMessagesReceived.size shouldBe >(0)
          parse(movementWatcher.rawMessagesReceived.head).right.get.as[List[MovementRecord]].right.get.size shouldBe >(
            0)
          queue.dequeueBatch1(3).unsafeRunSync().toList should have size 3
        }
      }
      .unsafeRunSync()
  }

  it should "persist movement records where all details exist to DB" in {

    withInitialState(testconfig.databaseConfig)(AppInitialState.empty) { fixture =>
      async
        .unboundedQueue[IO, MovementRecord]
        .map { queue =>
          val movementWatcher = new MovementHandlerWatcher(queue)
          val stompClient     = StompClient(testconfig.networkRailConfig)
          stompClient
            .subscribe(testconfig.networkRailConfig.movements.topic, movementWatcher)
            .unsafeRunSync()

          MovementProcessor(queue, fixture.movementLogTable).stream.run.unsafeRunTimed(10 seconds)

          fixture.movementLogTable
            .retrieveAllRecords()
            .map { retrievedRecords =>
              retrievedRecords.size shouldBe >(0)
            }
            .unsafeRunSync()
        }
    }
  }

  override protected def afterEach(): Unit =
    cleanUpFile(testconfig.networkRailConfig.scheduleData.tmpUnzipLocation.toString)

  private def cleanUpFile(location: String) =
    Paths.get(location).toFile.delete()
}
