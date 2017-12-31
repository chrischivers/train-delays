package traindelays.networkrail

import java.nio.file.Paths

import cats.effect.IO
import fs2.async.mutable.Queue
import org.scalactic.TripleEqualsSupport
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import traindelays.networkrail.movementdata.{MovementRecord, MovementsHandlerWatcher}
import traindelays.stomp.StompClient
import io.circe.parser._

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
        val movementsWatcher = new MovementsHandlerWatcher(queue)
        val stompClient      = StompClient(testconfig.networkRailConfig)
        stompClient
          .subscribe(testconfig.networkRailConfig.movements.topic, movementsWatcher)
          .unsafeRunSync()

        eventually {
          movementsWatcher.rawMessagesReceived.size shouldBe >(0)
          parse(movementsWatcher.rawMessagesReceived.head).right.get.as[List[MovementRecord]].right.get.size shouldBe >(
            0)
          queue.dequeueBatch1(3).unsafeRunSync().toList should have size 3
        }
      }
      .unsafeRunSync()
  }

  override protected def afterEach(): Unit =
    cleanUpFile(testconfig.networkRailConfig.scheduleData.tmpUnzipLocation.toString)

  private def cleanUpFile(location: String) =
    Paths.get(location).toFile.delete()
}
