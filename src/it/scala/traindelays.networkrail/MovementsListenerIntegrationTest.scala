package traindelays.networkrail

import java.nio.file.Paths

import org.scalactic.TripleEqualsSupport
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import traindelays.stomp.StompClient
import scala.concurrent.duration._

class MovementsListenerIntegrationTest
    extends FlatSpec
    with IntegrationTest
    with TripleEqualsSupport
    with BeforeAndAfterEach
    with Eventually {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(1000 seconds), interval = scaled(10 seconds))

  it should "subscribe to a topic and receive updates" in {

    val movementsWatcher = new MovementsHandlerWatcher()
    val stompClient      = StompClient(testconfig.networkRailConfig)
    stompClient.subscribe(testconfig.networkRailConfig.movements.topic, movementsWatcher).unsafeRunSync()

    eventually {
      movementsWatcher.messagesReceived should have size 1
    }
  }

  override protected def afterEach(): Unit =
    cleanUpFile(testconfig.networkRailConfig.scheduleData.tmpUnzipLocation.toString)

  private def cleanUpFile(location: String) =
    Paths.get(location).toFile.delete()
}
