package traindelays.networkrail

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.stomp.{StompClient, StompStreamListener}

import scala.collection.JavaConverters._

trait MockStompClient {
  val client: StompClient
  def sendMessage(topic: String, message: String)

}

object MockStompClient extends StrictLogging {

  private val subscribedTo       = new AtomicReference[Option[String]](None)
  private val listenerSubscribed = new AtomicReference[Option[StompStreamListener]](None)
  private val clientConnected    = new AtomicBoolean(false)

  def apply() = new MockStompClient {

    clientConnected.set(true)

    override def sendMessage(topic: String, message: String): Unit =
      if (subscribedTo.get().contains(topic)) {
        listenerSubscribed.get() foreach (listener => listener.message(Map.empty.asJava, message))
      }

    override val client: StompClient = new StompClient {
      override def disconnect: IO[Unit] = IO {
        logger.info(s"Mock client disconnecting")
        clientConnected.set(false)
      }

      override def subscribe(topic: String, listener: StompStreamListener): IO[Unit] = IO {
        logger.info(s"Mock client subscribing to $topic")
        subscribedTo.set(Some(topic))
        listenerSubscribed.set(Some(listener))
      }

      override def unsubscribe(topic: String, listener: StompStreamListener): IO[Unit] = IO {
        logger.info(s"Mock client unsubscribing to $topic")
        subscribedTo.set(None)
        listenerSubscribed.set(None)
      }

    }

  }
}
