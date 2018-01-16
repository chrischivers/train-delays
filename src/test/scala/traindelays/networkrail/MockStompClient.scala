package traindelays.networkrail

import cats.effect.IO
import traindelays.stomp.{StompClient, StompHandler}

import scala.collection.JavaConverters._

trait MockStompClient {
  val client: StompClient
  def sendMessage(topic: String, message: String)

}

object MockStompClient {

  private var subscribedTo: Option[String]             = None
  private var listenerSubscribed: Option[StompHandler] = None

  def apply() = new MockStompClient {

    override def sendMessage(topic: String, message: String): Unit =
      if (subscribedTo.contains(topic)) {
        listenerSubscribed.foreach(listener => listener.message(Map.empty.asJava, message))
      }

    override val client: StompClient = (topic: String, listener: StompHandler) =>
      IO {
        subscribedTo = Some(topic)
        listenerSubscribed = Some(listener)
    }
  }
}
