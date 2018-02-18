package traindelays.stomp

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import net.ser1.stomp.Client
import traindelays.NetworkRailConfig

trait StompClient {

  def subscribe(topic: String, listener: StompStreamListener): IO[Unit]

  def unsubscribe(topic: String, listener: StompStreamListener): IO[Unit]

  def disconnect: IO[Unit]

}

object StompClient extends StrictLogging {

  def apply(config: NetworkRailConfig) = {
    logger.info("Creating new Client")
    val client = new Client(config.host, config.port, config.username, config.password)
    new StompClient {
      logger.info("Creating new StompClient")
      override def subscribe(topic: String, listener: StompStreamListener): IO[Unit] = IO {
        logger.info(s"Subscribing to topic $topic")
        client.subscribe(topic, listener)
      }

      override def unsubscribe(topic: String, listener: StompStreamListener): IO[Unit] =
        IO(client.unsubscribe(topic, listener))

      override def disconnect: IO[Unit] = IO(client.disconnect())
    }
  }
}
