package traindelays.stomp

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import net.ser1.stomp.Client
import traindelays.NetworkRailConfig

trait StompClient {

  def subscribe(topic: String, handler: StompHandler): IO[Unit]

}

object StompClient extends StrictLogging {

  def apply(config: NetworkRailConfig) = new StompClient {
    override def subscribe(topic: String, handler: StompHandler): IO[Unit] = IO {
      println(s"Subscribing to topic $topic")
      new Client(config.host, config.port, config.username, config.password)
        .subscribe(topic, handler)
    }
  }
}
