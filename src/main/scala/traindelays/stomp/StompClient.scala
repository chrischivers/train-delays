package traindelays.stomp

import java.util

import net.ser1.stomp.Client
import traindelays.NetworkRailConfig

trait StompClient {

  def subscribe(topic: String, listener: StompListener): Unit

}

object StompClient {

  def apply(config: NetworkRailConfig) = new StompClient {
    override def subscribe(topic: String, listener: StompListener): Unit =
      new Client(config.host, config.port, config.username, config.password)
        .subscribe(topic, listener)
  }
}
