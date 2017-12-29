package traindelays.networkrail

import traindelays.stomp.{StompClient, StompListener}

object MockStompClient {
  def apply() = new StompClient {
    override def subscribe(topic: String, listener: StompListener): Unit = ???
  }
}
