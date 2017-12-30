package traindelays.networkrail.movements

import java.util

import traindelays.stomp.{StompClient, StompHandler}

object MovementsHandler {

  def apply() = new StompHandler {
    override def message(headers: util.Map[_, _], body: String): Unit = {
      println("headers: " + headers)
      println("BODY: " + body)
    }
  }
}
