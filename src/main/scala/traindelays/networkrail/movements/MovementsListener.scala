package traindelays.networkrail.movements

import java.util

import traindelays.stomp.{StompClient, StompListener}

object MovementsListener {

  def apply() = new StompListener {
    override def message(headers: util.Map[_, _], body: String): Unit = {
      println("headers: " + headers)
      println("BODY: " + body)
    }
  }
}
