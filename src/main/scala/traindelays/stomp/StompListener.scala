package traindelays.stomp

import java.util

import net.ser1.stomp.Listener

trait StompListener extends Listener {
  override def message(headers: util.Map[_, _], body: String): Unit
}
