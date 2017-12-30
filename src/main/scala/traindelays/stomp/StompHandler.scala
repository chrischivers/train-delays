package traindelays.stomp

import java.util

import net.ser1.stomp.Listener

trait StompHandler extends Listener {
  override def message(headers: util.Map[_, _], body: String): Unit
}
