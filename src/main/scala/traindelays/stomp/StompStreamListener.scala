package traindelays.stomp

import java.util

import cats.effect.IO
import fs2.async.mutable.Queue
import net.ser1.stomp.Listener

trait StompStreamListener extends Listener {

  val messageQueue: fs2.async.mutable.Queue[IO, String]

  override def message(headers: util.Map[_, _], body: String): Unit =
    messageQueue.enqueue1(body).unsafeRunSync()
}

object StompStreamListener {
  def apply(incomingMessageQueue: fs2.async.mutable.Queue[IO, String]) = new StompStreamListener {
    override val messageQueue: Queue[IO, String] = incomingMessageQueue
  }
}
