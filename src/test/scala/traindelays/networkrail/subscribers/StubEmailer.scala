package traindelays.networkrail.subscribers

import cats.effect.IO
import traindelays.networkrail.subscribers.Emailer.Email

import scala.collection.mutable.ListBuffer

object StubEmailer {

  def apply() = new Emailer {

    val emailsSent = new ListBuffer[Email]

    override def sendEmail(email: Email): IO[Unit] =
      IO(emailsSent += email)

  }

}
