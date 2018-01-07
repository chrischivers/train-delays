package traindelays.networkrail.subscribers

import java.util.Properties
import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.{Session, _}

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import traindelays.EmailerConfig
import traindelays.networkrail.subscribers.Emailer.Email

trait Emailer {
  def sendEmail(email: Email): IO[Unit]
}

object Emailer extends StrictLogging {

  case class Email(subject: String, body: String, to: String)

  def apply(emailerConfig: EmailerConfig) = new Emailer {

    val properties = new Properties()
    properties.put("mail.transport.protocol", "smtp")
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    properties.put("mail.smtp.user", emailerConfig.smtpUsername)
    properties.put("mail.smtp.password", emailerConfig.smtpPassword)
    properties.put("mail.smtp.host", emailerConfig.smtpHost)
    properties.put("mail.smtp.port", emailerConfig.smtpPort.toString)
    val session: Session = Session.getInstance(
      properties,
      new Authenticator() {
        override protected def getPasswordAuthentication =
          new PasswordAuthentication(emailerConfig.smtpUsername, emailerConfig.smtpPassword)
      }
    )
    session.setDebug(true)

    override def sendEmail(email: Email): IO[Unit] = {

      val message = new MimeMessage(session)

      message.setFrom(new InternetAddress(emailerConfig.fromAddress))
      message.setRecipient(javax.mail.Message.RecipientType.TO, new InternetAddress(email.to))
      message.setSubject(email.subject)
      message.setText(email.body, "utf-8", "html")

      retry(emailerConfig.numberAttempts, emailerConfig.secondsBetweenAttempts) {
        logger.info(s"Sending email to ${email.to}")
        IO(Transport.send(message))
      }
    }

  }

  private def retry[T](totalNumberOfAttempts: Int, secondsBetweenAttempts: Int)(fn: => IO[T]): IO[T] = {

    def retryHelper(n: Int)(fn: => IO[T]): IO[T] = {
      logger.info(s"attempting to run function. Attempt ${n + 1} of $totalNumberOfAttempts")
      fn.attempt.flatMap {
        case Right(x) => IO(x)
        case Left(e) if n < totalNumberOfAttempts =>
          logger.error("Error occurred during execution", e)
          logger.info(
            s"Attempt ${n + 1} of $totalNumberOfAttempts failed. Retrying operation after $secondsBetweenAttempts seconds.")
          Thread.sleep(secondsBetweenAttempts * 1000)
          retryHelper(n + 1)(fn)
        case Left(e) => IO.raiseError(e)
      }
    }
    retryHelper(n = 0)(fn)
  }
}
