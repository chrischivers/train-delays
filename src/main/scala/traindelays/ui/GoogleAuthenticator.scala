package traindelays.ui

import cats.effect.IO
import com.google.api.client.http.apache.ApacheHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import traindelays.networkrail.subscribers.UserId

import scala.collection.JavaConverters._
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.typesafe.scalalogging.StrictLogging

import scala.util.Try

case class AuthenticatedDetails(userId: UserId,
                                email: String,
                                emailVerified: Option[Boolean],
                                name: Option[String],
                                firstName: Option[String],
                                familyName: Option[String],
                                locale: Option[String])

trait GoogleAuthenticator extends StrictLogging {

  val verifier: GoogleIdTokenVerifier

  def verifyToken(idTokenString: String): IO[Option[AuthenticatedDetails]] = IO {
    val idTokenOpt: Option[GoogleIdToken] = Try(verifier.verify(idTokenString)).toOption
    idTokenOpt
      .map { idToken =>
        val payload = idToken.getPayload
        val userId  = payload.getSubject

        val email         = payload.getEmail
        val emailVerified = Option(payload.getEmailVerified.booleanValue())
        val name          = Option(payload.get("name")).map(_.asInstanceOf[String])
        val locale        = Option(payload.get("locale")).map(_.asInstanceOf[String])
        val familyName    = Option(payload.get("family_name")).map(_.asInstanceOf[String])
        val firstName     = Option(payload.get("given_name")).map(_.asInstanceOf[String])

        AuthenticatedDetails(UserId(userId), email, emailVerified, name, firstName, familyName, locale)
      }
      .orElse {
        logger.error(s"Unable to verify token string $idTokenString")
        None
      }
  }
}

object GoogleAuthenticator {

  def apply(clientId: String) = new GoogleAuthenticator {

    private val httpClient     = ApacheHttpTransport.newDefaultHttpClient()
    private val httpTransport  = new ApacheHttpTransport(httpClient)
    private val jacksonFactory = JacksonFactory.getDefaultInstance

    override val verifier: GoogleIdTokenVerifier = new GoogleIdTokenVerifier.Builder(httpTransport, jacksonFactory)
      .setAudience(List(clientId).asJavaCollection)
      .build
  }
}
