package traindelays.ui
import cats.effect.IO
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier

object MockGoogleAuthenticator {
  def apply(authenticatedDetailsToReturn: AuthenticatedDetails) = new GoogleAuthenticator {
    override val verifier: GoogleIdTokenVerifier = GoogleAuthenticator("").verifier

    override def verifyToken(idTokenString: String): IO[Option[AuthenticatedDetails]] =
      IO(Some(authenticatedDetailsToReturn))
  }
}
