package traindelays

import cats.effect.IO
import org.http4s.client.Client
import org.http4s.client.middleware.FollowRedirect

package object networkrail {

  def followRedirects(client: Client[IO], maxRedirects: Int): Client[IO] =
    FollowRedirect(maxRedirects)(client)
}
