package traindelays

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.Decoder
import org.http4s.client.Client
import org.http4s.client.middleware.FollowRedirect

package object networkrail {

  case class ServiceCode(value: String)
  object ServiceCode {
    implicit val decoder: Decoder[ServiceCode] = Decoder.decodeString.map(ServiceCode(_))
    implicit val meta: Meta[ServiceCode] =
      Meta[String].xmap(ServiceCode(_), _.value)
  }

  case class TOC(value: String)
  object TOC {
    implicit val decoder: Decoder[TOC] = Decoder.decodeString.map(TOC(_))

    implicit val meta: Meta[TOC] =
      Meta[String].xmap(TOC(_), _.value)
  }

  case class Stanox(value: String)
  object Stanox {
    implicit val decoder: Decoder[Stanox] = Decoder.decodeString.map(Stanox(_))
    implicit val meta: Meta[Stanox] =
      Meta[String].xmap(Stanox(_), _.value)
  }

  def followRedirects(client: Client[IO], maxRedirects: Int): Client[IO] =
    FollowRedirect(maxRedirects)(client)
}
