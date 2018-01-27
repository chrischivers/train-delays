package traindelays

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.generic.semiauto.deriveEncoder
import io.circe.{Decoder, Encoder}
import org.http4s.client.Client
import org.http4s.client.middleware.FollowRedirect

package object networkrail {

  case class ServiceCode(value: String)
  object ServiceCode {
    implicit val decoder: Decoder[ServiceCode] = Decoder.decodeString.map(ServiceCode(_))
    implicit val encoder: Encoder[ServiceCode] = deriveEncoder[ServiceCode]
    implicit val meta: Meta[ServiceCode] =
      Meta[String].xmap(ServiceCode(_), _.value)
  }

  case class TOC(value: String)
  object TOC {
    import io.circe.generic.semiauto._
    implicit val decoder: Decoder[TOC] = Decoder.decodeString.map(TOC(_))
    implicit val encoder: Encoder[TOC] = deriveEncoder[TOC]

    implicit val meta: Meta[TOC] =
      Meta[String].xmap(TOC(_), _.value)
  }

  case class Stanox(value: String)
  object Stanox {
    import io.circe.generic.semiauto._
    implicit val decoder: Decoder[Stanox] = Decoder.decodeString.map(Stanox(_))
    implicit val encoder: Encoder[Stanox] = deriveEncoder[Stanox]
    implicit val meta: Meta[Stanox] =
      Meta[String].xmap(Stanox(_), _.value)
  }

  case class CRS(value: String)
  object CRS {
    import io.circe.generic.semiauto._
    implicit val decoder: Decoder[CRS] = Decoder.decodeString.map(CRS(_))
    implicit val encoder: Encoder[CRS] = deriveEncoder[CRS]
    implicit val meta: Meta[CRS] =
      Meta[String].xmap(CRS(_), _.value)
  }

  def followRedirects(client: Client[IO], maxRedirects: Int): Client[IO] =
    FollowRedirect(maxRedirects)(client)
}
