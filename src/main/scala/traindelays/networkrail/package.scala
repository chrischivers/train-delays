package traindelays

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.{Decoder, Encoder, Json}
import org.http4s.client.Client
import org.http4s.client.middleware.FollowRedirect

package object networkrail {

  case class ServiceCode(value: String)
  object ServiceCode {
    implicit val decoder: Decoder[ServiceCode] = Decoder.decodeString.map(ServiceCode(_))
    implicit val encoder: Encoder[ServiceCode] = Encoder[ServiceCode](a => Json.fromString(a.value))
    implicit val meta: Meta[ServiceCode] =
      Meta[String].xmap(ServiceCode(_), _.value)
  }

  case class TrainCategory(value: String)
  object TrainCategory {
    implicit val decoder: Decoder[TrainCategory] = Decoder.decodeString.map(TrainCategory(_))
    implicit val encoder: Encoder[TrainCategory] = Encoder[TrainCategory](a => Json.fromString(a.value))
    implicit val meta: Meta[TrainCategory] =
      Meta[String].xmap(TrainCategory(_), _.value)
  }

  case class TrainStatus(value: String)
  object TrainStatus {
    implicit val decoder: Decoder[TrainStatus] = Decoder.decodeString.map(TrainStatus(_))
    implicit val encoder: Encoder[TrainStatus] = Encoder[TrainStatus](a => Json.fromString(a.value))
    implicit val meta: Meta[TrainStatus] =
      Meta[String].xmap(TrainStatus(_), _.value)
  }

  case class TOC(value: String)
  object TOC {
    implicit val decoder: Decoder[TOC] = Decoder.decodeString.map(TOC(_))
    implicit val encoder: Encoder[TOC] = Encoder[TOC](a => Json.fromString(a.value))

    implicit val meta: Meta[TOC] =
      Meta[String].xmap(TOC(_), _.value)
  }

  case class StanoxCode(value: String)
  object StanoxCode {
    import doobie.postgres.implicits._
    implicit val decoder: Decoder[StanoxCode] = Decoder.decodeString.map(StanoxCode(_))
    implicit val encoder: Encoder[StanoxCode] = Encoder[StanoxCode](a => Json.fromString(a.value))
    implicit val meta: Meta[StanoxCode] =
      Meta[String].xmap(StanoxCode(_), _.value)

    implicit val metaList: Meta[List[StanoxCode]] =
      Meta[List[String]].xmap(_.map(StanoxCode(_)), _.map(_.value))

  }

  case class CRS(value: String)
  object CRS {
    implicit val decoder: Decoder[CRS] = Decoder.decodeString.map(CRS(_))
    implicit val encoder: Encoder[CRS] = Encoder[CRS](a => Json.fromString(a.value))
    implicit val meta: Meta[CRS] =
      Meta[String].xmap(CRS(_), _.value)
  }

  case class TipLocCode(value: String)
  object TipLocCode {

    implicit val decoder: Decoder[TipLocCode] = Decoder.decodeString.map(TipLocCode(_))
    implicit val encoder: Encoder[TipLocCode] = Encoder[TipLocCode](a => Json.fromString(a.value))
    implicit val meta: Meta[TipLocCode] =
      Meta[String].xmap(TipLocCode(_), _.value)
  }

  def followRedirects(client: Client[IO], maxRedirects: Int): Client[IO] =
    FollowRedirect(maxRedirects)(client)
}
