package traindelays.networkrail

import java.time.format.DateTimeFormatter

import cats.effect.IO
import doobie.util.meta.Meta
import io.circe.Decoder.Result
import io.circe._

package object scheduledata {

  val timeFormatter = DateTimeFormatter.ofPattern("HHmm")

  trait JsonFilter[A] {
    implicit val jsonFilter: (Json => Boolean)
  }

  trait Transformer[A] {
    implicit val transform: _root_.fs2.Pipe[IO, A, A]
  }

  case class ScheduleTrainId(value: String)
  object ScheduleTrainId {
    implicit val decoder: Decoder[ScheduleTrainId] = Decoder.decodeString.map(ScheduleTrainId(_))
    implicit val encoder: Encoder[ScheduleTrainId] = Encoder[ScheduleTrainId](a => Json.fromString(a.value))

    implicit val meta: Meta[ScheduleTrainId] =
      Meta[String].xmap(ScheduleTrainId(_), _.value)
  }

  case class AtocCode(value: String)
  object AtocCode {
    implicit val decoder: Decoder[AtocCode] = Decoder.decodeString.map(AtocCode(_))
    implicit val encoder: Encoder[AtocCode] = Encoder[AtocCode](a => Json.fromString(a.value))

    implicit val meta: Meta[AtocCode] =
      Meta[String].xmap(AtocCode(_), _.value)
  }

  trait TransactionType {
    val value: String
  }
  case object TransactionType {

    private val transactionTypes = Seq(Create, Update, Delete)

    case object Create extends TransactionType {
      override val value: String = "Create"
    }

    case object Update extends TransactionType {
      override val value: String = "Update"
    }

    case object Delete extends TransactionType {
      override val value: String = "Delete"
    }

    def fromString(str: String): Option[TransactionType] =
      transactionTypes.find(_.value == str)

    implicit val encoder: Encoder[TransactionType] = (a: TransactionType) => Json.fromString(a.value)
    implicit val decoder: Decoder[TransactionType] = (c: HCursor) =>
      c.as[String]
        .flatMap(
          x =>
            fromString(x).fold[Result[TransactionType]](
              Left(DecodingFailure("No match for transaction type", List.empty)))(Right(_)))
  }

  trait StpIndicator {
    val value: String
    val description: String
  }
  case object StpIndicator {

    private val stopIndicators = Seq(C, N, O, P)

    case object C extends StpIndicator {
      override val value: String       = "C"
      override val description: String = "STP cancellation of permanent schedule"
    }

    case object N extends StpIndicator {
      override val value: String       = "N"
      override val description: String = "New STP schedule (not an overlay)"
    }

    case object O extends StpIndicator {
      override val value: String       = "O"
      override val description: String = "STP overlay of permanent schedule"
    }

    case object P extends StpIndicator {
      override val value: String       = "P"
      override val description: String = "Permanent"
    }

    def fromString(str: String): Option[StpIndicator] =
      stopIndicators.find(_.value == str)

    implicit val encoder: Encoder[StpIndicator] = (a: StpIndicator) => Json.fromString(a.value)
    implicit val decoder: Decoder[StpIndicator] = (c: HCursor) =>
      c.as[String]
        .flatMap(
          x =>
            fromString(x)
              .fold[Result[StpIndicator]](Left(DecodingFailure("No match for transaction type", List.empty)))(Right(_)))

    implicit val meta: Meta[StpIndicator] =
      Meta[String].xmap(str => StpIndicator.fromString(str).getOrElse(P), _.value)
  }

}
