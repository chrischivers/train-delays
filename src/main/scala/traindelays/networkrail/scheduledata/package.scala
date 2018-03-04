package traindelays.networkrail

import java.time.format.DateTimeFormatter

import com.typesafe.scalalogging.StrictLogging
import doobie.util.meta.Meta
import io.circe.Decoder.Result
import io.circe._

package object scheduledata extends StrictLogging {

  trait DecodedRecord

  case class OtherDecodedRecord(label: String) extends DecodedRecord

  object DecodedRecord {
    implicit val decoder = new Decoder[DecodedRecord] {
      override def apply(c: HCursor): Result[DecodedRecord] =
        c.keys
          .flatMap(_.headOption)
          .fold[Decoder.Result[DecodedRecord]](
            Left(DecodingFailure(s"Unable to get head record ${c.value}", c.history))) {
            case "JsonAssociationV1" => DecodedAssociationRecord.associationRecordDecoder(c)
            case "JsonScheduleV1"    => DecodedScheduleRecord.scheduleRecordDecoder(c)
            case "TiplocV1"          => DecodedStanoxRecord.stanoxRecordDecoder(c)
            case "JsonTimetableV1"   => Right(OtherDecodedRecord("JsonTimetableV1"))
            case other =>
              val msg = s"Unhandled record type $other while decoding."
              logger.error(msg)
              Left(DecodingFailure(msg, c.history))
          }
    }
  }

  val timeFormatter = DateTimeFormatter.ofPattern("HHmm")

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

  trait AssociationCategory {
    val value: String
  }
  case object AssociationCategory {

    private val associationCategories = Seq(Join, Divide, Next)

    case object Join extends AssociationCategory {
      override val value: String = "JJ"
    }

    case object Divide extends AssociationCategory {
      override val value: String = "VV"
    }

    case object Next extends AssociationCategory {
      override val value: String = "NP"
    }
    def fromString(str: String): Option[AssociationCategory] =
      associationCategories.find(_.value == str)

    implicit val encoder: Encoder[AssociationCategory] = (a: AssociationCategory) => Json.fromString(a.value)
    implicit val decoder: Decoder[AssociationCategory] = (c: HCursor) =>
      c.as[String]
        .flatMap(
          x =>
            fromString(x)
              .fold[Result[AssociationCategory]](
                Left(DecodingFailure("No match for association category", List.empty)))(Right(_)))

    implicit val meta: Meta[AssociationCategory] =
      Meta[String].xmap(str =>
                          AssociationCategory
                            .fromString(str)
                            .getOrElse(throw new RuntimeException(s"No association category found for [$str]")),
                        _.value)
  }

  sealed trait DaysRunPattern {
    val string: String
  }

  object DaysRunPattern {

    case object Weekdays extends DaysRunPattern {
      override val string: String = "Weekdays"
    }
    case object Saturdays extends DaysRunPattern {
      override val string: String = "Saturdays"
    }
    case object Sundays extends DaysRunPattern {
      override val string: String = "Sundays"
    }

    import doobie.util.meta.Meta

    def fromString(str: String): Option[DaysRunPattern] =
      str match {
        case Weekdays.string  => Some(Weekdays)
        case Saturdays.string => Some(Saturdays)
        case Sundays.string   => Some(Sundays)
        case _                => None
      }
    implicit val decoder: Decoder[DaysRunPattern] = Decoder.decodeString.map(str =>
      fromString(str).getOrElse {
        logger.error(s"Unknown days run pattern [$str]. Defaulting to 'weekdays'")
        Weekdays
    })

    implicit val encoder: Encoder[DaysRunPattern] = (a: DaysRunPattern) => Json.fromString(a.string)

    implicit val meta: Meta[DaysRunPattern] =
      Meta[String].xmap(str => DaysRunPattern.fromString(str).getOrElse(Weekdays), _.string)
  }

  def logDecodingErrors[A](cursor: HCursor, result: Either[DecodingFailure, A]): Either[DecodingFailure, A] =
    result.fold(failure => {
      logger.error(s"Error decoding ${cursor.value}", failure)
      Left(failure)
    }, _ => result)
}
