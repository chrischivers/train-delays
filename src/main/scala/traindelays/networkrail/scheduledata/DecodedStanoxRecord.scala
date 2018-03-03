package traindelays.networkrail.scheduledata

import cats.effect.IO
import fs2.Pipe
import io.circe.Decoder.Result
import io.circe._
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.{CRS, StanoxCode, TipLocCode}

trait DecodedStanoxRecord {
  val tipLocCode: TipLocCode
}

object DecodedStanoxRecord {

  case class Create(tipLocCode: TipLocCode,
                    stanoxCode: Option[StanoxCode],
                    crs: Option[CRS],
                    description: Option[String])
      extends DecodedStanoxRecord {
    def toStanoxRecord = StanoxRecord(tipLocCode, stanoxCode, crs, description)
  }

  case class Delete(tipLocCode: TipLocCode) extends DecodedStanoxRecord

  case class Update(tipLocCode: TipLocCode,
                    stanoxCode: Option[StanoxCode],
                    crs: Option[CRS],
                    description: Option[String])
      extends DecodedStanoxRecord {
    def toStanoxRecord = StanoxRecord(tipLocCode, stanoxCode, crs, description)
  }

  implicit case object JsonFilter extends JsonFilter[DecodedStanoxRecord] {
    override implicit val jsonFilter: Json => Boolean =
      _.hcursor.downField("TiplocV1").downField("tiploc_code").succeeded
  }

  implicit case object StanoxRecordTransformer extends Transformer[DecodedStanoxRecord] {
    override implicit val transform: Pipe[IO, DecodedStanoxRecord, DecodedStanoxRecord] = identity
  }

//  import io.circe.generic.semiauto._
//  implicit val stanoxRecordEncoder: Encoder[DecodedStanoxRecord] = deriveEncoder[DecodedStanoxRecord]

  implicit val stanoxRecordDecoder: Decoder[DecodedStanoxRecord] {
    def apply(c: HCursor): Result[DecodedStanoxRecord]
  } = new Decoder[DecodedStanoxRecord] {

    override def apply(c: HCursor): Result[DecodedStanoxRecord] = {
      val cursor = c.downField("TiplocV1")
      cursor.downField("transaction_type").as[TransactionType].flatMap {
        case TransactionType.Create => decodeStanoxCreateRecord(cursor)
        case TransactionType.Delete => decodeStanoxDeleteRecord(cursor)
        case TransactionType.Update => decodeStanoxUpdateRecord(cursor)
      }
    }
  }

  private def decodeStanoxMainFields(tipLocObject: ACursor) =
    for {
      tipLocCode  <- tipLocObject.downField("tiploc_code").as[TipLocCode]
      stanoxCode  <- tipLocObject.downField("stanox").as[Option[StanoxCode]]
      crs         <- tipLocObject.downField("crs_code").as[Option[CRS]]
      description <- tipLocObject.downField("tps_description").as[Option[String]]
    } yield (tipLocCode, stanoxCode, crs, description)

  private def decodeStanoxCreateRecord(tipLocObject: ACursor) =
    decodeStanoxMainFields(tipLocObject).map(DecodedStanoxRecord.Create.tupled)

  private def decodeStanoxUpdateRecord(tipLocObject: ACursor) =
    decodeStanoxMainFields(tipLocObject).map(DecodedStanoxRecord.Update.tupled)

  private def decodeStanoxDeleteRecord(tipLocObject: ACursor) =
    tipLocObject.downField("tiploc_code").as[TipLocCode].map(DecodedStanoxRecord.Delete)
}
