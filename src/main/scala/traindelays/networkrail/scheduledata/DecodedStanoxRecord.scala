package traindelays.networkrail.scheduledata

import io.circe.Decoder.Result
import io.circe._
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.{CRS, Definitions, StanoxCode, TipLocCode}

trait DecodedStanoxRecord extends DecodedRecord {
  val tipLocCode: TipLocCode
}

object DecodedStanoxRecord {

  case class Create(tipLocCode: TipLocCode,
                    stanoxCode: Option[StanoxCode],
                    crs: Option[CRS],
                    description: Option[String])
      extends DecodedStanoxRecord {
    def toStanoxRecord = {
      val primary = stanoxCode.map(stanox => Definitions.primaryStanoxTiplocCombinations.contains((stanox, tipLocCode)))
      StanoxRecord(tipLocCode, stanoxCode, crs, description, primary)
    }
  }

  case class Delete(tipLocCode: TipLocCode) extends DecodedStanoxRecord

  case class Update(tipLocCode: TipLocCode,
                    stanoxCode: Option[StanoxCode],
                    crs: Option[CRS],
                    description: Option[String])
      extends DecodedStanoxRecord {
    def toStanoxRecord = {
      val primary = stanoxCode.map(stanox => Definitions.primaryStanoxTiplocCombinations.contains((stanox, tipLocCode)))
      StanoxRecord(tipLocCode, stanoxCode, crs, description, primary)
    }
  }

  val stanoxRecordDecoder: Decoder[DecodedStanoxRecord] {
    def apply(c: HCursor): Result[DecodedStanoxRecord]
  } = new Decoder[DecodedStanoxRecord] {

    override def apply(c: HCursor): Result[DecodedStanoxRecord] = {
      val cursor = c.downField("TiplocV1")
      cursor.downField("transaction_type").as[TransactionType].flatMap {
        case TransactionType.Create => logDecodingErrors(c, decodeStanoxCreateRecord(cursor))
        case TransactionType.Delete => logDecodingErrors(c, decodeStanoxDeleteRecord(cursor))
        case TransactionType.Update => logDecodingErrors(c, decodeStanoxUpdateRecord(cursor))
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
