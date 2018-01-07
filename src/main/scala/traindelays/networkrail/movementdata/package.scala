package traindelays.networkrail

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}

package object movementdata {

  case class MovementRecord(trainId: String,
                            trainServiceCode: String,
                            eventType: Option[String],
                            toc: Option[String],
                            plannedEventType: Option[String],
                            actualTimestamp: Option[Long],
                            plannedTimestamp: Option[Long],
                            plannedPassengerTimestamp: Option[Long],
                            stanox: Option[String],
                            variationStatus: Option[String]) {

    def toMovementLog: Option[MovementLog] =
      for {
        eventType                 <- eventType
        toc                       <- toc
        stanox                    <- stanox
        plannedPassengerTimestamp <- plannedPassengerTimestamp
        actualTimestamp           <- actualTimestamp
        variationStatus           <- variationStatus
      } yield
        MovementLog(
          None,
          trainId,
          trainServiceCode,
          eventType,
          toc,
          stanox,
          plannedPassengerTimestamp,
          actualTimestamp,
          actualTimestamp - plannedPassengerTimestamp,
          variationStatus
        )

  }

  object MovementRecord {

    implicit val movementRecordDecoder: Decoder[MovementRecord] {
      def apply(c: HCursor): Result[MovementRecord]
    } = new Decoder[MovementRecord] {

      override def apply(c: HCursor): Result[MovementRecord] = {
        val bodyObject = c.downField("body")
        for {
          trainId          <- bodyObject.downField("train_id").as[String]
          trainServiceCode <- bodyObject.downField("train_service_code").as[String]
          eventType        <- bodyObject.downField("event_type").as[Option[String]]
          toc              <- bodyObject.downField("toc_id").as[Option[String]]
          plannedEventType <- bodyObject.downField("planned_event_type").as[Option[String]]
          actualTimestamp <- bodyObject
            .downField("actual_timestamp")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(_.toLong))
          plannedTimestamp <- bodyObject
            .downField("planned_timestamp")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(_.toLong))
          plannedPassengerTimestamp <- bodyObject
            .downField("gbtt_timestamp")
            .as[Option[String]]
            .map(emptyStringOptionToNone(_)(_.toLong))
          stanox          <- bodyObject.downField("loc_stanox").as[Option[String]]
          variationStatus <- bodyObject.downField("variation_status").as[Option[String]]

        } yield {
          MovementRecord(trainId,
                         trainServiceCode,
                         eventType,
                         toc,
                         plannedEventType,
                         actualTimestamp,
                         plannedTimestamp,
                         plannedPassengerTimestamp,
                         stanox,
                         variationStatus)
        }
      }
    }
    private def emptyStringToNone[A](in: String)(f: String => A): Option[A] =
      if (in == "") None else Some(f(in))

    private def emptyStringOptionToNone[A](in: Option[String])(f: String => A): Option[A] =
      if (in.contains("")) None else in.map(f)
  }

  case class MovementLog(id: Option[Int],
                         trainId: String,
                         serviceCode: String,
                         eventType: String,
                         toc: String,
                         stanox: String,
                         plannedPassengerTimestamp: Long,
                         actualTimestamp: Long,
                         difference: Long,
                         variationStatus: String)
}
