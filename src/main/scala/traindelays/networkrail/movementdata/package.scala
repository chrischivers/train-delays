package traindelays.networkrail

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor}

package object movementdata {

  case class MovementRecord(trainId: String,
                            trainServiceCode: String,
                            eventType: Option[String],
                            plannedEventType: Option[String],
                            actualTimestamp: Option[Long],
                            plannedTimestamp: Option[Long],
                            plannedPassengerTimestamp: Option[Long],
                            stanox: Option[String],
                            variationStatus: Option[String]) {

    def toMovementLog: Option[MovementLog] =
      for {
        eventType                 <- eventType
        stanox                    <- stanox
        plannedPassengerTimestamp <- plannedPassengerTimestamp
        actualTimestamp           <- actualTimestamp
      } yield
        MovementLog(None,
                    trainId,
                    trainServiceCode,
                    eventType,
                    stanox,
                    plannedPassengerTimestamp,
                    actualTimestamp,
                    actualTimestamp - plannedPassengerTimestamp)

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
                         stanox: String,
                         plannedPassengerTimestamp: Long,
                         actualTimestamp: Long,
                         difference: Long)
}
/*

CREATE TABLE IF NOT EXISTS movement_log (
  id SERIAL PRIMARY KEY,
  train_id     VARCHAR(10)    NOT NULL,
  service_code    VARCHAR(10)   NOT NULL,
  event_type VARCHAR(15) NOT NULL,
  stanox VARCHAR(10) NOT NULL,
  planned_passenger_timestamp TIMESTAMP NOT NULL,
  actual_timestamp TIMESTAMP NOT NULL,
  difference LONG NOT NULL
);

 */

/*
"body":{
            "event_type":"ARRIVAL",
            "gbtt_timestamp":"1514650800000",
            "original_loc_stanox":"",
            "planned_timestamp":"1514650830000",
            "timetable_variation":"2",
            "original_loc_timestamp":"",
            "current_train_id":"",
            "delay_monitoring_point":"true",
            "next_report_run_time":"2",
            "reporting_stanox":"72238",
            "actual_timestamp":"1514650680000",
            "correction_ind":"false",
            "event_source":"AUTOMATIC",
            "train_file_address":null,
            "platform":"",
            "division_code":"30",
            "train_terminated":"false",
            "train_id":"722Y81MR30",
            "offroute_ind":"false",
            "variation_status":"EARLY",
            "train_service_code":"22214000",
            "toc_id":"30",
            "loc_stanox":"72238",
            "auto_expected":"true",
            "direction_ind":"UP",
            "route":"0",
            "planned_event_type":"ARRIVAL",
            "next_report_stanox":"72242",
            "line_ind":""
         }
 */
