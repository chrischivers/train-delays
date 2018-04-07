package traindelays

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalTime}

import io.circe.generic.semiauto._
import io.circe.java8.time.{
  decodeLocalDateDefault,
  decodeLocalTimeDefault,
  encodeLocalDateDefault,
  encodeLocalTimeDefault
}
import io.circe.{Decoder, Encoder, Json}
import traindelays.networkrail.movementdata.CancellationType
import traindelays.networkrail.scheduledata.{AtocCode, DaysRunPattern, ScheduleTrainId}
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.{CRS, ServiceCode, StanoxCode}

package object ui {

  val dateFormatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy")
  val timeFormatter = DateTimeFormatter.ofPattern("HH:mm")

  case class ScheduleQueryRequest(idToken: Option[String],
                                  fromStanox: StanoxCode,
                                  toStanox: StanoxCode,
                                  daysRunPattern: DaysRunPattern)

  object ScheduleQueryRequest {
    implicit val decoder: Decoder[ScheduleQueryRequest] = deriveDecoder[ScheduleQueryRequest]
    implicit val encoder: Encoder[ScheduleQueryRequest] = (a: ScheduleQueryRequest) => {
      Json.obj(
        ("idToken", a.idToken.fold(Json.Null)(token => Json.fromString(token))),
        ("fromStanox", Json.fromString(a.fromStanox.value)),
        ("toStanox", Json.fromString(a.toStanox.value)),
        ("daysRunPattern", Json.fromString(a.daysRunPattern.string))
      )
    }
  }

  case class ScheduleQueryResponseId(recordType: String, value: Int)

  object ScheduleQueryResponseId {
    implicit val encoder: Encoder[ScheduleQueryResponseId] = deriveEncoder[ScheduleQueryResponseId]
    implicit val decoder: Decoder[ScheduleQueryResponseId] = deriveDecoder[ScheduleQueryResponseId]
  }

  case class ScheduleQueryResponse(id: ScheduleQueryResponseId,
                                   scheduleTrainId: ScheduleTrainId,
                                   atocCode: Option[AtocCode],
                                   tocName: String,
                                   fromStanoxCode: StanoxCode,
                                   fromCRS: CRS,
                                   departureTime: LocalTime,
                                   toStanoxCode: StanoxCode,
                                   toCRS: CRS,
                                   arrivalTime: LocalTime,
                                   daysRunPattern: DaysRunPattern,
                                   scheduleStart: LocalDate,
                                   scheduleEnd: LocalDate,
                                   subscribed: Boolean) {

    def matchesKeyFields(that: ScheduleQueryResponse) =
      fromStanoxCode == that.fromStanoxCode &&
        departureTime == that.departureTime &&
        toStanoxCode == that.toStanoxCode &&
        arrivalTime == that.arrivalTime &&
        daysRunPattern == daysRunPattern
  }

  object ScheduleQueryResponse {
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
    implicit val decoder: Decoder[ScheduleQueryResponse] = deriveDecoder[ScheduleQueryResponse]
  }

  case class SubscriberRecordsResponse(id: Int,
                                       scheduleTrainId: ScheduleTrainId,
                                       serviceCode: ServiceCode,
                                       fromStanoxCode: StanoxCode,
                                       fromCRS: CRS,
                                       departureTime: LocalTime,
                                       toStanoxCode: StanoxCode,
                                       toCRS: CRS,
                                       arrivalTime: LocalTime,
                                       daysRunPattern: DaysRunPattern)

  object SubscriberRecordsResponse {
    implicit val encoder: Encoder[SubscriberRecordsResponse] = deriveEncoder[SubscriberRecordsResponse]
    implicit val decoder: Decoder[SubscriberRecordsResponse] = deriveDecoder[SubscriberRecordsResponse]

    def subscriberRecordsResponseFrom(subscriberRecords: List[SubscriberRecord]): List[SubscriberRecordsResponse] =
      subscriberRecords.flatMap(r =>
        r.id.map { id =>
          SubscriberRecordsResponse(id,
                                    r.scheduleTrainId,
                                    r.serviceCode,
                                    r.fromStanoxCode,
                                    r.fromCRS,
                                    r.departureTime,
                                    r.toStanoxCode,
                                    r.toCRS,
                                    r.arrivalTime,
                                    r.daysRunPattern)
      })
  }

  case class SubscribeRequest(email: String, idToken: String, records: List[SubscribeRequestRecord])

  object SubscribeRequest {
    implicit val decoder: Decoder[SubscribeRequest] = deriveDecoder[SubscribeRequest]
    implicit val encoder: Encoder[SubscribeRequest] = deriveEncoder[SubscribeRequest]
  }

  case class SubscribeRequestRecord(id: ScheduleQueryResponseId,
                                    fromStanox: StanoxCode,
                                    fromCRS: CRS,
                                    toStanox: StanoxCode,
                                    departureTime: LocalTime,
                                    toCRS: CRS,
                                    daysRunPattern: DaysRunPattern,
                                    arrivalTime: LocalTime)

  object SubscribeRequestRecord {
    implicit val decoder: Decoder[SubscribeRequestRecord] = deriveDecoder[SubscribeRequestRecord]
    implicit val encoder: Encoder[SubscribeRequestRecord] = deriveEncoder[SubscribeRequestRecord]
  }

  case class HistoryQueryMovementRecord(scheduledDepartureDate: LocalDate,
                                        actualDepartureTime: LocalTime,
                                        differenceWithExpectedDeparture: Long,
                                        actualArrivalTime: LocalTime,
                                        differenceWithExpectedArrival: Long)

  case class HistoryQueryCancellationRecord(scheduledDepartureDate: LocalDate,
                                            cancellationType: CancellationType,
                                            cancellationReasonCode: String)

  case class HistoryQueryResponse(scheduleTrainId: ScheduleTrainId,
                                  atocCode: Option[AtocCode],
                                  fromStanoxCode: StanoxCode,
                                  fromCRS: CRS,
                                  toStanoxCode: StanoxCode,
                                  toCRS: CRS,
                                  expectedDepartureTime: LocalTime,
                                  expectedArrivalTime: LocalTime,
                                  movementRecords: List[HistoryQueryMovementRecord],
                                  cancellationRecords: List[HistoryQueryCancellationRecord])

  object HistoryQueryMovementRecord {
    implicit val decoder: Decoder[HistoryQueryMovementRecord] = deriveDecoder[HistoryQueryMovementRecord]
    implicit val encoder: Encoder[HistoryQueryMovementRecord] = deriveEncoder[HistoryQueryMovementRecord]
  }

  object HistoryQueryCancellationRecord {

    implicit val decoder: Decoder[HistoryQueryCancellationRecord] = deriveDecoder[HistoryQueryCancellationRecord]
    implicit val encoder: Encoder[HistoryQueryCancellationRecord] = deriveEncoder[HistoryQueryCancellationRecord]
  }

  object HistoryQueryResponse {
    implicit val decoder: Decoder[HistoryQueryResponse] = deriveDecoder[HistoryQueryResponse]
    implicit val encoder: Encoder[HistoryQueryResponse] = deriveEncoder[HistoryQueryResponse]

  }

  private def scheduleStartFormat(scheduleStart: LocalDate): String = {
    val now = LocalDate.now()
    if (scheduleStart.isBefore(now) || scheduleStart.isEqual(now)) "Current"
    else scheduleStart.format(dateFormatter)
  }
}
