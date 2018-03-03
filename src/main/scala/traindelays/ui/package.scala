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
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord.DaysRunPattern
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.movementdata.CancellationType
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId}
import traindelays.networkrail.subscribers.SubscriberRecord
import traindelays.networkrail.tocs.tocs
import traindelays.networkrail.{CRS, StanoxCode}

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

  case class ScheduleQueryResponse(id: Int,
                                   scheduleTrainId: ScheduleTrainId,
                                   atocCode: AtocCode,
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
                                   subscribed: Boolean)

  object ScheduleQueryResponse {
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
    implicit val decoder: Decoder[ScheduleQueryResponse] = deriveDecoder[ScheduleQueryResponse]
  }

  case class SubscribeRequest(email: String,
                              idToken: String,
                              fromStanox: StanoxCode,
                              toStanox: StanoxCode,
                              daysRunPattern: DaysRunPattern,
                              ids: List[Int])

  object SubscribeRequest {
    implicit val decoder: Decoder[SubscribeRequest] = deriveDecoder[SubscribeRequest]
    implicit val encoder: Encoder[SubscribeRequest] = deriveEncoder[SubscribeRequest]
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
                                  atocCode: AtocCode,
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

  //TODO test this
  def scheduleQueryResponsesFrom(
      scheduleLogs: List[ScheduleRecord],
      toStanoxCode: StanoxCode,
      stanoxRecordsWithCRS: Map[StanoxCode, List[StanoxRecord]],
      existingSubscriberRecords: Option[List[SubscriberRecord]]): List[ScheduleQueryResponse] =
    scheduleLogs.flatMap { log =>
      for {
        id            <- log.id
        departureTime <- log.departureTime
        tocName       <- tocs.mapping.get(log.atocCode)
        indexOfArrivalStopOpt = log.subsequentStanoxCodes.indexWhere(_ == toStanoxCode)
        indexOfArrivalStop <- if (indexOfArrivalStopOpt == -1) None else Some(indexOfArrivalStopOpt)
        arrivalTime = log.subsequentArrivalTimes(indexOfArrivalStop)
      } yield {
        ScheduleQueryResponse(
          id,
          log.scheduleTrainId,
          log.atocCode,
          tocName,
          log.stanoxCode,
          cRSFrom(log.stanoxCode, stanoxRecordsWithCRS).getOrElse(CRS("N/A")),
          departureTime,
          toStanoxCode,
          cRSFrom(toStanoxCode, stanoxRecordsWithCRS).getOrElse(CRS("N/A")),
          arrivalTime,
          log.daysRunPattern,
          log.scheduleStart,
          log.scheduleEnd,
          existingSubscriberRecords.fold(false)(
            _.exists(
              rec =>
                rec.scheduleTrainId == log.scheduleTrainId &&
                  rec.fromStanoxCode == log.stanoxCode &&
                  rec.toStanoxCode == toStanoxCode &&
                  rec.serviceCode == log.serviceCode &&
                  rec.daysRunPattern == log.daysRunPattern))
        )
      }
    }

  private def cRSFrom(stanoxCode: StanoxCode, stanoxRecordsWithCRS: Map[StanoxCode, List[StanoxRecord]]): Option[CRS] =
    stanoxRecordsWithCRS
      .get(stanoxCode)
      .map(x => x.find(_.primary.contains(true)).getOrElse(x.head))
      .flatMap(_.crs)

  private def scheduleStartFormat(scheduleStart: LocalDate): String = {
    val now = LocalDate.now()
    if (scheduleStart.isBefore(now) || scheduleStart.isEqual(now)) "Current"
    else scheduleStart.format(dateFormatter)
  }
}
