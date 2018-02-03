package traindelays

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto._
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId, StanoxRecord}
import traindelays.networkrail.subscribers.UserId
import traindelays.networkrail.tocs.tocs
import traindelays.networkrail.{CRS, StanoxCode}

package object ui {

  val dateFormatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy")
  val timeFormatter = DateTimeFormatter.ofPattern("HH:mm")

  case class ScheduleQueryResponse(id: Int,
                                   scheduleTrainId: ScheduleTrainId,
                                   atocCode: AtocCode,
                                   tocName: String,
                                   fromStanoxCode: StanoxCode,
                                   fromCRS: CRS,
                                   departureTime: String,
                                   toStanoxCode: StanoxCode,
                                   toCRS: CRS,
                                   arrivalTime: String,
                                   daysRunPattern: DaysRunPattern,
                                   scheduleStart: String,
                                   scheduleEnd: String)

  object ScheduleQueryResponse {
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
  }

  case class SubscribeRequest(email: String, idToken: String, ids: List[Int])

  object SubscribeRequest {
    implicit val decoder: Decoder[SubscribeRequest] = deriveDecoder[SubscribeRequest]
    implicit val encoder: Encoder[SubscribeRequest] = deriveEncoder[SubscribeRequest]
  }

  //TODO test this
  def queryResponsesFrom(scheduleLogs: List[ScheduleLog],
                         toStanoxCode: StanoxCode,
                         stanoxRecordsWithCRS: Map[StanoxCode, List[StanoxRecord]]): List[ScheduleQueryResponse] =
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
          departureTime.format(timeFormatter),
          toStanoxCode,
          cRSFrom(toStanoxCode, stanoxRecordsWithCRS).getOrElse(CRS("N/A")),
          arrivalTime.format(timeFormatter),
          log.daysRunPattern,
          scheduleStartFormat(log.scheduleStart),
          log.scheduleEnd.format(dateFormatter)
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