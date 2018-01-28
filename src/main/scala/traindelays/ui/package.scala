package traindelays

import java.time.{LocalDate, LocalTime}

import io.circe.Encoder
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.{ServiceCode, StanoxCode}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId}
import io.circe.generic.semiauto._
import traindelays.networkrail.tocs.tocs

package object ui {

  case class ScheduleQueryResponse(scheduleTrainId: ScheduleTrainId,
                                   atocCode: AtocCode,
                                   tocName: String,
                                   fromStanoxCode: StanoxCode,
                                   departureTime: LocalTime,
                                   toStanoxCode: StanoxCode,
                                   arrivalTime: LocalTime,
                                   daysRunPattern: DaysRunPattern,
                                   scheduleStart: LocalDate,
                                   scheduleEnd: LocalDate)

  object ScheduleQueryResponse {

    import io.circe.java8.time._
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
  }

  //TODO test this
  def queryResponsesFrom(scheduleLogs: List[ScheduleLog], toStanoxCode: StanoxCode): List[ScheduleQueryResponse] =
    scheduleLogs.flatMap { log =>
      for {
        departureTime <- log.departureTime
        tocName       <- tocs.mapping.get(log.atocCode)
        indexOfArrivalStopOpt = log.subsequentStanoxCodes.indexWhere(_ == toStanoxCode)
        indexOfArrivalStop <- if (indexOfArrivalStopOpt == -1) None else Some(indexOfArrivalStopOpt)
        arrivalTime = log.subsequentArrivalTimes(indexOfArrivalStop)
      } yield {
        ScheduleQueryResponse(
          log.scheduleTrainId,
          log.atocCode,
          tocName,
          log.stanoxCode,
          departureTime,
          toStanoxCode,
          arrivalTime,
          log.daysRunPattern,
          log.scheduleStart,
          log.scheduleEnd
        )
      }
    }
}
