package traindelays

import java.time.{LocalDate, LocalTime}

import io.circe.Encoder
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.{CRS, ServiceCode, StanoxCode}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId, StanoxRecord}
import io.circe.generic.semiauto._
import traindelays.networkrail.tocs.tocs

package object ui {

  case class ScheduleQueryResponse(scheduleTrainId: ScheduleTrainId,
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
                                   scheduleEnd: LocalDate)

  object ScheduleQueryResponse {

    import io.circe.java8.time._
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
  }

  //TODO test this
  def queryResponsesFrom(scheduleLogs: List[ScheduleLog],
                         toStanoxCode: StanoxCode,
                         stanoxRecords: List[StanoxRecord]): List[ScheduleQueryResponse] =
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
          stanoxRecords
            .find(rec => rec.stanoxCode == log.stanoxCode && rec.crs.isDefined)
            .flatMap(_.crs)
            .getOrElse(CRS("N/A")),
          departureTime,
          toStanoxCode,
          stanoxRecords
            .find(rec => rec.stanoxCode == toStanoxCode && rec.crs.isDefined)
            .flatMap(_.crs)
            .getOrElse(CRS("N/A")),
          arrivalTime,
          log.daysRunPattern,
          log.scheduleStart,
          log.scheduleEnd
        )
      }
    }
}
