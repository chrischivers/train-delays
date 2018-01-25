package traindelays

import java.time.{LocalDate, LocalTime}

import io.circe.Encoder
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.{ServiceCode, Stanox}
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId}
import io.circe.generic.semiauto._

package object ui {

  case class ScheduleQueryResponse(scheduleTrainId: ScheduleTrainId,
                                   serviceCode: ServiceCode,
                                   atocCode: AtocCode,
                                   fromTipLocCode: TipLocCode,
                                   fromStanox: Stanox,
                                   departureTime: LocalTime,
                                   toTipLocCode: TipLocCode,
                                   arrivalTime: LocalTime,
                                   daysRunPattern: DaysRunPattern,
                                   scheduleStart: LocalDate,
                                   scheduleEnd: LocalDate)

  object ScheduleQueryResponse {

    import io.circe.java8.time._
    implicit val encoder: Encoder[ScheduleQueryResponse] = deriveEncoder[ScheduleQueryResponse]
  }

  def queryResponsesFrom(scheduleLogs: List[ScheduleLog], toTipLocCode: TipLocCode): List[ScheduleQueryResponse] =
    scheduleLogs.flatMap { log =>
      for {
        departureTime <- log.departureTime
        indexOfArrivalStopOpt = log.subsequentTipLocCodes.indexWhere(_ == toTipLocCode)
        indexOfArrivalStop <- if (indexOfArrivalStopOpt == -1) None else Some(indexOfArrivalStopOpt)
        arrivalTime = log.subsequentArrivalTimes(indexOfArrivalStop)
      } yield {
        ScheduleQueryResponse(
          log.scheduleTrainId,
          log.serviceCode,
          log.atocCode,
          log.tiplocCode,
          log.stanox,
          departureTime,
          toTipLocCode,
          arrivalTime,
          log.daysRunPattern,
          log.scheduleStart,
          log.scheduleEnd
        )
      }
    }
}
