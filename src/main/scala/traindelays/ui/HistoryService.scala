package traindelays.ui

import java.time.{LocalDate, LocalTime}

import cats.Order
import cats.effect.IO
import io.circe.syntax._
import org.http4s.Response
import org.http4s.dsl.io.{NotFound, Ok, _}
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db.{CancellationLogTable, MovementLogTable, ScheduleTable, StanoxTable}
import traindelays.networkrail.movementdata.EventType.{Arrival, Departure}
import traindelays.networkrail.movementdata.{CancellationLog, MovementLog, TrainId}
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.ui.Service.getMainStanoxRecords

trait HistoryService {
  def handleHistoryRequest(scheduleTrainId: ScheduleTrainId,
                           fromStanox: StanoxCode,
                           toStanox: StanoxCode,
                           fromTimestamp: Option[Long],
                           toTimestamp: Option[Long]): IO[Response[IO]]
}

object HistoryService {

  def apply(movementLogTable: MovementLogTable,
            cancellationLogTable: CancellationLogTable,
            stanoxTable: StanoxTable,
            scheduleTable: ScheduleTable) =
    new HistoryService {

      override def handleHistoryRequest(scheduleTrainId: ScheduleTrainId,
                                        fromStanox: StanoxCode,
                                        toStanox: StanoxCode,
                                        fromTimestamp: Option[Long],
                                        toTimestamp: Option[Long]) = {

        //TODO probably dont need to pass all schedule logs
        def logsToHistory(scheduleTrainId: ScheduleTrainId,
                          fromStanoxCode: StanoxCode,
                          toStanoxCode: StanoxCode,
                          stanoxRecords: List[StanoxRecord],
                          movementLogs: List[MovementLog],
                          cancellationLogs: List[CancellationLog],
                          scheduleLogsFrom: List[ScheduleRecord],
                          scheduleLogsTo: List[ScheduleRecord]): Option[HistoryQueryResponse] = {

          val mainStanoxRecords = getMainStanoxRecords(stanoxRecords)

          val filteredMovementLogs = movementLogs.filter(l =>
            l.scheduleTrainId == scheduleTrainId && (l.stanoxCode == fromStanoxCode || l.stanoxCode == toStanoxCode))

          val filteredCancellationLogs = cancellationLogs.filter(l => l.scheduleTrainId == scheduleTrainId)

          def getHistoryRecordsFromMovementLogs(expectedDepartureTime: LocalTime,
                                                expectedArrivalTime: LocalTime): List[HistoryQueryMovementRecord] =
            filteredMovementLogs
              .groupBy(l => (l.scheduleTrainId, l.trainId, l.originDepartureDate))
              .flatMap {
                case (group, list) =>
                  for {
                    departureLog <- list.find(l => l.eventType == Departure && l.stanoxCode == fromStanoxCode)
                    arrivalLog   <- list.find(l => l.eventType == Arrival && l.stanoxCode == toStanoxCode)
                  } yield {
                    HistoryQueryMovementRecord(
                      group._3,
                      departureLog.actualTime,
                      java.time.Duration.between(expectedDepartureTime, departureLog.actualTime).toMinutes,
                      arrivalLog.actualTime,
                      java.time.Duration.between(expectedArrivalTime, arrivalLog.actualTime).toMinutes
                    )
                  }
              }
              .toList
              .sortBy(_.scheduledDepartureDate.toEpochDay)

          def getHistoryRecordsFromCancellationLogs: List[HistoryQueryCancellationRecord] = {
            import cats.implicits._
            implicit val ordering: Order[(ScheduleTrainId, TrainId, LocalDate)] =
              cats.Order.by[(ScheduleTrainId, TrainId, LocalDate), String](_._3.toString)
            filteredCancellationLogs
              .groupByNel(l => (l.scheduleTrainId, l.trainId, l.originDepartureDate))
              .map {
                case (group, list) =>
                  HistoryQueryCancellationRecord(
                    group._3,
                    list.head.cancellationType,
                    list.head.cancellationReasonCode
                  )
              }
              .toList
              .sortBy(_.scheduledDepartureDate.toEpochDay)
          }

          for {
            fromCRS              <- mainStanoxRecords.find(_.stanoxCode.get == fromStanoxCode).flatMap(_.crs)
            toCRS                <- mainStanoxRecords.find(_.stanoxCode.get == toStanoxCode).flatMap(_.crs)
            departureScheduleLog <- scheduleLogsFrom.find(_.stanoxCode == fromStanox)
            plannedDepartureTime <- departureScheduleLog.departureTime
            plannedArrivalTime <- scheduleLogsTo
              .find(l =>
                l.stanoxCode == toStanox && l.scheduleStart == departureScheduleLog.scheduleStart && l.scheduleEnd == departureScheduleLog.scheduleEnd)
              .flatMap(_.arrivalTime)
            atocCode = departureScheduleLog.atocCode
          } yield {
            HistoryQueryResponse(
              scheduleTrainId,
              atocCode,
              fromStanoxCode,
              fromCRS,
              toStanoxCode,
              toCRS,
              plannedDepartureTime,
              plannedArrivalTime,
              getHistoryRecordsFromMovementLogs(plannedDepartureTime, plannedArrivalTime),
              getHistoryRecordsFromCancellationLogs
            )
          }

        }

        val result = for {
          movementLogs <- movementLogTable
            .retrieveRecordsFor(scheduleTrainId = scheduleTrainId,
                                stanoxCodesAffected = Some(List(fromStanox, toStanox)),
                                fromTimestamp = fromTimestamp,
                                toTimestamp = toTimestamp)
          cancellationLogs <- cancellationLogTable.retrieveRecordsFor(scheduleTrainId = scheduleTrainId,
                                                                      fromTimestamp = fromTimestamp,
                                                                      toTimestamp = toTimestamp)
          scheduleRecordsFrom <- scheduleTable.retrieveScheduleLogRecordsFor(scheduleTrainId, fromStanox)
          scheduleRecordsTo   <- scheduleTable.retrieveScheduleLogRecordsFor(scheduleTrainId, toStanox)
          stanoxRecords       <- stanoxTable.retrieveAllNonEmptyRecords()
        } yield {
          logsToHistory(scheduleTrainId,
                        fromStanox,
                        toStanox,
                        stanoxRecords,
                        movementLogs,
                        cancellationLogs,
                        scheduleRecordsFrom,
                        scheduleRecordsTo)
        }
        result.flatMap(_.fold(NotFound())(history => Ok(history.asJson.noSpaces)))
      }
    }
}
