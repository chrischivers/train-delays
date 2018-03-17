package traindelays.ui

import java.time.temporal.ChronoUnit.DAYS

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import org.http4s.Response
import org.http4s.dsl.io._
import traindelays.UIConfig
import traindelays.networkrail.{CRS, Definitions, StanoxCode}
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecord, ScheduleRecordPrimary, ScheduleRecordSecondary}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata.StpIndicator
import traindelays.networkrail.subscribers.SubscriberRecord

trait ScheduleService {
  def handleScheduleRequest(req: ScheduleQueryRequest): IO[Response[IO]]
}

object ScheduleService extends StrictLogging {

  def apply(stanoxTable: StanoxTable,
            subscriberTable: SubscriberTable,
            scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
            scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
            googleAuthenticator: GoogleAuthenticator,
            uIConfig: UIConfig) =
    new ScheduleService {

      override def handleScheduleRequest(req: ScheduleQueryRequest): IO[Response[IO]] =
        (for {
          maybeAuthenticatedDetails <- req.idToken.fold[IO[Option[AuthenticatedDetails]]](IO.pure(None))(token =>
            googleAuthenticator.verifyToken(token))
          stanoxRecordsWithCRS <- stanoxTable.retrieveAllNonEmptyRecords()
          maybeExistingSubscriberRecords <- maybeAuthenticatedDetails.fold[IO[Option[List[SubscriberRecord]]]](
            IO.pure(None))(details => subscriberTable.subscriberRecordsFor(details.userId).map(Some(_)))
          queryResponsesPrimary <- queryResponsesFrom(scheduleTablePrimary,
                                                      req,
                                                      stanoxRecordsWithCRS,
                                                      maybeExistingSubscriberRecords)
          queryResponsesSecondary <- queryResponsesFrom(scheduleTableSecondary,
                                                        req,
                                                        stanoxRecordsWithCRS,
                                                        maybeExistingSubscriberRecords)
        } yield
          combinePrimaryAndSecondary(queryResponsesPrimary, queryResponsesSecondary)).attempt.flatMap(_.fold(err => {
          logger.error("Error processing schedule query request", err)
          InternalServerError()
        }, lst => Ok(lst.asJson.noSpaces)))

      private def combinePrimaryAndSecondary(primaryResponses: List[ScheduleQueryResponse],
                                             secondaryResponse: List[ScheduleQueryResponse]) =
        primaryResponses ++ secondaryResponse.filterNot(x => primaryResponses.exists(_.matchesKeyFields(x)))

      private def queryResponsesFrom[A <: ScheduleRecord](
          table: ScheduleTable[A],
          req: ScheduleQueryRequest,
          stanoxRecordsWithCRS: List[StanoxRecord],
          maybeExistingSubscriberRecords: Option[List[SubscriberRecord]]) =
        table
          .retrieveScheduleRecordsFor(req.fromStanox, req.toStanox, req.daysRunPattern, StpIndicator.P) // This will always be P as we are dealing with permanent records
          .map { scheduleLogs =>
            scheduleQueryResponsesFrom(
              filterOutInvalidOrDuplicates(scheduleLogs, uIConfig.minimumDaysScheduleDuration),
              req.toStanox,
              stanoxRecordsWithCRS
                .groupBy(_.stanoxCode)
                .collect { case (Some(stanoxCode), list) => stanoxCode -> list },
              maybeExistingSubscriberRecords
            )
          }
      private def filterOutInvalidOrDuplicates(scheduleLogs: List[ScheduleRecord], minimumDaysScheduleDuration: Int) =
        scheduleLogs
          .filter(x => DAYS.between(x.scheduleStart, x.scheduleEnd) > minimumDaysScheduleDuration)
          .groupBy(x => (x.scheduleTrainId, x.serviceCode, x.stanoxCode, x.daysRunPattern))
          .values
          .map(_.head)
          .toList

      //TODO test this
      private def scheduleQueryResponsesFrom(
          scheduleLogs: List[ScheduleRecord],
          toStanoxCode: StanoxCode,
          stanoxRecordsWithCRS: Map[StanoxCode, List[StanoxRecord]],
          existingSubscriberRecords: Option[List[SubscriberRecord]]): List[ScheduleQueryResponse] =
        scheduleLogs.flatMap { log =>
          for {
            id            <- log.scheduleQueryResponseId
            departureTime <- log.departureTime
            tocName       <- log.atocCode.flatMap(atoc => Definitions.atocToOperatorNameMapping.get(atoc))
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

      private def cRSFrom(stanoxCode: StanoxCode,
                          stanoxRecordsWithCRS: Map[StanoxCode, List[StanoxRecord]]): Option[CRS] =
        stanoxRecordsWithCRS
          .get(stanoxCode)
          .map(x => x.find(_.primary.contains(true)).getOrElse(x.head))
          .flatMap(_.crs)
    }
}
