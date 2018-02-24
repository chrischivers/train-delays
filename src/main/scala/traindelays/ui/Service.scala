package traindelays.ui

import java.time.{Instant, LocalDate}

import _root_.cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import io.circe.syntax._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, EntityEncoder, HttpService, Request, StaticFile, UrlForm}
import traindelays.networkrail.db.{MovementLogTable, ScheduleTable, StanoxTable, SubscriberTable}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import org.http4s.circe._
import traindelays.networkrail.{CRS, StanoxCode, TOC, TipLocCode}
import traindelays.networkrail.scheduledata.{AtocCode, ScheduleTrainId, StanoxRecord}
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.TemporalAmount

import scala.concurrent.duration._
import scalacache.memoization._
import scalacache.CatsEffect.modes._
import cats.kernel.Order
import org.postgresql.util.PSQLException
import traindelays.UIConfig
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.movementdata.EventType.{Arrival, Departure}
import traindelays.networkrail.movementdata.MovementLog
import traindelays.networkrail.subscribers.{SubscriberRecord, UserId}

import scala.concurrent.duration.FiniteDuration
import scalacache.Cache
import scalacache.guava.GuavaCache

object Service extends StrictLogging {

  import cats.instances.list._
  import cats.syntax.traverse._

  object ScheduleTrainIdParamMatcher extends QueryParamDecoderMatcher[String]("scheduleTrainId")

  object FromStanoxCodeParamMatcher extends QueryParamDecoderMatcher[String]("fromStanox")

  object ToStanoxCodeParamMatcher extends QueryParamDecoderMatcher[String]("toStanox")

  implicit val scheduleRequestEntityDecoder: EntityDecoder[IO, ScheduleQueryRequest] =
    jsonOf[IO, ScheduleQueryRequest]

  implicit val subscribeRequestEntityDecoder: EntityDecoder[IO, SubscribeRequest] =
    jsonOf[IO, SubscribeRequest]

  implicit protected val memoizeRoutesCache: Cache[String] = GuavaCache[String]

  def apply(scheduleTable: ScheduleTable,
            stanoxTable: StanoxTable,
            subscriberTable: SubscriberTable,
            movementLogTable: MovementLogTable,
            uiConfig: UIConfig,
            googleAuthenticator: GoogleAuthenticator) = HttpService[IO] {

    case request @ GET -> Root =>
      static("train-delay-notifications.html", request)

    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case request @ GET -> Root / "stations" =>
      Ok(stationsList(uiConfig.memoizeRouteListFor, stanoxTable, scheduleTable))

    case request @ POST -> Root / "schedule-query" =>
      request.as[ScheduleQueryRequest].attempt.flatMap {
        case Right(req) =>
          (for {
            maybeAuthenticatedDetails <- req.idToken.fold[IO[Option[AuthenticatedDetails]]](IO.pure(None))(token =>
              googleAuthenticator.verifyToken(token))
            stanoxRecordsWithCRS <- stanoxTable.retrieveAllRecordsWithCRS()
            maybeExistingSubscriberRecords <- maybeAuthenticatedDetails.fold[IO[Option[List[SubscriberRecord]]]](
              IO.pure(None))(details => subscriberTable.subscriberRecordsFor(details.userId).map(Some(_)))
            queryResponses <- scheduleTable
              .retrieveScheduleLogRecordsFor(req.fromStanox, req.toStanox, req.daysRunPattern)
              .map { scheduleLogs =>
                scheduleQueryResponsesFrom(
                  filterOutInvalidOrDuplicates(scheduleLogs, uiConfig.minimumDaysScheduleDuration),
                  req.toStanox,
                  stanoxRecordsWithCRS.groupBy(_.stanoxCode),
                  maybeExistingSubscriberRecords
                )
              }

          } yield queryResponses).attempt.flatMap(_.fold(err => {
            logger.error("Error processing schedule query request", err)
            InternalServerError()
          }, lst => Ok(lst.asJson.noSpaces)))
        case Left(err) =>
          logger.error(s"Unable to decode schedule-query ${request.toString()}", err)
          BadRequest()
      }

    case request @ POST -> Root / "subscribe" =>
      request
        .as[SubscribeRequest]
        .attempt
        .flatMap {
          case Right(subscriberRequest) =>
            logger.info(s"Received subscribe request [$subscriberRequest]")
            processSubscriberRequest(subscriberRequest, scheduleTable, subscriberTable, googleAuthenticator).attempt
              .flatMap {
                case Left(e)
                    if e.isInstanceOf[PSQLException] && e
                      .asInstanceOf[PSQLException]
                      .getMessage
                      .contains("duplicate key value") =>
                  logger.info(s"Subscriber already subscribed to route")
                  Conflict("Already subscribed")
                case Left(e) =>
                  logger.error(s"Unable to process subscriber request received", e)
                  InternalServerError()
                case Right(_) =>
                  logger.info("Subscriber request successful")
                  Ok()
              }
          case Left(err) =>
            logger.error(s"Unable to decode subscribe request ${request.toString()}", err)
            BadRequest()
        }

    case request @ GET -> Root / "history-query" :? ScheduleTrainIdParamMatcher(scheduleTrainIdStr) :? FromStanoxCodeParamMatcher(
          fromStanoxStr) :? ToStanoxCodeParamMatcher(toStanoxStr) =>
      val scheduleTrainId = ScheduleTrainId(scheduleTrainIdStr)
      val fromStanox      = StanoxCode(fromStanoxStr)
      val toStanox        = StanoxCode(toStanoxStr)
      val fromTimestamp   = Instant.now.minus(java.time.Duration.ofDays(30L)).toEpochMilli

      (for {
        movementLogs <- movementLogTable
          .retrieveRecordsFor(scheduleTrainId = scheduleTrainId,
                              stanoxCodesAffected = Some(List(fromStanox, toStanox)),
                              fromTimestamp = Some(fromTimestamp),
                              toTimestamp = None)
        stanoxRecords <- stanoxTable.retrieveAllRecordsWithCRS()
      } yield {
        //TODO factor in cancellations
        movementLogsToHistory(scheduleTrainId, fromStanox, toStanox, stanoxRecords, movementLogs)
      }).flatMap(_.fold(NotFound())(history => Ok(history.asJson.noSpaces)))
  }

  private def processSubscriberRequest(subscribeRequest: SubscribeRequest,
                                       scheduleTable: ScheduleTable,
                                       subscriberTable: SubscriberTable,
                                       googleAuthenticator: GoogleAuthenticator): IO[Unit] =
    for {
      maybeAuthenticatedDetails <- googleAuthenticator.verifyToken(subscribeRequest.idToken)
      _ <- maybeAuthenticatedDetails
        .fold[IO[List[Unit]]](
          IO.raiseError(new RuntimeException(
            s"Unable to retrieve authentication details for id token ${subscribeRequest.idToken}"))) {
          authenticatedDetails =>
            subscribeRequest.ids
              .map { id =>
                for {
                  scheduleRec <- scheduleTable.retrieveRecordBy(id)
                  maybeSubscriberRecord = scheduleRec.map { scheduleRec =>
                    SubscriberRecord(
                      None,
                      authenticatedDetails.userId,
                      authenticatedDetails.email,
                      authenticatedDetails.emailVerified,
                      authenticatedDetails.name,
                      authenticatedDetails.firstName,
                      authenticatedDetails.familyName,
                      authenticatedDetails.locale,
                      scheduleRec.scheduleTrainId,
                      scheduleRec.serviceCode,
                      subscribeRequest.fromStanox,
                      subscribeRequest.toStanox,
                      subscribeRequest.daysRunPattern
                    )
                  }
                  _ <- maybeSubscriberRecord.fold[IO[Unit]](
                    IO.raiseError(new RuntimeException(s"Invalid schedule ID $id")))(subscriberRec =>
                    subscriberTable.addRecord(subscriberRec))
                } yield ()
              }
              .sequence[IO, Unit]
        }
    } yield ()

  private def static(file: String, request: Request[IO]) = {
    val pathPrefix: Option[String] = file match {
      case _ if file.endsWith(".js")   => Some("js/")
      case _ if file.endsWith(".css")  => Some("css/")
      case _ if file.endsWith(".html") => Some("")
      case _ =>
        logger.info(s"Unknown file request $file")
        None
    }

    pathPrefix.fold(NotFound()) { prefix =>
      StaticFile.fromResource(s"/static/$prefix" + file, Some(request)).getOrElseF(NotFound())
    }
  }

  private def filterOutInvalidOrDuplicates(scheduleLogs: List[ScheduleLog], minimumDaysScheduleDuration: Int) =
    scheduleLogs
      .filter(x => DAYS.between(x.scheduleStart, x.scheduleEnd) > minimumDaysScheduleDuration)
      .groupBy(x => (x.scheduleTrainId, x.serviceCode, x.stanoxCode, x.daysRunPattern))
      .values
      .map(_.head)
      .toList

  private def stationsList(memoizeRouteListFor: FiniteDuration,
                           stanoxTable: StanoxTable,
                           scheduleTable: ScheduleTable): IO[String] =
    memoizeF(Some(memoizeRouteListFor)) {
      for {
        stanoxRecords      <- stanoxTable.retrieveAllRecords()
        distinctInSchedule <- scheduleTable.retrieveAllDistinctStanoxCodes
      } yield jsonStationsFrom(stanoxRecords.filter(x => distinctInSchedule.contains(x.stanoxCode))).noSpaces
    }

  private def jsonStationsFrom(stanoxRecords: List[StanoxRecord]): Json = {
    import cats.implicits._

    implicit val ordering: Order[StanoxCode] = cats.Order.by[StanoxCode, String](_.value)

    val records = stanoxRecords
      .filter(_.crs.isDefined)
      .groupByNel(_.stanoxCode)
      .map {
        case (stanoxCode, rec) =>
          val firstValidRecord = rec.find(_.primary.contains(true)).getOrElse(rec.head)
          Json.obj(
            "key" -> Json.fromString(stanoxCode.value),
            "value" -> Json.fromString(
              s"${firstValidRecord.description.getOrElse("")} [${firstValidRecord.crs.map(_.value).getOrElse("")}]")
          )
      }
      .toSeq
    Json.arr(records: _*)
  }

  private def movementLogsToHistory(scheduleTrainId: ScheduleTrainId,
                                    fromStanoxCode: StanoxCode,
                                    toStanoxCode: StanoxCode,
                                    stanoxRecords: List[StanoxRecord],
                                    movementLogs: List[MovementLog]): Option[HistoryQueryResponse] = {

    val filteredLogs = movementLogs.filter(l =>
      l.scheduleTrainId == scheduleTrainId && (l.stanoxCode == fromStanoxCode || l.stanoxCode == toStanoxCode))

    def getHistoryRecords: List[HistoryQueryRecord] =
      filteredLogs
        .groupBy(l => (l.scheduleTrainId, l.trainId, l.originDepartureDate))
        .flatMap {
          case (group, list) =>
            for {
              departureLog <- list.find(l => l.eventType == Departure && l.stanoxCode == fromStanoxCode)
              arrivalLog   <- list.find(l => l.eventType == Arrival && l.stanoxCode == toStanoxCode)
            } yield {
              HistoryQueryRecord(group._3, departureLog.actualTime, arrivalLog.actualTime)
            }
        }
        .toList
        .sortBy(_.departureDate.toEpochDay)

    for {
      fromCRS <- stanoxRecords.find(_.stanoxCode == fromStanoxCode).flatMap(_.crs)
      toCRS   <- stanoxRecords.find(_.stanoxCode == toStanoxCode).flatMap(_.crs)
      plannedDepartureTime <- filteredLogs
        .find(y => y.eventType == Departure && y.stanoxCode == fromStanoxCode)
        .map(_.plannedPassengerTime)
      plannedArrivalTime <- filteredLogs
        .find(y => y.eventType == Arrival && y.stanoxCode == toStanoxCode)
        .map(_.plannedPassengerTime)
      toc <- filteredLogs.headOption.map(_.toc)
    } yield {
      HistoryQueryResponse(scheduleTrainId,
                           toc,
                           fromStanoxCode,
                           fromCRS,
                           toStanoxCode,
                           toCRS,
                           plannedDepartureTime,
                           plannedArrivalTime,
                           getHistoryRecords)
    }

  }
}
