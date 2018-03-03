package traindelays.ui

import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS

import _root_.cats.effect.IO
import cats.kernel.Order
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, HttpService, Request, StaticFile}
import org.postgresql.util.PSQLException
import traindelays.UIConfig
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.db.ScheduleTable.ScheduleRecord
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata.{ScheduleTrainId, StpIndicator}
import traindelays.networkrail.subscribers.SubscriberRecord

import scala.concurrent.duration.FiniteDuration
import scalacache.Cache
import scalacache.CatsEffect.modes._
import scalacache.guava.GuavaCache
import scalacache.memoization._

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

  def apply(historyService: HistoryService,
            scheduleTable: ScheduleTable,
            stanoxTable: StanoxTable,
            subscriberTable: SubscriberTable,
            movementLogTable: MovementLogTable,
            cancellationLogTable: CancellationLogTable,
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
            stanoxRecordsWithCRS <- stanoxTable.retrieveAllNonEmptyRecords()
            maybeExistingSubscriberRecords <- maybeAuthenticatedDetails.fold[IO[Option[List[SubscriberRecord]]]](
              IO.pure(None))(details => subscriberTable.subscriberRecordsFor(details.userId).map(Some(_)))
            queryResponses <- scheduleTable
              .retrieveScheduleLogRecordsFor(req.fromStanox, req.toStanox, req.daysRunPattern, StpIndicator.P) //todo should this always be P
              .map { scheduleLogs =>
                scheduleQueryResponsesFrom(
                  filterOutInvalidOrDuplicates(scheduleLogs, uiConfig.minimumDaysScheduleDuration),
                  req.toStanox,
                  stanoxRecordsWithCRS
                    .groupBy(_.stanoxCode)
                    .collect { case (Some(stanoxCode), list) => stanoxCode -> list },
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
      val fromTimestamp   = Some(Instant.now.minus(java.time.Duration.ofDays(30L)).toEpochMilli) //todo set in config
      val toTimestamp     = None
      historyService.handleHistoryRequest(scheduleTrainId, fromStanox, toStanox, fromTimestamp, toTimestamp)
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

  private def filterOutInvalidOrDuplicates(scheduleLogs: List[ScheduleRecord], minimumDaysScheduleDuration: Int) =
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
      } yield jsonStationsFrom(stanoxRecords.filter(_.stanoxCode.fold(false)(distinctInSchedule.contains))).noSpaces
    }

  private def jsonStationsFrom(stanoxRecords: List[StanoxRecord]): Json = {

    val records = getMainStanoxRecords(stanoxRecords)
      .collect {
        case StanoxRecord(_, Some(stanoxCode), crs, description, _) =>
          Json.obj(
            "key"   -> Json.fromString(stanoxCode.value),
            "value" -> Json.fromString(s"${description.getOrElse("")} [${crs.map(_.value).getOrElse("")}]")
          )
      }
    Json.arr(records: _*)
  }

  def getMainStanoxRecords(allStanoxRecords: List[StanoxRecord]): List[StanoxRecord] = {
    import cats.implicits._
    implicit val ordering: Order[StanoxCode] = cats.Order.by[StanoxCode, String](_.value)

    allStanoxRecords
      .filter(_.crs.isDefined)
      .groupByNel(_.stanoxCode)
      .map { case (_, rec) => rec.find(_.primary.contains(true)).getOrElse(rec.head) }
      .toList
  }

}
