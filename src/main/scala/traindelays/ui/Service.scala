package traindelays.ui

import java.time.Instant

import _root_.cats.effect.IO
import cats.kernel.Order
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, EntityEncoder, HttpService, Request, StaticFile}
import org.postgresql.util.PSQLException
import scalacache.Cache
import scalacache.CatsEffect.modes._
import scalacache.guava.GuavaCache
import scalacache.memoization._
import traindelays.UIConfig
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.db.ScheduleTable.{ScheduleRecordPrimary, ScheduleRecordSecondary}
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata.ScheduleTrainId
import traindelays.networkrail.subscribers.SubscriberRecord

import scala.concurrent.duration.FiniteDuration

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

  implicit val subscriberRecordsListResponseEncoder: EntityEncoder[IO, List[SubscriberRecordsResponse]] =
    jsonEncoderOf[IO, List[SubscriberRecordsResponse]]

  implicit protected val memoizeRoutesCache: Cache[String] = GuavaCache[String]

  def apply(historyService: HistoryService,
            scheduleService: ScheduleService,
            scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
            scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
            stanoxTable: StanoxTable,
            subscriberTable: SubscriberTable,
            uiConfig: UIConfig,
            googleAuthenticator: GoogleAuthenticator) = HttpService[IO] {

    case request @ GET -> Root =>
      static("train-delay-notifications.html", request)

    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case _ @GET -> Root / "stations" =>
      Ok(stationsList(uiConfig.memoizeRouteListFor, stanoxTable, scheduleTablePrimary))

    case _ @GET -> Root / "subscriber-records" / subscriberToken =>
      println("Here. Subscriber Id " + subscriberToken)
      (for {
        maybeAuthenticatedDetails <- googleAuthenticator.verifyToken(subscriberToken)
        records <- maybeAuthenticatedDetails
          .fold[IO[List[SubscriberRecord]]](IO.raiseError(
            new RuntimeException(s"Unable to retrieve authentication details for id token $subscriberToken"))) {
            authenticatedDetails =>
              subscriberTable.subscriberRecordsFor(authenticatedDetails.userId)
          }
      } yield records).attempt.flatMap {
        case Right(subscriberRecords) => Ok(SubscriberRecordsResponse.subscriberRecordsResponseFrom(subscriberRecords))
        case Left(err) =>
          logger.error(s"Unable to get subscribers records for id $subscriberToken", err)
          BadRequest()
      }

    case request @ POST -> Root / "schedule-query" =>
      //TODO add in association records
      request.as[ScheduleQueryRequest].attempt.flatMap {
        case Right(req) =>
          scheduleService.handleScheduleRequest(req)
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
            processSubscriberRequest(subscriberRequest,
                                     scheduleTablePrimary,
                                     scheduleTableSecondary,
                                     subscriberTable,
                                     googleAuthenticator).attempt
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
                                       scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
                                       scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary],
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
                  scheduleRec <- retrieveRecordById(id, scheduleTablePrimary, scheduleTableSecondary)
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

  private def retrieveRecordById(
      scheduleQueryResponseId: ScheduleQueryResponseId,
      scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
      scheduleTableSecondary: ScheduleTable[ScheduleRecordSecondary]): IO[Option[ScheduleTable.ScheduleRecord]] =
    scheduleQueryResponseId.recordType match {
      case ScheduleRecordPrimary.recordTypeString =>
        scheduleTablePrimary.retrieveRecordBy(scheduleQueryResponseId.value)
      case ScheduleRecordSecondary.recordTypeString =>
        scheduleTableSecondary.retrieveRecordBy(scheduleQueryResponseId.value)
    }

  private def stationsList(memoizeRouteListFor: FiniteDuration,
                           stanoxTable: StanoxTable,
                           scheduleTable: ScheduleTable[ScheduleRecordPrimary]): IO[String] =
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
