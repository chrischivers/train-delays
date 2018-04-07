package traindelays.ui

import java.time.Instant

import _root_.cats.effect.IO
import cats.kernel.Order
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import org.http4s.circe._
import org.http4s.dsl.io._
import org.http4s.{EntityDecoder, HttpService, Request, StaticFile}
import scalacache.Cache
import scalacache.CatsEffect.modes._
import scalacache.guava.GuavaCache
import scalacache.memoization._
import traindelays.UIConfig
import traindelays.networkrail.StanoxCode
import traindelays.networkrail.db.ScheduleTable.ScheduleRecordPrimary
import traindelays.networkrail.db.StanoxTable.StanoxRecord
import traindelays.networkrail.db._
import traindelays.networkrail.scheduledata.ScheduleTrainId

import scala.concurrent.duration.FiniteDuration

object Service extends StrictLogging {

  object ScheduleTrainIdParamMatcher extends QueryParamDecoderMatcher[String]("scheduleTrainId")

  object FromStanoxCodeParamMatcher extends QueryParamDecoderMatcher[String]("fromStanox")

  object ToStanoxCodeParamMatcher extends QueryParamDecoderMatcher[String]("toStanox")

  implicit val scheduleRequestEntityDecoder: EntityDecoder[IO, ScheduleQueryRequest] =
    jsonOf[IO, ScheduleQueryRequest]

  implicit protected val memoizeRoutesCache: Cache[String] = GuavaCache[String]

  def apply(historyService: HistoryService,
            scheduleService: ScheduleService,
            subscriberService: SubscriberService,
            scheduleTablePrimary: ScheduleTable[ScheduleRecordPrimary],
            stanoxTable: StanoxTable,
            uiConfig: UIConfig) = HttpService[IO] {

    case request @ GET -> Root =>
      static("train-delay-notifications.html", request)

    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case _ @GET -> Root / "stations" =>
      Ok(stationsList(uiConfig.memoizeRouteListFor, stanoxTable, scheduleTablePrimary))

    case _ @GET -> Root / "subscriber-records" / subscriberToken =>
      subscriberService.handleSubscriberRecordsRequest(subscriberToken)

    case _ @DELETE -> Root / "subscriber-records" / subscriberToken / recordId =>
      subscriberService.deleteSubscriberRecord(subscriberToken, recordId)

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
      subscriberService.handleSubscribeRequest(request)

    case request @ GET -> Root / "history-query" :? ScheduleTrainIdParamMatcher(scheduleTrainIdStr) :? FromStanoxCodeParamMatcher(
          fromStanoxStr) :? ToStanoxCodeParamMatcher(toStanoxStr) =>
      val scheduleTrainId = ScheduleTrainId(scheduleTrainIdStr)
      val fromStanox      = StanoxCode(fromStanoxStr)
      val toStanox        = StanoxCode(toStanoxStr)
      val fromTimestamp   = Some(Instant.now.minus(java.time.Duration.ofDays(30L)).toEpochMilli) //todo set in config
      val toTimestamp     = None
      historyService.handleHistoryRequest(scheduleTrainId, fromStanox, toStanox, fromTimestamp, toTimestamp)
  }

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
