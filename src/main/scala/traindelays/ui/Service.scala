package traindelays.ui

import _root_.cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.Json
import io.circe.syntax._
import org.http4s.dsl.io._
import org.http4s.{HttpService, Request, StaticFile, UrlForm}
import traindelays.networkrail.db.{ScheduleTable, StanoxTable}
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import org.http4s.circe._
import traindelays.networkrail.{StanoxCode, TipLocCode}
import traindelays.networkrail.scheduledata.StanoxRecord

object Service extends StrictLogging {

  def apply(scheduleTable: ScheduleTable, stanoxTable: StanoxTable) = HttpService[IO] {
    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case request @ GET -> Root / "stations" =>
      Ok(
        stanoxTable
          .retrieveAllRecords()
          //TODO filter out those not in schedule table?
          .map(stanoxRecords => jsonStationsFrom(stanoxRecords).noSpaces))

    case request @ POST -> Root / "schedule-query" =>
      request.decode[UrlForm] { m =>
        println(m.values)
        val result: Option[IO[List[ScheduleQueryResponse]]] = for {
          fromStation    <- m.getFirst("fromStationStanox").map(str => StanoxCode(str))
          toStation      <- m.getFirst("toStationStanox").map(str => StanoxCode(str))
          weekdaysSatSun <- m.getFirst("weekdaysSatSun").flatMap(DaysRunPattern.fromString)
        } yield {
          //TODO check if tiploc valid?
          for {
            stanoxRecords <- stanoxTable.retrieveAllRecords()
            queryResponses <- scheduleTable
              .retrieveScheduleLogRecordsFor(fromStation, toStation, weekdaysSatSun)
              .map { scheduleLogs =>
                queryResponsesFrom(scheduleLogs, toStation, stanoxRecords)
              }
          } yield queryResponses
        }
        result.fold(BadRequest())(lst => Ok(lst.map(_.asJson.noSpaces)))
      }
  }

  private def static(file: String, request: Request[IO]) = {
    println(file)
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

  //TODO use cats NEL for GroupBy
  private def jsonStationsFrom(stanoxRecords: List[StanoxRecord]) = {
    val records = stanoxRecords
      .filter(_.crs.isDefined)
      .groupBy(_.stanoxCode)
      .map {
        case (stanoxCode, rec) =>
          Json.obj(
            "key" -> Json.fromString(stanoxCode.value),
            "value" -> Json.fromString(
              s"${rec.head.description.getOrElse("")} [${rec.head.crs.map(_.value).getOrElse("")}]")
          )
      }
      .toSeq
    Json.arr(records: _*)
  }
}
