package traindelays.ui

import _root_.cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import io.circe.syntax._
import org.http4s.dsl.io._
import org.http4s.{HttpService, Request, StaticFile, UrlForm}
import traindelays.networkrail.db.ScheduleTable
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import org.http4s.circe._

object Service extends StrictLogging {

  def apply(scheduleTable: ScheduleTable) = HttpService[IO] {
    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case request @ GET -> Root / "tiploc-codes" =>
      Ok(scheduleTable.retrieveDistinctTipLocCodes().map(_.map(_.value).asJson.noSpaces))

    case request @ POST -> Root / "schedule-query" =>
      request.decode[UrlForm] { m =>
        println(m.values)
        val result: Option[IO[List[ScheduleQueryResponse]]] = for {
          fromStation    <- m.getFirst("fromStation").map(str => TipLocCode(str.toUpperCase()))
          toStation      <- m.getFirst("toStation").map(str => TipLocCode(str.toUpperCase()))
          weekdaysSatSun <- m.getFirst("weekdaysSatSun").flatMap(DaysRunPattern.fromString)
        } yield {
          //TODO check if tiploc valid?
          scheduleTable.retrieveScheduleLogRecordsFor(fromStation, toStation, weekdaysSatSun).map { scheduleLogs =>
            queryResponsesFrom(scheduleLogs, toStation)
          }
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
}
