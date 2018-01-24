package traindelays.ui

import _root_.cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import org.http4s.dsl.io._
import org.http4s.{HttpService, Request, StaticFile, UrlForm}
import traindelays.networkrail.Stanox
import traindelays.networkrail.db.ScheduleTable
import traindelays.networkrail.db.ScheduleTable.ScheduleLog.DaysRunPattern
import traindelays.networkrail.scheduledata.ScheduleRecord.ScheduleLocationRecord.TipLocCode
import io.circe.syntax._
import io.circe.generic.auto._
import traindelays.networkrail.scheduledata._

object Service extends StrictLogging {

  def apply(scheduleTable: ScheduleTable) = HttpService[IO] {
    case request @ GET -> Root / path if List(".js", ".css", ".html").exists(path.endsWith) =>
      static(path, request)

    case request @ POST -> Root / "schedule-query" =>
      request.decode[UrlForm] { m =>
        (for {
          fromStation    <- m.getFirst("fromStation").map(TipLocCode(_))
          toStation      <- m.getFirst("toStation").map(TipLocCode(_))
          weekdaysSatSun <- m.getFirst("weekdaysSatSun").flatMap(DaysRunPattern.fromString)
        } yield {
          scheduleTable.retrieveScheduleLogRecordsFor(fromStation, toStation, weekdaysSatSun)
        }).fold(BadRequest())(result => Ok("TODO")) //TODO
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
