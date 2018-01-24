package traindelays.scripts

import cats.effect.IO
import org.http4s.util.{ExitCode, StreamApp}
import org.http4s.server.blaze._
import traindelays.ConfigLoader
import traindelays.networkrail.db.{ScheduleTable, withTransactor}
import traindelays.scripts.PopulateScheduleTable.config
import traindelays.ui.Service

object StartWebServer extends StreamApp[IO] {

  val config = ConfigLoader.defaultConfig

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, ExitCode] =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)

      BlazeBuilder[IO]
        .bindHttp(8080, "localhost")
        .mountService(Service(scheduleTable), "/")
        .serve

    }
}
