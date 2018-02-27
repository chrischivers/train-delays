package traindelays

import cats.effect.IO
import org.http4s.server.blaze._
import org.http4s.util.{ExitCode, StreamApp}
import traindelays.networkrail.db._
import traindelays.ui.{GoogleAuthenticator, HistoryService, Service}

object StartWebServer extends StreamApp[IO] {

  def config = TrainDelaysConfig()

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, ExitCode] =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable        = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val stanoxTable          = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val subscriberTable      = SubscriberTable(db, config.networkRailConfig.subscribersConfig.memoizeFor)
      val movementLogTable     = MovementLogTable(db)
      val cancellationLogTable = CancellationLogTable(db)
      val googleAuthenticator  = GoogleAuthenticator(config.uIConfig.clientId)
      val historyService       = HistoryService(movementLogTable, cancellationLogTable, stanoxTable, scheduleTable)

      BlazeBuilder[IO]
        .bindHttp(config.httpConfig.port, "localhost")
        .mountService(Service(historyService,
                              scheduleTable,
                              stanoxTable,
                              subscriberTable,
                              movementLogTable,
                              cancellationLogTable,
                              config.uIConfig,
                              googleAuthenticator),
                      "/")
        .serve

    }

//  def testconfig: TrainDelaysConfig = config.copy(
//    databaseConfig = DatabaseConfig(
//      "org.postgresql.Driver",
//      "jdbc:postgresql://localhost/traindelays",
//      "postgres",
//      "",
//      10
//    )
//  )
}
