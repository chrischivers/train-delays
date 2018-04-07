package traindelays

import cats.effect.IO
import fs2.StreamApp.ExitCode
import org.http4s.server.blaze._
import traindelays.networkrail.db._
import traindelays.ui._

import scala.concurrent.ExecutionContext.Implicits.global

object StartWebServer extends fs2.StreamApp[IO] {

  def config = TrainDelaysConfig()

  override def stream(args: List[String], requestShutdown: IO[Unit]): fs2.Stream[IO, ExitCode] =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTablePrimary   = SchedulePrimaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val scheduleTableSecondary = ScheduleSecondaryTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val stanoxTable            = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
      val subscriberTable        = SubscriberTable(db, config.networkRailConfig.subscribersConfig.memoizeFor)
      val movementLogTable       = MovementLogTable(db)
      val cancellationLogTable   = CancellationLogTable(db)
      val googleAuthenticator    = GoogleAuthenticator(config.uIConfig.clientId)
      val historyService = HistoryService(movementLogTable,
                                          cancellationLogTable,
                                          stanoxTable,
                                          scheduleTablePrimary,
                                          scheduleTableSecondary)
      val scheduleService =
        ScheduleService(stanoxTable,
                        subscriberTable,
                        scheduleTablePrimary,
                        scheduleTableSecondary,
                        googleAuthenticator,
                        config.uIConfig)

      val subscriberService =
        SubscriberService(subscriberTable, scheduleTablePrimary, scheduleTableSecondary, googleAuthenticator)

      BlazeBuilder[IO]
        .bindHttp(config.httpConfig.port, "localhost")
        .mountService(
          Service(
            historyService,
            scheduleService,
            subscriberService,
            scheduleTablePrimary,
            stanoxTable,
            config.uIConfig
          ),
          "/"
        )
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
