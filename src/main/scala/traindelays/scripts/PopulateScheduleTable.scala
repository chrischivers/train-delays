package traindelays.scripts

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.Pipe
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.ConfigLoader
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata._

object PopulateScheduleTable extends App with StrictLogging {

  val config = ConfigLoader.defaultConfig
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = withTransactor(config.databaseConfig)() { db =>
    val stanoxTable   = StanoxTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTable = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
        _ <- networkRailClient.deleteTmpFiles()
        _ <- IO.pure(logger.info("Downloading schedule data"))
        _ <- networkRailClient.downloadScheduleData
        _ <- IO.pure(logger.info("Unpacking schedule data"))
        _ <- networkRailClient.unpackScheduleData
        _ <- IO.pure(logger.info("Writing stanox records"))
        _ <- writeStanoxRecords(stanoxTable)
        _ <- IO.pure(logger.info("Writing schedule records"))
        _ <- writeScheduleRecords(stanoxTable, scheduleTable)
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }.run

  app.unsafeRunSync()

  private def writeStanoxRecords(stanoxTable: StanoxTable) =
    for {
      existingStanoxRecords <- stanoxTable.retrieveAllRecords(forceRefresh = true)
      _ <- scheduleDataReader
        .readData[StanoxRecord]
        .filter(
          rec =>
            !(existingStanoxRecords
              .exists(existingRec => existingRec.stanoxCode == rec.stanoxCode) && existingStanoxRecords.exists(
              existingRec => existingRec.tipLocCode == rec.tipLocCode)))
        .to(stanoxTable.dbWriter)
        .run
    } yield ()

  private def writeScheduleRecords(stanoxTable: StanoxTable, scheduleTable: ScheduleTable) = {
    val counter = new AtomicInteger(0) //TODO remove
    for {
      existingStanoxRecords   <- stanoxTable.retrieveAllRecords(forceRefresh = true)
      existingScheduleRecords <- scheduleTable.retrieveAllRecords(forceRefresh = true)

      recordsToLogsPipe: Pipe[IO, ScheduleRecord, List[ScheduleLog]] = (in: fs2.Stream[IO, ScheduleRecord]) => {
        in.map(_.toScheduleLogs(existingStanoxRecords))
      }

      removeExistingLogsPipe: Pipe[IO, List[ScheduleLog], List[ScheduleLog]] = (in: fs2.Stream[IO,
                                                                                               List[ScheduleLog]]) => {
        in.map(_.filterNot(scheduleLog =>
          existingScheduleRecords.exists(existingLog => scheduleLog.matchesKeyFields(existingLog))))
      }

      _ <- scheduleDataReader
        .readData[ScheduleRecord]
        .through(recordsToLogsPipe)
        .observe1(_ =>
          IO {
            val count = counter.incrementAndGet()
            if (count % 1000 == 0) println(count)
        })
        .through(removeExistingLogsPipe)
        .filter(_.nonEmpty)
        .observe1(x => IO(println(x)))
        .to(scheduleTable.dbWriterMultiple)
        .run
    } yield ()

  }
}
