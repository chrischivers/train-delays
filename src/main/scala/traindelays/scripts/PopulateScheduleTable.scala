package traindelays.scripts

import cats.effect.IO
import com.typesafe.scalalogging.StrictLogging
import fs2.Pipe
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.ConfigLoader
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.ScheduleTable.ScheduleLog
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord, TipLocRecord}

object PopulateScheduleTable extends App with StrictLogging {

  val config = ConfigLoader.defaultConfig
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = withTransactor(config.databaseConfig)() { db =>
    val tipLocTable   = TipLocTable(db, config.networkRailConfig.scheduleData.memoizeFor)
    val scheduleTable = ScheduleTable(db, config.networkRailConfig.scheduleData.memoizeFor)

    fs2.Stream.eval {
      for {
//        _ <- networkRailClient.deleteTmpFiles()
//        _ <- IO.pure(logger.info("Downloading schedule data"))
//        _ <- networkRailClient.downloadScheduleData
//        _ <- IO.pure(logger.info("Unpacking schedule data"))
//        _ <- networkRailClient.unpackScheduleData
        _ <- IO.pure(logger.info("Writing tiploc records"))
        _ <- writeTiplocRecords(tipLocTable)
        _ <- IO.pure(logger.info("Writing schedule records"))
        _ <- writeScheduleRecords(tipLocTable, scheduleTable)
        _ <- IO.pure(logger.info("Schedule Table population complete"))
      } yield ()
    }
  }.run

  app.unsafeRunSync()

  private def writeTiplocRecords(tipLocTable: TipLocTable) =
    for {
      existingTipLocRecords <- tipLocTable.retrieveAllRecords()
      _ <- scheduleDataReader
        .readData[TipLocRecord]
        .filter(rec => !existingTipLocRecords.exists(existingRec => existingRec.tipLocCode == rec.tipLocCode))
        .to(tipLocTable.dbWriter)
        .run
    } yield ()

  private def writeScheduleRecords(tipLocTable: TipLocTable, scheduleTable: ScheduleTable) =
    for {
      existingTipLocRecords   <- tipLocTable.retrieveAllRecords()
      existingScheduleRecords <- scheduleTable.retrieveAllRecords()

      recordsToLogsPipe: Pipe[IO, ScheduleRecord, List[ScheduleLog]] = (in: fs2.Stream[IO, ScheduleRecord]) => {
        in.map(_.toScheduleLogs(existingTipLocRecords))
      }

      removeExistingLogsPipe: Pipe[IO, List[ScheduleLog], List[ScheduleLog]] = (in: fs2.Stream[IO,
                                                                                               List[ScheduleLog]]) => {
        in.map(_.filterNot(scheduleLog =>
          existingScheduleRecords.exists(existingLog => scheduleLog.matchesKeyFields(existingLog))))
      }

      _ <- scheduleDataReader
        .readData[ScheduleRecord]
        .through(recordsToLogsPipe)
        .through(removeExistingLogsPipe)
        .to(scheduleTable.dbWriterMultiple)
        .run
    } yield ()

}
