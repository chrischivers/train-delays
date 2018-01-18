package traindelays.networkrail.scripts

import cats.Eval
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

  val app = for {
//    _                       <- networkRailClient.deleteTmpFiles()
//    _                       <- IO.eval(Eval.now(logger.info("Downloading schedule data")))
//    _                       <- networkRailClient.downloadScheduleData
//    _                       <- IO.eval(Eval.now(logger.info("Unpacking schedule data")))
//    _                       <- networkRailClient.unpackScheduleData
    existingScheduleRecords <- existingScheduleRecords
    existingTipLocRecords   <- existingTipLocRecords
    _                       <- IO.eval(Eval.now(logger.info("Writing tiploc records")))
    _                       <- writeTiplocRecords(existingTipLocRecords)
    _                       <- IO.eval(Eval.now(logger.info("Writing schedule records")))
    _                       <- writeScheduleRecords(existingScheduleRecords, existingTipLocRecords)
    _                       <- IO.eval(Eval.now(logger.info("Schedule Table population complete")))
  } yield ()

  private def writeTiplocRecords(existingTipLocRecords: List[TipLocRecord]) =
    withTransactor(config.databaseConfig)() { db =>
      val tipLocTable = TipLocTable(db)
      scheduleDataReader
        .readData[TipLocRecord]
        .filter(rec => !existingTipLocRecords.exists(existingRec => existingRec.tipLocCode == rec.tipLocCode))
        .to(tipLocTable.dbWriter)
    }.run

  private def writeScheduleRecords(existingScheduleRecords: List[ScheduleLog],
                                   existingTipLocRecords: List[TipLocRecord]) =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      val tipLocTable   = TipLocTable(db)

      val recordsToLogsPipe: Pipe[IO, ScheduleRecord, List[ScheduleLog]] = (in: fs2.Stream[IO, ScheduleRecord]) => {
        in.flatMap(x => fs2.Stream.eval(x.toScheduleLogs(existingTipLocRecords)))
      }

      val removeExistingLogsPipe: Pipe[IO, List[ScheduleLog], List[ScheduleLog]] =
        (in: fs2.Stream[IO, List[ScheduleLog]]) => {
          in.map(_.filterNot(scheduleLog =>
            existingScheduleRecords.exists(existingLog => scheduleLog.matchesKeyFields(existingLog))))
        }

      scheduleDataReader
        .readData[ScheduleRecord]
        .through(recordsToLogsPipe)
        .through(removeExistingLogsPipe)
        .to(scheduleTable.dbWriterMultiple)
    }.run

  private def existingScheduleRecords: IO[List[ScheduleLog]] =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      fs2.Stream.eval(scheduleTable.retrieveAllRecords())
    }.runLog.map(_.flatten.toList)

  private def existingTipLocRecords: IO[List[TipLocRecord]] =
    withTransactor(config.databaseConfig)() { db =>
      val tipLocTable = TipLocTable(db)
      fs2.Stream.eval(tipLocTable.retrieveAllRecords())
    }.runLog.map(_.flatten.toList)

  private def deleteAllScheduleRecords =
    withTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      fs2.Stream.eval(scheduleTable.deleteAllRecords())
    }.run

  app.unsafeRunSync()

}
