package traindelays.networkrail.scripts

import java.time.LocalTime

import cats.effect.IO
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.ConfigLoader
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord, TipLocRecord}

object PopulateScheduleTable extends App {

  val config = ConfigLoader.defaultConfig
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = for {
    _ <- networkRailClient.downloadScheduleData
    _ <- networkRailClient.unpackScheduleData
    _ <- deleteAllRecords
    _ <- writeScheduleRecords
    _ <- writeTiplocRecords
  } yield ()

  private def writeTiplocRecords =
    usingTransactor(config.databaseConfig)() { db =>
      val tipLocTable = TipLocTable(db)
      scheduleDataReader
        .readData[TipLocRecord]
        .to(tipLocTable.dbWriter)
    }.run

  private def writeScheduleRecords =
    usingTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      scheduleDataReader
        .readData[ScheduleRecord]
        .to(scheduleTable.dbWriter)
    }.run

  private def deleteAllRecords =
    usingTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      fs2.Stream.eval(scheduleTable.deleteAllRecords)
    }.run

  app.unsafeRunSync()

}
