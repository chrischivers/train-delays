package traindelays.networkrail.scripts

import cats.effect.IO
import org.http4s.client.blaze.PooledHttp1Client
import traindelays.ConfigLoader
import traindelays.networkrail.NetworkRailClient
import traindelays.networkrail.db.{ScheduleTable, _}
import traindelays.networkrail.scheduledata.{ScheduleDataReader, ScheduleRecord}

object PopulateScheduleTable extends App {

  val config = ConfigLoader.defaultConfig
  val client = PooledHttp1Client[IO]()

  val networkRailClient  = NetworkRailClient(config.networkRailConfig, client)
  val scheduleDataReader = ScheduleDataReader(config.networkRailConfig.scheduleData.tmpUnzipLocation)

  val app = for {
    _ <- networkRailClient.downloadScheduleData
    _ <- networkRailClient.unpackScheduleData
    _ <- usingTransactor(config.databaseConfig)() { db =>
      val scheduleTable = ScheduleTable(db)
      scheduleDataReader
        .readData[ScheduleRecord]
        .to(scheduleTable.dbWriter)
    }.run
  } yield ()

  app.unsafeRunSync()

}
