package traindelays.scripts

import cats.effect.IO
import traindelays.networkrail.NetworkRailClient

object UpdatePopulateScheduleTable extends PopulateScheduleTable {
  override protected def downloadScheduleData(networkRailClient: NetworkRailClient) =
    networkRailClient.downloadFullScheduleData.flatMap(_ => IO(logger.info("Downloaded updated schedule data")))
}
