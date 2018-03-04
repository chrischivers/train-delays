package traindelays.scripts

import cats.effect.IO
import traindelays.networkrail.NetworkRailClient

object FullPopulateScheduleTable extends PopulateScheduleTable {
  override protected def downloadScheduleData(networkRailClient: NetworkRailClient) =
    networkRailClient.downloadFullScheduleData.flatMap(_ => IO(logger.info("Downloaded full schedule data")))
}
