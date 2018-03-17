package traindelays.scripts

import cats.effect.IO
import traindelays.networkrail.NetworkRailClient

object UpdatePopulateScheduleTable extends PopulateScheduleTable {
  override protected def downloadScheduleData(networkRailClient: NetworkRailClient): IO[Unit] =
    networkRailClient.downloadUpdateScheduleData
}
