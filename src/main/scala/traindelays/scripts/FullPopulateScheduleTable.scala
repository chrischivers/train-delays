package traindelays.scripts

import cats.effect.IO
import traindelays.networkrail.NetworkRailClient

object FullPopulateScheduleTable extends PopulateScheduleTable {
  override protected def downloadScheduleData(networkRailClient: NetworkRailClient): fs2.Stream[IO, Unit] =
    networkRailClient.downloadFullScheduleData.handleErrorWith(err =>
      fs2.Stream.eval(IO(logger.error("Error downloading schedule data", err))))
}
