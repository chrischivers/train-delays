package traindelays.scripts

import cats.effect.IO
import org.http4s.Uri
import traindelays.networkrail.NetworkRailClient

object FullPopulateScheduleTable extends PopulateScheduleTable {
  override protected def downloadScheduleData(networkRailClient: NetworkRailClient, uri: Uri): IO[Unit] =
    networkRailClient.downloadFullScheduleData(uri)
}
