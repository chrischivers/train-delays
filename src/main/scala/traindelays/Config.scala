package traindelays

import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import org.http4s.Uri

case class NetworkRailConfig(host: String,
                             port: Int,
                             username: String,
                             password: String,
                             maxRedirects: Int,
                             scheduleDataConf: ScheduleDataConfig)
case class ScheduleDataConfig(downloadUrl: Uri, tmpDownloadLocation: Path, tmpUnzipLocation: Path)

case class TrainDelaysConfig(networkRailConfig: NetworkRailConfig)

object ConfigLoader {

  private val defaultConfigFactory = ConfigFactory.load()

  val defaultConfig: TrainDelaysConfig = {

    TrainDelaysConfig(
      NetworkRailConfig(
        defaultConfigFactory.getString("networkRail.host"),
        defaultConfigFactory.getInt("networkRail.port"),
        defaultConfigFactory.getString("networkRail.username"),
        defaultConfigFactory.getString("networkRail.password"),
        defaultConfigFactory.getInt("networkRail.maxRedirects"),
        ScheduleDataConfig(
          Uri.unsafeFromString(defaultConfigFactory.getString("networkRail.scheduleDataConfig.uri")),
          Paths.get(defaultConfigFactory.getString("networkRail.scheduleDataConfig.tmpDownloadLocation")),
          Paths.get(defaultConfigFactory.getString("networkRail.scheduleDataConfig.tmpUnzipLocation"))
        )
      )
    )
  }
}
