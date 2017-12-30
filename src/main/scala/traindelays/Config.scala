package traindelays

import java.nio.file.{Path, Paths}

import com.typesafe.config.ConfigFactory
import org.http4s.Uri
import scala.collection.JavaConverters._

case class NetworkRailConfig(host: String,
                             port: Int,
                             username: String,
                             password: String,
                             maxRedirects: Int,
                             scheduleData: ScheduleDataConfig,
                             movements: MovementsConfig)

case class ScheduleDataConfig(downloadUrl: Uri, tmpDownloadLocation: Path, tmpUnzipLocation: Path)

case class MovementsConfig(topic: String)

case class TrainDelaysConfig(networkRailConfig: NetworkRailConfig, databaseConfig: DatabaseConfig)

case class DatabaseConfig(driverClassName: String,
                          url: String,
                          username: String,
                          password: String,
                          migrationScripts: List[String],
                          maximumPoolSize: Int = 2)

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
          Uri.unsafeFromString(defaultConfigFactory.getString("networkRail.scheduleData.uri")),
          Paths.get(defaultConfigFactory.getString("networkRail.scheduleData.tmpDownloadLocation")),
          Paths.get(defaultConfigFactory.getString("networkRail.scheduleData.tmpUnzipLocation"))
        ),
        MovementsConfig(
          defaultConfigFactory.getString("networkRail.movements.topic"),
        )
      ),
      DatabaseConfig(
        defaultConfigFactory.getString("db.driverClassName"),
        defaultConfigFactory.getString("db.url"),
        defaultConfigFactory.getString("db.username"),
        defaultConfigFactory.getString("db.password"),
        defaultConfigFactory.getStringList("db.migrationScripts").asScala.toList,
        defaultConfigFactory.getInt("db.maximumPoolSize")
      )
    )
  }
}
