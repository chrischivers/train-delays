package traindelays

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import com.sun.xml.internal.xsom.impl.WildcardImpl.Finite
import com.typesafe.config.ConfigFactory
import org.http4s.Uri

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class NetworkRailConfig(host: String,
                             port: Int,
                             username: String,
                             password: String,
                             maxRedirects: Int,
                             scheduleData: ScheduleDataConfig,
                             movements: MovementsConfig,
                             subscribersConfig: SubscribersConfig)

case class ScheduleDataConfig(downloadUrl: Uri, tmpDownloadLocation: Path, tmpUnzipLocation: Path)

case class MovementsConfig(topic: String)

case class SubscribersConfig(memoizeFor: FiniteDuration)

case class EmailerConfig(fromAddress: String,
                         smtpHost: String,
                         smtpPort: Int,
                         smtpUsername: String,
                         smtpPassword: String,
                         numberAttempts: Int,
                         secondsBetweenAttempts: Int)

case class TrainDelaysConfig(networkRailConfig: NetworkRailConfig,
                             databaseConfig: DatabaseConfig,
                             emailerConfig: EmailerConfig)

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
          defaultConfigFactory.getString("networkRail.movements.topic")
        ),
        SubscribersConfig(
          FiniteDuration(defaultConfigFactory.getDuration("networkRail.subscribers.memoizeFor").toMillis,
                         TimeUnit.MILLISECONDS)
        )
      ),
      DatabaseConfig(
        defaultConfigFactory.getString("db.driverClassName"),
        defaultConfigFactory.getString("db.url"),
        defaultConfigFactory.getString("db.username"),
        defaultConfigFactory.getString("db.password"),
        defaultConfigFactory.getStringList("db.migrationScripts").asScala.toList,
        defaultConfigFactory.getInt("db.maximumPoolSize")
      ),
      EmailerConfig(
        defaultConfigFactory.getString("email.fromAddress"),
        defaultConfigFactory.getString("email.smtpHost"),
        defaultConfigFactory.getInt("email.smtpPort"),
        defaultConfigFactory.getString("email.smtpUsername"),
        defaultConfigFactory.getString("email.smtpPassword"),
        defaultConfigFactory.getInt("email.numberAttempts"),
        defaultConfigFactory.getInt("email.secondsBetweenAttempts")
      )
    )
  }
}
