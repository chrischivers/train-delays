package traindelays

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import org.http4s.Uri

import scala.concurrent.duration.FiniteDuration

case class NetworkRailConfig(host: String,
                             port: Int,
                             username: String,
                             password: String,
                             maxRedirects: Int,
                             scheduleData: ScheduleDataConfig,
                             movements: MovementsConfig,
                             subscribersConfig: SubscribersConfig)

case class ScheduleDataConfig(fullDownloadUris: List[Uri],
                              updateDownloadUris: List[Uri],
                              tmpDownloadLocation: Path,
                              tmpUnzipLocation: Path,
                              memoizeFor: FiniteDuration)

case class MovementsConfig(topics: List[String], activationExpiry: FiniteDuration)

case class SubscribersConfig(memoizeFor: FiniteDuration, lateNotifyAfter: FiniteDuration)

case class EmailerConfig(enabled: Boolean,
                         fromAddress: String,
                         smtpHost: String,
                         smtpPort: Int,
                         smtpUsername: String,
                         smtpPassword: String,
                         numberAttempts: Int,
                         secondsBetweenAttempts: Int)

case class HttpConfig(port: Int)

case class TrainDelaysConfig(networkRailConfig: NetworkRailConfig,
                             databaseConfig: DatabaseConfig,
                             redisConfig: RedisConfig,
                             emailerConfig: EmailerConfig,
                             httpConfig: HttpConfig,
                             uIConfig: UIConfig,
                             metricsConfig: MetricsConfig)

case class DatabaseConfig(driverClassName: String,
                          url: String,
                          username: String,
                          password: String,
                          maximumPoolSize: Int = 2)

case class MetricsConfig(host: String, port: Int, dbName: String, updateInterval: Int, enabled: Boolean)

case class UIConfig(minimumDaysScheduleDuration: Int, memoizeRouteListFor: FiniteDuration, clientId: String)

case class RedisConfig(host: String, port: Int, dbIndex: Int)

object TrainDelaysConfig {

  import scala.collection.JavaConverters._

  val defaultConfig = apply()

  def apply(config: Config = ConfigFactory.load()): TrainDelaysConfig =
    TrainDelaysConfig(
      NetworkRailConfig(
        config.getString("networkRail.host"),
        config.getInt("networkRail.port"),
        config.getString("networkRail.username"),
        config.getString("networkRail.password"),
        config.getInt("networkRail.maxRedirects"),
        ScheduleDataConfig(
          config.getStringList("networkRail.scheduleData.fullUris").asScala.toList.map(Uri.unsafeFromString),
          config.getStringList("networkRail.scheduleData.updateUris").asScala.toList.map(Uri.unsafeFromString),
          Paths.get(config.getString("networkRail.scheduleData.tmpDownloadLocation")),
          Paths.get(config.getString("networkRail.scheduleData.tmpUnzipLocation")),
          FiniteDuration(config.getDuration("networkRail.scheduleData.memoizeFor").toMillis, TimeUnit.MILLISECONDS)
        ),
        MovementsConfig(
          config.getStringList("networkRail.movements.topics").asScala.toList,
          FiniteDuration(config.getDuration("networkRail.movements.activationExpiry").toMillis, TimeUnit.MILLISECONDS)
        ),
        SubscribersConfig(
          FiniteDuration(config.getDuration("networkRail.subscribers.memoizeFor").toMillis, TimeUnit.MILLISECONDS),
          FiniteDuration(config.getDuration("networkRail.subscribers.lateNotifyAfter").toMillis, TimeUnit.MILLISECONDS)
        )
      ),
      DatabaseConfig(
        config.getString("db.driverClassName"),
        config.getString("db.url"),
        config.getString("db.username"),
        config.getString("db.password"),
        config.getInt("db.maximumPoolSize")
      ),
      RedisConfig(
        config.getString("redis.host"),
        config.getInt("redis.port"),
        config.getInt("redis.dbIndex")
      ),
      EmailerConfig(
        config.getBoolean("email.enabled"),
        config.getString("email.fromAddress"),
        config.getString("email.smtpHost"),
        config.getInt("email.smtpPort"),
        config.getString("email.smtpUsername"),
        config.getString("email.smtpPassword"),
        config.getInt("email.numberAttempts"),
        config.getInt("email.secondsBetweenAttempts")
      ),
      HttpConfig(
        config.getInt("http-server.port")
      ),
      UIConfig(
        config.getInt("ui.minimumDaysScheduleDuration"),
        FiniteDuration(config.getDuration("ui.memoizeRouteListFor").toMillis, TimeUnit.MILLISECONDS),
        config.getString("ui.clientId")
      ),
      MetricsConfig(
        config.getString("metrics.host"),
        config.getInt("metrics.port"),
        config.getString("metrics.dbName"),
        config.getInt("metrics.updateInterval"),
        config.getBoolean("metrics.enabled")
      )
    )

}
