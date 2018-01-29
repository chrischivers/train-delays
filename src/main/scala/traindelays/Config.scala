package traindelays

import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
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

case class ScheduleDataConfig(downloadUrl: Uri,
                              tmpDownloadLocation: Path,
                              tmpUnzipLocation: Path,
                              memoizeFor: FiniteDuration)

case class MovementsConfig(topic: String, activationExpiry: FiniteDuration)

case class SubscribersConfig(memoizeFor: FiniteDuration)

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
                             uIConfig: UIConfig)

case class DatabaseConfig(driverClassName: String,
                          url: String,
                          username: String,
                          password: String,
                          maximumPoolSize: Int = 2)

case class UIConfig(minimumDaysScheduleDuration: Int)

case class RedisConfig(host: String, port: Int, dbIndex: Int)

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
          Paths.get(defaultConfigFactory.getString("networkRail.scheduleData.tmpUnzipLocation")),
          FiniteDuration(defaultConfigFactory.getDuration("networkRail.scheduleData.memoizeFor").toMillis,
                         TimeUnit.MILLISECONDS)
        ),
        MovementsConfig(
          defaultConfigFactory.getString("networkRail.movements.topic"),
          FiniteDuration(defaultConfigFactory.getDuration("networkRail.movements.activationExpiry").toMillis,
                         TimeUnit.MILLISECONDS)
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
        defaultConfigFactory.getInt("db.maximumPoolSize")
      ),
      RedisConfig(
        defaultConfigFactory.getString("redis.host"),
        defaultConfigFactory.getInt("redis.port"),
        defaultConfigFactory.getInt("redis.dbIndex")
      ),
      EmailerConfig(
        defaultConfigFactory.getBoolean("email.enabled"),
        defaultConfigFactory.getString("email.fromAddress"),
        defaultConfigFactory.getString("email.smtpHost"),
        defaultConfigFactory.getInt("email.smtpPort"),
        defaultConfigFactory.getString("email.smtpUsername"),
        defaultConfigFactory.getString("email.smtpPassword"),
        defaultConfigFactory.getInt("email.numberAttempts"),
        defaultConfigFactory.getInt("email.secondsBetweenAttempts")
      ),
      HttpConfig(
        defaultConfigFactory.getInt("http-server.port")
      ),
      UIConfig(
        defaultConfigFactory.getInt("ui.minimumDaysScheduleDuration")
      )
    )
  }
}
