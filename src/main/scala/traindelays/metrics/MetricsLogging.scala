package traindelays.metrics

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging
import metrics_influxdb.{HttpInfluxdbProtocol, InfluxdbReporter}
import nl.grons.metrics.scala._
import traindelays.MetricsConfig

trait MetricsLogging extends StrictLogging with DefaultInstrumented {

  override lazy val metricBaseName: MetricName = MetricName("")

  val metricsConfig: MetricsConfig

  def setUpReporter =
    if (metricsConfig.enabled) {
      logger.info("Setting up metrics reporter")
      InfluxdbReporter
        .forRegistry(metricRegistry)
        .protocol(new HttpInfluxdbProtocol(metricsConfig.host, metricsConfig.port, metricsConfig.dbName))
        .tag("hostname", InetAddress.getLocalHost.getHostName)
        .convertRatesTo(TimeUnit.MINUTES)
        .build()
        .start(metricsConfig.updateInterval, TimeUnit.SECONDS)
    }

  private val activationRecordsReceivedMeter: Meter = metrics.meter("activation-records-received")
  def incrActivationRecordsReceived                 = if (metricsConfig.enabled) activationRecordsReceivedMeter.mark()

  private val movementRecordsReceivedMeter: Meter = metrics.meter("movement-records-received")
  def incrMovementRecordsReceived                 = if (metricsConfig.enabled) movementRecordsReceivedMeter.mark()

  private val cancellationRecordsReceivedMeter: Meter = metrics.meter("cancellation-records-received")
  def incrCancellationRecordsReceived                 = if (metricsConfig.enabled) cancellationRecordsReceivedMeter.mark()

  private val unhandledRecordsReceivedMeter: Meter = metrics.meter("unhandled-records-received")
  def incrUnhandledRecordsReceived                 = if (metricsConfig.enabled) unhandledRecordsReceivedMeter.mark()

  private val emailsSent: Meter = metrics.meter("emails-sent")
  def incrEmailsSent            = if (metricsConfig.enabled) emailsSent.mark()

}

object MetricsLogging {
  def apply(config: MetricsConfig) = new MetricsLogging {
    override val metricsConfig: MetricsConfig = config
    setUpReporter
  }
}
