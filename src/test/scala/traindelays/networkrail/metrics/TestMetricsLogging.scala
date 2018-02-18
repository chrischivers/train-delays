package traindelays.networkrail.metrics

import nl.grons.metrics.scala.Meter
import traindelays.MetricsConfig
import traindelays.metrics.MetricsLogging

import scala.util.Random

trait TestMetricsLogging extends MetricsLogging {
  override protected val activationRecordsReceivedMeter: Meter =
    metrics.meter(s"activation-records-received-$randomStringGenerator")
  override protected val movementRecordsReceivedMeter: Meter =
    metrics.meter(s"movement-records-received-$randomStringGenerator")
  override protected val cancellationRecordsReceivedMeter: Meter =
    metrics.meter(s"cancellation-records-received-$randomStringGenerator")
  override protected val unhandledRecordsReceivedMeter: Meter =
    metrics.meter(s"unhandled-records-received-$randomStringGenerator")
  override protected val emailsSent: Meter = metrics.meter(s"emails-sent-$randomStringGenerator")

  def getActivationRecordsCount   = activationRecordsReceivedMeter.count
  def getMovementRecordsCount     = movementRecordsReceivedMeter.count
  def getCancellationRecordsCount = cancellationRecordsReceivedMeter.count
  def getUnhandledRecordsCount    = unhandledRecordsReceivedMeter.count
  def getEmailsSentCount          = emailsSent.count

  private def randomStringGenerator = s"${Random.nextInt(99999)}-${System.currentTimeMillis()}"
}

object TestMetricsLogging {
  def apply(config: MetricsConfig) = new TestMetricsLogging {

    override val metricsConfig: MetricsConfig = config
  }
}
