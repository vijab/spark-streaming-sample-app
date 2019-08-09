package com.vijai.app.metrics

import java.net.{InetAddress, URL}
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.hadoop.metrics2.sink.timeline.{AbstractTimelineMetricsSink, TimelineMetric}
import org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache
import org.slf4j.LoggerFactory

abstract class MetricsSink(emitIntervalInMs: Long) extends AbstractTimelineMetricsSink with Serializable {

  val collectorUri: String

  val zkUrl: String

  val appId: String

  val instanceId: String

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[this] lazy val collectorUrl: URL = new URL(collectorUri)

  private[this] lazy val timelineMetricsCache: TimelineMetricsCache = new TimelineMetricsCache(1000, 60 * 1000)

  private[this] lazy val scheduledReporter = new Runnable with Serializable {
    override def run(): Unit = {
      timelineMetricsCache.synchronized{
        val metrics = timelineMetricsCache.getAllMetrics
        // TODO only emit when there is something to emit.
        logger.info(s"Emitting ${metrics.getMetrics.size()} metrics to ambari-metrics-collector")
        emitMetrics(metrics, true)
      }
    }
  }

  private[this] lazy val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  scheduler.scheduleAtFixedRate(scheduledReporter, emitIntervalInMs, emitIntervalInMs, TimeUnit.MILLISECONDS)

  def pushMetric(metricType: MetricType, metricName: String, ts: Long, value: Double): Unit = {
    val metric: TimelineMetric = Option(timelineMetricsCache.getTimelineMetric(metricName)).getOrElse {
      val timelineMetric: TimelineMetric = new TimelineMetric()
      timelineMetric.setMetricName(metricName)
      timelineMetric.setStartTime(ts)
      timelineMetric.setTimestamp(ts)
      timelineMetric.getMetricValues.put(ts, value)
      timelineMetric.setType(metricType.toString)
      timelineMetric.setAppId(appId)
      timelineMetric.setHostName(getHostname)
      timelineMetric.setInstanceId(instanceId)
      timelineMetric
    }

    metricType match {
      case Counter => timelineMetricsCache.putTimelineMetric(metric, true)
      case Gauge => timelineMetricsCache.putTimelineMetric(metric, false)
    }
  }

  def shutdown() = scheduler.shutdown()

  override def getCollectorUri(s: String): String = collectorUri

  override def getCollectorProtocol: String = collectorUrl.getProtocol

  override def getCollectorPort: String = String.valueOf(collectorUrl.getPort)

  override def getTimeoutSeconds: Int = 60

  override def getZookeeperQuorum: String = zkUrl

  override def getConfiguredCollectorHosts: util.Collection[String] = Collections.emptyList()

  override def getHostname: String = InetAddress.getLocalHost.getHostName

  override def isHostInMemoryAggregationEnabled: Boolean = true

  override def getHostInMemoryAggregationPort: Int = collectorUrl.getPort

  override def getHostInMemoryAggregationProtocol: String = getCollectorProtocol
}

sealed trait MetricType

case object Counter extends MetricType {
  override def toString: String = "COUNTER"
}

case object Gauge extends MetricType {
  override def toString: String = "GAUGE"
}
