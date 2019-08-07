package com.vijai.app.metrics

import java.net.{InetAddress, URL}
import java.util
import java.util.Collections
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import org.apache.hadoop.metrics2.sink.timeline.{AbstractTimelineMetricsSink, TimelineMetric}
import org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache
import org.slf4j.LoggerFactory

class SparkAmbariMetricsSink(collectorUri: String, zkUrl: String, appId: String, instanceId: String, emitIntervalInMs: Long) extends AbstractTimelineMetricsSink {

  private[this] val logger = LoggerFactory.getLogger("SparkAmbariMetricsSink")

  private[this] lazy val collectorUrl: URL = new URL(collectorUri)

  private[this] lazy val host: InetAddress = InetAddress.getLocalHost

  private[this] lazy val timelineMetricsCache: TimelineMetricsCache = new TimelineMetricsCache(1000, 60 * 1000)

  private[this] val scheduledReporter = new Runnable with Serializable {
    override def run(): Unit = {
      timelineMetricsCache.synchronized{
        val metrics = timelineMetricsCache.getAllMetrics
        // TODO only emit when there is something to emit.
        logger.info(s"Emitting ${metrics.getMetrics.size()} metrics to ambari-metrics-collector")
        emitMetrics(metrics)
      }
    }
  }

  private[this] lazy val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

  scheduler.scheduleAtFixedRate(scheduledReporter, emitIntervalInMs, emitIntervalInMs, TimeUnit.MILLISECONDS)

  def addGauge(metricName: String, ts: Long, value: Double): Unit = {
    Option(timelineMetricsCache.getTimelineMetric(metricName)) match {
      case Some(timelineMetric) => {
        // TODO metadata?
        timelineMetric.getMetricValues.put(ts, value)
        logger.info(s"Found existing metric with $metricName")
      }
      case None => {
        logger.info(s"No metrics found with $metricName, Adding new")
        val timelineMetric: TimelineMetric = new TimelineMetric(metricName, getHostname, appId, instanceId)
        timelineMetric.setStartTime(ts)
        timelineMetric.setTimestamp(System.currentTimeMillis())
        timelineMetric.setType("GAUGE")
        timelineMetric.setUnits("SECONDS")
        (0 until 5) foreach {i =>
          timelineMetric.getMetricValues.put(ts + (i * 1000), value)
        }
        timelineMetricsCache.putTimelineMetric(timelineMetric)
      }
    }
  }

  override def getCollectorUri(s: String): String = collectorUri

  override def getCollectorProtocol: String = collectorUrl.getProtocol

  override def getCollectorPort: String = String.valueOf(collectorUrl.getPort)

  override def getTimeoutSeconds: Int = 60

  override def getZookeeperQuorum: String = zkUrl

  override def getConfiguredCollectorHosts: util.Collection[String] = Collections.emptyList()

  override def getHostname: String = host.getHostName

  override def isHostInMemoryAggregationEnabled: Boolean = true

  override def getHostInMemoryAggregationPort: Int = collectorUrl.getPort

  override def getHostInMemoryAggregationProtocol: String = getCollectorProtocol
}
