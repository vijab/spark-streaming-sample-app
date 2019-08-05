package com.vijai.app.metrics

import java.net.{InetAddress, URL}
import java.util
import java.util.Collections

import org.apache.hadoop.metrics2.sink.timeline.{AbstractTimelineMetricsSink, TimelineMetric}
import org.apache.hadoop.metrics2.sink.timeline.cache.TimelineMetricsCache
import org.slf4j.LoggerFactory

import scala.util.Random

class SparkAmbariMetricsSink(collectorUri: String, zkUrl: String, appId: String, instanceId: String) extends AbstractTimelineMetricsSink {

  val logger = LoggerFactory.getLogger("SparkAmbariMetricsSink")

  lazy val collectorUrl: URL = new URL(collectorUri)

  lazy val host: InetAddress = InetAddress.getLocalHost

  lazy val timelineMetricsCache: TimelineMetricsCache = new TimelineMetricsCache(1000, 60 * 1000)

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
        (0 until 10) foreach {i =>
          timelineMetric.getMetricValues.put(ts + (i * 1000), value)
        }
        timelineMetricsCache.putTimelineMetric(timelineMetric)
      }
    }
  }

  def emit() = {
    val metrics = timelineMetricsCache.getAllMetrics
    emitMetrics(metrics)
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
