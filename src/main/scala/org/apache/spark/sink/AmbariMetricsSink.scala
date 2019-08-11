package org.apache.spark.sink

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Counter, Gauge, Histogram, Meter, MetricFilter, MetricRegistry, ScheduledReporter, Timer}
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

class AmbariMetricsSink(val property: Properties, val registry: MetricRegistry,
                        securityMgr: SecurityManager)
  extends Sink
  with Logging
  with DropwizardToAmbariAdapter
{

  protected class AmbariMetricsReporter(val registry: MetricRegistry) extends ScheduledReporter(
    registry,
    "spark-to-ambari-reporter",
    MetricFilter.ALL,
    TimeUnit.SECONDS,
    TimeUnit.MILLISECONDS
  ) {
    override def report(gauges: util.SortedMap[String, Gauge[_]], counters: util.SortedMap[String, Counter], histograms: util.SortedMap[String, Histogram], meters: util.SortedMap[String, Meter], timers: util.SortedMap[String, Timer]): Unit = {
      log.info(s"Attempting to report metrics to Ambari," +
        s" Counters: ${counters.size()}" +
        s" Gauges: ${gauges.size()}")
    }
  }

  val reporter = new AmbariMetricsReporter(registry)

  override def start(): Unit = {
    log.info("Started AmbariMetricsSink")
    log.info(s"Properties from Spark, ${property.toString}")
    reporter.start(15, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    log.info("Stopping AmbariMetricsSink")
    reporter.stop()
  }

  override def report(): Unit = {
    log.info("Emitting to metrics-collector-service")
    log.info(registry.getCounters.toString)
    reporter.report()
  }
}
