package org.apache.spark.sink

import java.util.Properties

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink

private[spark] class AmbariMetricsSink(val property: Properties, val registry: MetricRegistry,
                        securityMgr: SecurityManager) extends Sink with Logging {

  override def start(): Unit = {
    log.info("Started AmbariMetricsSink")
  }

  override def stop(): Unit = {
    log.info("Stopping AmbariMetricsSink")
  }

  override def report(): Unit = {
    log.info("Emitting to metrics-collector-service")
    log.info(registry.getCounters.toString)
  }
}
