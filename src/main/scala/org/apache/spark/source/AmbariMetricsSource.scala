package org.apache.spark.source

import com.codahale.metrics.{Counter, MetricRegistry}
import org.apache.spark.internal.Logging

class AmbariMetricsSource extends org.apache.spark.metrics.source.Source
  with Serializable
  with Logging {
  override def sourceName: String = "AmbariMetricsSource"

  override def metricRegistry: MetricRegistry = new MetricRegistry

  def counter(name: String): Counter = {
    log.info(s"Incrementing counter, $name")
    metricRegistry.counter(name)
  }

}
