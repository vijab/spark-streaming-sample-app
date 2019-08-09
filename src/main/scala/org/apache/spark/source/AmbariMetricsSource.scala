package org.apache.spark.source

import com.codahale.metrics.{Counter, MetricRegistry}
import org.apache.spark.metrics.source.Source

class AmbariMetricsSource extends Source {
  override def sourceName: String = "AmbariMetricsSource"

  override def metricRegistry: MetricRegistry = new MetricRegistry

  def counter(name: String): Counter = {
    metricRegistry.counter(name)
  }

}
