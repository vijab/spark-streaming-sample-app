package org.apache.spark.metrics.source

import com.codahale.metrics.{MetricRegistry}
import org.apache.spark.internal.Logging

class AmbariSource extends org.apache.spark.metrics.source.Source
  with Serializable
  with Logging {

  override val sourceName: String = "ambari"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  val COUNTER_MESSAGES_PUSHED = metricRegistry.counter(MetricRegistry.name("cds.messages.in"))

  val COUNTER_MESSAGES_OUT = metricRegistry.counter(MetricRegistry.name("cds.messages.out"))

  val COUNTER_MESSAGES_ERROR = metricRegistry.counter(MetricRegistry.name("cds.messages.error"))



}

