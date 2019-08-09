package com.vijai.app.main

import com.vijai.app.metrics.{Gauge, MetricsSink}
import com.vijai.app.schema.CDSRecord
import org.apache.spark.internal.Logging
import org.apache.spark.sql.ForeachWriter

class CdsRecordProcessor(metricsSink: MetricsSink) extends ForeachWriter[CDSRecord] with Logging with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: CDSRecord): Unit = {
    val ts = System.currentTimeMillis()
    metricsSink.pushMetric(Gauge, "EventPutSuccessCount", ts, 1)
    log.info("Processing record")
    if ((ts % 10) == 0) {
      metricsSink.pushMetric(Gauge, "ActiveThreads", System.currentTimeMillis(), 1)
    } else {
      metricsSink.pushMetric(Gauge, "EventTakeSuccessCount", System.currentTimeMillis(), 1)
    }
  }

  override def close(errorOrNull: Throwable): Unit = log.error("Error occured processing data", errorOrNull)
}
