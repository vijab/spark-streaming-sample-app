package com.vijai.app.main

import com.vijai.app.schema.CDSRecord
import org.apache.spark.internal.Logging
import org.apache.spark.source.AmbariMetricsSource
import org.apache.spark.sql.ForeachWriter

class CdsRecordProcessor(implicit ambariMetricsSource: AmbariMetricsSource) extends ForeachWriter[CDSRecord] with Logging with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: CDSRecord): Unit = {
    val ts = System.currentTimeMillis()
    ambariMetricsSource.counter("EventPutSuccessCount").inc()
    log.info("Processing record")
    if ((ts % 10) == 0) {
      ambariMetricsSource.counter("ActiveThreads").inc()
    } else {
      ambariMetricsSource.counter("EventTakeSuccessCount").inc()
    }
  }

  override def close(errorOrNull: Throwable): Unit = log.error("Error occured processing data", errorOrNull)
}
