package com.vijai.app.main

import com.vijai.app.schema.CDSRecord
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.AmbariSource
import org.apache.spark.sql.ForeachWriter

class CdsRecordProcessor extends ForeachWriter[CDSRecord] with Logging with Serializable {
  override def open(partitionId: Long, version: Long): Boolean = true

  override def process(value: CDSRecord): Unit = {

    def ambariSource = {
      SparkEnv.get.metricsSystem.getSourcesByName("ambari")(0).asInstanceOf[AmbariSource]
    }

    val ts = System.currentTimeMillis()
    ambariSource.COUNTER_MESSAGES_PUSHED.inc()
    log.info("Processing record")
    if ((ts % 10) == 0) {
      ambariSource.COUNTER_MESSAGES_ERROR.inc()
    } else {
      ambariSource.COUNTER_MESSAGES_OUT.inc()
    }
  }

  override def close(errorOrNull: Throwable): Unit = log.error("Error occured processing data", errorOrNull)
}
