package org.apache.spark.sink

import java.util

import com.codahale.metrics.Counter
import com.vijai.app.metrics.{Metric, Metrics}

import scala.collection.JavaConverters._

trait DropwizardToAmbariAdapter {

  // TODO make appId, instanceId, hostname implicit
  def fromCounters(counters: util.SortedMap[String, Counter],
                   appId: String,
                   instanceId: String,
                   hostName: String): Metrics = {
    val ts = System.currentTimeMillis()
    val metricsAsArray: Array[Metric] = counters.asScala.map { case (k, v) => {
      Metric(
        timestamp = ts,
        `type` = "COUNTER",
        units = "SECONDS",
        metricname = k,
        metadata = Map(),
        instanceid = instanceId,
        hostname = hostName,
        starttime = ts,
        metrics = Map(ts.toString -> v.getCount.doubleValue()),
        appid = appId
      )
    }
    }.toArray
    Metrics(metrics = metricsAsArray)
  }

}
