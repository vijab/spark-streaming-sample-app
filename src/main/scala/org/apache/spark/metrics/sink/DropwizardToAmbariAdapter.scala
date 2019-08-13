package org.apache.spark.metrics.sink

import java.util

import com.codahale.metrics.Counter
import com.vijai.app.metrics.{Metric, Metrics}

import scala.collection.JavaConverters._

trait DropwizardToAmbariAdapter {

  // TODO move this to a common place
  val ALLOWED_METRICS = Seq("AMBARI_METRICS.SmokeTest.FakeMetric","cds.messages.in","cds.messages.error","cds.messages.out")

  // TODO make appId, instanceId, hostname implicit
  def fromCounters(counters: util.SortedMap[String, Counter],
                   appId: String,
                   instanceId: String,
                   hostName: String): Metrics = {
    val ts = System.currentTimeMillis()
    val metricsAsArray: Array[Metric] = counters.asScala.map { case (k, v) => {
      val actualMetricName = getActualMetricName(k)
      if(ALLOWED_METRICS.contains(actualMetricName)) {
        Some(Metric(
          timestamp = ts,
          `type` = "COUNTER",
          units = "SECONDS",
          metricname = getActualMetricName(k),
          metadata = Map(),
          instanceid = instanceId,
          hostname = hostName,
          starttime = ts,
          metrics = Map(ts.toString -> v.getCount.doubleValue()),
          appid = appId
        ))
      } else Option.empty
    }
    }.flatten.toArray
    Metrics(metrics = metricsAsArray)
  }

  private def getActualMetricName(metricName: String) = {
    val tokens = metricName.split("\\.")
    tokens.slice(3, tokens.length).mkString(".")
  }

}
