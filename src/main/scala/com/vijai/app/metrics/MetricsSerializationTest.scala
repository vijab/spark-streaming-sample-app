package com.vijai.app.metrics

import scala.util.Random

object MetricsSerializationTest extends App {

  import io.circe.generic.auto._, io.circe.syntax._

  val currentTime = System.currentTimeMillis()

  val metricsKeys = (1 until 100) map (i => (currentTime + (i * 1000)).toString)

  val metricsValues = (1 until 100) map (_ => Math.abs(new Random().nextInt(1000)).doubleValue())

  val metric: Metric = Metric(
    timestamp = currentTime,
    `type` =  "GAUGE",
    units = "SECONDS",
    metadata = Map(),
    metricname = "test.metric.two",
    appid = "APP_ID_1",
    instanceid = "INSTANCE_LOCAL_2",
    hostname = "HOSTNAME_LOCAL",
    starttime = currentTime,
    metrics = metricsKeys zip metricsValues toMap
  )

  val metrics: Metrics = Metrics(
    metrics = Array(metric)
  )

  println(metrics.asJson)

}
