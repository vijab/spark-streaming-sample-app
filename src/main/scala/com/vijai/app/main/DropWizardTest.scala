package com.vijai.app.main

import java.net.InetAddress

import com.codahale.metrics.{Metric, MetricFilter, MetricRegistry}
import com.vijai.app.metrics.Metrics

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.metrics.sink.{DropwizardToAmbariAdapter, HttpSupport}

object DropWizardTest
  extends App
    with DropwizardToAmbariAdapter
    with HttpSupport {

  val metricRegistry = new MetricRegistry

  val metricsFilter = new MetricFilter {
    override def matches(name: String, metric: Metric): Boolean = name.startsWith("AMBARI_METRICS")
  }

  implicit val appId: String = "journalnode"

  implicit val instanceId: String = "test-instance"

  implicit lazy val hostName: String = InetAddress.getLocalHost.getHostName

  val counterName1: String = "AMBARI_METRICS.SmokeTest.FakeMetric1"
  val counterName2: String = "AMBARI_METRICS.SmokeTest.FakeMetric2"
  val counterName3: String = "OTHER_METRICS_1.SmokeTest.FakeMetric"

  override val collectorUri = "http://www.google.de"

  (0 until 10) foreach{i =>
    metricRegistry.counter(counterName1).inc()
    metricRegistry.counter(counterName2).inc()
    metricRegistry.counter(counterName3).inc()
  }

  private val metrics: Metrics = fromCounters(
    metricRegistry.getCounters(metricsFilter),
    appId,
    instanceId,
    hostName
  )
  println(metrics.asJson)

  postMetrics(metrics)

}
