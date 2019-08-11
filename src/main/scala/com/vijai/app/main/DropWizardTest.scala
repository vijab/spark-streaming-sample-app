package com.vijai.app.main

import java.net.InetAddress

import com.codahale.metrics.MetricRegistry
import com.vijai.app.metrics.Metrics
import org.apache.spark.sink.{DropwizardToAmbariAdapter, HttpSupport}
import io.circe.generic.auto._
import io.circe.syntax._

object DropWizardTest
  extends App
    with DropwizardToAmbariAdapter
    with HttpSupport {

  val metricRegistry = new MetricRegistry

  implicit val appId: String = "journalnode"

  implicit val instanceId: String = "test-instance"

  implicit lazy val hostName: String = InetAddress.getLocalHost.getHostName

  val counterName: String = "EventPutSuccessCount"

  override val collectorUri = "http://www.google.de"

  (0 until 10) foreach{i =>
    metricRegistry.counter(counterName).inc()
  }

  private val metrics: Metrics = fromCounters(
    metricRegistry.getCounters,
    appId,
    instanceId,
    hostName
  )
  println(metrics.asJson)

  postMetrics(metrics)

}
