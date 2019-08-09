package com.vijai.app.main

import com.vijai.app.metrics.{Counter, Gauge, MetricsSink}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.util.Random

object MetricsTest {

  val logger = LoggerFactory.getLogger("MetricsTest")

  def main(args: Array[String]): Unit = {

    val metricsSink: MetricsSink = new MetricsSink(emitIntervalInMs = (15 seconds).toMillis) {
      override val collectorUri: String = "http://sandbox-hdp.hortonworks.com:6188"
      override val zkUrl: String = "http://sandbox-hdp.hortonworks.com:2181"
      override val appId: String = "journalnode"
      override val instanceId: String = "localhost"
    }

    logger.info("Starting publishing of metrics.")

    (0 until 500) foreach {i =>
      Thread.sleep(2000)
      metricsSink.pushMetric(Counter,"AMBARI_METRICS.SmokeTest.FakeMetric", System.currentTimeMillis(), Math.abs(new Random().nextInt(100)))
      metricsSink.pushMetric(Gauge,"ActiveThreads", System.currentTimeMillis(), Math.abs(new Random().nextInt(100)))
    }

    metricsSink.shutdown()

  }

}
