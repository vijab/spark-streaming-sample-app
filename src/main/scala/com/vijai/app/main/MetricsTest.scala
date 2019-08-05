package com.vijai.app.main

import com.vijai.app.metrics.SparkAmbariMetricsSink
import org.slf4j.LoggerFactory

import scala.util.Random

object MetricsTest {

  val logger = LoggerFactory.getLogger("MetricsTest")

  def main(args: Array[String]): Unit = {
    logger.info("This is a test.")
    val sparkAmbariMetricsSink = new SparkAmbariMetricsSink(
      collectorUri = "http://sandbox-hdp.hortonworks.com:6188",
      zkUrl = "http://sandbox-hdp.hortonworks.com:2181",
      appId = "APP_TEST_1",
      instanceId = "INSTANCE_LOCAL_1"
    )

    val startTs = System.currentTimeMillis() - 10 * 60 * 60 * 1000

    (0 until 100) foreach {i =>
      //Thread.sleep(2000)
      sparkAmbariMetricsSink.addGauge("test.metric.gauge.one", startTs + (i * 60 * 1000), new Random().nextInt(100))
    }
    sparkAmbariMetricsSink.emit()
  }

}
