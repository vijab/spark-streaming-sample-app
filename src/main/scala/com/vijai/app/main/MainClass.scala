package com.vijai.app.main

import com.vijai.app.schema.CDSRecord
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.AmbariSource
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object MainClass extends Logging {

/*  lazy val metricsSink: MetricsSink = new MetricsSink(emitIntervalInMs = (15 seconds).toMillis) {
    override val collectorUri: String = "http://sandbox-hdp.hortonworks.com:6188"
    override val zkUrl: String = "http://sandbox-hdp.hortonworks.com:2181"
    override val appId: String = "journalnode"
    override val instanceId: String = "localhost"
  }*/

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SampleStreamingApp")
      .config("spark.streaming.stopGracefullyOnShutdown","true")
      .config("spark.metrics.conf.driver.sink.ambari.class", "org.apache.spark.metrics.sink.AmbariSink")
      .config("spark.metrics.conf.executor.sink.ambari.class", "org.apache.spark.metrics.sink.AmbariSink")
      .config("spark.metrics.conf.worker.sink.ambari.class", "org.apache.spark.metrics.sink.AmbariSink")
      .config("spark.metrics.conf.driver.source.ambari.class", "org.apache.spark.metrics.source.AmbariSource")
      .config("spark.metrics.conf.executor.source.ambari.class", "org.apache.spark.metrics.source.AmbariSource")
      .config("spark.metrics.conf.worker.source.ambari.class", "org.apache.spark.metrics.source.AmbariSource")
      .getOrCreate()

    val ambariSource = new AmbariSource

    SparkEnv.get.metricsSystem.registerSource(ambariSource)

    val cdsRecordProcessor = new CdsRecordProcessor

    import spark.implicits._

    val cdsSchema = Encoders.product[CDSRecord].schema

    val input: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "cds-messages-1")
      .load()

    val stream = input
      .select($"value" cast "string" as "json")
      .select(from_json($"json", cdsSchema) as "data")
      .select("data.*").as[CDSRecord]
      .writeStream
      .foreach(cdsRecordProcessor)
      .start()

    log.info("Started streaming records.")

    SparkEnv.get.metricsSystem.getSourcesByName("ambari").foreach{s =>
      log.info(s"Source name, ${s.sourceName}")
    }

    stream.awaitTermination()
  }

}
