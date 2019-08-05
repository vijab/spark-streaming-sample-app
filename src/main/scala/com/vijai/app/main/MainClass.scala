package com.vijai.app.main

import com.vijai.app.schema.CustomerList
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.TimestampType

object MainClass extends Logging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SampleStreamingApp")
      .getOrCreate()

    import spark.implicits._

    val customerListSchema = Encoders.product[CustomerList].schema

    val input: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "sandbox-hdp.hortonworks.com:6667")
      .option("subscribe", "customer-lists-2")
      .load()

    val customerList: Dataset[CustomerList] = input
      .select($"value" cast "string" as "json")
      .select(from_json($"json", customerListSchema) as "data")
      .select("data.*").as[CustomerList]

    val waterMarked = customerList
      .withColumn("createdOn", ($"createdOn" / 1000).cast(TimestampType))
      .withWatermark(eventTime = "createdOn", delayThreshold = "2 seconds")
      .groupBy(window($"createdOn", "5 seconds"), $"location")
      .agg(first("location"))


    val stream: StreamingQuery = waterMarked
      .writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .start()

    log.info("Started streaming records.")


    stream.awaitTermination()
  }

}
