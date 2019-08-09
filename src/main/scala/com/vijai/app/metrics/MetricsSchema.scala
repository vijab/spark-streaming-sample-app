package com.vijai.app.metrics

case class Metric(
                   timestamp: Long,
                   `type`: String,
                   units: String,
                   metadata: Map[String, String],
                   metricname: String,
                   appid: String,
                   instanceid: String,
                   hostname: String,
                   starttime: Long,
                   metrics: Map[String, Double]
                 )

case class Metrics(metrics: Array[Metric])


