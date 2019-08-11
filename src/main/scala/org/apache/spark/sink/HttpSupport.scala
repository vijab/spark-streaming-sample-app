package org.apache.spark.sink

import java.io.{BufferedReader, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.util.stream.Collectors

import com.vijai.app.metrics.Metrics
import io.circe.generic.auto._
import io.circe.syntax._

import scala.util.control.NonFatal

trait HttpSupport {

  val collectorUri: String

  lazy val collectorUrl = new URL(collectorUri)

  def postMetrics(metrics: Metrics): String = {
    val con: HttpURLConnection = collectorUrl.openConnection().asInstanceOf[HttpURLConnection]
    val jsonData = metrics.asJson.noSpaces.getBytes("utf-8")
    con.setRequestMethod("POST")
    con.setRequestProperty("Content-Type", "application/json; utf-8")
    con.setRequestProperty("Accept", "application/json")
    con.setDoOutput(true)
    withResources(con.getOutputStream)(os => {
      os.write(jsonData, 0, jsonData.length)
    })
    withResources(new BufferedReader(new InputStreamReader(con.getInputStream, "utf-8")))(br => {
      br.lines().collect(Collectors.joining())
    })
  }

  private def withResources[T <: AutoCloseable, V](r: => T)(f: T => V): V = {

    def closeAndAddSuppressed(e: Throwable,
                              resource: AutoCloseable): Unit = {
      if (e != null) {
        try {
          resource.close()
        } catch {
          case NonFatal(suppressed) =>
            e.addSuppressed(suppressed)
        }
      } else {
        resource.close()
      }
    }

    val resource: T = r
    require(resource != null, "resource is null")
    var exception: Throwable = null
    try {
      f(resource)
    } catch {
      case NonFatal(e) =>
        exception = e
        throw e
    } finally {
      closeAndAddSuppressed(exception, resource)
    }
  }

}
