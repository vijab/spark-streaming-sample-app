package com.vijai.app.metrics

import com.vijai.app.schema.CDSRecord

object JsonSerializeTest extends App {

  import io.circe.generic.auto._, io.circe.syntax._

  // scoringId: Long, contractNumber: Long, personId: Long, scoringData: Long, riskRelevant: Boolean, expertRules: Array[Long]
  val cdsRecord = new CDSRecord(
    scoringId = 10L,
    contractNumber = 100L,
    personId = 200L,
    scoringData = 300L,
    riskRelevant = true,
    expertRules = Array(20L)
  )

  println(cdsRecord.asJson.noSpaces)

}
