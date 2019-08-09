package com.vijai.app.schema

case class Customer(name: String, age: Int, amount: Double, createdOn: Long)

case class CustomerList(customers: Seq[Customer], location: String, createdOn: Long)

case class CustomerListTs(customers: Seq[Customer], location: String, createdOn: Long)

case class CDSRecord(scoringId: Long, contractNumber: Long, personId: Long, scoringData: Long, riskRelevant: Boolean, expertRules: Array[Long])