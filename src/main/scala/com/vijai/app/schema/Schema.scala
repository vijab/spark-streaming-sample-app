package com.vijai.app.schema

import org.apache.spark.sql.types.TimestampType

case class Customer(name: String, age: Int, amount: Double, createdOn: Long)

case class CustomerList(customers: Seq[Customer], location: String, createdOn: Long)

case class CustomerListTs(customers: Seq[Customer], location: String, createdOn: Long)