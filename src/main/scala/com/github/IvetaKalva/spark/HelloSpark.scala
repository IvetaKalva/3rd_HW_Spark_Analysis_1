package com.github.IvetaKalva.spark

import org.apache.spark.sql.SparkSession

object HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")


}
