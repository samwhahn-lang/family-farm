package com.cornbelt.utils

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkSessionProvider extends LazyLogging {

  private val config = ConfigFactory.load().getConfig("corn-belt.spark")

  lazy val session: SparkSession = {
    logger.info(s"Creating SparkSession")

    val spark = SparkSession.builder()
      .appName(config.getString("app-name"))
      .master(config.getString("master"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.datetime.java8API.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
