package com.cornbelt.bronze

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets

object BronzeQualityCheckJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg = config.getConfig("paths")

  private val DataTypes = Seq("TMAX", "TMIN", "PRCP", "SNOW", "SNWD")

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session
    val bronzePath = s"${pathsCfg.getString("bronze")}/noaa_observations"
    val outputFile = s"${pathsCfg.getString("bronze")}/row_count_by_year.csv"

    val df = spark.read.format("delta").load(bronzePath)

    // Count non-null values per stationId / year / dataType, then pivot wide.
    val counts = df
      .withColumn("year", substring(col("date"), 1, 4))
      .groupBy("stationId", "year")
      .pivot("dataType", DataTypes)
      .agg(count(when(col("value").isNotNull, 1)))
      .orderBy("stationId", "year")
      .collect()

    val header = (Seq("station_id", "year") ++ DataTypes).mkString(",")
    val writer = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(outputFile), StandardCharsets.UTF_8))
    writer.println(header)
    counts.foreach { row =>
      val stationId = row.getAs[String]("stationId")
      val year      = row.getAs[String]("year")
      val fields    = DataTypes.map { dt =>
        val v = row.getAs[Any](dt)
        if (v == null) "0" else v.toString
      }
      writer.println((Seq(stationId, year) ++ fields).mkString(","))
    }
    writer.close()

    logger.info(s"Wrote ${counts.length} station/year rows to $outputFile")
    counts.foreach { row =>
      val stationId = row.getAs[String]("stationId")
      val year      = row.getAs[String]("year")
      val fields    = DataTypes.map { dt =>
        val v = row.getAs[Any](dt)
        s"$dt=${if (v == null) 0 else v}"
      }.mkString("  ")
      logger.info(s"  $stationId  $year  $fields")
    }
  }
}
