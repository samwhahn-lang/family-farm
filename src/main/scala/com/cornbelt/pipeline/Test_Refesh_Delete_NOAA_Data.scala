package com.cornbelt.pipeline

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.col

/** Test pipeline — trims the bronze NOAA observations table to rows on or before
 *  2026-05-01 (no API call), then re-derives every downstream layer so the full
 *  bronze-to-platinum flow can be validated against a known historical snapshot.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.pipeline.Test_Refesh_Delete_NOAA_Data"
 */
object Test_Refesh_Delete_NOAA_Data extends LazyLogging {

  private val config     = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg   = config.getConfig("paths")
  private val CutoffDate = "2026-05-01"

  def main(args: Array[String]): Unit = {
    run("Bronze — Delete rows past 2026-05-01", () => trimBronze())
    run("Silver — Weather Transform",           () => com.cornbelt.silver.WeatherTransformJob.main(args))
    run("Gold   — Season Weather",              () => com.cornbelt.gold.SeasonWeatherJob.main(args))
    run("Platinum — Export",                    () => com.cornbelt.platinum.PlatinumExportJob.main(args))
    SparkSessionProvider.session.stop()
    logger.info(s"Test refresh complete — bronze trimmed to on or before $CutoffDate, dashboard rebuilt.")
  }

  private def trimBronze(): Unit = {
    val spark      = SparkSessionProvider.session
    val bronzePath = s"${pathsCfg.getString("bronze")}/noaa_observations"

    val before = spark.read.format("delta").load(bronzePath).count()
    DeltaTable.forPath(spark, bronzePath).delete(col("date") > CutoffDate)
    val after  = spark.read.format("delta").load(bronzePath).count()

    logger.info(s"Bronze trim: $before → $after rows (removed ${before - after} rows past $CutoffDate)")
  }

  private def run(name: String, job: () => Unit): Unit = {
    logger.info(s"==== Starting: $name ====")
    val t0 = System.currentTimeMillis()
    job()
    val elapsed = (System.currentTimeMillis() - t0) / 1000
    logger.info(s"==== Finished: $name (${elapsed}s) ====")
  }
}
