package com.cornbelt.bronze

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.delta.tables.DeltaTable

/** Ad-hoc test utility — deletes all bronze rows whose date falls in a given
 *  year-month (YYYY-MM), then logs the before/after row counts so you can
 *  verify the incremental refresh picks them back up on the next run.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.bronze.DeleteMonthJob 2026-05"
 *    sbt "runMain com.cornbelt.bronze.DeleteMonthJob 2026-04"
 */
object DeleteMonthJob extends LazyLogging {

  private val config    = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg  = config.getConfig("paths")

  def main(args: Array[String]): Unit = {
    val yearMonth = args.headOption.getOrElse {
      throw new IllegalArgumentException("Usage: DeleteMonthJob <YYYY-MM>  e.g. 2026-05")
    }

    require(yearMonth.matches("""\d{4}-\d{2}"""),
      s"Argument must be in YYYY-MM format, got: $yearMonth")

    val spark      = SparkSessionProvider.session
    val outputPath = s"${pathsCfg.getString("bronze")}/noaa_observations"

    val deletedCount = spark.read.format("delta").load(outputPath)
      .filter(s"date like '$yearMonth%'")
      .count()

    logger.info(s"Bronze rows matching '$yearMonth': $deletedCount — deleting...")

    DeltaTable.forPath(spark, outputPath)
      .delete(s"date like '$yearMonth%'")

    val remaining = spark.read.format("delta").load(outputPath).count()
    logger.info(s"Delete complete. Rows deleted: $deletedCount. Rows remaining: $remaining.")
    logger.info(s"Re-run Refresh_Full_NOAAWeather_Data to pull $yearMonth back from the NOAA API.")
  }
}
