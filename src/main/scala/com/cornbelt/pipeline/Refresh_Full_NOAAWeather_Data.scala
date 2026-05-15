package com.cornbelt.pipeline

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging

/** Refreshes NOAA weather data from bronze through platinum.
 *  Appends new NOAA observations since the last run (incremental), then
 *  re-derives every downstream layer so the dashboard reflects current data.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.pipeline.Refresh_Full_NOAAWeather_Data"
 */
object Refresh_Full_NOAAWeather_Data extends LazyLogging {

  def main(args: Array[String]): Unit = {
    run("Bronze — NOAA Incremental Update",  () => com.cornbelt.bronze.NoaaIncrementalUpdateJob.main(args))
    run("Silver — Weather Transform",        () => com.cornbelt.silver.WeatherTransformJob.main(args))
    run("Gold   — Season Weather",           () => com.cornbelt.gold.SeasonWeatherJob.main(args))
    run("Platinum — Export",                 () => com.cornbelt.platinum.PlatinumExportJob.main(args))
    SparkSessionProvider.session.stop()
    logger.info("Refresh complete — NOAA weather data is current through today.")
  }

  private def run(name: String, job: () => Unit): Unit = {
    logger.info(s"==== Starting: $name ====")
    val t0 = System.currentTimeMillis()
    try {
      job()
    } catch {
      case e: Exception =>
        val elapsed = (System.currentTimeMillis() - t0) / 1000
        logger.error(s"==== FAILED: $name after ${elapsed}s — ${e.getClass.getSimpleName}: ${e.getMessage} ====")
        throw e
    }
    val elapsed = (System.currentTimeMillis() - t0) / 1000
    logger.info(s"==== Finished: $name (${elapsed}s) ====")
  }
}
