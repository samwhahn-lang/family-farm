package com.cornbelt.pipeline

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging

object RunAll extends LazyLogging {

  def main(args: Array[String]): Unit = {
    run("Bronze — NOAA Ingest (Backfill)", () => com.cornbelt.bronze.IngestBackfill.main(args))
    run("Silver — Weather Transform", () => com.cornbelt.silver.WeatherTransformJob.main(args))
    run("Gold — Season Weather",      () => com.cornbelt.gold.SeasonWeatherJob.main(args))
    run("Platinum — Export",          () => com.cornbelt.platinum.PlatinumExportJob.main(args))
    SparkSessionProvider.session.stop()
    logger.info("Pipeline complete.")
  }

  private def run(name: String, job: () => Unit): Unit = {
    logger.info(s"==== Starting: $name ====")
    val t0 = System.currentTimeMillis()
    job()
    val elapsed = (System.currentTimeMillis() - t0) / 1000
    logger.info(s"==== Finished: $name (${elapsed}s) ====")
  }
}
