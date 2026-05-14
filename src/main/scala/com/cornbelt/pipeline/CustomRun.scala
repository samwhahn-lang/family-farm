package com.cornbelt.pipeline

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.scalalogging.LazyLogging

/**
 * Selectively run any subset of pipeline stages in dependency order.
 *
 * Usage:
 *   sbt "runMain com.cornbelt.pipeline.CustomRun silver"
 *   sbt "runMain com.cornbelt.pipeline.CustomRun silver gold"
 *   sbt "runMain com.cornbelt.pipeline.CustomRun bronze silver gold platinum"
 *
 * Stages always execute in pipeline order regardless of the order you list them.
 * Dependency note: gold requires silver output; silver requires bronze output.
 */
object CustomRun extends LazyLogging {

  private case class Stage(name: String, label: String, job: () => Unit)

  private def stages: Seq[Stage] = Seq(
    Stage("bronze",        "Bronze   — Ingest Backfill (2000-2026, overwrite)", () => com.cornbelt.bronze.IngestBackfill.main(Array.empty)),
    Stage("bronze-update", "Bronze   — Ingest Update (max date to today, append)", () => com.cornbelt.bronze.IngestUpdate.main(Array.empty)),
    Stage("silver",        "Silver   — Weather Transform",                      () => com.cornbelt.silver.WeatherTransformJob.main(Array.empty)),
    Stage("gold",          "Gold     — Season Weather",                         () => com.cornbelt.gold.SeasonWeatherJob.main(Array.empty)),
    Stage("platinum",      "Platinum — Export",                                 () => com.cornbelt.platinum.PlatinumExportJob.main(Array.empty))
  )

  def main(args: Array[String]): Unit = {
    val requested = args.map(_.toLowerCase.trim).toSet
    val valid     = stages.map(_.name).toSet

    if (requested.isEmpty) {
      printUsage()
      sys.exit(0)
    }

    val unknown = requested -- valid
    if (unknown.nonEmpty) {
      logger.error(s"Unknown stage(s): ${unknown.mkString(", ")}")
      printUsage()
      sys.exit(1)
    }

    val toRun = stages.filter(s => requested.contains(s.name))
    logger.info(s"==== CustomRun: ${toRun.map(_.name).mkString(" -> ")} ====")

    toRun.foreach { s =>
      logger.info(s"==== Starting: ${s.label} ====")
      val t0 = System.currentTimeMillis()
      s.job()
      val elapsed = (System.currentTimeMillis() - t0) / 1000
      logger.info(s"==== Finished: ${s.label} (${elapsed}s) ====")
    }

    SparkSessionProvider.session.stop()
    logger.info("==== CustomRun complete ====")
  }

  private def printUsage(): Unit = {
    logger.info("Usage:  CustomRun <stage> [stage ...]")
    logger.info("Stages: bronze  bronze-update  silver  gold  platinum")
    logger.info("")
    logger.info("  bronze        Full backfill 2000-2026, overwrites bronze table")
    logger.info("  bronze-update Incremental fetch from max bronze date to today, appends")
    logger.info("")
    logger.info("Examples:")
    logger.info("  sbt \"runMain com.cornbelt.pipeline.CustomRun bronze-update silver gold platinum\"")
    logger.info("  sbt \"runMain com.cornbelt.pipeline.CustomRun silver gold platinum\"")
    logger.info("  sbt \"runMain com.cornbelt.pipeline.CustomRun bronze silver gold platinum\"")
    logger.info("")
    logger.info("Stages always run in pipeline order: bronze -> bronze-update -> silver -> gold -> platinum")
  }
}
