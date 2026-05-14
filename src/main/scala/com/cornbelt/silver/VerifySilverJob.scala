package com.cornbelt.silver

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import java.io.{File, PrintWriter}

object VerifySilverJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg = config.getConfig("paths")

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session
    val silverBase = pathsCfg.getString("silver")
    val outputFile = s"$silverBase/silver_sample_data.csv"

    val writer = new PrintWriter(new File(outputFile))

    def sampleTable(name: String, path: String): Unit = {
      val fullPath = s"$silverBase/$path"
      if (new File(fullPath).exists()) {
        logger.info(s"Sampling $name from $fullPath")
        val df   = spark.read.format("delta").load(fullPath)
        val rows = df.limit(20).collect()
        val cols = df.columns
        writer.println(s"# $name  (${df.count()} total rows)")
        writer.println(cols.mkString(","))
        rows.foreach { row =>
          writer.println(cols.indices.map(i => Option(row(i)).getOrElse("")).mkString(","))
        }
        writer.println()
      } else {
        logger.warn(s"$name not found at $fullPath — skipping (job not yet run?)")
        writer.println(s"# $name  — NOT YET GENERATED")
        writer.println()
      }
    }

    sampleTable("weather_observations", "weather_observations")

    writer.close()
    logger.info(s"Wrote silver sample -> $outputFile")
    spark.stop()
  }
}
