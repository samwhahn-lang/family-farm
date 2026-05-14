package com.cornbelt.silver

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object CropTransformJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg = config.getConfig("paths")

  def main(args: Array[String]): Unit = {
    val spark     = SparkSessionProvider.session

    val bronzePath = s"${pathsCfg.getString("bronze")}/usda_crops"
    val silverPath = s"${pathsCfg.getString("silver")}/crop_records"

    logger.info(s"Reading bronze: $bronzePath")
    val raw = spark.read.format("delta").load(bronzePath)

    // USDA value strings are messy: "(D)" = suppressed by NASS, "(Z)" = rounds to zero,
    // thousands use commas ("1,234"), and leading/trailing whitespace is common.
    val parseValue = udf((s: String) =>
      Option(s)
        .map(_.trim)
        .filter(v => v.nonEmpty && v != "(D)" && v != "(Z)" && v != "(NA)")
        .flatMap(v => scala.util.Try(v.replace(",", "").toDouble).toOption)
    )

    val cleaned = raw
      .filter(
        col("state").isNotNull &&
        col("countyFips").isNotNull &&
        col("commodity").isNotNull &&
        col("statisticCat").isNotNull
      )
      .withColumn("parsedValue", parseValue(col("value")))
      .withColumn("year", col("year").cast("int"))

    // Pivot statisticCat long → wide. PRICE RECEIVED has a space so we rename immediately.
    val pivoted = cleaned
      .groupBy("year", "state", "countyFips", "commodity")
      .pivot("statisticCat", Seq("YIELD", "PRICE RECEIVED", "AREA HARVESTED"))
      .agg(first("parsedValue"))
      .withColumnRenamed("YIELD",          "yieldBuAcre")
      .withColumnRenamed("PRICE RECEIVED", "priceUsdBu")
      .withColumnRenamed("AREA HARVESTED", "areaHarvested")
      .withColumnRenamed("state",          "stateCode")
      .withColumnRenamed("commodity",      "cropName")

    if (pivoted.count() == 0) {
      logger.error("Zero crop records after transform — aborting. Has UsdaIngestJob run?")
      spark.stop()
      sys.exit(1)
    }

    logger.info(s"Silver crop records: ${pivoted.count()} rows")
    logger.info("Breakdown by crop:")
    pivoted.groupBy("cropName").count().orderBy("cropName").show(truncate = false)
    logger.info("Breakdown by year:")
    pivoted.groupBy("year").count().orderBy("year").show(truncate = false)

    // CropRecord.crop is a sealed trait — Spark has no auto-derived Encoder for it.
    // cropName is stored as String here; Gold converts to Crop via Crop.fromString.
    pivoted.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("stateCode", "year")
      .save(silverPath)

    logger.info(s"Silver crop write complete -> $silverPath")
    spark.stop()
  }
}
