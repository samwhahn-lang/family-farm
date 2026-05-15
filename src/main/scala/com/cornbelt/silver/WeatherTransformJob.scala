package com.cornbelt.silver

import com.cornbelt.models.WeatherObservation
import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions._
import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object WeatherTransformJob extends LazyLogging {

  private val config    = ConfigFactory.load().getConfig("corn-belt")
  private val anchor    = config.getConfig("anchor")
  private val pathsCfg  = config.getConfig("paths")

  def main(args: Array[String]): Unit = {
    val spark     = SparkSessionProvider.session
    import spark.implicits._

    val stationId        = anchor.getString("station-id")
    val stationName      = anchor.getString("station-name")
    val supplementalId   = anchor.getString("temp-supplemental-id")
    val supplementalName = anchor.getString("temp-supplemental-name")
    val bronzePath       = s"${pathsCfg.getString("bronze")}/noaa_observations"
    val silverPath       = s"${pathsCfg.getString("silver")}/weather_observations"

    // Load full bronze pool — needed for pool-median fallback on TMAX/TMIN.
    val allBronze     = spark.read.format("delta").load(bronzePath)
    val anchorRaw     = allBronze.filter(col("stationId") === stationId)
    val suppPresent   = allBronze.filter(col("stationId") === supplementalId).limit(1).count() > 0
    val supplementalRaw = if (suppPresent) allBronze.filter(col("stationId") === supplementalId)
                          else { logger.warn(s"Supplemental station $supplementalId not in bronze — run AbridgedIngestAddStation first"); spark.emptyDataFrame }

    logger.info(s"Supplemental station $supplementalId present in bronze: $suppPresent")

    logger.info(s"Reading bronze for anchor station $stationId: $bronzePath")

    // NOAA attributes format: measurement_flag,quality_flag,source_flag[,time_obs]
    // A non-blank character at index 1 means the quality check failed for that reading.
    val withSuspect = anchorRaw.withColumn("suspect",
      when(
        col("attributes").isNotNull &&
          size(split(col("attributes"), ",")) > 1 &&
          trim(split(col("attributes"), ",")(1)) =!= "",
        lit(true)
      ).otherwise(lit(false))
    )

    // Aggregate quality across all data types for each anchor station/date.
    // "MISSING" = no readings at all; "SUSPECT" = any quality flag raised; else "OK".
    val qualityAgg = withSuspect
      .groupBy("stationId", "date")
      .agg(
        max(col("suspect")).alias("anySuspect"),
        count(when(col("value").isNotNull, 1)).alias("presentCount")
      )

    // Pivot anchor long → wide. API was called with units=metric so values are in
    // °C (TMAX/TMIN) and mm (PRCP/SNOW/SNWD). Conversion to °F / inches applied in select.
    val anchorPivoted = anchorRaw
      .groupBy("stationId", "date")
      .pivot("dataType", Seq("TMAX", "TMIN", "PRCP", "SNOW", "SNWD"))
      .agg(first("value"))

    // Supplemental station pivot — TMAX and TMIN only. Used as the second fallback
    // before the pool median so a single well-maintained ASOS station fills gaps
    // with a directly attributable source rather than a blended estimate.
    val suppPivoted = if (suppPresent)
      supplementalRaw
        .groupBy("date")
        .pivot("dataType", Seq("TMAX", "TMIN"))
        .agg(first("value"))
        .withColumnRenamed("TMAX", "suppTMAX")
        .withColumnRenamed("TMIN", "suppTMIN")
    else
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[org.apache.spark.sql.Row],
        org.apache.spark.sql.types.StructType(Seq(
          org.apache.spark.sql.types.StructField("date",     org.apache.spark.sql.types.StringType),
          org.apache.spark.sql.types.StructField("suppTMAX", org.apache.spark.sql.types.DoubleType),
          org.apache.spark.sql.types.StructField("suppTMIN", org.apache.spark.sql.types.DoubleType)
        ))
      )

    // Pool median fallback: median of all non-null TMAX/TMIN values across every
    // station in the bronze pool, grouped by date. percentile_approx ignores nulls,
    // so stations that lack temperature data on a given date don't skew the result.
    // When no station has data for a date, the median is null (propagated downstream).
    val poolMedians = allBronze
      .groupBy("date")
      .agg(
        percentile_approx(
          when(col("dataType") === "TMAX", col("value")), lit(0.5), lit(10000)
        ).alias("poolTMAX"),
        percentile_approx(
          when(col("dataType") === "TMIN", col("value")), lit(0.5), lit(10000)
        ).alias("poolTMIN")
      )

    logger.info("Pool medians computed for TMAX/TMIN across all bronze stations")

    // Three-level join: anchor → supplemental → pool median.
    // Priority: anchor station wins, then supplemental ASOS, then pool median.
    val withPool = anchorPivoted
      .join(suppPivoted,  Seq("date"), "left")
      .join(poolMedians,  Seq("date"), "left")

    val ds: Dataset[WeatherObservation] = withPool
      .join(qualityAgg, Seq("stationId", "date"))
      .withColumn("qualityFlag",
        when(col("presentCount") === 0, lit("MISSING"))
          .when(col("anySuspect"),       lit("SUSPECT"))
          .otherwise(                    lit("OK"))
      )
      .withColumn("tempSource",
        when(col("TMAX").isNotNull    && col("TMIN").isNotNull,        lit("anchor"))
          .when(col("poolTMAX").isNotNull && col("poolTMIN").isNotNull, lit("pool"))
          .when(col("suppTMAX").isNotNull && col("suppTMIN").isNotNull, lit("supplemental"))
          .otherwise(                                                    lit("mixed"))
      )
      .withColumn("tempStationId",
        when(col("TMAX").isNotNull,      lit(stationId))
          .when(col("poolTMAX").isNotNull, lit("pool_median"))
          .when(col("suppTMAX").isNotNull, lit(supplementalId))
          .otherwise(lit(null).cast("string"))
      )
      .withColumn("tempStationName",
        when(col("TMAX").isNotNull,      lit(stationName))
          .when(col("poolTMAX").isNotNull, lit("Pool Median"))
          .when(col("suppTMAX").isNotNull, lit(supplementalName))
          .otherwise(lit(null).cast("string"))
      )
      .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
      // ── Temporal classification (date is now DateType) ────────────────────
      .withColumn("month", month(col("date")))
      .withColumn("season",
        when(
          ((month(col("date")) === 3)  && (dayofmonth(col("date")) >= 20)) ||
           month(col("date")).between(4, 5) ||
          ((month(col("date")) === 6)  && (dayofmonth(col("date")) <= 20)), lit("Spring"))
        .when(
          ((month(col("date")) === 6)  && (dayofmonth(col("date")) >= 21)) ||
           month(col("date")).between(7, 8) ||
          ((month(col("date")) === 9)  && (dayofmonth(col("date")) <= 22)), lit("Summer"))
        .when(
          ((month(col("date")) === 9)  && (dayofmonth(col("date")) >= 23)) ||
           month(col("date")).between(10, 11) ||
          ((month(col("date")) === 12) && (dayofmonth(col("date")) <= 20)), lit("Autumn"))
        .otherwise(lit("Winter"))
      )
      // ── Crop phase flags ─────────────────────────────────────────────────
      // Corn: planting late-Apr – mid-May | growing May–Sep | harvest Sep–Oct
      .withColumn("cornPlanting",
        ((month(col("date")) === 4) && (dayofmonth(col("date")) >= 20)) ||
        ((month(col("date")) === 5) && (dayofmonth(col("date")) <= 15))
      )
      .withColumn("cornGrowing",     month(col("date")).between(5, 9))
      .withColumn("cornHarvest",     (month(col("date")) === 9) || (month(col("date")) === 10))
      // Soybeans: planting early-May – early-Jun | growing May–Oct | harvest late-Sep – Oct
      .withColumn("soybeanPlanting",
        (month(col("date")) === 5) ||
        ((month(col("date")) === 6) && (dayofmonth(col("date")) <= 10))
      )
      .withColumn("soybeanGrowing",  month(col("date")).between(5, 10))
      .withColumn("soybeanHarvest",
        ((month(col("date")) === 9) && (dayofmonth(col("date")) >= 20)) ||
        (month(col("date")) === 10)
      )
      // Winter wheat: planting late-Sep – Oct | growing Sep 20–Jun 30 (overwinters) | harvest Jun – early-Jul
      .withColumn("wheatPlanting",
        ((month(col("date")) === 9) && (dayofmonth(col("date")) >= 20)) ||
        (month(col("date")) === 10)
      )
      .withColumn("wheatGrowing",
        ((month(col("date")) === 9) && (dayofmonth(col("date")) >= 20)) ||
        (month(col("date")) >= 10) ||
        (month(col("date")) <= 6)
      )
      .withColumn("wheatHarvest",
        (month(col("date")) === 6) ||
        ((month(col("date")) === 7) && (dayofmonth(col("date")) <= 10))
      )
      .select(
        col("stationId"),
        col("date"),
        (coalesce(col("TMAX"), col("poolTMAX"), col("suppTMAX")) * 9.0 / 5.0 + 32.0).alias("tempMaxF"),
        (coalesce(col("TMIN"), col("poolTMIN"), col("suppTMIN")) * 9.0 / 5.0 + 32.0).alias("tempMinF"),
        ((coalesce(col("TMAX"), col("poolTMAX"), col("suppTMAX")) +
          coalesce(col("TMIN"), col("poolTMIN"), col("suppTMIN"))) / 2.0 * 9.0 / 5.0 + 32.0).alias("tempAvgF"),
        (col("PRCP") / 25.4).alias("precipIn"),
        (col("SNOW") / 25.4).alias("snowIn"),
        (col("SNWD") / 25.4).alias("snowDepthIn"),
        col("qualityFlag"),
        col("tempSource"),
        col("tempStationId"),
        col("tempStationName"),
        col("month"),
        col("season"),
        col("cornPlanting"),
        col("cornGrowing"),
        col("cornHarvest"),
        col("soybeanPlanting"),
        col("soybeanGrowing"),
        col("soybeanHarvest"),
        col("wheatPlanting"),
        col("wheatGrowing"),
        col("wheatHarvest")
      )
      .as[WeatherObservation]

    logger.info(s"Silver row count: ${ds.count()}")
    logger.info(s"Writing silver: $silverPath")

    ds.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", "true")
      .partitionBy("stationId")
      .save(silverPath)

    logger.info(s"Silver write complete -> $silverPath")

    // Write top 100 rows (all columns) as a preview CSV for quick inspection.
    val previewPath = s"${pathsCfg.getString("silver")}/preview_top100.csv"
    val previewRows = spark.read.format("delta").load(silverPath)
      .orderBy("date")
      .limit(100)
      .collect()

    if (previewRows.nonEmpty) {
      val previewWriter = new PrintWriter(new OutputStreamWriter(
        new FileOutputStream(previewPath), StandardCharsets.UTF_8))
      previewWriter.println(previewRows.head.schema.fieldNames.mkString(","))
      previewRows.foreach { r =>
        previewWriter.println(r.schema.fieldNames.map(f => Option(r.getAs[Any](f)).getOrElse("")).mkString(","))
      }
      previewWriter.close()
      logger.info(s"Silver preview (top 100 rows) -> $previewPath")
    }

    val csvPath = s"${pathsCfg.getString("silver")}/row_count_by_year.csv"
    val counts = spark.read.format("delta").load(silverPath)
      .withColumn("year", year(col("date")))
      .groupBy("year")
      .count()
      .orderBy("year")
      .collect()

    Files.createDirectories(Paths.get(pathsCfg.getString("silver")))
    val writer = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(csvPath), StandardCharsets.UTF_8))
    writer.println("year,row_count")
    counts.foreach(r => writer.println(s"${r.getAs[Int]("year")},${r.getAs[Long]("count")}"))
    writer.close()

    logger.info(s"Silver row counts by year -> $csvPath")
    counts.foreach(r => logger.info(s"  ${r.getAs[Int]("year")}: ${r.getAs[Long]("count")} rows"))
  }
}
