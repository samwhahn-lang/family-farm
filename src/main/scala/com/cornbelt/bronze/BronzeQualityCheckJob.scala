package com.cornbelt.bronze

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object BronzeQualityCheckJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg = config.getConfig("paths")

  private val DataTypes = Seq("TMAX", "TMIN", "PRCP", "SNOW", "SNWD")

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session
    val bronzeBase = pathsCfg.getString("bronze")
    val bronzePath = s"$bronzeBase/noaa_observations"
    val outputFile = s"$bronzeBase/row_count_by_year.csv"

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

    // ── Validation checks ──────────────────────────────────────────────────────
    logger.info("Running bronze validation checks...")

    // Returns 366 for leap years, 365 otherwise.
    val isLeapYear = udf((y: Int) => if ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) 366 else 365)

    // 1. Coverage: distinct active dates per station/year vs calendar days expected.
    //    Uses all data types — a date counts as active if any observation exists for it.
    val coverage = df
      .withColumn("year", substring(col("date"), 1, 4).cast("int"))
      .groupBy("stationId", "year")
      .agg(countDistinct("date").alias("activeDays"))
      .withColumn("expectedDays", isLeapYear(col("year")))
      .withColumn("coveragePct", round(col("activeDays") * 100.0 / col("expectedDays"), 1))
      .withColumn("coverageStatus",
        when(col("activeDays") < col("expectedDays") * 0.5, lit("SPARSE"))
          .when(col("activeDays") < col("expectedDays") * 0.9, lit("INCOMPLETE"))
          .otherwise(lit("OK"))
      )

    // 2. TMAX < TMIN inversions: dates where the recorded high is below the recorded low.
    //    Pivot to wide so both values are on the same row for comparison.
    val tempInversions = df
      .filter(col("dataType").isin("TMAX", "TMIN") && col("value").isNotNull)
      .withColumn("year", substring(col("date"), 1, 4).cast("int"))
      .groupBy("stationId", "year", "date")
      .pivot("dataType", Seq("TMAX", "TMIN"))
      .agg(first("value"))
      .filter(col("TMAX").isNotNull && col("TMIN").isNotNull && col("TMAX") < col("TMIN"))
      .groupBy("stationId", "year")
      .agg(count("*").alias("inversions"))

    // 3. Extreme value outliers — physically implausible for Nebraska (raw metric units):
    //    TMAX > 55°C (~131°F) or < -45°C;  TMIN > 45°C or < -55°C;  PRCP > 250mm/day (~10 in)
    val extremeOutliers = df
      .filter(col("value").isNotNull)
      .withColumn("year", substring(col("date"), 1, 4).cast("int"))
      .filter(
        (col("dataType") === "TMAX" && (col("value") > 55.0  || col("value") < -45.0)) ||
        (col("dataType") === "TMIN" && (col("value") > 45.0  || col("value") < -55.0)) ||
        (col("dataType") === "PRCP" && col("value") > 250.0)
      )
      .groupBy("stationId", "year")
      .agg(count("*").alias("extremeVals"))

    // 4. Duplicate rows: same stationId / date / dataType appearing more than once.
    val dupeCount = df
      .groupBy("stationId", "date", "dataType")
      .agg(count("*").alias("n"))
      .filter(col("n") > 1)
      .count()

    if (dupeCount > 0)
      logger.warn(s"VALIDATION: $dupeCount duplicate stationId/date/dataType rows — investigate before running silver.")
    else
      logger.info("VALIDATION: No duplicate rows found in bronze.")

    // Join all checks into one report row per station/year.
    val report = coverage
      .join(tempInversions,  Seq("stationId", "year"), "left")
      .join(extremeOutliers, Seq("stationId", "year"), "left")
      .withColumn("inversions",  coalesce(col("inversions"),  lit(0L)))
      .withColumn("extremeVals", coalesce(col("extremeVals"), lit(0L)))
      .select("stationId", "year", "activeDays", "expectedDays", "coveragePct", "coverageStatus", "inversions", "extremeVals")
      .orderBy("stationId", "year")
      .collect()

    val issueCount = report.count { row =>
      row.getAs[String]("coverageStatus") != "OK" ||
      row.getAs[Long]("inversions")  > 0 ||
      row.getAs[Long]("extremeVals") > 0
    }
    if (issueCount > 0) logger.warn(s"VALIDATION: $issueCount station/year combinations have data quality issues.")
    else                logger.info("VALIDATION: All station/year combinations passed quality checks.")

    report.foreach { row =>
      val issues = Seq(
        if (row.getAs[String]("coverageStatus") != "OK")
          Some(s"coverage=${row.getAs[Double]("coveragePct")}% [${row.getAs[String]("coverageStatus")}]")
        else None,
        if (row.getAs[Long]("inversions") > 0)
          Some(s"${row.getAs[Long]("inversions")} TMAX<TMIN inversions")
        else None,
        if (row.getAs[Long]("extremeVals") > 0)
          Some(s"${row.getAs[Long]("extremeVals")} extreme outlier values")
        else None
      ).flatten
      if (issues.nonEmpty)
        logger.warn(s"  !! ${row.getAs[String]("stationId")} ${row.getAs[Int]("year")} — ${issues.mkString("; ")}")
    }

    Files.createDirectories(Paths.get(bronzeBase))
    val vFile = s"$bronzeBase/validation_report.csv"
    val vWriter = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(vFile), StandardCharsets.UTF_8))
    vWriter.println("station_id,year,active_days,expected_days,coverage_pct,coverage_status,inversions,extreme_vals")
    report.foreach { row =>
      vWriter.println(Seq(
        row.getAs[String]("stationId"),
        row.getAs[Int]("year").toString,
        row.getAs[Long]("activeDays").toString,
        row.getAs[Int]("expectedDays").toString,
        row.getAs[Double]("coveragePct").toString,
        row.getAs[String]("coverageStatus"),
        row.getAs[Long]("inversions").toString,
        row.getAs[Long]("extremeVals").toString
      ).mkString(","))
    }
    vWriter.close()
    logger.info(s"Bronze validation report (${report.length} station/year rows) -> $vFile")
  }
}
