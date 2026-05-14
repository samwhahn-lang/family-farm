package com.cornbelt.gold

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object SeasonWeatherJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val anchor   = config.getConfig("anchor")
  private val pathsCfg = config.getConfig("paths")

  // April–October covers corn and soy; winter wheat establishment extends to November.
  // Month 11 is included so a wheat-aware caller can filter further if needed.
  private val GrowingSeasonStart = 4
  private val GrowingSeasonEnd   = 11

  // GDD base 50°F — corn standard. Daily max capped at 86°F (plateau threshold).
  private val GddBase   = 50.0
  private val GddMaxCap = 86.0

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session

    val stationId  = anchor.getString("station-id")
    val silverPath = s"${pathsCfg.getString("silver")}/weather_observations"
    val goldPath   = s"${pathsCfg.getString("gold")}/season_weather"
    val countyFips = anchor.getString("county-fips")
    val stateCode  = anchor.getString("state")

    logger.info(s"Reading silver weather for station $stationId: $silverPath")
    val weather = spark.read.format("delta").load(silverPath)
      .filter(col("stationId") === stationId)

    // Diagnostic: show per-year temperature availability before any filtering.
    // If a year shows 0 temp days, the station stopped reporting TMAX/TMIN that year.
    logger.info("=== Silver temperature coverage by year (pre-filter) ===")
    weather
      .withColumn("yr", year(col("date")))
      .groupBy("yr")
      .agg(
        count("*").alias("total_days"),
        sum(when(col("tempMaxF").isNotNull && col("tempMinF").isNotNull, 1).otherwise(0)).alias("temp_days")
      )
      .orderBy("yr")
      .collect()
      .foreach { r =>
        val yr    = r.getAs[Int]("yr")
        val total = r.getAs[Long]("total_days")
        val temp  = r.getAs[Long]("temp_days")
        val pct   = if (total > 0) f"${temp * 100.0 / total}%.0f%%" else "n/a"
        val warn  = if (temp == 0) " *** NO TEMPERATURE DATA ***" else ""
        logger.info(f"  $yr: $temp%3d/$total%3d days have TMAX+TMIN ($pct)$warn")
      }
    logger.info("=== end temperature coverage ===")

    // Exclude suspect readings and rows where both temperatures are absent.
    // Month restriction for the general season is applied per-metric below so that
    // crop-specific flags (which span different month ranges, including wheat's
    // cross-year window) can be aggregated in the same groupBy.
    val qualified = weather
      .filter(col("qualityFlag") =!= "SUSPECT")
      .filter(col("tempMaxF").isNotNull && col("tempMinF").isNotNull)

    // GDD per day: average of capped-max and floored-min, minus base, floored at 0.
    // Formula: max(0, (min(TMAX, 30) + max(TMIN, 10)) / 2 - 10)
    val withGdd = qualified.withColumn("gdd",
      greatest(
        lit(0.0),
        (least(col("tempMaxF"), lit(GddMaxCap)) +
         greatest(col("tempMinF"), lit(GddBase))) / 2.0 - lit(GddBase)
      )
    )

    // One groupBy produces all aggregates:
    //   - General growing season (Apr–Nov) for the original metrics
    //   - Per-crop aggregates using the boolean phase flags carried from silver
    val annual = withGdd
      .withColumn("yr", year(col("date")))
      .groupBy("yr")
      .agg(
        round(sum(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd), col("gdd")).otherwise(0.0)), 1)
          .alias("growingDegreeDays"),
        round(sum(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd), coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2)
          .alias("totalPrecipIn"),
        sum(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd) && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int")
          .alias("frostFreeDays"),
        sum(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd) && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int")
          .alias("extremeHeatDays"),
        // Crop growing seasons — GDD, precip, frost-free, heat stress
        round(sum(when(col("cornGrowing"),    col("gdd")).otherwise(0.0)), 1).alias("cornGrowingGdd"),
        round(sum(when(col("cornGrowing"),    coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornGrowingPrecipIn"),
        sum(when(col("cornGrowing")    && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("cornGrowingFrostFreeDays"),
        sum(when(col("cornGrowing")    && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("cornGrowingHeatStressDays"),
        round(sum(when(col("soybeanGrowing"), col("gdd")).otherwise(0.0)), 1).alias("soybeanGrowingGdd"),
        round(sum(when(col("soybeanGrowing"), coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanGrowingPrecipIn"),
        sum(when(col("soybeanGrowing") && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("soybeanGrowingFrostFreeDays"),
        sum(when(col("soybeanGrowing") && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("soybeanGrowingHeatStressDays"),
        round(sum(when(col("wheatGrowing"),   col("gdd")).otherwise(0.0)), 1).alias("wheatGrowingGdd"),
        round(sum(when(col("wheatGrowing"),   coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatGrowingPrecipIn"),
        sum(when(col("wheatGrowing")   && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("wheatGrowingFrostFreeDays"),
        sum(when(col("wheatGrowing")   && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("wheatGrowingHeatStressDays"),
        // Crop planting seasons
        round(sum(when(col("cornPlanting"),    col("gdd")).otherwise(0.0)), 1).alias("cornPlantingGdd"),
        round(sum(when(col("cornPlanting"),    coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornPlantingPrecipIn"),
        sum(when(col("cornPlanting")    && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("cornPlantingFrostFreeDays"),
        sum(when(col("cornPlanting")    && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("cornPlantingHeatStressDays"),
        round(sum(when(col("soybeanPlanting"), col("gdd")).otherwise(0.0)), 1).alias("soybeanPlantingGdd"),
        round(sum(when(col("soybeanPlanting"), coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanPlantingPrecipIn"),
        sum(when(col("soybeanPlanting") && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("soybeanPlantingFrostFreeDays"),
        sum(when(col("soybeanPlanting") && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("soybeanPlantingHeatStressDays"),
        round(sum(when(col("wheatPlanting"),   col("gdd")).otherwise(0.0)), 1).alias("wheatPlantingGdd"),
        round(sum(when(col("wheatPlanting"),   coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatPlantingPrecipIn"),
        sum(when(col("wheatPlanting")   && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("wheatPlantingFrostFreeDays"),
        sum(when(col("wheatPlanting")   && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("wheatPlantingHeatStressDays"),
        // Crop harvest seasons
        round(sum(when(col("cornHarvest"),    col("gdd")).otherwise(0.0)), 1).alias("cornHarvestGdd"),
        round(sum(when(col("cornHarvest"),    coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornHarvestPrecipIn"),
        sum(when(col("cornHarvest")    && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("cornHarvestFrostFreeDays"),
        sum(when(col("cornHarvest")    && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("cornHarvestHeatStressDays"),
        round(sum(when(col("soybeanHarvest"), col("gdd")).otherwise(0.0)), 1).alias("soybeanHarvestGdd"),
        round(sum(when(col("soybeanHarvest"), coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanHarvestPrecipIn"),
        sum(when(col("soybeanHarvest") && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("soybeanHarvestFrostFreeDays"),
        sum(when(col("soybeanHarvest") && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("soybeanHarvestHeatStressDays"),
        round(sum(when(col("wheatHarvest"),   col("gdd")).otherwise(0.0)), 1).alias("wheatHarvestGdd"),
        round(sum(when(col("wheatHarvest"),   coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatHarvestPrecipIn"),
        sum(when(col("wheatHarvest")   && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("wheatHarvestFrostFreeDays"),
        sum(when(col("wheatHarvest")   && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("wheatHarvestHeatStressDays"),
        // Calendar month aggregates
        round(sum(when(month(col("date")) === 1,  col("gdd")).otherwise(0.0)), 1).alias("janGdd"),
        round(sum(when(month(col("date")) === 1,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("janPrecipIn"),
        sum(when(month(col("date")) === 1  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("janFrostFreeDays"),
        sum(when(month(col("date")) === 1  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("janHeatStressDays"),
        round(sum(when(month(col("date")) === 2,  col("gdd")).otherwise(0.0)), 1).alias("febGdd"),
        round(sum(when(month(col("date")) === 2,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("febPrecipIn"),
        sum(when(month(col("date")) === 2  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("febFrostFreeDays"),
        sum(when(month(col("date")) === 2  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("febHeatStressDays"),
        round(sum(when(month(col("date")) === 3,  col("gdd")).otherwise(0.0)), 1).alias("marGdd"),
        round(sum(when(month(col("date")) === 3,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("marPrecipIn"),
        sum(when(month(col("date")) === 3  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("marFrostFreeDays"),
        sum(when(month(col("date")) === 3  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("marHeatStressDays"),
        round(sum(when(month(col("date")) === 4,  col("gdd")).otherwise(0.0)), 1).alias("aprGdd"),
        round(sum(when(month(col("date")) === 4,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("aprPrecipIn"),
        sum(when(month(col("date")) === 4  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("aprFrostFreeDays"),
        sum(when(month(col("date")) === 4  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("aprHeatStressDays"),
        round(sum(when(month(col("date")) === 5,  col("gdd")).otherwise(0.0)), 1).alias("mayGdd"),
        round(sum(when(month(col("date")) === 5,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("mayPrecipIn"),
        sum(when(month(col("date")) === 5  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("mayFrostFreeDays"),
        sum(when(month(col("date")) === 5  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("mayHeatStressDays"),
        round(sum(when(month(col("date")) === 6,  col("gdd")).otherwise(0.0)), 1).alias("junGdd"),
        round(sum(when(month(col("date")) === 6,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("junPrecipIn"),
        sum(when(month(col("date")) === 6  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("junFrostFreeDays"),
        sum(when(month(col("date")) === 6  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("junHeatStressDays"),
        round(sum(when(month(col("date")) === 7,  col("gdd")).otherwise(0.0)), 1).alias("julGdd"),
        round(sum(when(month(col("date")) === 7,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("julPrecipIn"),
        sum(when(month(col("date")) === 7  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("julFrostFreeDays"),
        sum(when(month(col("date")) === 7  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("julHeatStressDays"),
        round(sum(when(month(col("date")) === 8,  col("gdd")).otherwise(0.0)), 1).alias("augGdd"),
        round(sum(when(month(col("date")) === 8,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("augPrecipIn"),
        sum(when(month(col("date")) === 8  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("augFrostFreeDays"),
        sum(when(month(col("date")) === 8  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("augHeatStressDays"),
        round(sum(when(month(col("date")) === 9,  col("gdd")).otherwise(0.0)), 1).alias("sepGdd"),
        round(sum(when(month(col("date")) === 9,  coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("sepPrecipIn"),
        sum(when(month(col("date")) === 9  && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("sepFrostFreeDays"),
        sum(when(month(col("date")) === 9  && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("sepHeatStressDays"),
        round(sum(when(month(col("date")) === 10, col("gdd")).otherwise(0.0)), 1).alias("octGdd"),
        round(sum(when(month(col("date")) === 10, coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("octPrecipIn"),
        sum(when(month(col("date")) === 10 && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("octFrostFreeDays"),
        sum(when(month(col("date")) === 10 && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("octHeatStressDays"),
        round(sum(when(month(col("date")) === 11, col("gdd")).otherwise(0.0)), 1).alias("novGdd"),
        round(sum(when(month(col("date")) === 11, coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("novPrecipIn"),
        sum(when(month(col("date")) === 11 && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("novFrostFreeDays"),
        sum(when(month(col("date")) === 11 && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("novHeatStressDays"),
        round(sum(when(month(col("date")) === 12, col("gdd")).otherwise(0.0)), 1).alias("decGdd"),
        round(sum(when(month(col("date")) === 12, coalesce(col("precipIn"), lit(0.0))).otherwise(0.0)), 2).alias("decPrecipIn"),
        sum(when(month(col("date")) === 12 && col("tempMinF") > 32.0, 1).otherwise(0)).cast("int").alias("decFrostFreeDays"),
        sum(when(month(col("date")) === 12 && col("tempMaxF") > 95.0, 1).otherwise(0)).cast("int").alias("decHeatStressDays"),
        // Average daily temperature (°F) — mean of (TMAX+TMIN)/2 for qualifying days in the period
        coalesce(round(avg(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd), col("tempAvgF"))), 1), lit(0.0)).alias("avgTempF"),
        coalesce(round(avg(when(col("cornGrowing"),    col("tempAvgF"))), 1), lit(0.0)).alias("cornGrowingAvgTempF"),
        coalesce(round(avg(when(col("soybeanGrowing"), col("tempAvgF"))), 1), lit(0.0)).alias("soybeanGrowingAvgTempF"),
        coalesce(round(avg(when(col("wheatGrowing"),   col("tempAvgF"))), 1), lit(0.0)).alias("wheatGrowingAvgTempF"),
        coalesce(round(avg(when(col("cornPlanting"),   col("tempAvgF"))), 1), lit(0.0)).alias("cornPlantingAvgTempF"),
        coalesce(round(avg(when(col("soybeanPlanting"),col("tempAvgF"))), 1), lit(0.0)).alias("soybeanPlantingAvgTempF"),
        coalesce(round(avg(when(col("wheatPlanting"),  col("tempAvgF"))), 1), lit(0.0)).alias("wheatPlantingAvgTempF"),
        coalesce(round(avg(when(col("cornHarvest"),    col("tempAvgF"))), 1), lit(0.0)).alias("cornHarvestAvgTempF"),
        coalesce(round(avg(when(col("soybeanHarvest"), col("tempAvgF"))), 1), lit(0.0)).alias("soybeanHarvestAvgTempF"),
        coalesce(round(avg(when(col("wheatHarvest"),   col("tempAvgF"))), 1), lit(0.0)).alias("wheatHarvestAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 1,  col("tempAvgF"))), 1), lit(0.0)).alias("janAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 2,  col("tempAvgF"))), 1), lit(0.0)).alias("febAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 3,  col("tempAvgF"))), 1), lit(0.0)).alias("marAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 4,  col("tempAvgF"))), 1), lit(0.0)).alias("aprAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 5,  col("tempAvgF"))), 1), lit(0.0)).alias("mayAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 6,  col("tempAvgF"))), 1), lit(0.0)).alias("junAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 7,  col("tempAvgF"))), 1), lit(0.0)).alias("julAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 8,  col("tempAvgF"))), 1), lit(0.0)).alias("augAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 9,  col("tempAvgF"))), 1), lit(0.0)).alias("sepAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 10, col("tempAvgF"))), 1), lit(0.0)).alias("octAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 11, col("tempAvgF"))), 1), lit(0.0)).alias("novAvgTempF"),
        coalesce(round(avg(when(month(col("date")) === 12, col("tempAvgF"))), 1), lit(0.0)).alias("decAvgTempF"),
        // Total snowfall (inches) for the period
        round(sum(when(month(col("date")).between(GrowingSeasonStart, GrowingSeasonEnd), coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("totalSnowIn"),
        round(sum(when(col("cornGrowing"),    coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornGrowingSnowIn"),
        round(sum(when(col("soybeanGrowing"), coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanGrowingSnowIn"),
        round(sum(when(col("wheatGrowing"),   coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatGrowingSnowIn"),
        round(sum(when(col("cornPlanting"),   coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornPlantingSnowIn"),
        round(sum(when(col("soybeanPlanting"),coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanPlantingSnowIn"),
        round(sum(when(col("wheatPlanting"),  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatPlantingSnowIn"),
        round(sum(when(col("cornHarvest"),    coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("cornHarvestSnowIn"),
        round(sum(when(col("soybeanHarvest"), coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("soybeanHarvestSnowIn"),
        round(sum(when(col("wheatHarvest"),   coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("wheatHarvestSnowIn"),
        round(sum(when(month(col("date")) === 1,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("janSnowIn"),
        round(sum(when(month(col("date")) === 2,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("febSnowIn"),
        round(sum(when(month(col("date")) === 3,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("marSnowIn"),
        round(sum(when(month(col("date")) === 4,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("aprSnowIn"),
        round(sum(when(month(col("date")) === 5,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("maySnowIn"),
        round(sum(when(month(col("date")) === 6,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("junSnowIn"),
        round(sum(when(month(col("date")) === 7,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("julSnowIn"),
        round(sum(when(month(col("date")) === 8,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("augSnowIn"),
        round(sum(when(month(col("date")) === 9,  coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("sepSnowIn"),
        round(sum(when(month(col("date")) === 10, coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("octSnowIn"),
        round(sum(when(month(col("date")) === 11, coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("novSnowIn"),
        round(sum(when(month(col("date")) === 12, coalesce(col("snowIn"), lit(0.0))).otherwise(0.0)), 2).alias("decSnowIn")
      )

    // Drought index for every precip window: (windowPrecip − meanWindowPrecip) / meanWindowPrecip.
    // A foldLeft computes one drought index per precip column using an unbounded cross-year window.
    val allYearsWindow = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val droughtTargets: Seq[(String, String)] = Seq(
      "totalPrecipIn"            -> "droughtIndex",
      "cornGrowingPrecipIn"      -> "cornGrowingDroughtIndex",
      "soybeanGrowingPrecipIn"   -> "soybeanGrowingDroughtIndex",
      "wheatGrowingPrecipIn"     -> "wheatGrowingDroughtIndex",
      "cornPlantingPrecipIn"     -> "cornPlantingDroughtIndex",
      "soybeanPlantingPrecipIn"  -> "soybeanPlantingDroughtIndex",
      "wheatPlantingPrecipIn"    -> "wheatPlantingDroughtIndex",
      "cornHarvestPrecipIn"      -> "cornHarvestDroughtIndex",
      "soybeanHarvestPrecipIn"   -> "soybeanHarvestDroughtIndex",
      "wheatHarvestPrecipIn"     -> "wheatHarvestDroughtIndex",
      "janPrecipIn" -> "janDroughtIndex", "febPrecipIn" -> "febDroughtIndex",
      "marPrecipIn" -> "marDroughtIndex", "aprPrecipIn" -> "aprDroughtIndex",
      "mayPrecipIn" -> "mayDroughtIndex", "junPrecipIn" -> "junDroughtIndex",
      "julPrecipIn" -> "julDroughtIndex", "augPrecipIn" -> "augDroughtIndex",
      "sepPrecipIn" -> "sepDroughtIndex", "octPrecipIn" -> "octDroughtIndex",
      "novPrecipIn" -> "novDroughtIndex", "decPrecipIn" -> "decDroughtIndex"
    )
    val withDrought = droughtTargets.foldLeft(annual) { case (df, (precipCol, idxCol)) =>
      df.withColumn("_mean", avg(col(precipCol)).over(allYearsWindow))
        .withColumn(idxCol, round((col(precipCol) - col("_mean")) / col("_mean"), 4))
        .drop("_mean")
    }

    val ds = withDrought
      .select(
        col("yr").alias("year"),
        lit(countyFips).alias("countyFips"),
        lit(stateCode).alias("stateCode"),
        col("growingDegreeDays"), col("totalPrecipIn"), col("frostFreeDays"), col("extremeHeatDays"), col("droughtIndex"),
        col("cornGrowingGdd"),    col("cornGrowingPrecipIn"),    col("soybeanGrowingGdd"),    col("soybeanGrowingPrecipIn"),    col("wheatGrowingGdd"),    col("wheatGrowingPrecipIn"),
        col("cornPlantingGdd"),   col("cornPlantingPrecipIn"),   col("soybeanPlantingGdd"),   col("soybeanPlantingPrecipIn"),   col("wheatPlantingGdd"),   col("wheatPlantingPrecipIn"),
        col("cornHarvestGdd"),    col("cornHarvestPrecipIn"),    col("soybeanHarvestGdd"),    col("soybeanHarvestPrecipIn"),    col("wheatHarvestGdd"),    col("wheatHarvestPrecipIn"),
        col("janGdd"), col("janPrecipIn"), col("febGdd"), col("febPrecipIn"),
        col("marGdd"), col("marPrecipIn"), col("aprGdd"), col("aprPrecipIn"),
        col("mayGdd"), col("mayPrecipIn"), col("junGdd"), col("junPrecipIn"),
        col("julGdd"), col("julPrecipIn"), col("augGdd"), col("augPrecipIn"),
        col("sepGdd"), col("sepPrecipIn"), col("octGdd"), col("octPrecipIn"),
        col("novGdd"), col("novPrecipIn"), col("decGdd"), col("decPrecipIn"),
        col("cornPlantingFrostFreeDays"),    col("cornPlantingHeatStressDays"),    col("cornPlantingDroughtIndex"),
        col("cornGrowingFrostFreeDays"),     col("cornGrowingHeatStressDays"),     col("cornGrowingDroughtIndex"),
        col("cornHarvestFrostFreeDays"),     col("cornHarvestHeatStressDays"),     col("cornHarvestDroughtIndex"),
        col("soybeanPlantingFrostFreeDays"), col("soybeanPlantingHeatStressDays"), col("soybeanPlantingDroughtIndex"),
        col("soybeanGrowingFrostFreeDays"),  col("soybeanGrowingHeatStressDays"),  col("soybeanGrowingDroughtIndex"),
        col("soybeanHarvestFrostFreeDays"),  col("soybeanHarvestHeatStressDays"),  col("soybeanHarvestDroughtIndex"),
        col("wheatPlantingFrostFreeDays"),   col("wheatPlantingHeatStressDays"),   col("wheatPlantingDroughtIndex"),
        col("wheatGrowingFrostFreeDays"),    col("wheatGrowingHeatStressDays"),    col("wheatGrowingDroughtIndex"),
        col("wheatHarvestFrostFreeDays"),    col("wheatHarvestHeatStressDays"),    col("wheatHarvestDroughtIndex"),
        col("janFrostFreeDays"), col("janHeatStressDays"), col("janDroughtIndex"),
        col("febFrostFreeDays"), col("febHeatStressDays"), col("febDroughtIndex"),
        col("marFrostFreeDays"), col("marHeatStressDays"), col("marDroughtIndex"),
        col("aprFrostFreeDays"), col("aprHeatStressDays"), col("aprDroughtIndex"),
        col("mayFrostFreeDays"), col("mayHeatStressDays"), col("mayDroughtIndex"),
        col("junFrostFreeDays"), col("junHeatStressDays"), col("junDroughtIndex"),
        col("julFrostFreeDays"), col("julHeatStressDays"), col("julDroughtIndex"),
        col("augFrostFreeDays"), col("augHeatStressDays"), col("augDroughtIndex"),
        col("sepFrostFreeDays"), col("sepHeatStressDays"), col("sepDroughtIndex"),
        col("octFrostFreeDays"), col("octHeatStressDays"), col("octDroughtIndex"),
        col("novFrostFreeDays"), col("novHeatStressDays"), col("novDroughtIndex"),
        col("decFrostFreeDays"), col("decHeatStressDays"), col("decDroughtIndex"),
        col("avgTempF"),
        col("cornGrowingAvgTempF"),     col("soybeanGrowingAvgTempF"),     col("wheatGrowingAvgTempF"),
        col("cornPlantingAvgTempF"),    col("soybeanPlantingAvgTempF"),    col("wheatPlantingAvgTempF"),
        col("cornHarvestAvgTempF"),     col("soybeanHarvestAvgTempF"),     col("wheatHarvestAvgTempF"),
        col("janAvgTempF"), col("febAvgTempF"), col("marAvgTempF"), col("aprAvgTempF"),
        col("mayAvgTempF"), col("junAvgTempF"), col("julAvgTempF"), col("augAvgTempF"),
        col("sepAvgTempF"), col("octAvgTempF"), col("novAvgTempF"), col("decAvgTempF"),
        col("totalSnowIn"),
        col("cornGrowingSnowIn"),       col("soybeanGrowingSnowIn"),       col("wheatGrowingSnowIn"),
        col("cornPlantingSnowIn"),      col("soybeanPlantingSnowIn"),      col("wheatPlantingSnowIn"),
        col("cornHarvestSnowIn"),       col("soybeanHarvestSnowIn"),       col("wheatHarvestSnowIn"),
        col("janSnowIn"), col("febSnowIn"), col("marSnowIn"), col("aprSnowIn"),
        col("maySnowIn"), col("junSnowIn"), col("julSnowIn"), col("augSnowIn"),
        col("sepSnowIn"), col("octSnowIn"), col("novSnowIn"), col("decSnowIn")
      )

    if (ds.count() == 0) {
      logger.error("Zero season summaries produced — is silver/weather_observations populated?")
      spark.stop()
      sys.exit(1)
    }

    logger.info(s"Season summaries produced: ${ds.count()} years")
    ds.orderBy("year").show(40, truncate = false)

    ds.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("overwriteSchema", "true")
      .save(goldPath)

    logger.info(s"Gold write complete -> $goldPath")

    val previewPath = s"${pathsCfg.getString("gold")}/preview_top100.csv"
    val previewRows = spark.read.format("delta").load(goldPath)
      .orderBy("year")
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
      logger.info(s"Gold preview (top 100 rows) -> $previewPath")
    }

    val csvPath = s"${pathsCfg.getString("gold")}/row_count_by_year.csv"
    val rows = spark.read.format("delta").load(goldPath)
      .orderBy("year")
      .collect()

    Files.createDirectories(Paths.get(pathsCfg.getString("gold")))
    val writer = new PrintWriter(new OutputStreamWriter(
      new FileOutputStream(csvPath), StandardCharsets.UTF_8))
    writer.println("year,row_count")
    rows.foreach(r => writer.println(s"${r.getAs[Int]("year")},1"))
    writer.close()

    logger.info(s"Gold row counts by year -> $csvPath")
    rows.foreach(r => logger.info(s"  ${r.getAs[Int]("year")}: 1 row"))
  }
}
