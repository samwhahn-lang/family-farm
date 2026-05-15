package com.cornbelt.bronze

import com.cornbelt.models.RawWeatherObservation
import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions.{col, max => sparkMax}
import java.time.LocalDate
import scala.collection.JavaConverters._

/** Incremental update — reads the highest date already in the bronze Delta table,
 *  then fetches only the dates from (max_date + 1) through today for all Gage County
 *  stations and APPENDS the new rows.  Safe to re-run; a second same-day run will
 *  find max_date == yesterday and skip (nothing past today to fetch).
 *
 *  Requires NoaaHistoricalBackfillJob to have been run at least once to seed the table.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.bronze.NoaaIncrementalUpdateJob"
 */
object NoaaIncrementalUpdateJob extends LazyLogging {

  private val config      = ConfigFactory.load().getConfig("corn-belt")
  private val noaaCfg     = config.getConfig("noaa")
  private val anchor      = config.getConfig("anchor")
  private val pathsCfg    = config.getConfig("paths")

  private val CdoBase = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

  private case class Station(id: String, name: String)

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session
    import spark.implicits._
    val token      = sys.env.getOrElse("NOAA_TOKEN", throw new RuntimeException("NOAA_TOKEN env var not set"))
    val countyFips = anchor.getString("county-fips")
    val dataTypes  = noaaCfg.getStringList("data-types").asScala.toList
    val outputPath = s"${pathsCfg.getString("bronze")}/noaa_observations"

    // ── Find the highest date already in bronze ───────────────────────────────
    val maxDateStr = spark.read.format("delta").load(outputPath)
      .agg(sparkMax(col("date")))
      .collect().head.getString(0)

    if (maxDateStr == null)
      throw new RuntimeException("Bronze table has no date data — run NoaaHistoricalBackfillJob first.")

    val maxDate   = LocalDate.parse(maxDateStr)
    val today     = LocalDate.now()
    val startDate = maxDate

    logger.info(s"Bronze max date: $maxDateStr")
    logger.info(s"Today:          $today")
    logger.info(s"Fetching from:  $startDate (inclusive)")

    if (!startDate.isBefore(today.plusDays(1))) {
      logger.info("Bronze max date is today or later — nothing to fetch.")
      return
    }

    // ── Discover stations ─────────────────────────────────────────────────────
    val startYear = startDate.getYear
    val endYear   = today.getYear

    logger.info(s"Discovering GHCND stations in Gage County (FIPS $countyFips) ...")
    val stations = listGageCountyStations(token, countyFips, startYear, endYear)
    if (stations.isEmpty) {
      logger.error("No stations found — aborting.")
      spark.stop()
      sys.exit(1)
    }
    logger.info(s"Found ${stations.size} stations:")
    stations.foreach(s => logger.info(s"  ${s.id}  ${s.name}"))

    // ── Fetch date range per station, split by year for API compatibility ─────
    val allObservations: Seq[RawWeatherObservation] =
      stations.flatMap { station =>
        logger.info(s"=== Fetching station ${station.id} (${station.name}) ===")
        (startYear to endYear).flatMap { year =>
          val rangeStart = if (year == startYear) startDate.toString else s"$year-01-01"
          val rangeEnd   = if (year == endYear)   today.toString    else s"$year-12-31"
          logger.info(s"  ${station.id} / $rangeStart – $rangeEnd ...")
          Thread.sleep(1000)
          val rows = NoaaHistoricalBackfillJob.fetchDateRange(station.id, rangeStart, rangeEnd, dataTypes, token)
          logger.info(s"  ${station.id} / $rangeStart – $rangeEnd -> ${rows.size} rows")
          rows
        }
      }

    if (allObservations.isEmpty) {
      logger.warn("Zero new observations fetched — nothing to append.")
      return
    }

    logger.info(s"Fetched ${allObservations.size} new observations. Appending to Bronze...")
    val ds: Dataset[RawWeatherObservation] = spark.createDataset(allObservations)
    ds.write.format("delta").mode(SaveMode.Append).save(outputPath)
    logger.info(s"Bronze append complete -> $outputPath  (max date was $maxDateStr, now through $today)")

    // ── Supplemental out-of-county temperature station ────────────────────────
    // Lincoln Airport ASOS (USW00014939) is outside Gage County so it never
    // appears in the CDO county query above. Fetched separately so silver has a
    // last-resort temperature fallback when no Gage County station has TMAX/TMIN.
    val suppId = anchor.getString("temp-supplemental-id")
    logger.info(s"=== Fetching supplemental station $suppId ===")
    val suppObs: Seq[RawWeatherObservation] =
      (startYear to endYear).flatMap { year =>
        val rangeStart = if (year == startYear) startDate.toString else s"$year-01-01"
        val rangeEnd   = if (year == endYear)   today.toString    else s"$year-12-31"
        logger.info(s"  $suppId / $rangeStart – $rangeEnd ...")
        Thread.sleep(1000)
        val rows = NoaaHistoricalBackfillJob.fetchDateRange(suppId, rangeStart, rangeEnd, dataTypes, token)
        logger.info(s"  $suppId / $rangeStart – $rangeEnd -> ${rows.size} rows")
        rows
      }
    if (suppObs.nonEmpty) {
      spark.createDataset(suppObs).write.format("delta").mode(SaveMode.Append).save(outputPath)
      logger.info(s"Supplemental $suppId: appended ${suppObs.size} rows")
    } else {
      logger.warn(s"Supplemental $suppId: no new rows fetched (NCEI archive may not include these dates yet)")
    }

    NoaaHistoricalBackfillJob.writePreview(outputPath)
  }

  private def listGageCountyStations(token: String, countyFips: String, startYear: Int, endYear: Int): Seq[Station] = {
    import sttp.client3._
    import io.circe.parser._
    val backend   = HttpURLConnectionBackend()
    var results   = Seq.empty[Station]
    var offset    = 1
    var keepGoing = true

    while (keepGoing) {
      val url = uri"$CdoBase/stations?datasetid=GHCND&locationid=FIPS:$countyFips&startdate=$startYear-01-01&enddate=$endYear-12-31&limit=1000&offset=$offset"
      basicRequest.header("token", token).get(url).send(backend).body match {
        case Right(body) =>
          parse(body).flatMap(_.hcursor.downField("results").as[List[io.circe.Json]]) match {
            case Right(rows) if rows.nonEmpty =>
              results   = results ++ rows.flatMap { json =>
                val c = json.hcursor
                for { id <- c.get[String]("id").toOption; name <- c.get[String]("name").toOption } yield Station(id, name)
              }
              offset   += 1000
              keepGoing = rows.size == 1000
            case _ => keepGoing = false
          }
        case Left(err) =>
          logger.warn(s"CDO station list error: $err")
          keepGoing = false
      }
    }
    results
  }
}
