package com.cornbelt.bronze

import com.cornbelt.models.RawWeatherObservation
import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}
import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Full historical backfill — fetches every year in [start-year, end-year] for all
 *  GHCND stations in Gage County and OVERWRITES the bronze Delta table entirely.
 *  Run this once to seed the table, or to do a complete refresh.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.bronze.NoaaHistoricalBackfillJob"
 */
object NoaaHistoricalBackfillJob extends LazyLogging {

  private val config      = ConfigFactory.load().getConfig("corn-belt")
  private val noaaCfg     = config.getConfig("noaa")
  private val anchor      = config.getConfig("anchor")
  private val pathsCfg    = config.getConfig("paths")
  private val analysisCfg = config.getConfig("analysis")

  private val CdoBase = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

  private case class Station(id: String, name: String)

  def main(args: Array[String]): Unit = {
    val spark      = SparkSessionProvider.session
    import spark.implicits._
    val token      = sys.env.getOrElse("NOAA_TOKEN", throw new RuntimeException("NOAA_TOKEN env var not set"))
    val startYear  = analysisCfg.getInt("start-year")
    val endYear    = analysisCfg.getInt("end-year")
    val countyFips = anchor.getString("county-fips")
    val dataTypes  = noaaCfg.getStringList("data-types").asScala.toList
    val outputPath = s"${pathsCfg.getString("bronze")}/noaa_observations"

    logger.info(s"=== IngestBackfill: $startYear-$endYear, all Gage County stations (FIPS $countyFips) ===")
    logger.info(s"NOAA_TOKEN present: ${token.nonEmpty}, length=${token.length}")

    val stations = listGageCountyStations(token, countyFips, startYear, endYear)
    if (stations.isEmpty) {
      logger.error("No stations found in Gage County — aborting.")
      spark.stop()
      sys.exit(1)
    }
    logger.info(s"Found ${stations.size} stations:")
    stations.foreach(s => logger.info(s"  ${s.id}  ${s.name}"))

    val allObservations: Seq[RawWeatherObservation] =
      stations.flatMap { station =>
        logger.info(s"=== Fetching station ${station.id} (${station.name}) ===")
        (startYear to endYear).flatMap { year =>
          logger.info(s"  ${station.id} / $year ...")
          Thread.sleep(1000)
          val rows = fetchDateRange(station.id, s"$year-01-01", s"$year-12-31", dataTypes, token)
          logger.info(s"  ${station.id} / $year -> ${rows.size} rows")
          rows
        }
      }

    if (allObservations.isEmpty) {
      logger.error("Zero observations fetched — aborting before Delta write.")
      spark.stop()
      sys.exit(1)
    }

    logger.info(s"Fetched ${allObservations.size} total observations. Writing to Bronze (overwrite)...")
    val ds: Dataset[RawWeatherObservation] = spark.createDataset(allObservations)
    ds.write.format("delta").mode(SaveMode.Overwrite).partitionBy("stationId").save(outputPath)
    logger.info(s"Bronze write complete -> $outputPath")

    // ── Supplemental out-of-county temperature station ────────────────────────
    // Lincoln Airport ASOS (USW00014939) is outside Gage County so it never
    // appears in the CDO county query above. Without it, Silver has no TMAX/TMIN
    // fallback for years after the anchor station went precip-only (~2013).
    val suppId = anchor.getString("temp-supplemental-id")
    logger.info(s"=== Fetching supplemental station $suppId ($startYear-$endYear) ===")
    val suppObs: Seq[RawWeatherObservation] =
      (startYear to endYear).flatMap { year =>
        logger.info(s"  $suppId / $year ...")
        Thread.sleep(1000)
        val rows = fetchDateRange(suppId, s"$year-01-01", s"$year-12-31", dataTypes, token)
        logger.info(s"  $suppId / $year -> ${rows.size} rows")
        rows
      }
    if (suppObs.nonEmpty) {
      spark.createDataset(suppObs).write.format("delta").mode(SaveMode.Append).save(outputPath)
      logger.info(s"Supplemental $suppId: appended ${suppObs.size} rows")
    } else {
      logger.warn(s"Supplemental $suppId: no rows fetched — temperature fallback will be unavailable")
    }

    writePreview(outputPath)
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

  private[bronze] def fetchDateRange(stationId: String, startDate: String, endDate: String, dataTypes: List[String], token: String): Seq[RawWeatherObservation] = {
    import sttp.client3._
    import io.circe.parser._
    val backend      = HttpURLConnectionBackend()
    val baseUrl      = noaaCfg.getString("base-url")
    val dataset      = noaaCfg.getString("dataset-id")
    val stationParam = stationId.stripPrefix("GHCND:")
    val dtParam      = dataTypes.mkString(",")

    val url = uri"$baseUrl?dataset=$dataset&stations=$stationParam&startDate=$startDate&endDate=$endDate&dataTypes=$dtParam&units=metric&format=json"
    logger.info(s"  GET $url")

    basicRequest.get(url).send(backend).body match {
      case Right(body) =>
        parse(body).flatMap(_.as[List[io.circe.Json]]) match {
          case Right(rows) if rows.nonEmpty =>
            rows.flatMap { json =>
              val c      = json.hcursor
              val rawSid = c.get[String]("STATION").getOrElse("")
              val sid    = if (rawSid.startsWith("GHCND:")) rawSid else s"GHCND:$rawSid"
              val date   = c.get[String]("DATE").map(_.take(10)).getOrElse("")
              dataTypes.flatMap { dt =>
                val value = c.get[Double](dt).toOption
                  .orElse(c.get[String](dt).toOption.flatMap(s => scala.util.Try(s.toDouble).toOption))
                Some(RawWeatherObservation(sid, date, dt, value, None))
              }
            }
          case Right(_) =>
            logger.warn(s"  No data for $stationId [$startDate – $endDate] (empty response)")
            Seq.empty
          case Left(_) =>
            logger.warn(s"  Unexpected response for $stationId [$startDate – $endDate]: ${body.take(300)}")
            Seq.empty
        }
      case Left(err) =>
        logger.warn(s"  NCEI API error for $stationId [$startDate – $endDate]: $err")
        Seq.empty
    }
  }

  private[bronze] def writePreview(outputPath: String): Unit = {
    val spark       = SparkSessionProvider.session
    val previewPath = s"${pathsCfg.getString("bronze")}/preview_top100.csv"
    val previewRows = spark.read.format("delta").load(outputPath)
      .orderBy("stationId", "date").limit(100).collect()

    if (previewRows.nonEmpty) {
      Files.createDirectories(Paths.get(pathsCfg.getString("bronze")))
      val w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(previewPath), StandardCharsets.UTF_8))
      w.println(previewRows.head.schema.fieldNames.mkString(","))
      previewRows.foreach(r => w.println(r.schema.fieldNames.map(f => Option(r.getAs[Any](f)).getOrElse("")).mkString(",")))
      w.close()
      logger.info(s"Bronze preview (top 100 rows) -> $previewPath")
    }
  }
}
