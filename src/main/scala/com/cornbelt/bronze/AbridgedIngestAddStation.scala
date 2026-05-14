package com.cornbelt.bronze

import com.cornbelt.models.RawWeatherObservation
import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}
import scala.collection.JavaConverters._

/** Single-station ingest. Fetches one station for all analysis years and
 *  merges it into the bronze Delta table via replaceWhere, so the job is
 *  safe to re-run without duplicating data.
 *
 *  Usage:
 *    sbt "runMain com.cornbelt.bronze.AbridgedIngestAddStation USC00050945"
 *
 *  The station argument is the bare NCEI ID (with or without the GHCND: prefix).
 *  Defaults to USC00050945 if omitted.
 */
object AbridgedIngestAddStation extends LazyLogging {

  private val config      = ConfigFactory.load().getConfig("corn-belt")
  private val noaaCfg     = config.getConfig("noaa")
  private val pathsCfg    = config.getConfig("paths")
  private val analysisCfg = config.getConfig("analysis")

  def main(args: Array[String]): Unit = {
    val spark     = SparkSessionProvider.session
    import spark.implicits._

    val token     = sys.env.getOrElse("NOAA_TOKEN", throw new RuntimeException("NOAA_TOKEN env var not set"))
    val startYear = analysisCfg.getInt("start-year")
    val endYear   = analysisCfg.getInt("end-year")
    val dataTypes = noaaCfg.getStringList("data-types").asScala.toList
    val outputPath = s"${pathsCfg.getString("bronze")}/noaa_observations"

    val rawArg    = if (args.nonEmpty) args(0) else "USC00050945"
    val stationId = if (rawArg.startsWith("GHCND:")) rawArg else s"GHCND:$rawArg"

    logger.info(s"Abridged ingest: station=$stationId, years=$startYear-$endYear")
    logger.info(s"NOAA_TOKEN present: ${token.nonEmpty}, length=${token.length}")

    val observations: Seq[RawWeatherObservation] =
      (startYear to endYear).flatMap { year =>
        logger.info(s"  $stationId / $year ...")
        Thread.sleep(1000)
        val rows = fetchYear(stationId, year, dataTypes, token)
        logger.info(s"  $stationId / $year -> ${rows.size} rows")
        rows
      }

    if (observations.isEmpty) {
      logger.error(s"Zero observations fetched for $stationId — aborting before Delta write.")
      spark.stop()
      sys.exit(1)
    }

    logger.info(s"Fetched ${observations.size} observations. Writing to Bronze (replaceWhere stationId=$stationId)...")
    val ds: Dataset[RawWeatherObservation] = spark.createDataset(observations)

    // replaceWhere overwrites only the partition for this station, leaving all
    // other stations' data intact. Running twice is safe — no duplication.
    ds.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("replaceWhere", s"stationId = '$stationId'")
      .save(outputPath)

    logger.info(s"Bronze write complete -> $outputPath")
  }

  private def fetchYear(stationId: String, year: Int, dataTypes: List[String], token: String): Seq[RawWeatherObservation] = {
    import sttp.client3._
    import io.circe.parser._
    val backend      = HttpURLConnectionBackend()
    val baseUrl      = noaaCfg.getString("base-url")
    val dataset      = noaaCfg.getString("dataset-id")
    val stationParam = stationId.stripPrefix("GHCND:")
    val dtParam      = dataTypes.mkString(",")

    val url = uri"$baseUrl?dataset=$dataset&stations=$stationParam&startDate=$year-01-01&endDate=$year-12-31&dataTypes=$dtParam&units=metric&format=json"
    logger.info(s"  GET $url")
    val response = basicRequest.get(url).send(backend)

    response.body match {
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
            logger.warn(s"  No data returned for $stationId / $year (empty response)")
            Seq.empty
          case Left(_) =>
            logger.warn(s"  Unexpected response for $stationId / $year: ${body.take(300)}")
            Seq.empty
        }
      case Left(err) =>
        logger.warn(s"  NCEI API error for $stationId / $year: $err")
        Seq.empty
    }
  }
}
