package com.cornbelt.bronze

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import java.io.{File, PrintWriter}

/** Standalone job — no Spark, no Delta. Reads directly from NOAA APIs.
 *  Uses CDO API to list all GHCND stations in Gage County, NE,
 *  then queries the NCEI API year-by-year per station to count rows.
 *  Output: data/bronze/gage_county_station_row_count.csv
 */
object NoaaStationInventoryJob extends LazyLogging {

  private val config   = ConfigFactory.load().getConfig("corn-belt")
  private val noaaCfg  = config.getConfig("noaa")
  private val pathsCfg = config.getConfig("paths")

  private val CdoBase    = "https://www.ncdc.noaa.gov/cdo-web/api/v2"
  private val CountyFips = "31067"   // Gage County, Nebraska
  private val DataTypes  = "TMAX,TMIN,PRCP,SNOW,SNWD"
  private val StartYear  = 2000
  private val EndYear    = 2026

  def main(args: Array[String]): Unit = {
    val token      = sys.env.getOrElse("NOAA_TOKEN", throw new RuntimeException("NOAA_TOKEN env var not set"))
    val nceiBase   = noaaCfg.getString("base-url")
    val dataset    = noaaCfg.getString("dataset-id")
    val outputFile = s"${pathsCfg.getString("bronze")}/gage_county_station_row_count.csv"

    // ── Step 1: list all stations in Gage County ──────────────────────────
    logger.info(s"Listing GHCND stations in Gage County (FIPS $CountyFips) ...")
    val stations = listStations(token)
    logger.info(s"Found ${stations.size} stations")
    stations.foreach { s => logger.info(s"  ${s.id}  ${s.name}  (${s.minDate} – ${s.maxDate})") }

    // ── Step 2: count rows per station per year from NCEI ─────────────────
    val writer = new PrintWriter(new File(outputFile))
    writer.println("station_id,station_name,latitude,longitude,year,row_count")

    stations.foreach { station =>
      val bareId   = station.id.stripPrefix("GHCND:")
      var stationTotal = 0

      (StartYear to EndYear).foreach { year =>
        Thread.sleep(250)
        val count = countRows(nceiBase, dataset, bareId, year)
        stationTotal += count
        logger.info(f"  ${station.id}%-25s $year  $count%4d rows")
        writer.println(s"${station.id},${station.name},${station.lat},${station.lon},$year,$count")
      }

      logger.info(f"  ${station.id}%-25s TOTAL  $stationTotal rows")
    }

    writer.close()
    logger.info(s"Written -> $outputFile")
  }

  // ── Station listing via CDO API ──────────────────────────────────────────

  private case class Station(id: String, name: String, lat: Double, lon: Double, minDate: String, maxDate: String)

  private def listStations(token: String): Seq[Station] = {
    import sttp.client3._
    import io.circe.parser._
    val backend    = HttpURLConnectionBackend()
    var results    = Seq.empty[Station]
    var offset     = 1
    var keepGoing  = true

    while (keepGoing) {
      val url = uri"$CdoBase/stations?datasetid=GHCND&locationid=FIPS:$CountyFips&startdate=$StartYear-01-01&enddate=$EndYear-12-31&limit=1000&offset=$offset"
      basicRequest.header("token", token).get(url).send(backend).body match {
        case Right(body) =>
          parse(body).flatMap(_.hcursor.downField("results").as[List[io.circe.Json]]) match {
            case Right(rows) if rows.nonEmpty =>
              val batch = rows.flatMap { json =>
                val c = json.hcursor
                for {
                  id      <- c.get[String]("id").toOption
                  name    <- c.get[String]("name").toOption
                  lat     <- c.get[Double]("latitude").toOption
                  lon     <- c.get[Double]("longitude").toOption
                  minDate <- c.get[String]("mindate").toOption
                  maxDate <- c.get[String]("maxdate").toOption
                } yield Station(id, name, lat, lon, minDate, maxDate)
              }
              results    = results ++ batch
              offset    += 1000
              keepGoing  = rows.size == 1000
            case _ => keepGoing = false
          }
        case Left(err) =>
          logger.warn(s"CDO station list error: $err")
          keepGoing = false
      }
    }
    results
  }

  // ── Row count via NCEI API (one request per station/year) ────────────────

  private def countRows(nceiBase: String, dataset: String, stationId: String, year: Int): Int = {
    import sttp.client3._
    import io.circe.parser._
    val backend = HttpURLConnectionBackend()
    val url     = uri"$nceiBase?dataset=$dataset&stations=$stationId&startDate=$year-01-01&endDate=$year-12-31&dataTypes=$DataTypes&units=metric&format=json"
    basicRequest.get(url).send(backend).body match {
      case Right(body) =>
        parse(body).flatMap(_.as[List[io.circe.Json]]) match {
          case Right(rows) => rows.size
          case Left(_)     => 0
        }
      case Left(_) => 0
    }
  }
}
