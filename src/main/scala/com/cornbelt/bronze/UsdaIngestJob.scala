package com.cornbelt.bronze

import com.cornbelt.models.RawCropRecord
import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{Dataset, SaveMode}
import scala.collection.JavaConverters._

object UsdaIngestJob extends LazyLogging {

  private val config      = ConfigFactory.load().getConfig("corn-belt")
  private val usdaCfg     = config.getConfig("usda")
  private val pathsCfg    = config.getConfig("paths")
  private val analysisCfg = config.getConfig("analysis")

  def main(args: Array[String]): Unit = {
    val spark  = SparkSessionProvider.session
    import spark.implicits._
    val apiKey    = sys.env.getOrElse("USDA_NASS_KEY", throw new RuntimeException("USDA_NASS_KEY env var not set"))
    val states    = config.getStringList("states").asScala.toList
    val crops     = config.getStringList("crops").asScala.toList
    val stats     = usdaCfg.getStringList("statistics").asScala.toList
    val startYear = analysisCfg.getInt("start-year")
    val endYear   = analysisCfg.getInt("end-year")
    val outputPath = s"${pathsCfg.getString("bronze")}/usda_crops"

    logger.info(s"Starting USDA NASS ingest: ${states.size} states")

    val allRecords: Seq[RawCropRecord] = (for {
      state <- states; crop <- crops; stat <- stats
    } yield {
      logger.info(s"  Fetching $crop / $stat / $state ...")
      fetchRecords(state, crop, stat, startYear, endYear, apiKey)
    }).flatten

    logger.info(s"Fetched ${allRecords.size} crop records. Writing to Bronze...")
    val ds: Dataset[RawCropRecord] = spark.createDataset(allRecords)
    ds.write.format("delta").mode(SaveMode.Overwrite).partitionBy("year", "state").save(outputPath)
    logger.info(s"Bronze write complete -> $outputPath")
    spark.stop()
  }

  private def fetchRecords(state: String, commodity: String, statisticCat: String, startYear: Int, endYear: Int, apiKey: String): Seq[RawCropRecord] = {
    import sttp.client3._
    import io.circe.parser._
    val backend = HttpURLConnectionBackend()
    val baseUrl = usdaCfg.getString("base-url")
    val url     = uri"$baseUrl/api_GET/?key=$apiKey&commodity_desc=$commodity&statisticcat_desc=$statisticCat&agg_level_desc=COUNTY&state_alpha=$state&year__GE=$startYear&year__LE=$endYear&format=JSON"
    val response = basicRequest.get(url).send(backend)
    response.body match {
      case Right(body) =>
        parse(body).flatMap(_.hcursor.downField("data").as[List[io.circe.Json]]) match {
          case Right(rows) =>
            rows.map { json =>
              val c = json.hcursor
              RawCropRecord(
                year         = c.get[String]("year").getOrElse(""),
                state        = c.get[String]("state_alpha").toOption,
                countyName   = c.get[String]("county_name").toOption,
                countyFips   = c.get[String]("county_ansi").toOption,
                commodity    = c.get[String]("commodity_desc").toOption,
                statisticCat = c.get[String]("statisticcat_desc").toOption,
                unit         = c.get[String]("unit_desc").toOption,
                value        = c.get[String]("Value").toOption
              )
            }
          case Left(err) =>
            logger.warn(s"USDA parse error: $err")
            Seq.empty
        }
      case Left(err) =>
        logger.warn(s"USDA API error: $err")
        Seq.empty
    }
  }
}
