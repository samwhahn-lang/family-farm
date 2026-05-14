package com.cornbelt.gold

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Integration tests — require SeasonWeatherJob to have run first.
 *  Run with: sbt test
 *  These guard against silent bad-data bugs (unit drift, empty tables, nulls).
 */
class SeasonWeatherSpec extends AnyFunSuite with Matchers {

  private lazy val spark   = SparkSessionProvider.session
  private lazy val config  = ConfigFactory.load().getConfig("corn-belt")
  private lazy val goldPath = s"${config.getConfig("paths").getString("gold")}/season_weather"

  private lazy val rows = {
    import spark.implicits._
    spark.read.format("delta").load(goldPath).collect()
  }

  test("gold season_weather table is non-empty") {
    rows.length should be > 0
  }

  test("every row has a valid year") {
    rows.foreach { r =>
      val yr = r.getAs[Int]("year")
      yr should (be >= 1990 and be <= 2030)
    }
  }

  test("growingDegreeDays is positive and within plausible range for Nebraska") {
    // A Nebraska growing season (Apr–Nov) accumulates roughly 800–2200 GDD base 10°C
    rows.foreach { r =>
      val gdd = r.getAs[Double]("growingDegreeDays")
      gdd should (be > 800.0 and be < 2500.0)
    }
  }

  test("totalPrecipMm is positive — rules out divide-by-10 unit error") {
    // Growing season precip for Beatrice NE is typically 300–700 mm
    // If we accidentally stored tenths-of-mm, values would be 3000–7000 — caught here
    rows.foreach { r =>
      val p = r.getAs[Double]("totalPrecipMm")
      p should (be > 50.0 and be < 1200.0)
    }
  }

  test("frostFreeDays is a plausible count for Apr–Nov window") {
    // Apr–Nov = ~214 days; most should be frost-free in Nebraska
    rows.foreach { r =>
      val d = r.getAs[Int]("frostFreeDays")
      d should (be > 100 and be <= 214)
    }
  }

  test("extremeHeatDays is non-negative and does not exceed growing season length") {
    rows.foreach { r =>
      val d = r.getAs[Int]("extremeHeatDays")
      d should (be >= 0 and be < 120)
    }
  }

  test("droughtIndex is a normalised ratio — not raw precip totals") {
    // If the formula broke and stored raw mm instead, values would be 300–700, not −1..1
    rows.foreach { r =>
      val di = r.getAs[Double]("droughtIndex")
      di should (be > -2.0 and be < 2.0)
    }
  }

  test("no null values in required fields") {
    rows.foreach { r =>
      r.getAs[Any]("year")              should not be null
      r.getAs[Any]("growingDegreeDays") should not be null
      r.getAs[Any]("totalPrecipMm")     should not be null
      r.getAs[Any]("frostFreeDays")     should not be null
      r.getAs[Any]("extremeHeatDays")   should not be null
      r.getAs[Any]("droughtIndex")      should not be null
    }
  }

  test("countyFips and stateCode are populated from anchor config") {
    rows.foreach { r =>
      r.getAs[String]("countyFips") should not be empty
      r.getAs[String]("stateCode")  should not be empty
    }
  }
}
