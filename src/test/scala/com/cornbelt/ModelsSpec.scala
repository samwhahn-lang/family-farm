package com.cornbelt

import com.cornbelt.models.Crop
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import java.time.LocalDate

class ModelsSpec extends AnyFunSuite with Matchers {

  test("Crop.fromString handles all valid commodity names") {
    Crop.fromString("CORN")     shouldBe Some(Crop.Corn)
    Crop.fromString("corn")     shouldBe Some(Crop.Corn)
    Crop.fromString("SOYBEANS") shouldBe Some(Crop.Soybeans)
    Crop.fromString("SOYBEAN")  shouldBe Some(Crop.Soybeans)
    Crop.fromString("WHEAT")    shouldBe Some(Crop.Wheat)
  }

  test("Crop.fromString returns None for unknown commodities") {
    Crop.fromString("COTTON") shouldBe None
    Crop.fromString("")       shouldBe None
  }

  test("Crop.all contains exactly three crops") {
    Crop.all should have size 3
    Crop.all should contain allOf (Crop.Corn, Crop.Soybeans, Crop.Wheat)
  }

  test("Revenue per acre equals yield times price") {
    val yld    = Some(180.0)
    val price  = Some(4.50)
    val result = for { y <- yld; p <- price } yield y * p
    result shouldBe Some(810.0)
  }

  test("Revenue is None when yield is suppressed") {
    val result = for { y <- Option.empty[Double]; p <- Some(4.50) } yield y * p
    result shouldBe None
  }

  test("parseValue converts suppressed USDA values to None") {
    parseUsdaValue(Some("(D)")) shouldBe None
    parseUsdaValue(Some("(Z)")) shouldBe None
    parseUsdaValue(Some(""))    shouldBe None
    parseUsdaValue(None)        shouldBe None
  }

  test("parseValue correctly parses numeric strings with commas") {
    parseUsdaValue(Some("1,234.5")) shouldBe Some(1234.5)
    parseUsdaValue(Some("56.8"))    shouldBe Some(56.8)
  }

  private def parseUsdaValue(raw: Option[String]): Option[Double] =
    raw.map(_.trim.replaceAll(",", "")).filterNot(v => v.isEmpty || v.startsWith("(")).flatMap(_.toDoubleOption)
}
