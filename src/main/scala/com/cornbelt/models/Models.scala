package com.cornbelt.models

import java.time.LocalDate

// â”€â”€â”€ BRONZE models â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// These mirror the raw shapes coming off the APIs â€” everything is Option
// because we can't trust that the source will always send every field.

/** One row from NOAA GHCND daily endpoint */
case class RawWeatherObservation(
    stationId:   String,
    date:        String,          // raw "YYYY-MM-DD" string â€” parsed in Silver
    dataType:    String,          // e.g. "TMAX", "TMIN", "PRCP"
    value:       Option[Double],  // tenths of Â°C for temp; tenths of mm for precip
    attributes:  Option[String]   // NOAA quality flags
)

/** One row from USDA NASS QuickStats */
case class RawCropRecord(
    year:          String,
    state:         Option[String],
    countyName:    Option[String],
    countyFips:    Option[String],
    commodity:     Option[String], // "CORN", "SOYBEANS", "WHEAT"
    statisticCat:  Option[String], // "YIELD", "PRICE RECEIVED", "AREA HARVESTED"
    unit:          Option[String],
    value:         Option[String]  // raw string â€” may contain "(D)" for suppressed data
)

// â”€â”€â”€ SILVER models â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Clean, typed, validated. The compiler enforces these shapes throughout Gold.

/** Daily weather observation with proper types and units in US customary */
case class WeatherObservation(
    stationId:       String,
    date:            LocalDate,
    tempMaxF:        Option[Double],  // °F
    tempMinF:        Option[Double],  // °F
    tempAvgF:        Option[Double],  // °F — (tempMaxF + tempMinF) / 2
    precipIn:        Option[Double],  // inches
    snowIn:          Option[Double],  // inches
    snowDepthIn:     Option[Double],  // inches
    qualityFlag:     String,          // "OK", "SUSPECT", "MISSING"
    tempSource:      String,          // "anchor" | "supplemental" | "pool" | "mixed"
    tempStationId:   Option[String],  // GHCND ID of station that provided TMAX, or "pool_median", or null
    tempStationName: Option[String],  // human-readable name of that station
    month:           Int,             // calendar month 1–12
    season:          String,          // "Spring" | "Summer" | "Autumn" | "Winter" (astronomical)
    cornPlanting:    Boolean,         // Apr 20 – May 15
    cornGrowing:     Boolean,         // May 1 – Sep 30
    cornHarvest:     Boolean,         // Sep 1 – Oct 31
    soybeanPlanting: Boolean,         // May 1 – Jun 10
    soybeanGrowing:  Boolean,         // May 1 – Oct 31
    soybeanHarvest:  Boolean,         // Sep 20 – Oct 31
    wheatPlanting:   Boolean,         // Sep 20 – Oct 31
    wheatGrowing:    Boolean,         // Sep 20 – Jun 30 (overwinters — wraps calendar year)
    wheatHarvest:    Boolean          // Jun 1 – Jul 10
)

/** Annual crop record â€” suppressed USDA values become None */
case class CropRecord(
    year:          Int,
    stateCode:     String,
    countyFips:    String,
    crop:          Crop,
    yieldBuAcre:   Option[Double], // bushels / acre
    priceUsdBu:    Option[Double], // USD / bushel  (NASS "price received")
    areaHarvested: Option[Double]  // acres
)

// â”€â”€â”€ GOLD models â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

/** Growing-season weather summary for one county-year */
case class SeasonWeatherSummary(
    year:                    Int,
    countyFips:              String,
    stateCode:               String,
    // General Apr–Nov season
    growingDegreeDays:       Double,
    totalPrecipIn:           Double,
    frostFreeDays:           Int,
    extremeHeatDays:         Int,
    droughtIndex:            Double,
    // Crop growing seasons
    cornGrowingGdd:          Double,
    cornGrowingPrecipIn:     Double,
    soybeanGrowingGdd:       Double,
    soybeanGrowingPrecipIn:  Double,
    wheatGrowingGdd:         Double,
    wheatGrowingPrecipIn:    Double,
    // Crop planting seasons
    cornPlantingGdd:         Double,
    cornPlantingPrecipIn:    Double,
    soybeanPlantingGdd:      Double,
    soybeanPlantingPrecipIn: Double,
    wheatPlantingGdd:        Double,
    wheatPlantingPrecipIn:   Double,
    // Crop harvest seasons
    cornHarvestGdd:          Double,
    cornHarvestPrecipIn:     Double,
    soybeanHarvestGdd:       Double,
    soybeanHarvestPrecipIn:  Double,
    wheatHarvestGdd:         Double,
    wheatHarvestPrecipIn:    Double,
    // Calendar month GDD and precip (jan–dec)
    janGdd: Double, janPrecipIn: Double,
    febGdd: Double, febPrecipIn: Double,
    marGdd: Double, marPrecipIn: Double,
    aprGdd: Double, aprPrecipIn: Double,
    mayGdd: Double, mayPrecipIn: Double,
    junGdd: Double, junPrecipIn: Double,
    julGdd: Double, julPrecipIn: Double,
    augGdd: Double, augPrecipIn: Double,
    sepGdd: Double, sepPrecipIn: Double,
    octGdd: Double, octPrecipIn: Double,
    novGdd: Double, novPrecipIn: Double,
    decGdd: Double, decPrecipIn: Double,
    // Crop phase frost-free, heat stress, drought index
    cornPlantingFrostFreeDays:    Int, cornPlantingHeatStressDays:    Int, cornPlantingDroughtIndex:    Double,
    cornGrowingFrostFreeDays:     Int, cornGrowingHeatStressDays:     Int, cornGrowingDroughtIndex:     Double,
    cornHarvestFrostFreeDays:     Int, cornHarvestHeatStressDays:     Int, cornHarvestDroughtIndex:     Double,
    soybeanPlantingFrostFreeDays: Int, soybeanPlantingHeatStressDays: Int, soybeanPlantingDroughtIndex: Double,
    soybeanGrowingFrostFreeDays:  Int, soybeanGrowingHeatStressDays:  Int, soybeanGrowingDroughtIndex:  Double,
    soybeanHarvestFrostFreeDays:  Int, soybeanHarvestHeatStressDays:  Int, soybeanHarvestDroughtIndex:  Double,
    wheatPlantingFrostFreeDays:   Int, wheatPlantingHeatStressDays:   Int, wheatPlantingDroughtIndex:   Double,
    wheatGrowingFrostFreeDays:    Int, wheatGrowingHeatStressDays:    Int, wheatGrowingDroughtIndex:    Double,
    wheatHarvestFrostFreeDays:    Int, wheatHarvestHeatStressDays:    Int, wheatHarvestDroughtIndex:    Double,
    // Calendar month frost-free, heat stress, drought index (jan–dec)
    janFrostFreeDays: Int, janHeatStressDays: Int, janDroughtIndex: Double,
    febFrostFreeDays: Int, febHeatStressDays: Int, febDroughtIndex: Double,
    marFrostFreeDays: Int, marHeatStressDays: Int, marDroughtIndex: Double,
    aprFrostFreeDays: Int, aprHeatStressDays: Int, aprDroughtIndex: Double,
    mayFrostFreeDays: Int, mayHeatStressDays: Int, mayDroughtIndex: Double,
    junFrostFreeDays: Int, junHeatStressDays: Int, junDroughtIndex: Double,
    julFrostFreeDays: Int, julHeatStressDays: Int, julDroughtIndex: Double,
    augFrostFreeDays: Int, augHeatStressDays: Int, augDroughtIndex: Double,
    sepFrostFreeDays: Int, sepHeatStressDays: Int, sepDroughtIndex: Double,
    octFrostFreeDays: Int, octHeatStressDays: Int, octDroughtIndex: Double,
    novFrostFreeDays: Int, novHeatStressDays: Int, novDroughtIndex: Double,
    decFrostFreeDays: Int, decHeatStressDays: Int, decDroughtIndex: Double,
    // Average daily temperature °F — mean of (TMAX+TMIN)/2 across qualifying days in the period
    avgTempF:                Double,
    cornGrowingAvgTempF:     Double, soybeanGrowingAvgTempF:     Double, wheatGrowingAvgTempF:     Double,
    cornPlantingAvgTempF:    Double, soybeanPlantingAvgTempF:    Double, wheatPlantingAvgTempF:    Double,
    cornHarvestAvgTempF:     Double, soybeanHarvestAvgTempF:     Double, wheatHarvestAvgTempF:     Double,
    janAvgTempF: Double, febAvgTempF: Double, marAvgTempF: Double, aprAvgTempF: Double,
    mayAvgTempF: Double, junAvgTempF: Double, julAvgTempF: Double, augAvgTempF: Double,
    sepAvgTempF: Double, octAvgTempF: Double, novAvgTempF: Double, decAvgTempF: Double,
    // Total snowfall inches across the period (from silver snowIn — NOAA SNOW element)
    totalSnowIn:             Double,
    cornGrowingSnowIn:       Double, soybeanGrowingSnowIn:       Double, wheatGrowingSnowIn:       Double,
    cornPlantingSnowIn:      Double, soybeanPlantingSnowIn:      Double, wheatPlantingSnowIn:      Double,
    cornHarvestSnowIn:       Double, soybeanHarvestSnowIn:       Double, wheatHarvestSnowIn:       Double,
    janSnowIn: Double, febSnowIn: Double, marSnowIn: Double, aprSnowIn: Double,
    maySnowIn: Double, junSnowIn: Double, julSnowIn: Double, augSnowIn: Double,
    sepSnowIn: Double, octSnowIn: Double, novSnowIn: Double, decSnowIn: Double
)

/** Profitability estimate for one crop in one county-year */
case class CropProfitability(
    year:           Int,
    countyFips:     String,
    stateCode:      String,
    crop:           Crop,
    revenueUsdAcre: Option[Double], // yield Ã— price
    yieldBuAcre:    Option[Double],
    priceUsdBu:     Option[Double]
)

/** Optimal crop mix recommendation for a county-year based on weather + price */
case class OptimalMix(
    year:             Int,
    countyFips:       String,
    stateCode:        String,
    recommendedCrop:  Crop,
    cornScore:        Double,
    soybeanScore:     Double,
    wheatScore:       Double,
    topCropRevenue:   Option[Double],
    weatherSummary:   SeasonWeatherSummary
)

// â”€â”€â”€ Enum-style sealed trait for crop type â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// Sealed means the compiler knows ALL possible cases â€” no surprises at runtime.

sealed trait Crop { def nassName: String }
object Crop {
  case object Corn     extends Crop { val nassName = "CORN"     }
  case object Soybeans extends Crop { val nassName = "SOYBEANS" }
  case object Wheat    extends Crop { val nassName = "WHEAT"    }

  val all: List[Crop] = List(Corn, Soybeans, Wheat)

  def fromString(s: String): Option[Crop] = s.toUpperCase.trim match {
    case "CORN"                  => Some(Corn)
    case "SOYBEANS" | "SOYBEAN" => Some(Soybeans)
    case "WHEAT"                 => Some(Wheat)
    case _                       => None
  }
}
