# Pipeline Reference — Gage County Crop Analysis

End-to-end guide covering every job in the project: what it does, how data flows through it, and what it produces. Written for both a human reader and a new agent picking up this codebase cold.

---

## Table of Contents

1. [Project overview](#1-project-overview)
2. [How to run](#2-how-to-run)
3. [Configuration](#3-configuration)
4. [Data model at each layer](#4-data-model-at-each-layer)
5. [Bronze layer — raw ingest](#5-bronze-layer--raw-ingest)
6. [Silver layer — clean and transform](#6-silver-layer--clean-and-transform)
7. [Gold layer — growing season aggregations](#7-gold-layer--growing-season-aggregations)
8. [Platinum layer — dashboard export](#8-platinum-layer--dashboard-export)
9. [Pipeline orchestrators](#9-pipeline-orchestrators)
10. [Utility and audit jobs](#10-utility-and-audit-jobs)
11. [Tests](#11-tests)
12. [Flags: dead code, orphaned jobs, and inconsistencies](#12-flags-dead-code-orphaned-jobs-and-inconsistencies)

---

## 1. Project overview

**Central question:** Given NOAA daily weather observations from 2000 to present at Beatrice, Nebraska, how do growing conditions vary year-over-year for corn, soybeans, and winter wheat in Gage County (FIPS 31067)?

The pipeline is a **four-layer medallion architecture** running on Apache Spark with Delta Lake storage:

```
NOAA API
   │
   ▼
Bronze  (raw long-format daily observations, one row per measurement per day)
   │
   ▼
Silver  (wide-format, typed, quality-flagged, with crop phase boolean columns)
   │
   ▼
Gold    (one row per year — growing season aggregates: GDD, precip, temp, snow, drought)
   │
   ▼
Platinum  (self-contained HTML dashboard with Plotly.js charts)
```

**Anchor station:** `GHCND:USC00250622` — "Beatrice 1 N, NE US" (40.27°N, 96.75°W).

**Known data gap:** This station reports TMAX/TMIN only through March 2013. After that it became precipitation-only. Pool medians from other Gage County stations partially backfill temperature for post-2013 years. See `docs/known-issues.md`.

---

## 2. How to run

```bash
# Full pipeline from scratch (bronze backfill + silver + gold + platinum)
sbt "runMain com.cornbelt.pipeline.RunAll"

# Selective run — always executes in dependency order
sbt "runMain com.cornbelt.pipeline.CustomRun silver gold platinum"
sbt "runMain com.cornbelt.pipeline.CustomRun bronze-update silver gold platinum"

# Individual jobs
sbt "runMain com.cornbelt.bronze.IngestBackfill"      # first-time bronze seed
sbt "runMain com.cornbelt.bronze.IngestUpdate"        # incremental refresh to today
sbt "runMain com.cornbelt.silver.WeatherTransformJob"
sbt "runMain com.cornbelt.gold.SeasonWeatherJob"
sbt "runMain com.cornbelt.platinum.PlatinumExportJob"
```

**Required environment variable:**
```
NOAA_TOKEN=<your NCEI CDO token>   # free at https://www.ncdc.noaa.gov/cdo-web/token
```

**Never use `sbt console`** — Spark JVM flags only apply to forked runs.

---

## 3. Configuration

All runtime values live in `src/main/resources/application.conf` under the `corn-belt` namespace.

| Key | Value | Used by |
|---|---|---|
| `anchor.station-id` | `GHCND:USC00250622` | Silver, Gold |
| `anchor.county-fips` | `31067` | Bronze ingest, Gold |
| `anchor.state` | `NE` | Gold |
| `analysis.start-year` | `2000` | Bronze ingest |
| `analysis.end-year` | `2026` | Bronze ingest |
| `noaa.base-url` | `https://www.ncei.noaa.gov/access/services/data/v1` | Bronze |
| `noaa.dataset-id` | `daily-summaries` | Bronze |
| `noaa.data-types` | `[TMAX, TMIN, PRCP, SNOW, SNWD]` | Bronze |
| `paths.bronze` | `data/bronze` | All layers |
| `paths.silver` | `data/silver` | Silver, Gold |
| `paths.gold` | `data/gold` | Gold, Platinum |
| `spark.master` | `local[*]` | SparkSessionProvider |

> `paths.raw = "data/raw"` is configured but never used by any job.

---

## 4. Data model at each layer

### Bronze — `RawWeatherObservation`
Long-format. One row per measurement per station per day.

| Field | Type | Notes |
|---|---|---|
| `stationId` | String | e.g. `GHCND:USC00250622` |
| `date` | String | `YYYY-MM-DD` — kept as string, parsed in Silver |
| `dataType` | String | `TMAX`, `TMIN`, `PRCP`, `SNOW`, or `SNWD` |
| `value` | Option[Double] | °C for temps, mm for precip/snow. Null when station didn't report |
| `attributes` | Option[String] | NOAA quality flags: `meas,quality,source[,time]` |

Each day produces up to **5 rows** — one per data type — for each station.

### Silver — `WeatherObservation`
Wide-format. One row per station per day. All five measurements are columns.

| Field | Type | Notes |
|---|---|---|
| `stationId` | String | Anchor station ID |
| `date` | LocalDate | Parsed from bronze string |
| `tempMaxC` | Option[Double] | Coalesced: anchor TMAX → pool median |
| `tempMinC` | Option[Double] | Coalesced: anchor TMIN → pool median |
| `tempAvgC` | Option[Double] | `(tempMaxC + tempMinC) / 2` |
| `precipMm` | Option[Double] | Anchor PRCP |
| `snowMm` | Option[Double] | Anchor SNOW |
| `snowDepthMm` | Option[Double] | Anchor SNWD |
| `qualityFlag` | String | `OK`, `SUSPECT`, or `MISSING` |
| `tempSource` | String | `source`, `derived`, or mixed |
| `month` | Int | 1–12 |
| `season` | String | `Spring`, `Summer`, `Autumn`, or `Winter` (astronomical) |
| `cornPlanting` | Boolean | Apr 20 – May 15 |
| `cornGrowing` | Boolean | May 1 – Sep 30 |
| `cornHarvest` | Boolean | Sep 1 – Oct 31 |
| `soybeanPlanting` | Boolean | May 1 – Jun 10 |
| `soybeanGrowing` | Boolean | May 1 – Oct 31 |
| `soybeanHarvest` | Boolean | Sep 20 – Oct 31 |
| `wheatPlanting` | Boolean | Sep 20 – Oct 31 |
| `wheatGrowing` | Boolean | Sep 20 – Jun 30 (overwinters across calendar year) |
| `wheatHarvest` | Boolean | Jun 1 – Jul 10 |

### Gold — `SeasonWeatherSummary`
One row per year. Contains ~157 aggregate columns across four groups:

| Group | Fields | Example |
|---|---|---|
| General growing season (Apr–Nov) | GDD, precip, avg temp, snow, frost-free days, heat stress days, drought index | `growingDegreeDays`, `totalPrecipMm`, `avgTempC` |
| Per crop phase (9 phases × 7 metrics) | GDD, precip, avg temp, snow, frost-free, heat stress, drought | `cornGrowingGdd`, `wheatPlantingSnowMm` |
| Calendar month (12 months × 7 metrics) | Same metrics per month | `julAvgTempC`, `janSnowMm` |
| Identifiers | Year, county, state | `year`, `countyFips`, `stateCode` |

### Platinum — `platinum/index.html`
Static self-contained HTML file. All gold data is embedded as a JavaScript array literal. No server required — open in any browser.

---

## 5. Bronze layer — raw ingest

### 5.1 `IngestBackfill` ⭐ Primary bronze job

**What it does:** Seeds or fully refreshes the bronze Delta table. Fetches all years from `start-year` to `end-year` for every GHCND station in Gage County. Overwrites the entire table.

**When to run:** First time setup, or when you need a complete refresh of all historical data.

**Steps:**

1. Read `NOAA_TOKEN` from environment. Fail fast if absent.
2. Call NOAA CDO API to list all GHCND stations in Gage County (FIPS 31067).
   ```
   GET https://www.ncdc.noaa.gov/cdo-web/api/v2/stations
     ?datasetid=GHCND
     &locationid=FIPS:31067
     &startdate=2000-01-01
     &enddate=2026-12-31
     &limit=1000
   ```
3. For each station × year, call the NCEI Data Services API (no auth needed):
   ```
   GET https://www.ncei.noaa.gov/access/services/data/v1
     ?dataset=daily-summaries
     &stations=USC00250622
     &startDate=2000-01-01
     &endDate=2000-12-31
     &dataTypes=TMAX,TMIN,PRCP,SNOW,SNWD
     &units=metric
     &format=json
   ```
4. The API returns a **wide** JSON array (one object per day). The job converts each day into **5 long rows** (one per data type):
   ```
   -- conceptually, for one day of wide API response:
   SELECT station, date, 'TMAX' as dataType, TMAX as value FROM api_row
   UNION ALL
   SELECT station, date, 'TMIN' as dataType, TMIN as value FROM api_row
   -- ... repeat for PRCP, SNOW, SNWD
   ```
5. Write all observations to Delta Lake with `SaveMode.Overwrite`, partitioned by `stationId`.
6. Write a 100-row preview CSV to `data/bronze/preview_top100.csv`.

**Output path:** `data/bronze/noaa_observations/`
**Write mode:** Overwrite (full replace)
**Partition by:** `stationId`

---

### 5.2 `IngestUpdate` — Incremental refresh

**What it does:** Finds the most recent date already in bronze, then fetches only the gap from `(max_date + 1)` through today and **appends** it. Safe to re-run; a second same-day run finds nothing new and exits cleanly.

**When to run:** Routine refreshes after the initial backfill is done.

**Steps:**

1. Read `max(date)` from the bronze Delta table.
2. If `max_date + 1 >= today`, log "already current" and return.
3. Discover stations active in the new date window via CDO API.
4. Fetch the gap year-by-year per station using `IngestBackfill.fetchDateRange()`.
5. Append new rows: `ds.write.format("delta").mode(SaveMode.Append).save(outputPath)`.

**Output path:** `data/bronze/noaa_observations/` (append only)

---

### 5.3 `AbridgedIngestAddStation` — Single-station patch

**What it does:** Fetches one specific station for all analysis years and merges it into bronze using Delta's `replaceWhere` — overwrites only that station's partition without touching any other station's data. Safe to re-run.

**When to run:** When adding a new supplemental temperature station to the bronze pool.

```bash
sbt "runMain com.cornbelt.bronze.AbridgedIngestAddStation USC00250622"
# Pass the bare NCEI station ID; GHCND: prefix is optional
```

**Output path:** `data/bronze/noaa_observations/` (replaceWhere stationId)

---

## 6. Silver layer — clean and transform

### `WeatherTransformJob`

**What it does:** Reads the raw long-format bronze table, pivots it wide, applies quality flags, fills missing temperature via pool medians from other stations, adds derived fields, and writes a clean wide table.

**Input:** `data/bronze/noaa_observations/` (all stations)
**Output:** `data/silver/weather_observations/` (anchor station only, wide format)

**Steps in plain English:**

**Step 1 — Load bronze and isolate the anchor station**
```sql
-- Full bronze pool (all stations) — needed for the pool median fallback
SELECT * FROM bronze.noaa_observations;

-- Anchor rows only
SELECT * FROM bronze.noaa_observations
WHERE stationId = 'GHCND:USC00250622';
```

**Step 2 — Flag suspect readings**

The NOAA `attributes` column contains comma-separated flags. The second token is the quality flag — any non-blank character means the reading is suspect.
```
attributes = ",,S," → quality flag is "S" → suspect = true
attributes = ",,,"  → quality flag is "" → suspect = false
```

**Step 3 — Compute per-day quality summary across all data types**
```sql
SELECT stationId, date,
  MAX(suspect)            AS anySuspect,
  COUNT(value) FILTER (WHERE value IS NOT NULL) AS presentCount
FROM anchor_with_suspect_flag
GROUP BY stationId, date;
-- qualityFlag = CASE WHEN presentCount = 0 THEN 'MISSING'
--                    WHEN anySuspect        THEN 'SUSPECT'
--                    ELSE 'OK' END
```

**Step 4 — Pivot long → wide for the anchor station**
```sql
SELECT stationId, date,
  MAX(CASE WHEN dataType = 'TMAX' THEN value END) AS TMAX,
  MAX(CASE WHEN dataType = 'TMIN' THEN value END) AS TMIN,
  MAX(CASE WHEN dataType = 'PRCP' THEN value END) AS PRCP,
  MAX(CASE WHEN dataType = 'SNOW' THEN value END) AS SNOW,
  MAX(CASE WHEN dataType = 'SNWD' THEN value END) AS SNWD
FROM anchor_raw
GROUP BY stationId, date;
```

**Step 5 — Compute pool medians as a temperature fallback**

When the anchor station has no TMAX or TMIN (as happens after March 2013), the median across all other Gage County stations on the same date is used instead.
```sql
SELECT date,
  PERCENTILE_APPROX(CASE WHEN dataType='TMAX' THEN value END, 0.5) AS poolTMAX,
  PERCENTILE_APPROX(CASE WHEN dataType='TMIN' THEN value END, 0.5) AS poolTMIN
FROM bronze.noaa_observations   -- all stations
GROUP BY date;
```

**Step 6 — Coalesce anchor with pool, derive average temperature**
```sql
SELECT
  COALESCE(anchor.TMAX, pool.poolTMAX) AS tempMaxC,
  COALESCE(anchor.TMIN, pool.poolTMIN) AS tempMinC,
  (COALESCE(anchor.TMAX, pool.poolTMAX)
   + COALESCE(anchor.TMIN, pool.poolTMIN)) / 2.0  AS tempAvgC,
  anchor.PRCP AS precipMm,
  anchor.SNOW AS snowMm,
  anchor.SNWD AS snowDepthMm
FROM anchor_pivoted
LEFT JOIN pool_medians USING (date);
```

**Step 7 — Add temporal classification and crop phase booleans**

Each row gets a calendar month (1–12), an astronomical season (Spring/Summer/Autumn/Winter), and nine boolean flags for whether the date falls inside each crop phase. Example:
```sql
cornPlanting = (month = 4 AND day >= 20) OR (month = 5 AND day <= 15)
cornGrowing  = month BETWEEN 5 AND 9
cornHarvest  = month IN (9, 10)
wheatGrowing = (month >= 10) OR (month <= 6)   -- overwinters
```

**Output:** One row per day for the anchor station, 2000–2026, with all fields above. Written as Delta Lake, partitioned by `stationId`.

---

## 7. Gold layer — growing season aggregations

### `SeasonWeatherJob`

**What it does:** Reads clean silver data and collapses it to **one row per year** with ~157 aggregated metrics covering the growing season, each crop phase, and each calendar month.

**Input:** `data/silver/weather_observations/`
**Output:** `data/gold/season_weather/`

**Pre-filter:**
```sql
SELECT * FROM silver.weather_observations
WHERE stationId = 'GHCND:USC00250622'
  AND qualityFlag <> 'SUSPECT'
  AND tempMaxC IS NOT NULL
  AND tempMinC IS NOT NULL;
-- This filter is why gold only produces years with temperature data.
-- Years where the pool median also fails produce no gold row.
```

**GDD formula (base 10°C, corn standard, max capped at 30°C):**
```sql
gdd_per_day = GREATEST(0,
  (LEAST(tempMaxC, 30) + GREATEST(tempMinC, 10)) / 2.0 - 10
)
```

**Main aggregation — one GROUP BY produces everything:**
```sql
SELECT
  YEAR(date)                                                  AS yr,

  -- General growing season (Apr–Nov)
  ROUND(SUM(CASE WHEN month BETWEEN 4 AND 11 THEN gdd   ELSE 0 END), 1) AS growingDegreeDays,
  ROUND(SUM(CASE WHEN month BETWEEN 4 AND 11 THEN precipMm ELSE 0 END), 1) AS totalPrecipMm,
  ROUND(AVG(CASE WHEN month BETWEEN 4 AND 11 THEN tempAvgC END), 1)    AS avgTempC,
  ROUND(SUM(CASE WHEN month BETWEEN 4 AND 11 THEN COALESCE(snowMm,0) ELSE 0 END), 1) AS totalSnowMm,
  SUM(CASE WHEN month BETWEEN 4 AND 11 AND tempMinC > 0 THEN 1 ELSE 0 END) AS frostFreeDays,
  SUM(CASE WHEN month BETWEEN 4 AND 11 AND tempMaxC > 35 THEN 1 ELSE 0 END) AS extremeHeatDays,

  -- Corn growing phase (same pattern for all 9 crop phases)
  ROUND(SUM(CASE WHEN cornGrowing THEN gdd    ELSE 0 END), 1) AS cornGrowingGdd,
  ROUND(SUM(CASE WHEN cornGrowing THEN precipMm ELSE 0 END), 1) AS cornGrowingPrecipMm,
  ROUND(AVG(CASE WHEN cornGrowing THEN tempAvgC END), 1)        AS cornGrowingAvgTempC,
  ROUND(SUM(CASE WHEN cornGrowing THEN COALESCE(snowMm,0) END), 1) AS cornGrowingSnowMm,
  -- ... repeated for all 9 crop phases and 12 calendar months

  -- Calendar months (same 7 metrics each)
  ROUND(SUM(CASE WHEN month=7 THEN gdd ELSE 0 END), 1)  AS julGdd,
  ROUND(AVG(CASE WHEN month=7 THEN tempAvgC END), 1)    AS julAvgTempC,
  -- ...

FROM qualified_silver
GROUP BY YEAR(date);
```

**Drought index** — computed after the main aggregation via a window function over all years:
```sql
-- For each precip column (22 total: season, 9 crop phases, 12 months):
droughtIndex = ROUND(
  (precipMm - AVG(precipMm) OVER (all years)) / AVG(precipMm) OVER (all years),
4)
-- 0 = average year; negative = drier than average; positive = wetter
```

**Output:** Written as Delta Lake (no partition). Also writes `preview_top100.csv` and `row_count_by_year.csv` for quick inspection.

---

## 8. Platinum layer — dashboard export

### `PlatinumExportJob`

**What it does:** Reads the gold Delta table, serialises all rows into a JavaScript array literal, and injects it into a self-contained HTML template. The output file `platinum/index.html` is the final deliverable — open it in any browser, no server needed.

**Input:** `data/gold/season_weather/`
**Output:** `platinum/index.html`

**Serialisation (Scala → JavaScript):**
```scala
// Each gold row becomes one JS object literal:
{year:2000, growingDegreeDays:2183.7, totalPrecipMm:490.2,
 avgTempC:17.6, totalSnowMm:64.0, frostFreeDays:205, extremeHeatDays:32, droughtIndex:-0.1559,
 cornGrowingGdd:1817.8, cornGrowingPrecipMm:367.5, cornGrowingAvgTempC:22.8, ...
 janGdd:12.0, janPrecipMm:2.5, janAvgTempC:-1.7, janSnowMm:33.0, ...}
```

**Dashboard features:**

| Control | What it does |
|---|---|
| Year range filter | Slices `DATA` to the selected window |
| Crop season buttons | Switches all chart/table/stats to the crop phase's metric columns |
| Calendar month buttons | Switches to monthly metric columns |
| Metric chips | Toggle individual series on/off in the chart |
| Line / Bar toggle | Switches Plotly trace type |
| Stats cards | Shows period average, min, max for each active metric |
| Data table | Shows year-by-year values and a Conditions tag (Normal / Watch / Stress) |

**Available metrics in the chart:**

| Metric | Field key | Axis |
|---|---|---|
| GDD | `{phase}Gdd` | Left (y) |
| Precipitation | `{phase}PrecipMm` | Right (y2) |
| Avg Temperature | `{phase}AvgTempC` | Left (y) |
| Snowfall | `{phase}SnowMm` | Right (y2) |
| Frost-Free Days | `{phase}FrostFreeDays` | Left (y) |
| Heat Stress Days | `{phase}HeatStressDays` | Left (y) |
| Drought Index | `{phase}DroughtIndex` | Right (y2) |

Default on page load: **Avg Temperature** across All Season (Apr–Nov).

---

## 9. Pipeline orchestrators

### `RunAll`

Runs the full pipeline in order: `IngestBackfill → WeatherTransformJob → SeasonWeatherJob → PlatinumExportJob`. Stops Spark exactly once at the end.

```bash
sbt "runMain com.cornbelt.pipeline.RunAll"
```

### `CustomRun`

Runs any subset of pipeline stages, always in dependency order. Accepts stage names as arguments.

```bash
sbt "runMain com.cornbelt.pipeline.CustomRun bronze-update silver gold platinum"
```

| Stage name | Job |
|---|---|
| `bronze` | `IngestBackfill` — full historical overwrite |
| `bronze-update` | `IngestUpdate` — incremental append to today |
| `silver` | `WeatherTransformJob` |
| `gold` | `SeasonWeatherJob` |
| `platinum` | `PlatinumExportJob` |

### `SparkSessionProvider`

A lazy singleton. All jobs call `SparkSessionProvider.session` to get the shared `SparkSession`. **Never call `spark.stop()` inside individual jobs** — only the orchestrators (`RunAll`, `CustomRun`) stop Spark, once, at the very end. Calling `stop()` mid-pipeline leaves a dead session that causes NPEs.

---

## 10. Utility and audit jobs

These are standalone tools — **not part of any pipeline run**. Run manually as needed.

### `GageCountyStationsJob`

Lists every GHCND station in Gage County via the CDO API, then counts how many rows per station per year the NCEI API returns. Useful for selecting a new anchor station or auditing data coverage.

```bash
sbt "runMain com.cornbelt.bronze.GageCountyStationsJob"
```

Output: `data/bronze/gage_county_station_row_count.csv`
No Spark required — pure HTTP calls.

### `VerifyBronzeJob`

Reads the bronze Delta table and pivots it to show non-null value counts per station per year per data type. Reveals years where TMAX/TMIN went missing.

```bash
sbt "runMain com.cornbelt.bronze.VerifyBronzeJob"
```

Output: `data/bronze/row_count_by_year.csv`

### `VerifySilverJob`

Samples 20 rows from each silver Delta table and writes them to `data/silver/silver_sample_data.csv` for quick human inspection.

```bash
sbt "runMain com.cornbelt.silver.VerifySilverJob"
```

### `DiagramJob`

Generates `platinum/diagrams/bronze.html` — a visual architecture diagram of the bronze layer showing the API request shape, ingest logic, and output schema. Reads live config values so it stays accurate when config changes.

```bash
sbt "runMain com.cornbelt.docs.DiagramJob"
```

---

## 11. Tests

### `ModelsSpec` (unit tests)

Pure unit tests — no Spark, no file I/O. Runs fast.

| Test | What it checks |
|---|---|
| `Crop.fromString` | All valid commodity name variants resolve correctly |
| `Crop.fromString` | Unknown commodities return None |
| `Crop.all` | Exactly three crops defined |
| Revenue formula | `yield × price` math |
| Suppressed revenue | Revenue is None when yield is None |
| `parseValue "(D)"` | USDA suppressed codes become None |
| `parseValue "1,234.5"` | Comma-formatted numeric strings parse correctly |

### `SeasonWeatherSpec` (integration tests)

**Requires the gold Delta table to already exist** (run `SeasonWeatherJob` first). Validates the gold output against known plausible ranges for Beatrice, NE:

| Test | Guard against |
|---|---|
| Table is non-empty | Silent empty write |
| Valid year range (1990–2030) | Year column corruption |
| GDD in 800–2500 | Unit drift, missing season |
| Precip in 50–1200 mm | Divide-by-10 error (tenths of mm stored as mm) |
| Frost-free days 100–214 | Logic error in TMIN > 0 filter |
| Heat stress days 0–120 | TMAX threshold bug |
| Drought index in −2.0..2.0 | Raw precip stored instead of ratio |
| No nulls in required fields | Encoder coalesce not applied |
| countyFips/stateCode non-empty | Config not wired to lit() |

Run with: `sbt test`

---

## 12. Flags: dead code, orphaned jobs, and inconsistencies

### ✅ Resolved — `NoaaIngestJob` deleted

Was a near-duplicate of `IngestBackfill` writing to the same output path with `SaveMode.Overwrite`, not wired into any orchestrator. File deleted; `DiagramJob` HTML label updated to `IngestBackfill`.

---

### 🔴 Orphaned pipeline — `UsdaIngestJob` + `CropTransformJob`

**Files:**
- `src/main/scala/com/cornbelt/bronze/UsdaIngestJob.scala`
- `src/main/scala/com/cornbelt/silver/CropTransformJob.scala`

These two jobs form a complete USDA NASS crop yield data pipeline:

- `UsdaIngestJob` calls the USDA QuickStats API to fetch yield, price, and area harvested for corn/soy/wheat across 8 states. Requires a `USDA_NASS_KEY` environment variable (not documented in CLAUDE.md).
- `CropTransformJob` reads the USDA bronze output, pivots it wide (YIELD, PRICE RECEIVED, AREA HARVESTED become columns), and writes `data/silver/crop_records/`.

Neither job is referenced in `RunAll`, `CustomRun`, or any gold/platinum job. The `SeasonWeatherSummary` model has no yield or price fields. The dashboard doesn't display crop economics. The `CropRecord`, `CropProfitability`, and `OptimalMix` models in `Models.scala` are defined but never populated by any active job.

**Recommendation:** Either wire this pipeline into the main flow (gold job joining weather + yield → profitability score) or flag it clearly as work-in-progress and document that `USDA_NASS_KEY` is required to run it.

---

### 🟡 Dangerous default — `AbridgedIngestAddStation`

**File:** `src/main/scala/com/cornbelt/bronze/AbridgedIngestAddStation.scala`

The default station when no argument is passed is `USC00050945` — the old station documented in `known-issues.md` as having genuine data gaps in 2012–2013 and having been retired from use. A developer running this job without an argument would silently overwrite the anchor station's partition with bad data.

**Recommendation:** Remove the default. Fail with a usage error if no station argument is provided.

---

### 🟡 Stale hardcoded value — `DiagramJob`

**File:** `src/main/scala/com/cornbelt/docs/DiagramJob.scala`, line 33

```scala
val skipYears = "2020 – 2024"
```

No years are skipped in the actual ingest jobs. This value is hardcoded and wrong. The diagram reads all other config values dynamically, making this inconsistency misleading.

Additionally, `DiagramJob` creates its own `SparkSession` inline (not via `SparkSessionProvider`). If called within a multi-job pipeline run, a second SparkSession would be created — and then stopped — leaving `SparkSessionProvider.session` in a broken state. This is safe only when run standalone.

**Recommendation:** Remove `skipYears` or derive it from config. Use `SparkSessionProvider.session` instead of an inline `SparkSession.builder`.

---

### 🟡 Stale sample file — `data/bronze/noaa_sample.csv`

This CSV contains rows for station `GHCND:USC00050945` — the old, retired station. The current anchor is `GHCND:USC00250622`. The sample doesn't reflect what bronze actually contains.

**Recommendation:** Regenerate from `VerifyBronzeJob` or replace with rows from the current anchor station.

---

### 🟡 Unused config path

In `application.conf`:
```
paths {
  raw = "data/raw"   # never referenced by any job
}
```

**Recommendation:** Remove if the `data/raw` directory is not used.

---

### 🟡 `VerifySilverJob` references non-existent table

`VerifySilverJob` calls `sampleTable("weather_observations", "weather_observations")` only, but also has the infrastructure to sample `crop_records` — it just doesn't call it. If `CropTransformJob` were wired in, this would be the place to add it. Currently the comment about crop records is missing, making it look like a truncated job.

---

### ℹ️ `SeasonWeatherSpec` requires gold to exist

Running `sbt test` before `SeasonWeatherJob` has run will fail with a Delta table not found error. There is no guard or skip annotation. A new developer cloning the repo and running `sbt test` immediately will see test failures that look like bugs but are actually missing prerequisites.

**Recommendation:** Add a `assume(new File(goldPath).exists())` guard or document the prerequisite prominently in the test file header.

---

### ℹ️ Template CSVs are static reference documents

`data/bronze/template_data.csv` and `data/bronze/template_data_project.csv` are hand-authored reference sheets documenting NOAA data types. They are not produced or consumed by any job. That is fine — they serve as human-readable schema documentation. Just don't confuse them for pipeline output.
