# Architecture

## Overview

Four-layer medallion pipeline. Each layer writes a Delta Lake table that the
next layer reads. All layers share a single lazy Spark session via
`SparkSessionProvider`.

```
NCEI API (NOAA)
     |
     v
[BRONZE]  data/bronze/noaa_observations/        partitioned by stationId
     |      Long format: one row per (station, date, dataType)
     v
[SILVER]  data/silver/weather_observations/     partitioned by stationId
     |      Wide format: one row per (station, date), typed columns
     v
[GOLD]    data/gold/season_weather/             unpartitioned
     |      One row per year: growing-season aggregates
     v
[PLATINUM] platinum/index.html
            Self-contained Plotly.js dashboard, no server required
```

---

## Bronze — `NoaaIngestJob`

**Purpose:** Pull raw NOAA GHCND daily summaries and persist them as-is.

**API:** NCEI Data Services v1 (`daily-summaries` dataset)
- URL: `https://www.ncei.noaa.gov/access/services/data/v1`
- Returns wide JSON: one object per calendar day, each data type is a field
- Does NOT require authentication (token param is a no-op for this endpoint)
- Rate limit: 1 000 ms sleep between year fetches

**Unpivoting:** The wide JSON is exploded into long format — 5 rows per day
(TMAX, TMIN, PRCP, SNOW, SNWD). Each row is a `RawWeatherObservation`.

**Schema:**
```
stationId   String           e.g. "GHCND:USC00250622"
date        String           "YYYY-MM-DD"  (parsed in silver)
dataType    String           "TMAX" | "TMIN" | "PRCP" | "SNOW" | "SNWD"
value       Option[Double]   null when station did not report that element
attributes  Option[String]   always null for NCEI wide-format responses
```

**Write mode:** `SaveMode.Overwrite` — full re-pull every run.

**Verification:** `VerifyBronzeJob` writes `data/bronze/row_count_by_year.csv`.
`GageCountyStationsJob` surveys all GHCND stations in Gage County (FIPS 31067)
and writes `data/bronze/gage_county_station_row_count.csv`.

---

## Silver — `WeatherTransformJob`

**Purpose:** Pivot bronze from long to wide, parse dates, assign quality flags.
Produces typed `WeatherObservation` Datasets.

**Station filter:** Reads only `anchor.station-id` from bronze. Any other
station data present in bronze is ignored.

**Quality flag logic:**
- `MISSING` — zero non-null values for that station/date
- `SUSPECT` — any NOAA quality flag character found in `attributes`
- `OK` — everything else

**Pivot:** Groups by `(stationId, date)`, pivots `dataType` into columns
`TMAX → tempMaxC`, `TMIN → tempMinC`, `PRCP → precipMm`, `SNOW → snowMm`,
`SNWD → snowDepthMm`. Uses `first("value")` as the aggregation.

**Units:** NCEI `units=metric` returns °C for temperature and mm for
precipitation directly — no divide-by-10 needed (unlike the old CDO API which
returned tenths of °C).

**Schema (`WeatherObservation`):**
```
stationId    String
date         LocalDate        requires spark.sql.datetime.java8API.enabled=true
tempMaxC     Option[Double]   null when station did not report TMAX
tempMinC     Option[Double]   null when station did not report TMIN
precipMm     Option[Double]
snowMm       Option[Double]
snowDepthMm  Option[Double]
qualityFlag  String           "OK" | "SUSPECT" | "MISSING"
```

**Write mode:** `SaveMode.Overwrite`, partitioned by `stationId`.
**CSV output:** `data/silver/row_count_by_year.csv` — one row per year,
count = number of calendar days in silver for that year.

---

## Gold — `SeasonWeatherJob`

**Purpose:** Aggregate silver daily rows into one growing-season summary per
year. Produces typed `SeasonWeatherSummary` Datasets.

**Station filter:** Explicitly filters silver to `anchor.station-id` before
any aggregation — defensive guard in case silver ever contains multiple
stations.

**Growing season:** April (month 4) through November (month 11).
Winter wheat establishment extends to November; caller can filter further.

**Temperature filter:** Rows with null `tempMaxC` or `tempMinC` are dropped
before GDD calculation. This is intentional — GDD is undefined without both
readings. Any year where the station did not report temperature will produce
no gold row for that year.

**GDD formula (base 10°C / 50°F, corn standard):**
```
gdd_per_day = max(0, (min(TMAX, 30) + max(TMIN, 10)) / 2 - 10)
```
TMAX is capped at 30°C because corn growth plateaus above that threshold.

**Drought index:**
```
droughtIndex = (year_precip - mean_precip) / mean_precip
```
`mean_precip` is the mean across all years present in the dataset, computed
with an unbounded window function. Negative = drier than average.

**Schema (`SeasonWeatherSummary`):**
```
year               Int
countyFips         String    from anchor config
stateCode          String    from anchor config
growingDegreeDays  Double    sum of daily GDD, April-November
totalPrecipMm      Double    sum of daily precip (coalesces null to 0)
frostFreeDays      Int       days where tempMinC > 0
extremeHeatDays    Int       days where tempMaxC > 35°C (95°F)
droughtIndex       Double    4 decimal places
```

**Diagnostic logging:** On every run, gold logs per-year temperature coverage
before filtering:
```
2013:   0/365 days have TMAX+TMIN (0%) *** NO TEMPERATURE DATA ***
```
This makes station data gaps immediately visible without inspecting Delta files.

**CSV output:** `data/gold/row_count_by_year.csv` — one row per year (always
count=1, since gold is one summary row per year). Useful as a year-coverage
check.

---

## Platinum — `PlatinumExportJob`

**Purpose:** Serialize gold data to a self-contained HTML file with an
interactive Plotly.js dashboard. No web server needed — open in any browser.

**Output:** `platinum/index.html`

**Serialization:** Gold rows are collected to the driver and serialized as a
JavaScript array literal embedded directly in the HTML `<script>` block. Scala
s-string interpolation is used; no JavaScript template literals (backtick
strings) are allowed because they conflict with Scala's string interpolation.

**Dashboard features:**
- Line and bar chart via Plotly.js 2.27.0
- Dual y-axis (right axis for precipitation and drought index)
- Year range filter
- Metric toggle chips (show/hide individual metrics)
- Summary stats cards (avg, min, max per metric)
- Data table with per-year condition tags (Normal / Watch / Stress)

**Encoding:** File is written with UTF-8 via `OutputStreamWriter` +
`FileOutputStream`. Do not use `new PrintWriter(File)` — on Windows that
defaults to CP1252 and corrupts degree symbols and em-dashes.

---

## Pipeline orchestration

### `RunAll`
Runs all four jobs in order. One `SparkSessionProvider.session.stop()` at the
end. Use when you want a full refresh from the NOAA API.

### `CustomRun`
Accepts stage names as args. Always runs in pipeline order regardless of input
order. Valid names: `bronze`, `silver`, `gold`, `platinum`.

```bash
sbt "runMain com.cornbelt.pipeline.CustomRun silver gold platinum"
```

---

## Key design decisions

| Decision | Rationale |
|---|---|
| Delta Lake over plain Parquet | ACID overwrites — safe to re-run without partial state |
| `SaveMode.Overwrite` on every write | Idempotent pipeline; re-running from any stage is safe |
| `SparkSessionProvider` lazy singleton | Jobs are designed to chain; stop() is only called by orchestrators |
| Long format in bronze, wide in silver | Bronze mirrors the API shape exactly; silver mirrors the analysis shape |
| Station filter in both silver AND gold | Silver filters at ingest; gold provides a defensive second guard |
| `java8API.enabled = true` | Required for `LocalDate` encoding in Spark Datasets |
| No `spark.stop()` in individual jobs | Prevents NPE when jobs are chained via RunAll/CustomRun |
