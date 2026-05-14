# corn-belt-weather

A Scala / Apache Spark medallion-architecture pipeline that ingests NOAA daily
weather observations and USDA NASS crop data for the Midwestern Corn Belt,
with Beatrice, Nebraska (Gage County) as the anchor station.

The pipeline answers: **Given historical weather patterns, which mix of corn,
soybeans, and winter wheat would have been most profitable to plant?**

---

## Architecture

```
NOAA GHCND API          USDA NASS API
    │                        │
    ▼                        ▼
┌─────────────── BRONZE ───────────────────┐
│  noaa_observations/    usda_crops/       │
│  Raw, as-is, Delta Lake partitioned      │
│  by stationId / by year+state            │
└────────────────────┬─────────────────────┘
                     │
                     ▼
┌─────────────── SILVER ───────────────────┐
│  weather_observations/  crop_records/    │
│  Typed Scala case classes                │
│  SI units, quality flags, FIPS codes     │
└────────────────────┬─────────────────────┘
                     │
                     ▼
┌─────────────── GOLD ─────────────────────┐
│  season_weather_summary/                 │
│  crop_profitability/                     │
│  optimal_mix/       ← main output        │
└──────────────────────────────────────────┘
```

### States included
Nebraska · Kansas · Iowa · Missouri · Illinois · Indiana · Ohio · South Dakota

### Crops
Corn · Soybeans · Winter Wheat

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| JDK  | 11 or 17 | https://adoptium.net |
| sbt  | 1.9.x (auto via `project/build.properties`) | https://www.scala-sbt.org/download |
| Scala | 2.13.12 (managed by sbt) | — |

You do **not** need to install Spark separately — sbt pulls it as a dependency.

---

## API Keys (free)

```bash
# NOAA Climate Data Online — https://www.ncdc.noaa.gov/cdo-web/token
export NOAA_TOKEN=your_token_here

# USDA NASS QuickStats — https://quickstats.nass.usda.gov/api
export USDA_NASS_KEY=your_key_here
```

Add these to your shell profile (`~/.zshrc` or `~/.bashrc`) so they persist.
Never commit them to git — they are in `.gitignore`.

---

## Running the Pipeline

Each layer is a standalone Spark job.  Run them in order:

```bash
# 1. Bronze — ingest raw data
sbt "runMain com.cornbelt.bronze.NoaaIngestJob"
sbt "runMain com.cornbelt.bronze.UsdaIngestJob"

# 2. Silver — clean and type
sbt "runMain com.cornbelt.silver.WeatherTransformJob"
sbt "runMain com.cornbelt.silver.CropTransformJob"

# 3. Gold — compute profitability and optimal mix
sbt "runMain com.cornbelt.gold.OptimalMixJob"

# Tests (no API keys needed)
sbt test
```

---

## Project Structure

```
corn-belt-weather/
├── build.sbt                          # Dependencies and build settings
├── project/
│   ├── build.properties               # sbt version pin
│   └── plugins.sbt                    # sbt-assembly for fat jar builds
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf       # All config: stations, states, API URLs
│   │   └── scala/com/cornbelt/
│   │       ├── models/Models.scala    # ALL domain types live here
│   │       ├── utils/SparkSessionProvider.scala
│   │       ├── bronze/
│   │       │   ├── NoaaIngestJob.scala
│   │       │   └── UsdaIngestJob.scala
│   │       ├── silver/
│   │       │   ├── WeatherTransformJob.scala
│   │       │   └── CropTransformJob.scala
│   │       └── gold/
│   │           └── OptimalMixJob.scala
│   └── test/scala/com/cornbelt/
│       └── ModelsSpec.scala
├── data/                              # Gitignored — created at runtime
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
└── docs/                              # Design notes, data dictionaries
```

---

## Why Scala?

Your mentor put it well: *"the type system and compiler work for you."*

The `Crop` sealed trait is a concrete example.  In Python you'd pass around the
string `"CORN"` everywhere and a typo like `"corn"` silently produces wrong
results.  In Scala:

```scala
sealed trait Crop
object Crop {
  case object Corn     extends Crop
  case object Soybeans extends Crop
  case object Wheat    extends Crop
}
```

If you write a `match` expression that forgets `Wheat`, the **compiler warns
you at build time** — before you run a single byte.  No runtime surprises.

---

## Next Steps

- [ ] Add growing-degree-day models per crop variety (DSSAT reference data)
- [ ] Pull CME futures price history to compare NASS "price received" vs market
- [ ] Extend to all Corn Belt counties (not just Gage County anchor)
- [ ] Add a simple regression: does GDD or drought index predict yield better?
- [ ] Visualise optimal mix year-by-year in a notebook

---

## Data Sources

- [NOAA GHCND](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/doc/GHCND_documentation.pdf)
- [USDA NASS QuickStats](https://quickstats.nass.usda.gov/)
- [Delta Lake](https://delta.io/)
