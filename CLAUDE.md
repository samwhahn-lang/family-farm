# CLAUDE.md — Agent Onboarding Guide

This file is read automatically by Claude Code on every session start.
Read this before touching any code.

---

## What this project is

A Scala/Spark **medallion-architecture pipeline** that ingests NOAA daily weather
observations for Beatrice, Nebraska and produces growing-season agricultural
metrics (GDD, precipitation, drought index, heat stress days) visualised in a
self-contained Plotly.js HTML dashboard.

**The central question:** Given historical weather patterns from 2000 to present,
how do growing conditions vary year-over-year for corn, soy, and winter wheat
in Gage County, Nebraska (FIPS 31067)?

---

## How to run

```bash
# Full pipeline — all four layers in order
sbt "runMain com.cornbelt.pipeline.RunAll"

# Selective run — any subset, always executes in dependency order
sbt "runMain com.cornbelt.pipeline.CustomRun silver"
sbt "runMain com.cornbelt.pipeline.CustomRun silver gold"
sbt "runMain com.cornbelt.pipeline.CustomRun gold platinum"
sbt "runMain com.cornbelt.pipeline.CustomRun bronze silver gold platinum"

# Individual jobs
sbt "runMain com.cornbelt.bronze.NoaaIngestJob"
sbt "runMain com.cornbelt.silver.WeatherTransformJob"
sbt "runMain com.cornbelt.gold.SeasonWeatherJob"
sbt "runMain com.cornbelt.platinum.PlatinumExportJob"
```

**Never use `sbt console`** — it does not inherit the `--add-opens` JVM flags
from `build.sbt` and Spark will fail with InaccessibleObjectException.

---

## Layer summary

| Layer | Job | Input | Output | Key output path |
|---|---|---|---|---|
| Bronze | `NoaaIngestJob` | NCEI API | Raw long-format Delta table | `data/bronze/noaa_observations/` |
| Silver | `WeatherTransformJob` | Bronze Delta | Wide-format typed Delta table | `data/silver/weather_observations/` |
| Gold | `SeasonWeatherJob` | Silver Delta | Annual growing-season summary | `data/gold/season_weather/` |
| Platinum | `PlatinumExportJob` | Gold Delta | `platinum/index.html` dashboard | `platinum/index.html` |

Each layer also writes a `row_count_by_year.csv` to its data folder for
quick verification without re-reading the Delta table.

---

## Required environment variable

```
NOAA_TOKEN=<your NCEI token>
```

Set as a Windows User environment variable. The bronze job throws
`RuntimeException` immediately if it is absent.
The token is free: https://www.ncdc.noaa.gov/cdo-web/token

---

## Anchor station

**GHCND:USC00250622** — "Beatrice 1 N, NE US" (40.2994, -96.75)

This station has complete daily records 2000–2026 but **reports TMAX/TMIN
only through March 2013**. After that it became precipitation-only. Gold
requires non-null TMAX+TMIN to compute GDD, so the current gold output
covers **2000–2012 only**. See `docs/known-issues.md` for options.

---

## Critical rules for Spark lifecycle

- **Never call `spark.stop()` at the end of an individual job.**
  `SparkSessionProvider.session` is a lazy singleton. Calling `stop()` in one
  job destroys the SparkContext; the next job's `getOrCreate()` returns a dead
  session and throws `NullPointerException: SparkEnv.get() is null`.
- `spark.stop()` is allowed **only** before `sys.exit(1)` in early-abort paths.
- The pipeline orchestrators (`RunAll`, `CustomRun`) call
  `SparkSessionProvider.session.stop()` exactly once at the very end.

---

## Docs index

| File | Contents |
|---|---|
| `docs/pipeline.md` | **Start here.** Full job-by-job walkthrough, SQL-level data flow, flags for dead/orphaned code |
| `docs/architecture.md` | Medallion layers, data models, design decisions |
| `docs/configuration.md` | `application.conf` reference, env vars, build flags |
| `docs/known-issues.md` | Bugs found, gotchas, station data gaps, workarounds |
