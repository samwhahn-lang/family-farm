# Project Context for Claude Code

## What this project is
Scala/Spark medallion architecture pipeline analyzing NOAA weather data
against USDA crop data for Midwestern corn, soy, and wheat.
Anchor location: Beatrice, Nebraska (Gage County, FIPS 31067).

## Current status
- Bronze NOAA ingest job exists but is writing 0 rows due to NOAA API
  rate limiting (429 errors). The fix is in progress.
- No Silver or Gold jobs exist yet.
- Tests pass (sbt test — 7/7).
- GitHub repo not yet created.

## The immediate problem to fix
NoaaIngestJob.scala is hitting NOAA's 5 requests/second limit.
Need to increase sleep to 1000ms between years and fix the
datatypeid URL parameter construction in fetchYear().

## Environment
- Windows 11, VS Code, sbt 1.12.9, Java 17, Scala 2.13.12
- Spark 3.5.0 (non-provided, runs locally)
- HADOOP_HOME=C:\hadoop, hadoop.dll copied to JDK bin
- NOAA_TOKEN and USDA_NASS_KEY set as User env vars
- Project path: C:\Users\samwh\OneDrive\Documents\GitHub\Commodity

## Key files
- build.sbt — dependencies and javaOptions with hadoop flags
- src/main/scala/com/cornbelt/models/Models.scala — all domain types
- src/main/scala/com/cornbelt/bronze/NoaaIngestJob.scala — needs fix
- src/main/scala/com/cornbelt/bronze/UsdaIngestJob.scala
- src/main/scala/com/cornbelt/silver/WeatherTransformJob.scala — not yet created
- src/main/scala/com/cornbelt/silver/CropTransformJob.scala — not yet created
- src/main/scala/com/cornbelt/gold/OptimalMixJob.scala — not yet created

## Data sources
- NOAA CDO API: https://www.ncdc.noaa.gov/cdo-web/api/v2
- USDA NASS QuickStats: https://quickstats.nass.usda.gov/api
- Both API keys are set as environment variables

## Next steps in order
1. Fix NoaaIngestJob rate limiting and get Bronze NOAA data writing
2. Run UsdaIngestJob to get Bronze crop data
3. Build Silver transform jobs
4. Build Gold optimal mix job
5. Push to GitHub (username: samwhahn-lang)