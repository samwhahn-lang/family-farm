# Configuration Reference

## application.conf

All runtime configuration lives in `src/main/resources/application.conf`
under the `corn-belt` namespace.

```hocon
corn-belt {
  anchor {
    station-id  = "GHCND:USC00250622"   # Primary NOAA station
    latitude    = 40.2994
    longitude   = -96.75
    county-fips = "31067"               # Gage County, Nebraska
    state       = "NE"
  }

  analysis {
    start-year = 2000
    end-year   = 2026
  }

  noaa {
    base-url   = "https://www.ncei.noaa.gov/access/services/data/v1"
    dataset-id = "daily-summaries"
    data-types = ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD"]
  }

  paths {
    bronze = "data/bronze"
    silver = "data/silver"
    gold   = "data/gold"
    raw    = "data/raw"
  }

  spark {
    app-name = "CornBeltWeather"
    master   = "local[*]"
  }
}
```

### Key values to know

| Key | Current value | Notes |
|---|---|---|
| `anchor.station-id` | `GHCND:USC00250622` | Temperature data only through 2012 — see known-issues.md |
| `anchor.county-fips` | `31067` | Gage County, NE — NOT 31061 (that was a bug, now fixed) |
| `analysis.start-year` | 2000 | Inclusive — bronze fetches Jan 1 of this year |
| `analysis.end-year` | 2026 | Inclusive — bronze fetches through Dec 31 of this year |
| `noaa.dataset-id` | `daily-summaries` | NCEI v1 API dataset name |

---

## Environment variables

| Variable | Required by | How to set (Windows) |
|---|---|---|
| `NOAA_TOKEN` | `NoaaIngestJob` | User env vars in System Properties |
| `USDA_NASS_KEY` | `UsdaIngestJob` | User env vars in System Properties |

The NOAA token is technically not validated by the current NCEI v1 endpoint
but the bronze job throws `RuntimeException` if it is absent, as a safety
check. Get a free token at https://www.ncdc.noaa.gov/cdo-web/token

---

## build.sbt — JVM flags

Spark 3.5 on Java 17 requires `--add-opens` flags or it throws
`InaccessibleObjectException`. These are set in `build.sbt`:

```scala
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  // ... additional opens for Hadoop/Spark internals
)
```

These flags apply only to `sbt run` / `sbt "runMain ..."`. They do NOT apply
to `sbt console` — this is why console sessions fail with Spark.

---

## Key dependencies (build.sbt)

| Library | Version | Purpose |
|---|---|---|
| `spark-core` / `spark-sql` | 3.5.0 | Distributed processing |
| `delta-spark` | 3.1.0 | ACID Delta Lake writes |
| `sttp-client3` (okhttp) | 3.9.3 | HTTP calls to NOAA/USDA |
| `circe` (core + parser + generic) | 0.14.6 | JSON parsing |
| `typesafe-config` | 1.4.3 | `application.conf` loading |
| `scala-logging` / `logback` | — | Logging |

---

## SparkSession settings

Set programmatically in `SparkSessionProvider`:

| Setting | Value | Why |
|---|---|---|
| `spark.sql.extensions` | `io.delta.sql.DeltaSparkSessionExtension` | Enable Delta |
| `spark.sql.catalog.spark_catalog` | `DeltaCatalog` | Enable Delta DDL |
| `spark.driver.bindAddress` | `127.0.0.1` | Prevents network binding errors on Windows |
| `spark.sql.datetime.java8API.enabled` | `true` | Required for `LocalDate` in Datasets |

---

## Data paths

All paths are relative to the sbt working directory (project root).

```
data/
  bronze/
    noaa_observations/          Delta table, partitioned by stationId
    row_count_by_year.csv       Written by VerifyBronzeJob
    gage_county_station_row_count.csv  Written by GageCountyStationsJob
  silver/
    weather_observations/       Delta table, partitioned by stationId
    row_count_by_year.csv       Written by WeatherTransformJob
  gold/
    season_weather/             Delta table, unpartitioned
    row_count_by_year.csv       Written by SeasonWeatherJob
platinum/
  index.html                    Final dashboard output
```

---

## Logging

`src/main/resources/logback.xml` configures logging:
- `com.cornbelt.*` → INFO
- All Spark, Hadoop, Jetty, Delta loggers → WARN

This suppresses the high-volume Spark DEBUG output that makes job logs
unreadable. The SLF4J "multiple providers" warning at startup is cosmetic
— logback wins and everything works correctly.
