# Known Issues and Bugs

## Active data limitation

### Station USC00250622 is precipitation-only after March 2013

**Symptom:** Gold produces only 13 years (2000–2012). The platinum dashboard
shows data truncating at 2012.

**Root cause:** GHCND:USC00250622 ("Beatrice 1 N, NE US") transitioned to
precipitation-only reporting around March 2013. The station continued
submitting daily records (so row counts look complete — 365/year), but the
TMAX and TMIN fields are absent from the JSON response for all post-2012 dates.

Bronze null-value evidence from Delta log:

| Date range | Null values (of ~5,990 rows) |
|---|---|
| 2000–2003 | 0 |
| 2003–2006 | 2 |
| 2006–2009 | 4 |
| 2009–2013 | 784 |
| 2013–2016 | 3,010 |
| 2016–2019 | 2,503 |
| 2019–2026 | ~2,400 |

**Why row counts look fine:** `gage_county_station_row_count.csv` counts JSON
objects returned per year (one per day). A precipitation-only day still
produces one JSON object, so the count shows 365. It does not indicate whether
TMAX/TMIN are present.

**Gold's behavior is correct.** It drops rows where TMAX or TMIN is null
because GDD is undefined without both readings.

**Options to resolve:**
1. Switch `anchor.station-id` to a station with complete temperature data —
   best candidates are nearby ASOS airport stations which have uninterrupted
   records (e.g., Beatrice Municipal Airport KBIE, or Lincoln airport LNK
   ~50 mi north, GHCND:USW00014939)
2. Supplement USC00250622 with a secondary temperature station for 2013+ and
   merge in silver (the silver job has the station-filter hook ready)
3. Accept the 2000–2012 window as the current analysis period

---

## Resolved bugs (do not re-introduce)

### spark.stop() NullPointerException in chained jobs

**Symptom:**
```
NullPointerException: Cannot invoke "org.apache.spark.SparkEnv.conf()"
because the return value of "org.apache.spark.SparkEnv$.get()" is null
```

**Cause:** Individual jobs called `spark.stop()` at the end of `main()`.
`SparkSessionProvider.session` is a `lazy val` singleton. After `stop()`,
`SparkSession.getOrCreate()` returns the same dead session object. The next
job attempts to use it and Spark internals throw NPE because `SparkEnv` has
been torn down.

**Fix:** Removed `spark.stop()` from the end of all four pipeline jobs.
`spark.stop()` is called only in two places:
1. `RunAll` / `CustomRun` — once at the very end of the full pipeline run
2. Individual jobs — only on the `sys.exit(1)` early-abort path

**Do not add `spark.stop()` to any job's normal completion path.**

---

### Old station USC00050945 had missing years 2012–2013

**Symptom:** Bronze showed 0 rows for 2012 and 2013, 450 rows for 2011.

**Cause:** GHCND:USC00050945 had genuine data gaps — equipment failure or
lost COOP observer. The NCEI API returned empty arrays for those years.
This was not a code bug. Station was switched to USC00250622.

**Lesson:** High row counts in the NCEI response do not guarantee temperature
data. Always check the gold diagnostic log for `*** NO TEMPERATURE DATA ***`
warnings.

---

### JavaScript template literals conflict with Scala s-strings

**Symptom:** Compilation errors like `illegal character '\''` or
`identifier expected` inside the `buildHtml` method of `PlatinumExportJob`.

**Cause:** Scala s-string interpolation parses `${...}` inside the string.
JavaScript template literals use the same syntax. The Scala compiler treats
JS expressions like `${d.year}` as Scala interpolation targets and fails.

**Fix:** Never use JavaScript backtick template strings inside a Scala
s-string. Convert all JS template literals to string concatenation:
```javascript
// Wrong (inside Scala s"""...""")
`<td>${r.year}</td>`

// Right
'<td>'+r.year+'</td>'
```

---

### Windows file encoding — degree symbols written as `?`

**Symptom:** `°` (degree symbol), `–` (en-dash), and similar characters
appear as `?` in the generated HTML.

**Cause:** `new PrintWriter(new File(...))` on Windows uses the platform
default encoding (CP1252 / Windows-1252), which cannot represent some Unicode
characters.

**Fix:** Always use `OutputStreamWriter` with explicit UTF-8:
```scala
new PrintWriter(new OutputStreamWriter(
  new FileOutputStream(file), StandardCharsets.UTF_8))
```
This pattern is used in every file-writing job in this project.

---

### Emoji characters in log strings on Windows

**Symptom:** Log lines containing emoji or box-drawing characters (e.g., `════`)
appear as `∩┐╜` or `?` in the sbt console on Windows.

**Cause:** Windows console default code page does not support Unicode box
characters.

**Fix:** Use plain ASCII in all logger strings. `====` instead of `════`.
The `CustomRun` and `RunAll` job labels use `—` (em-dash via `&mdash;` in
HTML, plain hyphen-minus in Scala strings) and `====` delimiters.

---

### Plotly dual-axis phantom second trace

**Symptom:** Selecting a single metric renders two bars or two lines.

**Cause:** When `yaxis2: {overlaying: 'y'}` is always present in the Plotly
layout, every trace without an explicit `yaxis` property gets duplicated
across both axes.

**Fix:** The layout's `yaxis2` is only added when both y-axis groups have
active metrics (`needsY2` flag). All traces are mapped to `'y'` when only
one axis group is active. This logic is in `PlatinumExportJob.buildHtml`.

---

### FIPS 31061 typo in platinum HTML

**Symptom:** Dashboard subtitle showed "FIPS 31061" (Dodge County) instead
of "FIPS 31067" (Gage County).

**Status:** Fixed in `PlatinumExportJob.buildHtml`. Re-run platinum to
regenerate the HTML.

---

### sbt console fails with Spark

**Symptom:** `java.lang.reflect.InaccessibleObjectException` immediately
after starting `sbt console`.

**Cause:** The `javaOptions` in `build.sbt` (the `--add-opens` flags required
by Spark on Java 17) apply only to forked JVM processes (`sbt run`,
`sbt "runMain ..."`). The sbt console REPL runs in the sbt JVM itself, which
does not inherit those flags.

**Workaround:** Always use `sbt "runMain com.cornbelt.xxx.SomeJob"`.
There is no way to use the REPL with Spark in this project setup.

---

### Stale sbt server process

**Symptom:** `ServerAlreadyBootingException` when starting sbt.

**Fix:**
```powershell
Get-Process -Name "java" | Stop-Process -Force
```
Then re-run sbt normally.

---

### CDO API vs NCEI API format difference

**Background:** This project was migrated from the old NOAA CDO API
(`https://www.ncdc.noaa.gov/cdo-web/api/v2`) to the NCEI Data Services v1
API (`https://www.ncei.noaa.gov/access/services/data/v1`).

| | CDO API | NCEI API (current) |
|---|---|---|
| Format | Long (one row per measurement) | Wide (one object per day, all measurements as fields) |
| Authentication | `token:` header required | No auth required |
| Pagination | Yes (limit/offset) | No — full year in one response |
| Units | Tenths of °C / tenths of mm | Actual °C / mm with `units=metric` |
| Token param | Header | URL param (ignored) |

The bronze job is written for the NCEI wide format. Do not revert to CDO-style
pagination or `token:` header auth.
