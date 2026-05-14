package com.cornbelt.docs

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._
import scala.util.Try

object DiagramJob extends LazyLogging {

  private val config    = ConfigFactory.load().getConfig("corn-belt")
  private val noaaCfg  = config.getConfig("noaa")
  private val anchor   = config.getConfig("anchor")
  private val pathsCfg = config.getConfig("paths")
  private val analysisCfg = config.getConfig("analysis")

  def main(args: Array[String]): Unit = {
    val outDir  = "platinum/diagrams"
    val outFile = s"$outDir/bronze.html"
    Files.createDirectories(Paths.get(outDir))

    // Read live config values — diagram stays accurate when config changes
    val stationId  = anchor.getString("station-id")
    val startYear  = analysisCfg.getInt("start-year")
    val endYear    = analysisCfg.getInt("end-year")
    val dataTypes  = noaaCfg.getStringList("data-types").asScala.toList
    val baseUrl    = noaaCfg.getString("base-url")
    val bronzePath = s"${pathsCfg.getString("bronze")}/noaa_observations"
    val skipYears  = "2020 – 2024"
    val generatedAt = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))

    // Try to read actual row count from the Delta table if it exists
    val rowCount: Option[Long] = Try {
      import org.apache.spark.sql.SparkSession
      val spark = SparkSession.builder
        .master("local")
        .appName("DiagramJob")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val count = spark.read.format("delta").load(bronzePath).count()
      spark.stop()
      count
    }.toOption

    val rowCountLabel = rowCount match {
      case Some(n) => f"$n%,d rows (live)"
      case None    => "table not yet written"
    }

    val html = buildHtml(
      stationId, startYear, endYear, dataTypes,
      skipYears, bronzePath, rowCountLabel, baseUrl, generatedAt
    )

    val outFileObj = new File(outFile)
    val existed    = outFileObj.exists()
    val writer     = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outFileObj), StandardCharsets.UTF_8))
    writer.print(html)
    writer.close()

    val action = if (existed) "updated" else "created"
    logger.info(s"Bronze diagram $action -> $outFile")
  }

  private def buildHtml(
    stationId:    String,
    startYear:    Int,
    endYear:      Int,
    dataTypes:    List[String],
    skipYears:    String,
    bronzePath:   String,
    rowCount:     String,
    baseUrl:      String,
    generatedAt:  String
  ): String = {

    val dtBadges = dataTypes.map(dt =>
      s"""<span class="badge">$dt</span>"""
    ).mkString("\n          ")

    val urlTemplate =
      s"$baseUrl/data" +
      s"?datasetid=GHCND" +
      s"&stationid=$stationId" +
      s"&startdate=[YEAR]-01-01" +
      s"&enddate=[YEAR]-12-31" +
      s"&datatypeid=${dataTypes.mkString("&datatypeid=")}" +
      s"&limit=1000&offset=[N]&units=metric"

s"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Bronze Layer — Data Diagram</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {
      --bg:        #0d1117;
      --surface:   #161b22;
      --border:    #30363d;
      --text:      #e6edf3;
      --muted:     #8b949e;
      --blue:      #388bfd;
      --blue-dim:  #1c2d4a;
      --purple:    #bc8cff;
      --purple-dim:#2a1a4a;
      --orange:    #e3813a;
      --orange-dim:#3a2010;
      --green:     #3fb950;
      --green-dim: #122d1a;
      --teal:      #39d0c8;
      --teal-dim:  #0d2e2c;
      --red:       #f85149;
      --red-dim:   #3d1111;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Inter', system-ui, sans-serif;
      font-size: 13px;
      line-height: 1.6;
      min-height: 100vh;
    }

    /* ── Header ── */
    .page-header {
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 20px 40px;
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .page-header h1 { font-size: 1.1rem; font-weight: 600; color: var(--text); }
    .page-header h1 span { color: var(--orange); }
    .meta { font-size: .75rem; color: var(--muted); }
    .layer-pill {
      display: inline-block;
      padding: 2px 10px;
      border-radius: 12px;
      font-size: .7rem;
      font-weight: 700;
      letter-spacing: .06em;
      text-transform: uppercase;
      background: var(--orange-dim);
      color: var(--orange);
      border: 1px solid var(--orange);
      margin-right: 10px;
    }

    /* ── Canvas ── */
    .canvas {
      padding: 40px;
      overflow-x: auto;
    }

    /* ── Flow row ── */
    .flow {
      display: flex;
      align-items: flex-start;
      gap: 0;
      min-width: max-content;
    }

    /* ── Arrow ── */
    .arrow {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      padding: 0 4px;
      margin-top: 54px;
      flex-shrink: 0;
    }
    .arrow svg { display: block; }

    /* ── Card ── */
    .card {
      width: 230px;
      flex-shrink: 0;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: var(--surface);
      overflow: hidden;
      box-shadow: 0 4px 24px rgba(0,0,0,.4);
    }
    .card-header {
      padding: 10px 14px 9px;
      display: flex;
      align-items: center;
      gap: 8px;
      border-bottom: 1px solid var(--border);
    }
    .card-header .icon {
      width: 26px; height: 26px;
      border-radius: 6px;
      display: flex; align-items: center; justify-content: center;
      font-size: 13px; flex-shrink: 0;
    }
    .card-header .title { font-size: .78rem; font-weight: 700; letter-spacing: .02em; }
    .card-header .sub   { font-size: .68rem; color: var(--muted); margin-top: 1px; }
    .card-body { padding: 12px 14px; display: flex; flex-direction: column; gap: 10px; }

    /* ── Section inside card ── */
    .section-label {
      font-size: .62rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: .08em;
      color: var(--muted);
      margin-bottom: 5px;
    }
    .field-row {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 8px;
      padding: 3px 0;
      border-bottom: 1px solid rgba(255,255,255,.04);
    }
    .field-row:last-child { border-bottom: none; }
    .field-key {
      font-family: 'JetBrains Mono', monospace;
      font-size: .7rem;
      color: var(--muted);
      flex-shrink: 0;
    }
    .field-val {
      font-family: 'JetBrains Mono', monospace;
      font-size: .7rem;
      color: var(--text);
      text-align: right;
      word-break: break-all;
    }
    .field-type {
      font-size: .65rem;
      color: var(--muted);
      font-style: italic;
    }

    /* ── Badges ── */
    .badge {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 4px;
      font-family: 'JetBrains Mono', monospace;
      font-size: .65rem;
      font-weight: 500;
      background: rgba(255,255,255,.07);
      border: 1px solid var(--border);
      margin: 2px 2px 2px 0;
    }

    /* ── Logic step ── */
    .logic-step {
      display: flex;
      gap: 8px;
      align-items: flex-start;
      padding: 5px 0;
      border-bottom: 1px solid rgba(255,255,255,.04);
      font-size: .72rem;
    }
    .logic-step:last-child { border-bottom: none; }
    .step-num {
      width: 16px; height: 16px;
      border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: .6rem; font-weight: 700;
      flex-shrink: 0; margin-top: 1px;
    }
    .step-text { color: var(--text); line-height: 1.5; }
    .step-text code {
      font-family: 'JetBrains Mono', monospace;
      font-size: .65rem;
      background: rgba(255,255,255,.07);
      padding: 0 4px;
      border-radius: 3px;
    }

    /* ── URL block ── */
    .url-block {
      font-family: 'JetBrains Mono', monospace;
      font-size: .62rem;
      background: rgba(0,0,0,.3);
      border: 1px solid var(--border);
      border-radius: 6px;
      padding: 8px 10px;
      line-height: 1.8;
      word-break: break-all;
      color: var(--muted);
    }
    .url-block .hl { color: var(--blue); }
    .url-block .val { color: var(--teal); }

    /* ── Warn chip ── */
    .warn-chip {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 5px 9px;
      border-radius: 6px;
      font-size: .7rem;
      background: var(--red-dim);
      border: 1px solid var(--red);
      color: var(--red);
    }

    /* ── Color variants ── */
    .c-blue   { color: var(--blue);   } .bg-blue   { background: var(--blue-dim);   border-color: var(--blue);   }
    .c-purple { color: var(--purple); } .bg-purple { background: var(--purple-dim); border-color: var(--purple); }
    .c-orange { color: var(--orange); } .bg-orange { background: var(--orange-dim); border-color: var(--orange); }
    .c-green  { color: var(--green);  } .bg-green  { background: var(--green-dim);  border-color: var(--green);  }
    .c-teal   { color: var(--teal);   } .bg-teal   { background: var(--teal-dim);   border-color: var(--teal);   }

    .divider { border: none; border-top: 1px solid var(--border); margin: 2px 0; }

    /* ── Footer ── */
    .page-footer {
      text-align: center;
      padding: 24px;
      color: var(--muted);
      font-size: .72rem;
      border-top: 1px solid var(--border);
      margin-top: 20px;
    }
  </style>
</head>
<body>

<div class="page-header">
  <div>
    <h1><span class="layer-pill">Bronze</span> Data Pipeline — Architecture Diagram</h1>
    <div class="meta">Corn Belt Weather · Station $stationId · Generated $generatedAt</div>
  </div>
  <div class="meta" style="text-align:right">
    Midwest Crop Analysis<br>
    <span style="color:var(--orange)">Stage 1 of 4</span>
  </div>
</div>

<div class="canvas">
<div class="flow">

  <!-- ════════════════════ 1. CONFIG ════════════════════ -->
  <div class="card">
    <div class="card-header bg-blue" style="border-bottom-color:rgba(56,139,253,.3)">
      <div class="icon bg-blue" style="background:rgba(56,139,253,.2)">&#9881;</div>
      <div>
        <div class="title c-blue">Configuration</div>
        <div class="sub">application.conf</div>
      </div>
    </div>
    <div class="card-body">
      <div>
        <div class="section-label">Analysis window</div>
        <div class="field-row">
          <span class="field-key">start-year</span>
          <span class="field-val c-blue">$startYear</span>
        </div>
        <div class="field-row">
          <span class="field-key">end-year</span>
          <span class="field-val c-blue">$endYear</span>
        </div>
        <div class="field-row">
          <span class="field-key">skip-years</span>
          <span class="field-val" style="color:var(--red);font-size:.65rem">$skipYears</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Station</div>
        <div class="field-row" style="flex-direction:column;align-items:flex-start;gap:2px">
          <span class="field-key">station-id</span>
          <span class="field-val c-blue" style="font-size:.65rem">$stationId</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Data types</div>
        <div>$dtBadges</div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Auth</div>
        <div class="field-row">
          <span class="field-key">NOAA_TOKEN</span>
          <span class="field-val" style="color:var(--muted)">env var</span>
        </div>
      </div>
    </div>
  </div>

  <!-- arrow -->
  <div class="arrow">
    <svg width="44" height="20" viewBox="0 0 44 20">
      <defs><linearGradient id="g1" x1="0" x2="1"><stop offset="0%" stop-color="#388bfd"/><stop offset="100%" stop-color="#bc8cff"/></linearGradient></defs>
      <line x1="2" y1="10" x2="38" y2="10" stroke="url(#g1)" stroke-width="1.5"/>
      <polygon points="38,5 44,10 38,15" fill="#bc8cff"/>
    </svg>
  </div>

  <!-- ════════════════════ 2. NOAA API ════════════════════ -->
  <div class="card">
    <div class="card-header bg-purple" style="border-bottom-color:rgba(188,140,255,.3)">
      <div class="icon" style="background:rgba(188,140,255,.2)">&#9729;</div>
      <div>
        <div class="title c-purple">NOAA CDO API</div>
        <div class="sub">ncei.noaa.gov · daily-summaries dataset</div>
      </div>
    </div>
    <div class="card-body">
      <div>
        <div class="section-label">Request template</div>
        <div class="url-block">
          <span class="hl">GET</span> /data<br>
          &nbsp;<span class="hl">?datasetid=</span><span class="val">GHCND</span><br>
          &nbsp;<span class="hl">&stationid=</span><span class="val">$stationId</span><br>
          &nbsp;<span class="hl">&startdate=</span><span class="val">[YEAR]-01-01</span><br>
          &nbsp;<span class="hl">&enddate=</span><span class="val">[YEAR]-12-31</span><br>
          &nbsp;<span class="hl">&datatypeid=</span><span class="val">${dataTypes.mkString(",")}</span><br>
          &nbsp;<span class="hl">&limit=</span><span class="val">1000</span><br>
          &nbsp;<span class="hl">&offset=</span><span class="val">[N]</span><br>
          &nbsp;<span class="hl">&units=</span><span class="val">metric</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Constraints</div>
        <div class="field-row">
          <span class="field-key">rate limit</span>
          <span class="field-val">5 req / sec</span>
        </div>
        <div class="field-row">
          <span class="field-key">page size</span>
          <span class="field-val">1000 rows</span>
        </div>
        <div class="field-row">
          <span class="field-key">units</span>
          <span class="field-val">metric (°C, mm)</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Response format</div>
        <div class="field-row">
          <span class="field-key">type</span>
          <span class="field-val">JSON</span>
        </div>
        <div class="field-row">
          <span class="field-key">root key</span>
          <span class="field-val c-purple">results[]</span>
        </div>
      </div>
    </div>
  </div>

  <!-- arrow -->
  <div class="arrow">
    <svg width="44" height="20" viewBox="0 0 44 20">
      <defs><linearGradient id="g2" x1="0" x2="1"><stop offset="0%" stop-color="#bc8cff"/><stop offset="100%" stop-color="#e3813a"/></linearGradient></defs>
      <line x1="2" y1="10" x2="38" y2="10" stroke="url(#g2)" stroke-width="1.5"/>
      <polygon points="38,5 44,10 38,15" fill="#e3813a"/>
    </svg>
  </div>

  <!-- ════════════════════ 3. INGEST LOGIC ════════════════════ -->
  <div class="card">
    <div class="card-header bg-orange" style="border-bottom-color:rgba(227,129,58,.3)">
      <div class="icon" style="background:rgba(227,129,58,.2)">&#9654;</div>
      <div>
        <div class="title c-orange">IngestBackfill</div>
        <div class="sub">com.cornbelt.bronze</div>
      </div>
    </div>
    <div class="card-body">
      <div>
        <div class="section-label">Fetch loop</div>
        <div class="logic-step">
          <div class="step-num" style="background:rgba(227,129,58,.2);color:var(--orange)">1</div>
          <div class="step-text">Loop years <code>$startYear</code> to <code>$endYear</code>, skip <code>$skipYears</code></div>
        </div>
        <div class="logic-step">
          <div class="step-num" style="background:rgba(227,129,58,.2);color:var(--orange)">2</div>
          <div class="step-text">Sleep <code>1000ms</code> between years (rate limit)</div>
        </div>
        <div class="logic-step">
          <div class="step-num" style="background:rgba(227,129,58,.2);color:var(--orange)">3</div>
          <div class="step-text">Paginate: <code>offset += 1000</code> until <code>rows &lt; 1000</code></div>
        </div>
        <div class="logic-step">
          <div class="step-num" style="background:rgba(227,129,58,.2);color:var(--orange)">4</div>
          <div class="step-text">Sleep <code>1000ms</code> between pages</div>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Error handling</div>
        <div class="warn-chip" style="margin-bottom:5px">
          <span>&#9888;</span> 429 &rarr; backoff <code>5000ms</code>, retry
        </div>
        <div class="warn-chip" style="background:var(--orange-dim);border-color:var(--orange);color:var(--orange)">
          <span>&#9888;</span> 0 rows &rarr; abort, log config
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">HTTP client</div>
        <div class="field-row">
          <span class="field-key">library</span>
          <span class="field-val">sttp 3.9.3</span>
        </div>
        <div class="field-row">
          <span class="field-key">backend</span>
          <span class="field-val">HttpURLConnection</span>
        </div>
        <div class="field-row">
          <span class="field-key">json</span>
          <span class="field-val">circe 0.14.6</span>
        </div>
      </div>
    </div>
  </div>

  <!-- arrow -->
  <div class="arrow">
    <svg width="44" height="20" viewBox="0 0 44 20">
      <defs><linearGradient id="g3" x1="0" x2="1"><stop offset="0%" stop-color="#e3813a"/><stop offset="100%" stop-color="#3fb950"/></linearGradient></defs>
      <line x1="2" y1="10" x2="38" y2="10" stroke="url(#g3)" stroke-width="1.5"/>
      <polygon points="38,5 44,10 38,15" fill="#3fb950"/>
    </svg>
  </div>

  <!-- ════════════════════ 4. RAW SCHEMA ════════════════════ -->
  <div class="card">
    <div class="card-header bg-green" style="border-bottom-color:rgba(63,185,80,.3)">
      <div class="icon" style="background:rgba(63,185,80,.2)">&#11835;</div>
      <div>
        <div class="title c-green">RawWeatherObservation</div>
        <div class="sub">com.cornbelt.models</div>
      </div>
    </div>
    <div class="card-body">
      <div>
        <div class="section-label">Case class fields</div>
        <div class="field-row">
          <span class="field-key">stationId</span>
          <span class="field-type">String</span>
        </div>
        <div class="field-row">
          <span class="field-key">date</span>
          <span class="field-type">String &nbsp;<span style="color:var(--muted);font-size:.6rem">YYYY-MM-DD</span></span>
        </div>
        <div class="field-row">
          <span class="field-key">dataType</span>
          <span class="field-type">String</span>
        </div>
        <div class="field-row">
          <span class="field-key">value</span>
          <span class="field-type">Option[Double]</span>
        </div>
        <div class="field-row">
          <span class="field-key">attributes</span>
          <span class="field-type">Option[String]</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">dataType values</div>
        <div>$dtBadges</div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Units (metric)</div>
        <div class="field-row">
          <span class="field-key">TMAX / TMIN</span>
          <span class="field-val">°C</span>
        </div>
        <div class="field-row">
          <span class="field-key">PRCP / SNOW / SNWD</span>
          <span class="field-val">mm</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Attributes format</div>
        <div class="field-row" style="flex-direction:column;align-items:flex-start;gap:2px">
          <span class="field-key">meas,quality,source,time</span>
          <span class="field-val" style="font-size:.62rem;color:var(--muted)">quality[1] != '' = SUSPECT</span>
        </div>
      </div>
    </div>
  </div>

  <!-- arrow -->
  <div class="arrow">
    <svg width="44" height="20" viewBox="0 0 44 20">
      <defs><linearGradient id="g4" x1="0" x2="1"><stop offset="0%" stop-color="#3fb950"/><stop offset="100%" stop-color="#39d0c8"/></linearGradient></defs>
      <line x1="2" y1="10" x2="38" y2="10" stroke="url(#g4)" stroke-width="1.5"/>
      <polygon points="38,5 44,10 38,15" fill="#39d0c8"/>
    </svg>
  </div>

  <!-- ════════════════════ 5. DELTA OUTPUT ════════════════════ -->
  <div class="card">
    <div class="card-header bg-teal" style="border-bottom-color:rgba(57,208,200,.3)">
      <div class="icon" style="background:rgba(57,208,200,.2)">&#11208;</div>
      <div>
        <div class="title c-teal">Bronze Delta Table</div>
        <div class="sub">noaa_observations</div>
      </div>
    </div>
    <div class="card-body">
      <div>
        <div class="section-label">Storage</div>
        <div class="field-row" style="flex-direction:column;align-items:flex-start;gap:2px">
          <span class="field-key">path</span>
          <span class="field-val" style="font-size:.62rem;color:var(--teal)">$bronzePath/</span>
        </div>
        <div class="field-row">
          <span class="field-key">format</span>
          <span class="field-val">Delta Lake</span>
        </div>
        <div class="field-row">
          <span class="field-key">compression</span>
          <span class="field-val">Snappy Parquet</span>
        </div>
        <div class="field-row">
          <span class="field-key">write mode</span>
          <span class="field-val c-teal">Append</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Partitioning</div>
        <div class="field-row">
          <span class="field-key">partition by</span>
          <span class="field-val c-teal">stationId</span>
        </div>
        <div class="field-row" style="flex-direction:column;align-items:flex-start;gap:2px">
          <span class="field-key">partition path</span>
          <span class="field-val" style="font-size:.62rem;color:var(--muted)">stationId=GHCND%3A...</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Row count</div>
        <div class="field-row">
          <span class="field-key">current</span>
          <span class="field-val c-teal">$rowCount</span>
        </div>
        <div class="field-row">
          <span class="field-key">expected (full)</span>
          <span class="field-val">~48,000 rows</span>
        </div>
      </div>
      <hr class="divider">
      <div>
        <div class="section-label">Transaction log</div>
        <div class="field-row">
          <span class="field-key">_delta_log/</span>
          <span class="field-val">JSON commits</span>
        </div>
      </div>
    </div>
  </div>

</div><!-- /flow -->
</div><!-- /canvas -->

<div class="page-footer">
  Generated by <code>com.cornbelt.docs.DiagramJob</code> &middot; $generatedAt &middot;
  Re-run after config changes to keep this diagram current &middot;
  Next stage: <strong>Silver &rarr; WeatherTransformJob</strong>
</div>

</body>
</html>"""
  }
}
