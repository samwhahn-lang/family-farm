package com.cornbelt.platinum

import com.cornbelt.utils.SparkSessionProvider
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import java.io.{File, FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object PlatinumExportJob extends LazyLogging {

  private val config    = ConfigFactory.load().getConfig("corn-belt")
  private val pathsCfg  = config.getConfig("paths")
  private val anchorCfg = config.getConfig("anchor")

  def main(args: Array[String]): Unit = {
    val spark    = SparkSessionProvider.session
    val goldPath = s"${pathsCfg.getString("gold")}/season_weather"
    val outDir   = "platinum"
    val outFile  = s"$outDir/index.html"

    Files.createDirectories(Paths.get(outDir))

    logger.info(s"Reading gold: $goldPath")
    val df = spark.read.format("delta").load(goldPath)

    if (df.count() == 0) {
      logger.error("Gold table is empty — run SeasonWeatherJob first.")
      spark.stop()
      sys.exit(1)
    }

    // Serialize gold rows to a JavaScript array literal
    val rows = df.orderBy("year").collect()
    val cropPhases = Seq("cornPlanting","cornGrowing","cornHarvest",
                         "soybeanPlanting","soybeanGrowing","soybeanHarvest",
                         "wheatPlanting","wheatGrowing","wheatHarvest")
    val months     = Seq("jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec")

    val jsArray = rows.map { r =>
      def d(f: String) = r.getAs[Double](f)
      def i(f: String) = r.getAs[Int](f)
      val base =
        s"""{year:${r.getAs[Int]("year")},""" +
        s"""growingDegreeDays:${d("growingDegreeDays")},totalPrecipIn:${d("totalPrecipIn")},""" +
        s"""frostFreeDays:${i("frostFreeDays")},extremeHeatDays:${i("extremeHeatDays")},droughtIndex:${d("droughtIndex")},""" +
        s"""avgTempF:${d("avgTempF")},totalSnowIn:${d("totalSnowIn")}"""
      val cropFields = cropPhases.map { p =>
        s"""${p}Gdd:${d(p+"Gdd")},${p}PrecipIn:${d(p+"PrecipIn")},""" +
        s"""${p}FrostFreeDays:${i(p+"FrostFreeDays")},${p}HeatStressDays:${i(p+"HeatStressDays")},${p}DroughtIndex:${d(p+"DroughtIndex")},""" +
        s"""${p}AvgTempF:${d(p+"AvgTempF")},${p}SnowIn:${d(p+"SnowIn")}"""
      }.mkString(",")
      val monthFields = months.map { m =>
        s"""${m}Gdd:${d(m+"Gdd")},${m}PrecipIn:${d(m+"PrecipIn")},""" +
        s"""${m}FrostFreeDays:${i(m+"FrostFreeDays")},${m}HeatStressDays:${i(m+"HeatStressDays")},${m}DroughtIndex:${d(m+"DroughtIndex")},""" +
        s"""${m}AvgTempF:${d(m+"AvgTempF")},${m}SnowIn:${d(m+"SnowIn")}"""
      }.mkString(",")
      base + "," + cropFields + "," + monthFields + "}"
    }.mkString("[\n  ", ",\n  ", "\n]")

    // Read the latest date with a valid average temperature from silver for the header subtitle
    val stationId  = anchorCfg.getString("station-id")
    val silverPath = s"${pathsCfg.getString("silver")}/weather_observations"
    val latestTempRow = spark.read.format("delta").load(silverPath)
      .filter(col("stationId") === stationId && col("tempAvgF").isNotNull)
      .orderBy(col("date").desc)
      .limit(1)
      .collect()

    val (latestTempStr, latestDateStr) =
      if (latestTempRow.nonEmpty) {
        val r    = latestTempRow(0)
        val temp = r.getAs[Double]("tempAvgF")
        val date = Option(r.getAs[Any]("date")).map { d =>
          val ld = d match {
            case sd: java.sql.Date => sd.toLocalDate
            case _                 => java.time.LocalDate.parse(d.toString)
          }
          ld.format(java.time.format.DateTimeFormatter.ofPattern("EEEE, MMM d, yyyy"))
        }.getOrElse("--")
        (f"$temp%.1f°F", date)
      } else ("--", "--")

    logger.info(s"Header: latest temp = $latestTempStr as of $latestDateStr")

    // Read last 5 years of daily silver data for the drill-down chart.
    // Includes all non-suspect rows so precip/snow show even on temp-missing days.
    val maxGoldYear   = rows.map(_.getAs[Int]("year")).max
    val dailyFromYear = java.time.LocalDate.now().getYear - 4

    val dailyRows = spark.read.format("delta").load(silverPath)
      .filter(col("stationId") === stationId && col("qualityFlag") =!= "SUSPECT")
      .filter(year(col("date")) >= dailyFromYear)
      .withColumn("dailyGdd",
        when(col("tempMaxF").isNotNull && col("tempMinF").isNotNull,
          greatest(lit(0.0),
            (least(col("tempMaxF"), lit(86.0)) + greatest(col("tempMinF"), lit(50.0))) / 2.0 - lit(50.0)
          )
        )
      )
      .withColumn("frostFreeFlag",  when(col("tempMinF") > 32.0, 1).otherwise(0))
      .withColumn("heatStressFlag", when(col("tempMaxF") > 95.0, 1).otherwise(0))
      .orderBy("date")
      .collect()

    logger.info(s"Daily silver rows ($dailyFromYear–$maxGoldYear): ${dailyRows.length}")

    val dailyJsArray = dailyRows.map { r =>
      val dateStr = Option(r.getAs[Any]("date")).map(_.toString).getOrElse("")
      def nd(f: String) = Option(r.getAs[Any](f)).map(_.toString).getOrElse("null")
      def bv(f: String) = Option(r.getAs[Any](f)) match {
        case Some(true) => "true"; case _ => "false"
      }
      s"""{date:'$dateStr',tempAvgF:${nd("tempAvgF")},precipIn:${nd("precipIn")},snowIn:${nd("snowIn")},""" +
      s"""dailyGdd:${nd("dailyGdd")},frostFreeFlag:${nd("frostFreeFlag")},heatStressFlag:${nd("heatStressFlag")},""" +
      s"""month:${r.getAs[Int]("month")},""" +
      s"""cornPlanting:${bv("cornPlanting")},cornGrowing:${bv("cornGrowing")},cornHarvest:${bv("cornHarvest")},""" +
      s"""soybeanPlanting:${bv("soybeanPlanting")},soybeanGrowing:${bv("soybeanGrowing")},soybeanHarvest:${bv("soybeanHarvest")},""" +
      s"""wheatPlanting:${bv("wheatPlanting")},wheatGrowing:${bv("wheatGrowing")},wheatHarvest:${bv("wheatHarvest")}}"""
    }.mkString("[\n  ", ",\n  ", "\n]")

    val outFileObj = new File(outFile)
    val existed    = outFileObj.exists()
    val html       = buildHtml(jsArray, dailyJsArray, latestTempStr, latestDateStr)
    val writer     = new PrintWriter(new OutputStreamWriter(new FileOutputStream(outFileObj), StandardCharsets.UTF_8))
    writer.print(html)
    writer.close()

    val action = if (existed) "updated" else "created"
    logger.info(s"index.html $action -> $outFile  (${rows.length} years, ${dailyRows.length} daily rows, ${html.length} bytes)")
  }

  private def buildHtml(jsArray: String, dailyJsArray: String, latestTempStr: String, latestDateStr: String): String = s"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Gage County Crop Analysis</title>
  <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
  <style>
    :root{
      --bg:#f5f0e8;--card:#ffffff;--border:#e2d6c4;--text:#2b1f0e;--muted:#8a7a65;
      --accent:#5c4020;--gold:#9a7318;--corn:#c47d0a;--soy:#4a7527;--wheat:#9a6118;
      --sky:#4878a0;--shadow:0 1px 3px rgba(0,0,0,.09);
    }
    *{box-sizing:border-box}
    body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:var(--bg);color:var(--text);font-size:14px;line-height:1.4}
    /* header */
    .header{background:var(--accent);color:#f5f0e8;padding:16px 28px;border-bottom:3px solid var(--gold);text-align:center}
    .header h1{margin:0;font-size:1.9rem;font-weight:700;letter-spacing:.01em}
    .header p{margin:3px 0 0;opacity:.7;font-size:.8rem;letter-spacing:.02em}
    /* layout */
    .main{display:flex;gap:18px;padding:18px 24px;max-width:1700px;margin:0 auto;align-items:flex-start}
    .sidebar{flex:0 0 295px;display:flex;flex-direction:column;gap:14px}
    .content{flex:1;min-width:0;display:flex;flex-direction:column;gap:14px}
    /* panels */
    .panel{background:var(--card);border:1px solid var(--border);border-radius:5px;box-shadow:var(--shadow);overflow:hidden}
    .panel-hd{padding:8px 14px;background:#f9f5ee;border-bottom:1px solid var(--border);font-size:.67rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);text-align:center}
    .panel-bd{padding:13px 14px;display:flex;flex-direction:column;gap:13px}
    /* form */
    .flbl{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:5px;text-align:center}
    .year-row{display:flex;align-items:center;gap:6px;justify-content:center}
    .year-row input{width:66px;padding:4px 6px;border:1px solid var(--border);border-radius:3px;font-size:.83rem;text-align:center;background:#faf6f0;color:var(--text)}
    .year-row input:focus{outline:none;border-color:var(--gold)}
    /* season reference panel */
    .ref-crop{margin-bottom:10px}
    .ref-crop:last-child{margin-bottom:0}
    .ref-crop-name{font-size:.65rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;margin-bottom:4px;padding-bottom:2px;border-bottom:1px solid var(--border)}
    .ref-row{display:flex;justify-content:space-between;align-items:baseline;padding:2px 0;font-size:.72rem}
    .ref-phase{color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.04em;font-size:.62rem}
    .ref-dates{color:var(--text);font-weight:500}
    .sep{color:var(--muted)}
    /* divider */
    .divider{border:none;border-top:1px solid var(--border);margin:0}
    /* crop grid */
    .crop-grid{display:grid;grid-template-columns:44px 1fr;gap:4px 5px;align-items:center}
    .clbl{font-size:.65rem;font-weight:700;text-align:right;padding-right:3px;text-transform:uppercase;letter-spacing:.04em}
    .clbl-corn{color:var(--corn)}.clbl-soy{color:var(--soy)}.clbl-wheat{color:var(--wheat)}
    /* buttons */
    .bt{display:flex;flex-wrap:wrap;gap:3px}
    .bt button{padding:3px 8px;border:1px solid var(--border);border-radius:3px;background:var(--card);cursor:pointer;font-size:.73rem;color:var(--muted);transition:all .1s;white-space:nowrap}
    .bt button:hover{border-color:var(--gold);color:var(--text)}
    .bt button.on{background:var(--gold);color:#fff;border-color:var(--gold);font-weight:600}
    .corn-btn.on{background:var(--corn)!important;border-color:var(--corn)!important}
    .soy-btn.on{background:var(--soy)!important;border-color:var(--soy)!important}
    .wheat-btn.on{background:var(--wheat)!important;border-color:var(--wheat)!important}
    .month-btn.on{background:var(--sky)!important;border-color:var(--sky)!important}
    /* month grid */
    .month-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:3px}
    .month-grid button{text-align:center}
    /* chips */
    .chips{display:flex;flex-direction:column;gap:5px}
    .chip{display:flex;align-items:center;gap:6px;padding:5px 10px;border-radius:3px;border:1px solid;cursor:pointer;font-size:.75rem;font-weight:600;user-select:none;transition:all .1s}
    .chip input{display:none}
    /* inline stats */
    .stat-chip{display:flex;align-items:baseline;gap:4px;padding:2px 9px;background:#f9f5ee;border:1px solid var(--border);border-radius:10px;white-space:nowrap}
    .stat-lbl{font-size:.62rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
    .stat-val{font-size:.78rem;font-weight:700}
    /* chart */
    .chart-card{background:var(--card);border:1px solid var(--border);border-radius:5px;box-shadow:var(--shadow);padding:10px 6px 4px}
    .view-badge{display:inline-block;padding:2px 10px;border-radius:10px;font-size:.7rem;font-weight:700;letter-spacing:.04em;margin:0 8px 4px;border:1px solid var(--border);background:#f5f0e8;color:var(--muted)}
    .view-badge.daily{background:#e8f2e0;color:#2d6020;border-color:#b8d8a8}
    /* table */
    .tbl-card{background:var(--card);border:1px solid var(--border);border-radius:5px;box-shadow:var(--shadow);overflow:hidden}
    .tbl-hd{padding:9px 14px;background:#f9f5ee;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:7px;font-size:.8rem;font-weight:600;color:var(--text)}
    .badge{padding:2px 8px;border-radius:10px;font-size:.65rem;font-weight:600;background:#ede5d4;color:var(--accent);border:1px solid #d8ccb4}
    table{width:100%;border-collapse:collapse}
    th{padding:7px 13px;text-align:left;font-size:.65rem;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);background:#f9f5ee;border-bottom:1px solid var(--border);font-weight:600}
    td{padding:7px 13px;font-size:.82rem;border-bottom:1px solid #f0ebe0}
    tr:last-child td{border-bottom:none}
    tr:hover td{background:#faf6f0}
    .tag{display:inline-block;padding:2px 7px;border-radius:10px;font-size:.68rem;font-weight:700}
    .ok{background:#e8f2e0;color:#2d6020}.watch{background:#f8edcc;color:#7a5200}.stress{background:#f5e0dd;color:#8b1a0e}
    /* metric doc tooltip */
    #metricTip{position:fixed;display:none;background:#2b1f0e;color:#f5f0e8;padding:10px 13px;border-radius:5px;font-size:.72rem;line-height:1.55;width:260px;z-index:1000;pointer-events:none;box-shadow:0 3px 10px rgba(0,0,0,.35)}
    #metricTip strong{display:block;font-size:.78rem;margin-bottom:5px;color:#d4a830}
    #metricTip .formula{font-family:monospace;font-size:.68rem;background:rgba(255,255,255,.1);padding:3px 6px;border-radius:3px;margin:4px 0;display:block}
  </style>
</head>
<body>
<header class="header">
  <h1>Gage County Crop Analysis</h1>
  <p>Beatrice, Nebraska</p>
  <p style="margin-top:6px;font-size:1.05rem;opacity:.92;letter-spacing:.01em">
    Local temperature: <strong>$latestTempStr</strong> &nbsp;&middot;&nbsp; as of $latestDateStr
  </p>
</header>
<div class="main">

  <!-- ── SIDEBAR ─────────────────────────────── -->
  <aside class="sidebar">

    <div class="panel">
      <div class="panel-hd">Filters</div>
      <div class="panel-bd">

        <div>
          <div class="flbl">Year Range</div>
          <div class="year-row">
            <input type="number" id="yFrom"><span class="sep">&ndash;</span><input type="number" id="yTo">
          </div>
        </div>

        <div>
          <div class="flbl">Crop Season</div>
          <div class="crop-grid">
            <span class="clbl clbl-corn">Corn</span>
            <div class="bt" id="cornBtns">
              <button class="corn-btn" data-s="cornPlanting">Planting</button>
              <button class="corn-btn" data-s="cornGrowing">Growing</button>
              <button class="corn-btn" data-s="cornHarvest">Harvest</button>
            </div>
            <span class="clbl clbl-soy">Soy</span>
            <div class="bt" id="soyBtns">
              <button class="soy-btn" data-s="soybeanPlanting">Planting</button>
              <button class="soy-btn" data-s="soybeanGrowing">Growing</button>
              <button class="soy-btn" data-s="soybeanHarvest">Harvest</button>
            </div>
            <span class="clbl clbl-wheat">Wheat</span>
            <div class="bt" id="wheatBtns">
              <button class="wheat-btn" data-s="wheatPlanting">Planting</button>
              <button class="wheat-btn" data-s="wheatGrowing">Growing</button>
              <button class="wheat-btn" data-s="wheatHarvest">Harvest</button>
            </div>
          </div>
          <div class="bt" style="margin-top:5px">
            <button id="allBtn" class="on" data-s="all">All Season</button>
          </div>
        </div>

        <hr class="divider">

        <div>
          <div class="flbl">Calendar Month</div>
          <div class="month-grid" id="monthBtns"></div>
        </div>

      </div>
    </div>

    <div class="panel">
      <div class="panel-hd">Values</div>
      <div class="panel-bd">

        <div>
          <div class="flbl">Metrics</div>
          <div class="chips" id="chips"></div>
        </div>

        <div>
          <div class="flbl">Chart Type</div>
          <div class="bt" id="typeBtns">
            <button class="on" data-t="lines+markers">Line</button>
            <button data-t="bar">Bar</button>
          </div>
        </div>

      </div>
    </div>

    <div class="panel">
      <div class="panel-hd">Season Reference</div>
      <div class="panel-bd" style="padding:12px 14px">
        <div class="ref-crop">
          <div class="ref-crop-name" style="color:var(--corn)">Corn</div>
          <div class="ref-row"><span class="ref-phase">Planting</span><span class="ref-dates">Apr 20 &ndash; May 15</span></div>
          <div class="ref-row"><span class="ref-phase">Growing</span><span class="ref-dates">May 1 &ndash; Sep 30</span></div>
          <div class="ref-row"><span class="ref-phase">Harvest</span><span class="ref-dates">Sep 1 &ndash; Oct 31</span></div>
        </div>
        <div class="ref-crop">
          <div class="ref-crop-name" style="color:var(--soy)">Soybeans</div>
          <div class="ref-row"><span class="ref-phase">Planting</span><span class="ref-dates">May 1 &ndash; Jun 10</span></div>
          <div class="ref-row"><span class="ref-phase">Growing</span><span class="ref-dates">May 1 &ndash; Oct 31</span></div>
          <div class="ref-row"><span class="ref-phase">Harvest</span><span class="ref-dates">Sep 20 &ndash; Oct 31</span></div>
        </div>
        <div class="ref-crop">
          <div class="ref-crop-name" style="color:var(--wheat)">Winter Wheat</div>
          <div class="ref-row"><span class="ref-phase">Planting</span><span class="ref-dates">Sep 20 &ndash; Oct 31</span></div>
          <div class="ref-row"><span class="ref-phase">Growing</span><span class="ref-dates">Sep 20 &ndash; Jun 30 *</span></div>
          <div class="ref-row"><span class="ref-phase">Harvest</span><span class="ref-dates">Jun 1 &ndash; Jul 10</span></div>
          <div style="font-size:.62rem;color:var(--muted);margin-top:3px">* overwinters across calendar year</div>
        </div>
      </div>
    </div>

  </aside>

  <!-- ── CONTENT ──────────────────────────────── -->
  <div class="content">
    <div class="chart-card">
      <div style="padding:4px 8px 2px;display:flex;align-items:center;gap:12px;flex-wrap:wrap">
        <span id="viewBadge" class="view-badge">Annual View</span>
        <div id="statsInline" style="display:flex;gap:8px;flex-wrap:wrap"></div>
      </div>
      <div id="chart" style="height:440px"></div>
    </div>
    <div class="tbl-card">
      <div class="tbl-hd">Season Data <span class="badge" id="filterBadge"></span></div>
      <table><thead><tr id="thead-row"></tr></thead><tbody id="tbody"></tbody></table>
    </div>
  </div>

</div>
<script>
// Filter out years where no growing-season data exists yet.
// This happens for the current partial year when Apr-Nov observations haven't
// been reported to NCEI yet — gold stores them as 0.0 which is misleading to plot.
const DATA = ($jsArray).filter(function(d){
  return d.frostFreeDays > 0 || d.growingDegreeDays > 0 || d.totalPrecipIn > 0;
});

const DAILY_DATA = $dailyJsArray;

// Maps metric id to the field name used in DAILY_DATA rows
const DAILY_FIELD = {
  gdd:'dailyGdd', precip:'precipIn', avgTemp:'tempAvgF', snow:'snowIn',
  frostFree:'frostFreeFlag', heatStress:'heatStressFlag', drought:null
};

// Month key -> number for daily season filtering
const MONTH_NUM = {jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12};

function isDailyMode(){
  if(!DAILY_DATA.length) return false;
  var from=parseInt(yf.value), to=parseInt(yt.value);
  if(to-from > 4) return false;
  return DAILY_DATA.some(function(d){ var y=parseInt(d.date); return y>=from&&y<=to; });
}

function fdDaily(){
  var from=parseInt(yf.value), to=parseInt(yt.value);
  var rows=DAILY_DATA.filter(function(d){
    var y=parseInt(d.date); return y>=from&&y<=to;
  });
  if(season==='all') return rows;
  if(MONTH_NUM[season]) return rows.filter(function(d){ return d.month===MONTH_NUM[season]; });
  return rows.filter(function(d){ return d[season]===true; });
}

const SEASON_MAP = {all:{gdd:'growingDegreeDays',precip:'totalPrecipIn',frostFree:'frostFreeDays',heatStress:'extremeHeatDays',drought:'droughtIndex',avgTemp:'avgTempF',snow:'totalSnowIn',label:'All Season (Apr-Nov)'}};
var _cropDefs=[
  {k:'cornPlanting',   lb:'Corn Planting (Apr 20 - May 15)'},
  {k:'cornGrowing',    lb:'Corn Growing (May - Sep)'},
  {k:'cornHarvest',    lb:'Corn Harvest (Sep - Oct)'},
  {k:'soybeanPlanting',lb:'Soybean Planting (May 1 - Jun 10)'},
  {k:'soybeanGrowing', lb:'Soybean Growing (May - Oct)'},
  {k:'soybeanHarvest', lb:'Soybean Harvest (Sep 20 - Oct)'},
  {k:'wheatPlanting',  lb:'Wheat Planting (Sep 20 - Oct)'},
  {k:'wheatGrowing',   lb:'Wheat Growing (Sep 20 - Jun 30)'},
  {k:'wheatHarvest',   lb:'Wheat Harvest (Jun 1 - Jul 10)'},
];
_cropDefs.forEach(function(d){
  SEASON_MAP[d.k]={gdd:d.k+'Gdd',precip:d.k+'PrecipIn',frostFree:d.k+'FrostFreeDays',heatStress:d.k+'HeatStressDays',drought:d.k+'DroughtIndex',avgTemp:d.k+'AvgTempF',snow:d.k+'SnowIn',label:d.lb};
});
var _monthDefs=[
  {k:'jan',lb:'January'},{k:'feb',lb:'February'},{k:'mar',lb:'March'},{k:'apr',lb:'April'},
  {k:'may',lb:'May'},{k:'jun',lb:'June'},{k:'jul',lb:'July'},{k:'aug',lb:'August'},
  {k:'sep',lb:'September'},{k:'oct',lb:'October'},{k:'nov',lb:'November'},{k:'dec',lb:'December'},
];
_monthDefs.forEach(function(d){
  SEASON_MAP[d.k]={gdd:d.k+'Gdd',precip:d.k+'PrecipIn',frostFree:d.k+'FrostFreeDays',heatStress:d.k+'HeatStressDays',drought:d.k+'DroughtIndex',avgTemp:d.k+'AvgTempF',snow:d.k+'SnowIn',label:d.lb};
});

const MONTH_KEYS  = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
const MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

const METRICS = [
  {id:'avgTemp',    lbl:'Avg Temperature',  unit:'°F',       col:'#e07030', ax:'y',
   doc:'<strong>Average Temperature (°F)</strong><span class="formula">mean of (Tmax + Tmin) ÷ 2</span>Computed across all days in the selected season or month.'},
  {id:'precip',     lbl:'Precipitation',    unit:'in',       col:'#4878a0', ax:'y2',
   doc:'<strong>Precipitation (inches)</strong>Sum of daily PRCP observations over the selected period. Source: NOAA GHCND PRCP field.'},
  {id:'snow',       lbl:'Snowfall',         unit:'in',       col:'#6090c0', ax:'y2',
   doc:'<strong>Snowfall (inches)</strong>Sum of daily SNOW field values over the selected period. Source: NOAA GHCND SNOW field.'},
  {id:'frostFree',  lbl:'Frost-Free Days',  unit:'days',     col:'#4a7527', ax:'y',
   doc:'<strong>Frost-Free Days</strong><span class="formula">count of days where Tmin &gt; 32°F</span>Indicates the frost-free window available for crop growth.'},
  {id:'heatStress', lbl:'Heat Stress Days', unit:'days',     col:'#b34030', ax:'y',
   doc:'<strong>Heat Stress Days</strong><span class="formula">count of days where Tmax &gt; 95°F</span>Prolonged heat above 95°F reduces corn and soybean yields.'},
  {id:'gdd',        lbl:'GDD',              unit:'deg-days', col:'#c47d0a', ax:'y',
   doc:'<strong>Growing Degree Days</strong><span class="formula">max(0, (min(Tmax,86°F) + max(Tmin,50°F)) ÷ 2 − 50°F)</span>Accumulated daily over the selected season. Measures the cumulative heat units available for crop development.'},
  {id:'drought',    lbl:'Drought Index',    unit:'',         col:'#7a5c2a', ax:'y2',
   doc:'<strong>Drought Index</strong><span class="formula">(actual precip − baseline avg) ÷ baseline avg</span>Positive = wetter than average.<br>Negative = drier than average.'},
];

function resolveKey(m){ return SEASON_MAP[season][m.id]; }

let active = 'avgTemp';  // single active metric id
let ctype  = 'lines+markers';
let season = 'all';

const years=DATA.map(d=>d.year);
const mn=Math.min.apply(null,years), mx=Math.max.apply(null,years);
const dailyYears=DAILY_DATA.map(function(d){return parseInt(d.date);}).filter(function(y){return !isNaN(y);});
const mxAll=dailyYears.length?Math.max(mx,Math.max.apply(null,dailyYears)):mx;
const yf=document.getElementById('yFrom'), yt=document.getElementById('yTo');
yf.min=mn; yf.max=mxAll; yt.min=mn; yt.max=mxAll;
yf.value=Math.max(mn,mxAll-1); yt.value=mxAll;

MONTH_KEYS.forEach(function(k,i){
  var b=document.createElement('button');
  b.className='month-btn'; b.setAttribute('data-s',k); b.textContent=MONTH_NAMES[i];
  b.onclick=function(){ selectSeason(k,'month'); };
  document.getElementById('monthBtns').appendChild(b);
});

document.querySelectorAll('[data-s]').forEach(function(b){
  if(b.id==='allBtn'){ b.onclick=function(){ selectSeason('all','all'); }; return; }
  if(!b.classList.contains('month-btn')){
    b.onclick=function(){ selectSeason(b.getAttribute('data-s'),'crop'); };
  }
});

function selectSeason(s,group){
  season=s;
  document.querySelectorAll('[data-s]').forEach(function(b){ b.classList.remove('on'); });
  document.querySelectorAll('[data-s="'+s+'"]').forEach(function(b){ b.classList.add('on'); });
  var cropSection=document.getElementById('cornBtns').closest('.panel-bd');
  var monthSection=document.getElementById('monthBtns').parentElement;
  cropSection.style.opacity=(group==='month')?'0.38':'1';
  monthSection.style.opacity=(group==='crop')?'0.38':'1';
  render();
}

function refreshChipStyles(){
  document.querySelectorAll('.chip').forEach(function(c){
    var inp=c.querySelector('input');
    var met=METRICS.find(function(x){return x.id===inp.value;});
    if(!met)return;
    var on=inp.value===active;
    c.style.background=on?met.col:'#fff'; c.style.color=on?'#fff':met.col;
  });
}

var metricTip=document.createElement('div');
metricTip.id='metricTip';
document.body.appendChild(metricTip);

function posMetricTip(e){
  var x=e.clientX+18,y=e.clientY-10;
  var w=metricTip.offsetWidth,h=metricTip.offsetHeight;
  if(x+w>window.innerWidth)x=e.clientX-w-18;
  if(y+h>window.innerHeight)y=e.clientY-h-10;
  metricTip.style.left=x+'px'; metricTip.style.top=y+'px';
}

METRICS.forEach(function(m){
  var c=document.createElement('label');
  c.className='chip'; c.style.borderColor=m.col;
  var on=(m.id===active);
  c.style.background=on?m.col:'#fff'; c.style.color=on?'#fff':m.col;
  c.innerHTML='<input type="radio" name="metric" value="'+m.id+'"'+(on?' checked':'')+'>  '+m.lbl;
  c.querySelector('input').onchange=function(e){
    if(e.target.checked){ active=m.id; refreshChipStyles(); render(); }
  };
  if(m.doc){
    c.addEventListener('mouseenter',function(e){ metricTip.innerHTML=m.doc; metricTip.style.display='block'; posMetricTip(e); });
    c.addEventListener('mousemove',posMetricTip);
    c.addEventListener('mouseleave',function(){ metricTip.style.display='none'; });
  }
  document.getElementById('chips').appendChild(c);
});

document.querySelectorAll('#typeBtns button').forEach(function(b){
  b.onclick=function(){
    ctype=b.getAttribute('data-t');
    document.querySelectorAll('#typeBtns button').forEach(function(x){x.classList.remove('on');});
    b.classList.add('on');
    render();
  };
});

function fd(){ return DATA.filter(function(d){return d.year>=parseInt(yf.value)&&d.year<=parseInt(yt.value);}); }

var _monthNames={jan:'January',feb:'February',mar:'March',apr:'April',
  may:'May',jun:'June',jul:'July',aug:'August',
  sep:'September',oct:'October',nov:'November',dec:'December'};

function buildChartTitle(daily){
  var m=METRICS.find(function(x){return x.id===active;});
  var sm=SEASON_MAP[season];
  if(!m||!sm)return'';
  var mLbl=m.lbl;
  var t;
  if(_monthNames[season]) t=mLbl+' in '+_monthNames[season]+(daily?' (Daily)':'');
  else if(season==='all') t=daily ? mLbl+' — Full Year (Daily)' : mLbl+' — Growing Season (Apr-Nov)';
  else { var phaseName=sm.label.split('(')[0].trim(); t=mLbl+' — '+phaseName+(daily?' (Daily)':''); }
  return '<b>'+t+'</b>';
}

function render(){
  var m=METRICS.find(function(x){return x.id===active;});
  if(!m){Plotly.purge('chart');return;}
  var daily=isDailyMode();
  var badge=document.getElementById('viewBadge');
  badge.textContent=daily?'Daily View':'Annual View';
  badge.className='view-badge'+(daily?' daily':'');
  var ff="-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif";
  var axFont={family:ff,size:14,color:'#2b1f0e',weight:700};
  var tickFont={family:ff,size:12,color:'#2b1f0e',weight:700};
  var yLabel='<b>'+m.lbl+(m.unit?' ('+m.unit+')':'')+'</b>';
  var titleLayout={font:{family:ff,size:18,color:'#2b1f0e',weight:700},x:0.5,xanchor:'center'};
  var yAxisBase={title:{text:yLabel,font:axFont,standoff:10},showgrid:false,
    zeroline:true,zerolinecolor:'#c8bca8',zerolinewidth:1,
    tickfont:tickFont,linecolor:'#d8ccb4',linewidth:1};
  var baseLayout={margin:{t:64,r:55,b:72,l:80},hovermode:'x unified',showlegend:false,
    plot_bgcolor:'#fff',paper_bgcolor:'#fff',font:{family:ff,size:11,color:'#2b1f0e'},barmode:'group',
    yaxis:yAxisBase};

  if(daily){
    var rows=fdDaily();
    var fk=DAILY_FIELD[active];
    var traces=[{
      x:rows.map(function(r){return r.date;}),
      y:rows.map(function(r){return fk?r[fk]:null;}),
      name:m.lbl,
      type:ctype==='bar'?'bar':'scatter',
      mode:ctype==='bar'?undefined:'lines',
      marker:{color:m.col,size:3}, line:{color:m.col,width:1.5},
      connectgaps:false,
      hovertemplate:'<b>'+m.lbl+'</b><br>%{x}<br>%{y:.2f} '+m.unit+'<extra></extra>',
    }];
    var layout=Object.assign({},baseLayout,{
      title:Object.assign({text:buildChartTitle(true)},titleLayout),
      xaxis:{title:{text:'<b>Date</b>',font:axFont,standoff:10},type:'date',tickangle:-45,
        showgrid:false,tickfont:tickFont,linecolor:'#d8ccb4',linewidth:1}
    });
    Plotly.react('chart',traces,layout,{responsive:true,displayModeBar:true,modeBarButtonsToRemove:['lasso2d','select2d']});
    renderStats(rows,true);
    renderTable(fd());
  } else {
    var d=fd(), ys=d.map(function(r){return r.year;});
    var k=resolveKey(m);
    var traces=[{
      x:ys, y:d.map(function(r){return r[k];}), name:m.lbl,
      type:ctype==='bar'?'bar':'scatter',
      mode:ctype==='bar'?undefined:'lines+markers',
      marker:{color:m.col,size:6}, line:{color:m.col,width:2.5},
      hovertemplate:'<b>'+m.lbl+'</b><br>Year: %{x}<br>%{y:.2f} '+m.unit+'<extra></extra>',
    }];
    var layout=Object.assign({},baseLayout,{
      title:Object.assign({text:buildChartTitle(false)},titleLayout),
      xaxis:{title:{text:'<b>Year</b>',font:axFont,standoff:10},tickmode:'linear',dtick:1,tickangle:-45,
        showgrid:false,tickfont:tickFont,linecolor:'#d8ccb4',linewidth:1,mirror:false}
    });
    Plotly.react('chart',traces,layout,{responsive:true,displayModeBar:true,modeBarButtonsToRemove:['lasso2d','select2d']});
    renderStats(d,false);
    renderTable(d);
  }
}

function renderStats(d,daily){
  var el=document.getElementById('statsInline'); el.innerHTML='';
  var m=METRICS.find(function(x){return x.id===active;});
  if(!m)return;
  var k=daily?DAILY_FIELD[active]:resolveKey(m);
  if(!k)return;
  var vs=d.map(function(r){return r[k];}).filter(function(v){return v!=null&&!isNaN(v);});
  if(!vs.length)return;
  var n=vs.length;
  var sum=vs.reduce(function(a,b){return a+b;},0);
  var avg=sum/n;
  var mn=Math.min.apply(null,vs);
  var mx=Math.max.apply(null,vs);
  var sd=Math.sqrt(vs.reduce(function(a,v){return a+(v-avg)*(v-avg);},0)/n);
  var u=m.unit?' '+m.unit:'';
  [{lbl:'Avg',val:avg},{lbl:'Min',val:mn},{lbl:'Max',val:mx},{lbl:'σ',val:sd}].forEach(function(s){
    var c=document.createElement('span');
    c.className='stat-chip';
    c.innerHTML='<span class="stat-lbl">'+s.lbl+'</span><span class="stat-val" style="color:'+m.col+'">'+s.val.toFixed(1)+u+'</span>';
    el.appendChild(c);
  });
}

function tag(r,sm){
  var di=r[sm.drought], hs=r[sm.heatStress];
  if(di<-0.2||hs>20)return'<span class="tag stress">Stress</span>';
  if(di<0||hs>10)   return'<span class="tag watch">Watch</span>';
  return'<span class="tag ok">Normal</span>';
}

function renderTable(d){
  var sm=SEASON_MAP[season];
  document.getElementById('filterBadge').textContent=sm.label;
  document.getElementById('thead-row').innerHTML=
    '<th>Year</th><th>GDD</th><th>Precip (in)</th><th>Avg Temp (°F)</th><th>Snow (in)</th>'+
    '<th>Frost-Free Days</th><th>Heat Stress Days</th><th>Drought Index</th><th>Conditions</th>';
  document.getElementById('tbody').innerHTML=d.slice().reverse().map(function(r){
    return '<tr>'+
      '<td><strong>'+r.year+'</strong></td>'+
      '<td>'+r[sm.gdd].toFixed(0)+'</td>'+
      '<td>'+r[sm.precip].toFixed(1)+'</td>'+
      '<td>'+(r[sm.avgTemp]!=null?r[sm.avgTemp].toFixed(1):'—')+'</td>'+
      '<td>'+(r[sm.snow]!=null?r[sm.snow].toFixed(1):'—')+'</td>'+
      '<td>'+r[sm.frostFree]+'</td>'+
      '<td>'+r[sm.heatStress]+'</td>'+
      '<td>'+(r[sm.drought]>=0?'+':'')+r[sm.drought].toFixed(3)+'</td>'+
      '<td>'+tag(r,sm)+'</td></tr>';
  }).join('');
}

yf.onchange=render; yt.onchange=render;
render();
</script>
</body>
</html>"""
}
