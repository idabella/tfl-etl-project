# 🥈 Silver Layer — Bronze → Silver Transformations

This folder contains PySpark jobs that read raw JSON from the **Bronze layer (S3)**, apply cleaning, typing, deduplication, and flattening transformations, then write **Parquet files** to the **Silver layer (S3)**.

---

## 📐 Architecture

```
S3 Bronze (raw JSON)
        │
        ▼
  PySpark Silver Jobs
        │
        ▼
S3 Silver (cleaned Parquet, partitioned by year/month/day)
```

---

## 📁 Scripts

| Script | Input Topic | Output Path | Description |
|--------|-------------|-------------|-------------|
| `arrivals_silver.py` | `tfl.arrivals` | `s3a://tfl-silver-bucket/arrivals/` | Vehicle arrival predictions |
| `line_status_silver.py` | `tfl.line_status` | `s3a://tfl-silver-bucket/line_status/` | TFL line status snapshots |
| `bikepoint_silver.py` | `tfl.bikepoints` | `s3a://tfl-silver-bucket/bikepoints/` | Santander bike dock availability |
| `stoppoint_silver.py` | `tfl.stoppoints` | `s3a://tfl-silver-bucket/stoppoints/` | TFL stop points master data |
| `airquality_silver.py` | `tfl.air_quality` | `s3a://tfl-silver-bucket/air_quality/` | London air quality forecasts |

---

## 🔧 Transformations Applied

### `arrivals_silver.py`
**Source:** `tfl.arrivals` — Vehicle arrival predictions from TFL Unified API

| Transformation | Detail |
|----------------|--------|
| Schema enforcement | Explicit Spark schema applied at read time |
| Timestamp casting | `timestamp`, `expectedArrival`, `timeToLive`, `_ingested_at` → `TimestampType` |
| String cleaning | `stationName`, `destinationName`, `currentLocation`, `towards` → `trim()` |
| Normalization | `direction` → `lower() + trim()` |
| Null filtering | Rows with null `id`, `naptanId`, or `lineId` removed |
| Deduplication | On `id + timestamp` |
| Partitioning | By `_year`, `_month`, `_day` derived from `timestamp` |
| Audit column | `_silver_processed_at` added |
| Dropped field | `$type` (TFL internal metadata) |

**Output columns:**
```
id, operationType, vehicleId, naptanId, stationName, lineId, lineName,
platformName, direction, destinationNaptanId, destinationName, timestamp,
timeToStation, currentLocation, towards, expectedArrival, timeToLive,
modeName, _ingested_at, _silver_processed_at, _year, _month, _day
```

---

### `line_status_silver.py`
**Source:** `tfl.line_status` — TFL line status with nested `lineStatuses[]` array

| Transformation | Detail |
|----------------|--------|
| Array explosion | `lineStatuses[]` → one row per status per line via `explode_outer()` |
| Field extraction | `statusSeverity`, `statusSeverityDescription`, `reason` extracted from nested struct |
| Type casting | `statusSeverity` → `IntegerType` |
| Timestamp casting | `created`, `modified`, `_ingested_at` → `TimestampType` |
| Normalization | `modeName` → `lower() + trim()` |
| Null filtering | Rows with null `id` removed |
| Deduplication | On `id + _ingested_at` |
| Partitioning | By `_year`, `_month`, `_day` derived from `_ingested_at` |

**Output columns:**
```
id, name, modeName, statusSeverity, statusSeverityDescription, reason,
created, modified, _ingested_at, _silver_processed_at, _year, _month, _day
```

---

### `bikepoint_silver.py`
**Source:** `tfl.bikepoints` — Santander bike docks with nested `additionalProperties[]` array

| Transformation | Detail |
|----------------|--------|
| Property flattening | `additionalProperties[]` key-value array → individual columns using `filter()` |
| Extracted properties | `TerminalName`, `Installed`, `Locked`, `Temporary`, `NbBikes`, `NbEmptyDocks`, `NbDocks`, `NbStandardBikes`, `NbEBikes` |
| Numeric casting | `NbBikes`, `NbEmptyDocks`, `NbDocks`, `NbStandardBikes`, `NbEBikes` → `IntegerType` |
| Boolean casting | `Installed`, `Locked`, `Temporary` → `BooleanType` |
| Coordinate casting | `lat`, `lon` → `DoubleType` |
| Timestamp casting | `_ingested_at` → `TimestampType` |
| Null filtering | Rows with null `id` removed |
| Deduplication | On `id + _ingested_at` |
| Partitioning | By `_year`, `_month`, `_day` derived from `_ingested_at` |

**Output columns:**
```
id, commonName, placeType, lat, lon, terminalname, installed, locked,
temporary, nbbikes, nbemptydocks, nbdocks, nbstandardbikes, nbebikes,
_ingested_at, _silver_processed_at, _year, _month, _day
```

---

### `stoppoint_silver.py`
**Source:** `tfl.stoppoints` — TFL stop points with nested `additionalProperties[]` and `modes[]`

| Transformation | Detail |
|----------------|--------|
| Property extraction | `Zone` and `WiFi` extracted from `additionalProperties[]` using `filter()` |
| Array flattening | `modes[]` array → comma-separated string via `array_join()` |
| Boolean casting | `status` → `BooleanType` |
| Coordinate casting | `lat`, `lon` → `DoubleType` |
| Timestamp casting | `_ingested_at` → `TimestampType` |
| Null filtering | Rows with null `id` removed |
| Deduplication | On `id` (static reference data) |
| Partitioning | By `_year`, `_month`, `_day` derived from `_ingested_at` |

**Output columns:**
```
id, naptanId, commonName, placeType, stopType, stationNaptan, indicator,
stopLetter, status, lat, lon, zone, wifi, modes,
_ingested_at, _silver_processed_at, _year, _month, _day
```

---

### `airquality_silver.py`
**Source:** `tfl.air_quality` — London air quality forecasts with nested `currentForecast[]` array

| Transformation | Detail |
|----------------|--------|
| Array explosion | `currentForecast[]` → one row per forecast type via `explode_outer()` |
| Field extraction | `forecastType`, `forecastId`, `fromDate`, `toDate`, `forecastBand`, all pollutant bands |
| HTML cleaning | `forecastText` cleaned of `&lt;`, `&gt;`, `&#39;`, `<br/>` using `regexp_replace()` |
| Timestamp casting | `fromDate`, `toDate`, `_ingested_at` → `TimestampType` |
| Null filtering | Rows with null `forecastId` removed |
| Deduplication | On `forecastId + forecastType` |
| Partitioning | By `_year`, `_month`, `_day` derived from `_ingested_at` |

**Output columns:**
```
updatePeriod, updateFrequency, forecastType, forecastId, fromDate, toDate,
forecastBand, forecastSummary, no2Band, o3Band, pm10Band, pm25Band, so2Band,
forecastText, _ingested_at, _silver_processed_at, _year, _month, _day
```

---

## 🏃 Running the Jobs

### On Databricks
```bash
# Submit a single silver job
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  transformation/silver/arrivals_silver.py

# Run all silver jobs
for script in arrivals line_status bikepoint stoppoint airquality; do
  spark-submit transformation/silver/${script}_silver.py
done
```

### Environment Variables Required
```bash
AWS_ACCESS_KEY_ID=<your_key>
AWS_SECRET_ACCESS_KEY=<your_secret>
```

---

## 📦 Output Format

All silver jobs write **Parquet** files partitioned by date:

```
s3a://tfl-silver-bucket/
├── arrivals/
│   └── _year=2026/
│       └── _month=03/
│           └── _day=04/
│               └── part-00000-*.parquet
├── line_status/
├── bikepoints/
├── stoppoints/
└── air_quality/
```

---

## ✅ Data Quality Checks Applied

| Check | Applied In |
|-------|-----------|
| Null ID filtering | All scripts |
| Type enforcement | All scripts |
| Deduplication | All scripts |
| Timestamp validation | All scripts |
| HTML entity cleaning | `airquality_silver.py` |
| Array bounds safety | `explode_outer()` used (handles empty arrays) |

---

*Part of the TFL ETL Pipeline — Bronze → **Silver** → Gold medallion architecture.*