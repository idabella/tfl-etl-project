# transformation/

PySpark jobs implementing the **Bronze → Silver → Gold** medallion architecture.

## Sub-folders

### `schemas/`
Spark/Avro schema definitions for each data source. Used by both streaming and batch jobs to enforce types at ingestion time.

| File | Schema for |
|------|-----------|
| `arrivals_schema.py` | Vehicle arrivals payload |
| `bikepoint_schema.py` | Santander bike docks payload |
| `line_status_schema.py` | Line status payload |
| `stoppoint_schema.py` | Stop points payload |

### `silver/`
**Bronze → Silver** transformations: cast types, remove duplicates, flatten nested JSON, add audit columns.

| File | Input topic/table | Output |
|------|-------------------|--------|
| `arrivals_silver.py` | `tfl.arrivals` | `silver.arrivals` |
| `airquality_silver.py` | `tfl.airquality` | `silver.airquality` |
| `bikepoint_silver.py` | `tfl.bikepoint` | `silver.bikepoint` |
| `line_status_silver.py` | `tfl.line_status` | `silver.line_status` |
| `stoppoint_silver.py` | `tfl.stoppoint` | `silver.stoppoint` |

### `gold/`
**Silver → Gold** transformations: build the star schema dimensions and fact tables (Delta Lake).

**Dimensions:** `dim_date`, `dim_time`, `dim_line`, `dim_station`, `dim_zone`, `dim_disruption`  
**Facts:** `fact_arrivals`, `fact_line_status`

### `utils/`
Shared utilities used across all Spark jobs.

| File | Purpose |
|------|---------|
| `spark_session.py` | Configured `SparkSession` factory |
| `delta_utils.py` | Delta Lake merge / upsert helpers |
| `date_utils.py` | Date parsing and formatting helpers |
| `schema_utils.py` | Schema evolution helpers |

## Running a Job

```bash
# Silver job example
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0 \
  transformation/silver/arrivals_silver.py

# Gold dimension refresh
spark-submit transformation/gold/dim_station.py
```
