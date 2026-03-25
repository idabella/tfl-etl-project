# orchestration/

Apache **Airflow** DAGs, custom operators, and hooks that orchestrate the full pipeline.

## Sub-folders

### `dags/`

| DAG | Schedule | Description |
|-----|----------|-------------|
| `tfl_etl_pipeline.py` | `@hourly` | Main ETL pipeline: ingest → transform → load |
| `tfl_historical_load.py` | `@once` | Backfill historical data from TFL API |
| `tfl_dimensions_refresh.py` | `@daily` | Refresh all dimension tables |
| `tfl_data_quality.py` | `@daily` | Run Great Expectations validation suites |
| `tfl_powerbi_refresh.py` | `@daily` | Trigger Power BI dataset refresh via API |

### `plugins/`

| File | Type | Description |
|------|------|-------------|
| `tfl_hook.py` | Hook | Airflow Hook for TFL API authentication |
| `tfl_api_operator.py` | Operator | Custom operator to call TFL endpoints |
| `redshift_upsert_op.py` | Operator | Custom operator for Redshift upsert via stored procedure |

## Setup

Add these to Airflow:
```bash
# Import Airflow variables (from config/)
airflow variables import config/variables.json

# Import Airflow connections (from config/)
airflow connections import config/connections.json
```

Then copy the `dags/` and `plugins/` folders into your Airflow home:
```bash
cp orchestration/dags/*    $AIRFLOW_HOME/dags/
cp orchestration/plugins/* $AIRFLOW_HOME/plugins/
```
