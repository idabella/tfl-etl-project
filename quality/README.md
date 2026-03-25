# quality/

Data quality checks powered by **Great Expectations**.

## Files & Folders

| Path | Description |
|------|-------------|
| `great_expectations.yml` | GE project configuration |
| `pipeline_health_check.py` | Script to run all checkpoints and report results |
| `suites/tfl_arrivals_suite.json` | Expectations for arrivals data |
| `suites/tfl_dimensions_suite.json` | Expectations for dimension tables |
| `suites/tfl_line_status_suite.json` | Expectations for line status data |
| `checkpoints/checkpoint_silver.yml` | Checkpoint config for Silver layer |
| `checkpoints/checkpoint_gold.yml` | Checkpoint config for Gold layer |

## Running Quality Checks

```bash
# Run all checkpoints
python quality/pipeline_health_check.py

# Or trigger via Airflow
airflow dags trigger tfl_data_quality
```

## Adding New Expectations

```python
from great_expectations.data_context import DataContext

context = DataContext("quality/")
suite = context.get_expectation_suite("tfl_arrivals_suite")

# Add expectation
suite.add_expectation(
    ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "vehicle_id"}
    )
)
context.save_expectation_suite(suite)
```
