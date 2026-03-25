# tests/

Unit and integration tests using **pytest**.

## Test Files

| File | What it tests |
|------|--------------|
| `test_producers.py` | Kafka producer message serialisation & publishing |
| `test_tfl_client.py` | TFL API client (mocked HTTP responses) |
| `test_spark_jobs.py` | Silver/Gold Spark job transformations |
| `test_transformations.py` | Individual transformation functions |
| `test_kafka_pipeline.py` | End-to-end Kafka → S3 pipeline |
| `test_redshift_load.py` | Redshift upsert stored procedure |
| `test_dim_scd2.py` | SCD Type-2 dimension update logic |

## Running Tests

```bash
# All tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=ingestion --cov=transformation --cov-report=html

# Specific module
pytest tests/test_spark_jobs.py -v

# Integration tests only (requires running containers)
pytest tests/ -m integration
```

## Environment

Tests use `config/dev.env` by default. For CI, environment variables are injected via GitHub Actions secrets.
