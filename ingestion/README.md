# ingestion/

Kafka producers that poll the **TFL Unified API** and publish events to Kafka topics.

## Files

| File | Description |
|------|-------------|
| `base_producer.py` | Abstract base class for all producers (auth, retry, backoff) |
| `client.py` | TFL API HTTP client (rate limiting, pagination) |
| `endpoints.py` | API endpoint constants and URL builders |
| `arrivals_producer.py` | Vehicle arrivals → `tfl.arrivals` topic |
| `line_status_producer.py` | Line status → `tfl.line_status` topic |
| `bikepoint_producer.py` | Santander bike docks → `tfl.bikepoint` topic |
| `stoppoint_producer.py` | Stop points → `tfl.stoppoint` topic |
| `accidents_producer.py` | Road accident data → `tfl.accidents` topic |
| `airquality_producer.py` | Air quality readings → `tfl.airquality` topic |

## Kafka Topics

| Topic | Partitions | Retention |
|-------|-----------|-----------|
| `tfl.arrivals` | 12 | 7 days |
| `tfl.line_status` | 4 | 30 days |
| `tfl.bikepoint` | 4 | 7 days |
| `tfl.stoppoint` | 4 | 7 days |
| `tfl.accidents` | 4 | 90 days |
| `tfl.airquality` | 4 | 30 days |

## Usage

```bash
python ingestion/arrivals_producer.py
python ingestion/line_status_producer.py
```
