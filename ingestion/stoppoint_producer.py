"""
stoppoint_producer.py
─────────────────────────────────────────────────────────────────────────────
TFL Stop Point Kafka producer.

Fetches all stop points for configured transport modes
(``/StopPoint/Mode/{modes}``) and publishes each stop as a JSON message
to the ``tfl.stoppoints`` Kafka topic.

Module-level API
────────────────
  _require_env / build_kafka_config / build_http_session
  fetch_stoppoints(session, modes)    → list[dict]
  make_message_key(record)            → bytes
  enrich_stoppoint(record, ts)        → dict
  stoppoint_records(records, ts)      → Iterator[(bytes, bytes)]
  ensure_topic / on_delivery / run_once

Env vars: KAFKA_*, STOPPOINT_POLL_INTERVAL (default 86400 s),
          STOPPOINT_MODES (default: tube,overground,dlr,elizabeth-line)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

KAFKA_TOPIC: str = "tfl.stoppoints"
TOPIC_PARTITIONS: int = 4
TOPIC_REPLICATION: int = 1
TOPIC_RETENTION_MS: str = str(7 * 24 * 60 * 60 * 1_000)
DEFAULT_BOOTSTRAP: str = "localhost:9092"
FLUSH_TIMEOUT: int = 30
STOPPOINT_ENDPOINT: str = "/StopPoint/Mode/{modes}"
TFL_BASE_URL: str = "https://api.tfl.gov.uk"
DEFAULT_MODES: str = "tube,overground,dlr,elizabeth-line"


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Required env var '{name}' is not set. Check config/dev.env.")
    return value


def build_kafka_config() -> dict[str, Any]:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    security = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    cfg: dict[str, Any] = {
        "bootstrap.servers": bootstrap,
        "security.protocol": security,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 500,
        "linger.ms": 100,
        "batch.size": 65_536,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 100_000,
        "queue.buffering.max.kbytes": 102_400,
    }
    if security in ("SASL_SSL", "SASL_PLAINTEXT"):
        cfg["sasl.mechanisms"] = "PLAIN"
        cfg["sasl.username"] = _require_env("KAFKA_SASL_USERNAME")
        cfg["sasl.password"] = _require_env("KAFKA_SASL_PASSWORD")
        if security == "SASL_SSL":
            cfg["ssl.ca.location"] = os.getenv("KAFKA_SSL_CA_LOCATION", "")
    return cfg


def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json", "User-Agent": "tfl-etl-pipeline/1.0"})
    retry = Retry(total=5, backoff_factor=1.5, status_forcelist=(500, 502, 503, 504),
                  allowed_methods=["GET"], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_stoppoints(session: requests.Session, modes: str = DEFAULT_MODES) -> list[dict]:
    """
    Fetch all stop points for *modes*.

    The TFL API returns a paginated wrapper: ``{"stopPoints": [...], "total": N}``.
    We collect all pages until an empty page is returned.
    """
    url = f"{TFL_BASE_URL}{STOPPOINT_ENDPOINT.format(modes=modes)}"
    all_stops: list[dict] = []
    page = 1

    while True:
        while True:  # inner retry loop for 429
            resp = session.get(url, params={"page": page}, timeout=(10, 120))
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 60)))
                continue
            break

        if resp.status_code == 404:
            logger.warning("StopPoint: modes '%s' not found (404).", modes)
            break
        if not resp.ok:
            resp.raise_for_status()

        data = resp.json()
        # TFL wraps results in {"stopPoints": [...]}
        if isinstance(data, dict):
            page_items = data.get("stopPoints", [])
        elif isinstance(data, list):
            page_items = data
        else:
            logger.warning("Unexpected stoppoint response type: %s", type(data))
            break

        if not page_items:
            break

        all_stops.extend(page_items)
        logger.debug("StopPoint page %d → %d items (total so far: %d)", page, len(page_items), len(all_stops))

        # TFL default page size is 100; if less, we're on the last page
        if len(page_items) < 100:
            break
        page += 1

    logger.info("Fetched %d stop-point records for modes '%s'.", len(all_stops), modes)
    return all_stops


def make_message_key(record: dict) -> bytes:
    """Key by ``naptanId`` or ``id``."""
    stop_id = record.get("naptanId") or record.get("id", "unknown")
    return str(stop_id).encode()


def enrich_stoppoint(record: dict, ingested_at: str) -> dict:
    return {**record, "_ingested_at": ingested_at, "_source": "tfl_api_stoppoint", "_topic": KAFKA_TOPIC}


def stoppoint_records(records: list[dict], ingested_at: str) -> Iterator[tuple[bytes, bytes]]:
    for record in records:
        key = make_message_key(record)
        value = json.dumps(enrich_stoppoint(record, ingested_at), default=str).encode()
        yield key, value


def ensure_topic(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    if KAFKA_TOPIC in existing:
        return
    new_topic = NewTopic(topic=KAFKA_TOPIC, num_partitions=TOPIC_PARTITIONS,
                         replication_factor=TOPIC_REPLICATION,
                         config={"retention.ms": TOPIC_RETENTION_MS})
    for topic, fut in admin.create_topics([new_topic]).items():
        try:
            fut.result()
            logger.info("Created Kafka topic '%s'.", topic)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise


def on_delivery(err: Any, msg: Any) -> None:
    if err:
        logger.error("Delivery failed | topic=%s key=%s | %s", msg.topic(), msg.key(), err)
    else:
        logger.debug("Delivered | topic=%s partition=%d offset=%d", msg.topic(), msg.partition(), msg.offset())


def run_once(
    producer: Producer,
    session: requests.Session,
    modes: str = DEFAULT_MODES,
) -> int:
    """Fetch stop points and publish to Kafka. Returns messages produced."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    total = 0
    try:
        records = fetch_stoppoints(session, modes)
    except Exception as exc:
        logger.error("Failed to fetch stop points: %s", exc)
        return 0
    for key, value in stoppoint_records(records, ingested_at):
        producer.poll(0)
        producer.produce(topic=KAFKA_TOPIC, key=key, value=value, on_delivery=on_delivery)
        total += 1
    if total:
        remaining = producer.flush(timeout=FLUSH_TIMEOUT)
        if remaining:
            logger.warning("%d messages still in queue after flush timeout.", remaining)
    return total


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    poll_interval = int(os.getenv("STOPPOINT_POLL_INTERVAL", str(24 * 3600)))
    modes = os.getenv("STOPPOINT_MODES", DEFAULT_MODES)
    ensure_topic(bootstrap)
    producer = Producer(build_kafka_config())
    session = build_http_session()
    logger.info("StopPointProducer started | modes=%s poll_interval=%ds", modes, poll_interval)
    try:
        while True:
            count = run_once(producer, session, modes)
            logger.info("Published %d stop-point messages. Sleeping %ds …", count, poll_interval)
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=FLUSH_TIMEOUT)
        session.close()


if __name__ == "__main__":
    main()
