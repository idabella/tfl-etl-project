"""
line_status_producer.py
─────────────────────────────────────────────────────────────────────────────
TFL Line Status Kafka producer.

Fetches current status for all TFL lines (``/Line/Mode/{modes}/Status``)
and publishes each line record as a JSON message to ``tfl.line_status``.

Module-level API
────────────────
  _require_env / build_kafka_config / build_http_session
  fetch_line_status(session, modes)   → list[dict]
  make_message_key(record)            → bytes
  enrich_line_status(record, ts)      → dict
  line_status_records(records, ts)    → Iterator[(bytes, bytes)]
  ensure_topic / on_delivery / run_once

Env vars: KAFKA_*, LINE_STATUS_POLL_INTERVAL (default 60 s),
          LINE_STATUS_MODES (default: tube,overground,dlr,elizabeth-line,bus)
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

KAFKA_TOPIC: str = "tfl.line_status"
TOPIC_PARTITIONS: int = 4
TOPIC_REPLICATION: int = 1
TOPIC_RETENTION_MS: str = str(7 * 24 * 60 * 60 * 1_000)
DEFAULT_BOOTSTRAP: str = "localhost:9092"
FLUSH_TIMEOUT: int = 30
LINE_STATUS_ENDPOINT: str = "/Line/Mode/{modes}/Status"
TFL_BASE_URL: str = "https://api.tfl.gov.uk"
DEFAULT_MODES: str = "tube,overground,dlr,elizabeth-line,bus"


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


def fetch_line_status(session: requests.Session, modes: str = DEFAULT_MODES) -> list[dict]:
    """Fetch current status for all lines matching *modes*."""
    url = f"{TFL_BASE_URL}{LINE_STATUS_ENDPOINT.format(modes=modes)}"
    while True:
        resp = session.get(url, timeout=(10, 60))
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", 60)))
            continue
        if resp.status_code == 404:
            logger.warning("Line status: modes '%s' not found (404).", modes)
            return []
        if not resp.ok:
            resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Unexpected line_status response type: %s", type(data))
            return []
        logger.info("Fetched %d line-status records.", len(data))
        return data


def make_message_key(record: dict) -> bytes:
    """Key by line ``id`` field."""
    line_id = record.get("id") or record.get("lineId", "unknown")
    return str(line_id).encode()


def enrich_line_status(record: dict, ingested_at: str) -> dict:
    return {**record, "_ingested_at": ingested_at, "_source": "tfl_api_line_status", "_topic": KAFKA_TOPIC}


def line_status_records(records: list[dict], ingested_at: str) -> Iterator[tuple[bytes, bytes]]:
    for record in records:
        key = make_message_key(record)
        value = json.dumps(enrich_line_status(record, ingested_at), default=str).encode()
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
    """Fetch line statuses and publish to Kafka. Returns messages produced."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    total = 0
    try:
        records = fetch_line_status(session, modes)
    except Exception as exc:
        logger.error("Failed to fetch line statuses: %s", exc)
        return 0
    for key, value in line_status_records(records, ingested_at):
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
    poll_interval = int(os.getenv("LINE_STATUS_POLL_INTERVAL", "60"))
    modes = os.getenv("LINE_STATUS_MODES", DEFAULT_MODES)
    ensure_topic(bootstrap)
    producer = Producer(build_kafka_config())
    session = build_http_session()
    logger.info("LineStatusProducer started | modes=%s poll_interval=%ds", modes, poll_interval)
    try:
        while True:
            count = run_once(producer, session, modes)
            logger.info("Published %d line-status messages. Sleeping %ds …", count, poll_interval)
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=FLUSH_TIMEOUT)
        session.close()


if __name__ == "__main__":
    main()
