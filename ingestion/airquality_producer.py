"""
airquality_producer.py
─────────────────────────────────────────────────────────────────────────────
TFL Air Quality Kafka producer.

Fetches the current London air quality forecast from the TFL AirQuality
endpoint and publishes a single enriched JSON message to the
``tfl.air_quality`` Kafka topic on each poll cycle.

Module-level API
────────────────
  _require_env(name)                 → str
  build_kafka_config()               → dict
  build_http_session()               → requests.Session
  fetch_air_quality(session)         → list[dict]   (always 0 or 1 element)
  make_message_key(record)           → bytes
  enrich_air_quality(record, ts)     → dict
  air_quality_records(records, ts)   → Iterator[(bytes, bytes)]
  ensure_topic(bootstrap)            → None
  on_delivery(err, msg)              → None
  run_once(producer, session)        → int

Environment variables
─────────────────────
  KAFKA_BOOTSTRAP_SERVERS     (default: localhost:9092)
  KAFKA_SECURITY_PROTOCOL     (default: PLAINTEXT)
  KAFKA_SASL_USERNAME / _PASSWORD  (required for SASL modes)
  AIR_QUALITY_POLL_INTERVAL   (default: 3600 s = 1 h)
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

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
KAFKA_TOPIC: str = "tfl.air_quality"
TOPIC_PARTITIONS: int = 2
TOPIC_REPLICATION: int = 1
TOPIC_RETENTION_MS: str = str(7 * 24 * 60 * 60 * 1_000)  # 7 days

DEFAULT_BOOTSTRAP: str = "localhost:9092"
FLUSH_TIMEOUT: int = 30

AIR_QUALITY_ENDPOINT: str = "/AirQuality"
TFL_BASE_URL: str = "https://api.tfl.gov.uk"

DEFAULT_MAX_RETRIES: int = 5
DEFAULT_BACKOFF_FACTOR: float = 1.5


# ── Configuration helpers ────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(
            f"Required env var '{name}' is not set. Check config/dev.env."
        )
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


# ── HTTP session ─────────────────────────────────────────────────────────────

def build_http_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "tfl-etl-pipeline/1.0",
        }
    )
    retry = Retry(
        total=DEFAULT_MAX_RETRIES,
        backoff_factor=DEFAULT_BACKOFF_FACTOR,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# ── TFL fetch ────────────────────────────────────────────────────────────────

def fetch_air_quality(session: requests.Session) -> list[dict]:
    """
    Fetch the current air quality forecast from TFL.

    The endpoint returns a single dict; we wrap it in a list so the rest of
    the pipeline (which expects ``list[dict]``) is uniform.  Returns ``[]``
    on unexpected response types.
    """
    url = f"{TFL_BASE_URL}{AIR_QUALITY_ENDPOINT}"

    while True:
        resp = session.get(url, timeout=(10, 60))

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning("Rate-limited (429). Sleeping %d s …", retry_after)
            time.sleep(retry_after)
            continue

        if not resp.ok:
            logger.error("HTTP %d fetching AirQuality.", resp.status_code)
            resp.raise_for_status()

        data = resp.json()
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            return data
        logger.warning("Unexpected AirQuality response type: %s", type(data))
        return []


# ── Message building ─────────────────────────────────────────────────────────

def make_message_key(record: dict) -> bytes:
    """Key is the UTC date so downstream deduplication is easy."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return date_str.encode()


def enrich_air_quality(record: dict, ingested_at: str) -> dict:
    """Return a copy of *record* with pipeline metadata injected."""
    return {
        **record,
        "_ingested_at": ingested_at,
        "_source": "tfl_api_air_quality",
        "_topic": KAFKA_TOPIC,
    }


def air_quality_records(
    records: list[dict],
    ingested_at: str,
) -> Iterator[tuple[bytes, bytes]]:
    for record in records:
        key = make_message_key(record)
        value = json.dumps(enrich_air_quality(record, ingested_at), default=str).encode()
        yield key, value


# ── Kafka topic management ───────────────────────────────────────────────────

def ensure_topic(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics

    if KAFKA_TOPIC in existing:
        logger.debug("Topic '%s' already exists.", KAFKA_TOPIC)
        return

    new_topic = NewTopic(
        topic=KAFKA_TOPIC,
        num_partitions=TOPIC_PARTITIONS,
        replication_factor=TOPIC_REPLICATION,
        config={"retention.ms": TOPIC_RETENTION_MS},
    )
    futures = admin.create_topics([new_topic])
    for topic, fut in futures.items():
        try:
            fut.result()
            logger.info("Created Kafka topic '%s'.", topic)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                logger.debug("Topic '%s' already exists (race).", topic)
            else:
                raise


# ── Delivery callback ────────────────────────────────────────────────────────

def on_delivery(err: Any, msg: Any) -> None:
    if err:
        logger.error(
            "Delivery failed | topic=%s partition=%s key=%s | %s",
            msg.topic(), msg.partition(), msg.key(), err,
        )
    else:
        logger.debug(
            "Delivered | topic=%s partition=%d offset=%d",
            msg.topic(), msg.partition(), msg.offset(),
        )


# ── Run once ─────────────────────────────────────────────────────────────────

def run_once(producer: Producer, session: requests.Session) -> int:
    """Fetch air quality and publish to Kafka. Returns messages produced."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    total = 0

    try:
        records = fetch_air_quality(session)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to fetch AirQuality: %s", exc)
        return 0

    for key, value in air_quality_records(records, ingested_at):
        producer.poll(0)
        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=value,
            on_delivery=on_delivery,
        )
        total += 1

    if total:
        remaining = producer.flush(timeout=FLUSH_TIMEOUT)
        if remaining:
            logger.warning("%d messages still in queue after flush timeout.", remaining)

    return total


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)
    poll_interval = int(os.getenv("AIR_QUALITY_POLL_INTERVAL", "3600"))

    ensure_topic(bootstrap)
    producer = Producer(build_kafka_config())
    session = build_http_session()

    logger.info("AirQualityProducer started | poll_interval=%ds", poll_interval)

    try:
        while True:
            count = run_once(producer, session)
            logger.info("Published %d air quality messages. Sleeping %ds …", count, poll_interval)
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=FLUSH_TIMEOUT)
        session.close()


if __name__ == "__main__":
    main()
