"""
arrivals_producer.py
─────────────────────────────────────────────────────────────────────────────
TFL Vehicle Arrivals Kafka producer.

Polls the TFL ``/Mode/{mode}/Arrivals`` endpoint for each configured
transport mode and publishes every arrival prediction as a JSON message
to the ``tfl.arrivals`` Kafka topic.

Module-level API
────────────────
  _require_env(name)                → str
  build_kafka_config()              → dict
  build_http_session()              → requests.Session
  fetch_arrivals(session, mode)     → list[dict]
  make_message_key(record)          → bytes
  enrich_arrival(record, ts)        → dict
  arrival_records(records, ts)      → Iterator[(bytes, bytes)]
  ensure_topic(bootstrap)           → None
  on_delivery(err, msg)             → None
  run_once(producer, session, modes)→ int

Environment variables
─────────────────────

  KAFKA_BOOTSTRAP_SERVERS     (default: localhost:9092)
  KAFKA_SECURITY_PROTOCOL     (default: PLAINTEXT)
  KAFKA_SASL_USERNAME / _PASSWORD  (required for SASL modes)
  ARRIVALS_POLL_INTERVAL      (default: 30 s  – live data changes quickly)
  ARRIVALS_MODES              (default: tube,overground,dlr,elizabeth-line)
"""

from __future__ import annotations

import hashlib
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
KAFKA_TOPIC: str = "tfl.arrivals"
TOPIC_PARTITIONS: int = 8
TOPIC_REPLICATION: int = 1
TOPIC_RETENTION_MS: str = str(2 * 60 * 60 * 1_000)  # 2 hours – live data

DEFAULT_BOOTSTRAP: str = "localhost:9092"
FLUSH_TIMEOUT: int = 30

ARRIVALS_ENDPOINT: str = "/Mode/{mode}/Arrivals"
TFL_BASE_URL: str = "https://api.tfl.gov.uk"
DEFAULT_MODES: str = "tube,overground,dlr,elizabeth-line"

DEFAULT_MAX_RETRIES: int = 3
DEFAULT_BACKOFF_FACTOR: float = 0.5


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
        "linger.ms": 50,
        "batch.size": 65_536,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500_000,
        "queue.buffering.max.kbytes": 204_800,
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

def fetch_arrivals(session: requests.Session, mode: str) -> list[dict]:
    """
    Fetch live arrival predictions for *mode*.

    Returns an empty list if the server returns nothing or on 404.
    """
    url = f"{TFL_BASE_URL}{ARRIVALS_ENDPOINT.format(mode=mode)}"

    while True:
        resp = session.get(url, timeout=(10, 60))

        if resp.status_code == 404:
            logger.warning("Arrivals: mode '%s' not found (404).", mode)
            return []

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning("Rate-limited (429). Sleeping %d s …", retry_after)
            time.sleep(retry_after)
            continue

        if not resp.ok:
            logger.error("HTTP %d fetching arrivals for mode '%s'.", resp.status_code, mode)
            resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Unexpected arrivals response type for mode '%s': %s", mode, type(data))
            return []

        logger.info("Fetched %d arrivals for mode '%s'.", len(data), mode)
        return data


# ── Message building ─────────────────────────────────────────────────────────

def make_message_key(record: dict) -> bytes:
    """
    Key by ``vehicleId`` when present; fall back to a SHA-1 content hash.
    """
    vehicle_id = record.get("vehicleId")
    if vehicle_id:
        return str(vehicle_id).encode()

    fingerprint = json.dumps(record, sort_keys=True, default=str).encode()
    return hashlib.sha1(fingerprint).hexdigest().encode()


def enrich_arrival(record: dict, ingested_at: str) -> dict:
    """Return a copy of *record* with pipeline metadata injected."""
    return {
        **record,
        "_ingested_at": ingested_at,
        "_source": "tfl_api_arrivals",
        "_topic": KAFKA_TOPIC,
    }


def arrival_records(
    records: list[dict],
    ingested_at: str,
) -> Iterator[tuple[bytes, bytes]]:
    for record in records:
        key = make_message_key(record)
        value = json.dumps(enrich_arrival(record, ingested_at), default=str).encode()
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

def run_once(
    producer: Producer,
    session: requests.Session,
    modes: list[str],
) -> int:
    """Fetch arrivals for all *modes* and publish to Kafka. Returns count."""
    ingested_at = datetime.now(timezone.utc).isoformat()
    total = 0

    for mode in modes:
        try:
            records = fetch_arrivals(session, mode)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to fetch arrivals for mode '%s': %s", mode, exc)
            continue

        for key, value in arrival_records(records, ingested_at):
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
    poll_interval = int(os.getenv("ARRIVALS_POLL_INTERVAL", "30"))
    modes_str = os.getenv("ARRIVALS_MODES", DEFAULT_MODES)
    modes = [m.strip() for m in modes_str.split(",") if m.strip()]

    ensure_topic(bootstrap)
    producer = Producer(build_kafka_config())
    session = build_http_session()

    logger.info("ArrivalsProducer started | modes=%s poll_interval=%ds", modes, poll_interval)

    try:
        while True:
            count = run_once(producer, session, modes)
            logger.info("Published %d arrival messages. Sleeping %ds …", count, poll_interval)
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=FLUSH_TIMEOUT)
        session.close()


if __name__ == "__main__":
    main()
