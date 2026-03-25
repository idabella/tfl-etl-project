"""
accidents_producer.py
─────────────────────────────────────────────────────────────────────────────
TFL Accident Statistics Kafka producer.

Fetches road accident data from the TFL AccidentStats endpoint for one or
more calendar years and publishes each accident record as a JSON message
to the ``tfl.accidents`` Kafka topic.

Environment variables
─────────────────────
  KAFKA_BOOTSTRAP_SERVERS     (default: localhost:9092)
  KAFKA_SECURITY_PROTOCOL     (default: PLAINTEXT)
  KAFKA_SASL_USERNAME         (required when SASL_SSL / SASL_PLAINTEXT)
  KAFKA_SASL_PASSWORD         (required when SASL_SSL / SASL_PLAINTEXT)
  ACCIDENTS_POLL_INTERVAL     (default: 86400 s = 24 h)
  ACCIDENTS_YEARS_LOOKBACK    (default: 1; how many past years to ingest)
  TFL_APP_KEY                 (optionnel mais recommandé)
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
KAFKA_TOPIC: str = "tfl.accidents"
TOPIC_PARTITIONS: int = 4
TOPIC_REPLICATION: int = 1
TOPIC_RETENTION_MS: str = str(90 * 24 * 60 * 60 * 1_000)  # 90 days

DEFAULT_BOOTSTRAP: str = "localhost:9092"
FLUSH_TIMEOUT: int = 30  # seconds

ACCIDENTS_ENDPOINT: str = "/AccidentStats/{year}"
TFL_BASE_URL: str = "https://api.tfl.gov.uk"

DEFAULT_MAX_RETRIES: int = 5
DEFAULT_BACKOFF_FACTOR: float = 1.5

# ✅ FIX: TfL publie les stats avec ~2 ans de délai.
# 2022 est la dernière année fiable disponible (en 2026).
TFL_LAST_AVAILABLE_YEAR: int = 2022


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
    session.params = {}  # type: ignore[assignment]

    tfl_app_key = os.getenv("TFL_APP_KEY", "")
    if tfl_app_key:
        session.params["app_key"] = tfl_app_key  # type: ignore[index]
        logger.info("TfL API key loaded from TFL_APP_KEY env var.")
    else:
        logger.warning("TFL_APP_KEY not set — requests may be rate-limited (500 req/min).")

    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "tfl-etl-pipeline/1.0",
    })

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

def fetch_accidents(session: requests.Session, year: int) -> list[dict]:
    """
    Fetch accident records for *year* from the TFL AccidentStats endpoint.
    Returns an empty list on 404 (year not available yet).
    """
    url = f"{TFL_BASE_URL}{ACCIDENTS_ENDPOINT.format(year=year)}"

    while True:
        resp = session.get(url, timeout=(10, 60))

        if resp.status_code == 404:
            logger.warning("AccidentStats: year %d not found (404) — skipping.", year)
            return []

        if resp.status_code == 400:
            logger.warning(
                "AccidentStats: year %d returned 400 Bad Request — "
                "données probablement pas encore publiées par TfL. "
                "Dernière année disponible estimée : %d.",
                year, TFL_LAST_AVAILABLE_YEAR
            )
            return []

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 60))
            logger.warning("Rate-limited (429). Sleeping %d s …", retry_after)
            time.sleep(retry_after)
            continue

        if not resp.ok:
            logger.error("HTTP %d fetching AccidentStats/%d.", resp.status_code, year)
            resp.raise_for_status()

        data = resp.json()
        if not isinstance(data, list):
            logger.warning("Unexpected response type for AccidentStats/%d: %s", year, type(data))
            return []

        logger.info("Fetched %d accident records for year %d.", len(data), year)
        return data


# ── Message building ─────────────────────────────────────────────────────────

def make_message_key(record: dict) -> bytes:
    accident_id = record.get("id")
    if accident_id is not None:
        return str(accident_id).encode()
    fingerprint = json.dumps(record, sort_keys=True, default=str).encode()
    return hashlib.sha1(fingerprint).hexdigest().encode()


def enrich_accident(record: dict, ingested_at: str) -> dict:
    return {
        **record,
        "_ingested_at": ingested_at,
        "_source": "tfl_api_accident_stats",
        "_topic": KAFKA_TOPIC,
    }


def accident_records(
    records: list[dict],
    ingested_at: str,
) -> Iterator[tuple[bytes, bytes]]:
    for record in records:
        key = make_message_key(record)
        value = json.dumps(enrich_accident(record, ingested_at), default=str).encode()
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
        except Exception as exc:
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
    years: list[int],
) -> int:
    ingested_at = datetime.now(timezone.utc).isoformat()
    total = 0

    for year in years:
        try:
            records = fetch_accidents(session, year)
        except Exception as exc:
            logger.error("Failed to fetch AccidentStats/%d: %s", year, exc)
            continue

        for key, value in accident_records(records, ingested_at):
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
    poll_interval = int(os.getenv("ACCIDENTS_POLL_INTERVAL", str(24 * 3600)))
    lookback = int(os.getenv("ACCIDENTS_YEARS_LOOKBACK", "1"))

    current_year = datetime.now(timezone.utc).year

    # ✅ FIX PRINCIPAL : on plafonne à TFL_LAST_AVAILABLE_YEAR (2022)
    # car TfL publie les stats avec ~2 ans de délai.
    end_year = min(current_year, TFL_LAST_AVAILABLE_YEAR + 1)
    start_year = end_year - lookback
    years = list(range(start_year, end_year))

    logger.info(
        "AccidentsProducer started | years=%s poll_interval=%ds | "
        "(TfL dernière année dispo estimée : %d)",
        years, poll_interval, TFL_LAST_AVAILABLE_YEAR
    )

    ensure_topic(bootstrap)

    producer = Producer(build_kafka_config())
    session = build_http_session()

    try:
        while True:
            count = run_once(producer, session, years)
            logger.info("Published %d accident messages. Sleeping %ds …", count, poll_interval)
            time.sleep(poll_interval)
    finally:
        producer.flush(timeout=FLUSH_TIMEOUT)
        session.close()


if __name__ == "__main__":
    main()