"""
base_producer.py
─────────────────────────────────────────────────────────────────────────────
Abstract base class for all TFL Kafka producers.

All domain producers (arrivals, line_status, bikepoint, etc.) inherit from
BaseProducer. Subclasses only need to implement two methods:

    fetch()    → fetch records from the TFL API
    transform() → convert a raw record into (key_bytes, value_bytes)

The base class handles:
  - Environment configuration
  - TFLClient construction
  - Kafka Producer construction (idempotent, compressed)
  - Kafka topic creation / validation
  - The continuous polling loop with configurable interval
  - Graceful shutdown on SIGTERM / SIGINT
  - Delivery report callbacks
  - Structured logging

Usage
─────
    class MyProducer(BaseProducer):
        TOPIC = "tfl.my_topic"
        TOPIC_PARTITIONS = 4
        TOPIC_RETENTION_MS = str(7 * 24 * 3600 * 1000)
        POLL_INTERVAL_ENV = "MY_POLL_INTERVAL"
        DEFAULT_POLL_INTERVAL = 60

        def fetch(self) -> list[dict]:
            return self.client.get("/SomeEndpoint")

        def transform(self, record: dict) -> tuple[bytes, bytes]:
            key = str(record["id"]).encode()
            value = json.dumps({**record, **self._meta()}).encode()
            return key, value
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from ingestion.client import TFLClient

logger = logging.getLogger(__name__)

# ── Shared Kafka defaults ─────────────────────────────────────────────────────
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
FLUSH_TIMEOUT = 30  # seconds


class BaseProducer(ABC):
    """
    Abstract base for all TFL Kafka producers.

    Class-level attributes to override in subclasses
    ─────────────────────────────────────────────────
    TOPIC : str
        Kafka topic name.
    TOPIC_PARTITIONS : int
        Number of partitions to create (if topic doesn't yet exist).
    TOPIC_REPLICATION : int
        Replication factor (default 1, override for prod clusters).
    TOPIC_RETENTION_MS : str
        Retention in milliseconds as a string.
    POLL_INTERVAL_ENV : str
        Name of the env var that controls polling frequency.
    DEFAULT_POLL_INTERVAL : int
        Fallback poll interval in seconds when env var is not set.
    SOURCE_TAG : str
        Value of the ``_source`` metadata field injected into each message.
    """

    TOPIC: str = ""
    TOPIC_PARTITIONS: int = 4
    TOPIC_REPLICATION: int = 1
    TOPIC_RETENTION_MS: str = str(7 * 24 * 60 * 60 * 1_000)
    POLL_INTERVAL_ENV: str = "POLL_INTERVAL"
    DEFAULT_POLL_INTERVAL: int = 60
    SOURCE_TAG: str = "tfl_api"

    def __init__(self) -> None:
        if not self.TOPIC:
            raise NotImplementedError("Subclass must define TOPIC.")

        # ── Config ──────────────────────────────────────────────────────────
        self._app_key = self._require_env("TFL_APP_KEY")
        self._bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS)
        self._poll_interval = int(
            os.getenv(self.POLL_INTERVAL_ENV, str(self.DEFAULT_POLL_INTERVAL))
        )
        self._running = False

        # ── Clients ─────────────────────────────────────────────────────────
        self.client = TFLClient(app_key=self._app_key)
        self._producer = self._build_producer()

        # ── Kafka topic setup ────────────────────────────────────────────────
        self._ensure_topic()

        # ── Signal handlers ──────────────────────────────────────────────────
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        logger.info(
            "%s initialised | topic=%s poll_interval=%ds",
            self.__class__.__name__,
            self.TOPIC,
            self._poll_interval,
        )

    # ── Abstract interface ─────────────────────────────────────────────────────

    @abstractmethod
    def fetch(self) -> list[dict[str, Any]]:
        """
        Fetch raw records from the TFL API.

        Returns a list of dicts. May return an empty list if no data is
        available; errors should raise exceptions (they are caught by the run loop).
        """

    @abstractmethod
    def transform(self, record: dict[str, Any]) -> tuple[bytes, bytes]:
        """
        Convert a raw TFL record into a Kafka (key, value) pair.

        Both key and value should be bytes.  Enrich the value with at least:
            _ingested_at, _source, _topic
        (use self._meta() for a consistent metadata dict).
        """

    # ── Helpers for subclasses ─────────────────────────────────────────────────

    def _meta(self) -> dict[str, str]:
        """Return standard pipeline metadata fields."""
        return {
            "_ingested_at": datetime.now(timezone.utc).isoformat(),
            "_source": self.SOURCE_TAG,
            "_topic": self.TOPIC,
        }

    @staticmethod
    def _require_env(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise EnvironmentError(
                f"Required env var '{name}' is not set. Check config/dev.env."
            )
        return value

    # ── Kafka internals ────────────────────────────────────────────────────────

    def _build_producer(self) -> Producer:
        security = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
        cfg: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap,
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
            cfg["sasl.username"] = self._require_env("KAFKA_SASL_USERNAME")
            cfg["sasl.password"] = self._require_env("KAFKA_SASL_PASSWORD")
            if security == "SASL_SSL":
                cfg["ssl.ca.location"] = os.getenv("KAFKA_SSL_CA_LOCATION", "")
        return Producer(cfg)

    def _ensure_topic(self) -> None:
        admin = AdminClient({"bootstrap.servers": self._bootstrap})
        existing = admin.list_topics(timeout=10).topics
        if self.TOPIC in existing:
            logger.debug("Topic '%s' already exists.", self.TOPIC)
            return
        new_topic = NewTopic(
            topic=self.TOPIC,
            num_partitions=self.TOPIC_PARTITIONS,
            replication_factor=self.TOPIC_REPLICATION,
            config={"retention.ms": self.TOPIC_RETENTION_MS},
        )
        futures = admin.create_topics([new_topic])
        for topic, fut in futures.items():
            try:
                fut.result()
                logger.info("Created Kafka topic '%s'.", topic)
            except KafkaException as exc:
                if "already exists" in str(exc).lower():
                    logger.debug("Topic '%s' already exists (race).", topic)
                else:
                    raise

    def _on_delivery(self, err: Any, msg: Any) -> None:
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

    # ── Publish ────────────────────────────────────────────────────────────────

    def _publish_batch(self, records: list[dict[str, Any]]) -> int:
        """Transform and publish *records* to Kafka. Returns count produced."""
        produced = 0
        for record in records:
            try:
                key, value = self.transform(record)
            except Exception as exc:  # noqa: BLE001
                logger.warning("transform() failed for record, skipping: %s", exc)
                continue
            self._producer.poll(0)
            self._producer.produce(
                topic=self.TOPIC,
                key=key,
                value=value,
                on_delivery=self._on_delivery,
            )
            produced += 1

        if produced:
            remaining = self._producer.flush(timeout=FLUSH_TIMEOUT)
            if remaining:
                logger.warning(
                    "%d messages still queued after %ds flush timeout.",
                    remaining, FLUSH_TIMEOUT,
                )
        return produced

    # ── Main loop ──────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        Start the continuous polling → publish loop.

        Runs until SIGTERM / SIGINT is received or a fatal Kafka error occurs.
        """
        self._running = True
        logger.info("%s starting poll loop …", self.__class__.__name__)

        while self._running:
            try:
                logger.info("[%s] Fetching data …", self.TOPIC)
                records = self.fetch()
                count = self._publish_batch(records)
                logger.info("[%s] Published %d messages.", self.TOPIC, count)
            except KafkaException as exc:
                logger.critical("Fatal Kafka error — shutting down: %s", exc)
                sys.exit(1)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[%s] Error in poll cycle: %s", self.TOPIC, exc)

            logger.info("[%s] Sleeping %ds …", self.TOPIC, self._poll_interval)
            # Sleep in 1-second increments so SIGINT is handled promptly
            for _ in range(self._poll_interval):
                if not self._running:
                    break
                time.sleep(1)

        logger.info("%s shut down gracefully.", self.__class__.__name__)

    # ── Shutdown ───────────────────────────────────────────────────────────────

    def _handle_shutdown(self, signum: int, frame: Any) -> None:
        logger.info(
            "%s received signal %d — shutting down after current cycle …",
            self.__class__.__name__, signum,
        )
        self._running = False

    def close(self) -> None:
        """Flush producer and close HTTP session."""
        self._running = False
        self._producer.flush(timeout=FLUSH_TIMEOUT)
        self.client.close()

    def __enter__(self) -> "BaseProducer":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
