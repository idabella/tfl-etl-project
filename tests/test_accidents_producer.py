"""
tests/test_accidents_producer.py
────────────────────────────────────────────────────────────────────────────
Unit tests for ingestion/accidents_producer.py.

All external I/O (HTTP, Kafka) is mocked so tests run without any
real broker or TFL credentials.

Run:
    pytest tests/test_accidents_producer.py -v
    pytest tests/test_accidents_producer.py -v --tb=short
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import types
from unittest.mock import MagicMock, Mock, call, patch

import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Stub out confluent_kafka BEFORE importing the module under test so that
# tests work even when the library is not installed in the test environment.
# ──────────────────────────────────────────────────────────────────────────────
def _make_confluent_kafka_stub() -> None:
    """Inject a minimal confluent_kafka stub into sys.modules."""
    if "confluent_kafka" in sys.modules:
        return

    ck = types.ModuleType("confluent_kafka")
    ck.Producer = MagicMock()  # type: ignore[attr-defined]
    ck.KafkaException = Exception  # type: ignore[attr-defined]

    admin_mod = types.ModuleType("confluent_kafka.admin")
    admin_mod.AdminClient = MagicMock()  # type: ignore[attr-defined]
    admin_mod.NewTopic = MagicMock()  # type: ignore[attr-defined]

    sys.modules["confluent_kafka"] = ck
    sys.modules["confluent_kafka.admin"] = admin_mod


_make_confluent_kafka_stub()

# Now import the module under test
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import ingestion.accidents_producer as ap  # noqa: E402


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_ACCIDENT: dict = {
    "id": 123456,
    "date": "2024-06-15T08:30:00",
    "severity": "Slight",
    "borough": "CAMDEN",
    "lat": 51.5321,
    "lon": -0.1234,
    "location": "GRAYS INN ROAD",
    "casualties": [
        {"age": 35, "class": "Pedestrian", "severity": "Slight", "mode": "Pedestrian"}
    ],
    "vehicles": [
        {"type": "Car", "casualties": 0}
    ],
}

SAMPLE_ACCIDENTS_LIST: list[dict] = [
    SAMPLE_ACCIDENT,
    {**SAMPLE_ACCIDENT, "id": 999, "severity": "Serious", "borough": "SOUTHWARK"},
]


@pytest.fixture()
def mock_session():
    """Return a Mock that mimics requests.Session."""
    session = MagicMock()
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.json.return_value = SAMPLE_ACCIDENTS_LIST
    session.get.return_value = resp
    return session, resp


@pytest.fixture()
def mock_producer():
    """Return a Mock that mimics confluent_kafka.Producer."""
    prod = MagicMock()
    prod.flush.return_value = 0  # 0 messages remaining = success
    return prod


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Remove TFL / Kafka env vars so tests are isolated."""
    for var in [
        "TFL_APP_KEY",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SECURITY_PROTOCOL",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        "ACCIDENTS_POLL_INTERVAL",
        "ACCIDENTS_YEARS_LOOKBACK",
    ]:
        monkeypatch.delenv(var, raising=False)


# ══════════════════════════════════════════════════════════════════════════════
# 1. Configuration helpers
# ══════════════════════════════════════════════════════════════════════════════


class TestRequireEnv:
    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("TFL_APP_KEY", "abc123")
        assert ap._require_env("TFL_APP_KEY") == "abc123"

    def test_raises_when_missing(self):
        with pytest.raises(EnvironmentError, match="TFL_APP_KEY"):
            ap._require_env("TFL_APP_KEY")

    def test_raises_when_empty_string(self, monkeypatch):
        monkeypatch.setenv("TFL_APP_KEY", "")
        with pytest.raises(EnvironmentError):
            ap._require_env("TFL_APP_KEY")


class TestBuildKafkaConfig:
    def test_plaintext_defaults(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
        cfg = ap.build_kafka_config()
        assert cfg["bootstrap.servers"] == "broker:9092"
        assert cfg["security.protocol"] == "PLAINTEXT"
        assert cfg["enable.idempotence"] is True
        assert cfg["acks"] == "all"
        assert "sasl.username" not in cfg

    def test_sasl_ssl_adds_credentials(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "user")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pass")
        cfg = ap.build_kafka_config()
        assert cfg["sasl.username"] == "user"
        assert cfg["sasl.password"] == "pass"
        assert cfg["sasl.mechanisms"] == "PLAIN"

    def test_sasl_ssl_missing_credential_raises(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        # No KAFKA_SASL_USERNAME set → should raise
        with pytest.raises(EnvironmentError):
            ap.build_kafka_config()


# ══════════════════════════════════════════════════════════════════════════════
# 2. HTTP client & fetch_accidents
# ══════════════════════════════════════════════════════════════════════════════


class TestBuildHttpSession:
    def test_no_api_key_in_params(self):
        session = ap.build_http_session()
        assert session.params.get("app_key") is None  # type: ignore[union-attr]

    def test_accept_header(self):
        session = ap.build_http_session()
        assert session.headers.get("Accept") == "application/json"


class TestFetchAccidents:
    def test_returns_list_on_200(self, mock_session):
        session, resp = mock_session
        resp.status_code = 200
        resp.ok = True
        resp.json.return_value = SAMPLE_ACCIDENTS_LIST

        result = ap.fetch_accidents(session, 2024)

        session.get.assert_called_once()
        call_url = session.get.call_args[0][0]
        assert "/AccidentStats/2024" in call_url
        assert result == SAMPLE_ACCIDENTS_LIST

    def test_404_returns_empty_list(self, mock_session):
        session, resp = mock_session
        resp.status_code = 404
        resp.ok = False

        result = ap.fetch_accidents(session, 1900)
        assert result == []

    def test_429_sleeps_and_retries(self, mock_session):
        """On a 429 response, the producer sleeps then retries once."""
        session, resp = mock_session

        ok_resp = MagicMock()
        ok_resp.status_code = 200
        ok_resp.ok = True
        ok_resp.json.return_value = SAMPLE_ACCIDENTS_LIST

        rate_limit_resp = MagicMock()
        rate_limit_resp.status_code = 429
        rate_limit_resp.ok = False
        rate_limit_resp.headers = {"Retry-After": "1"}

        session.get.side_effect = [rate_limit_resp, ok_resp]

        with patch("ingestion.accidents_producer.time.sleep") as mock_sleep:
            result = ap.fetch_accidents(session, 2024)

        mock_sleep.assert_called_once_with(1)
        assert result == SAMPLE_ACCIDENTS_LIST

    def test_500_raises(self, mock_session):
        session, resp = mock_session
        resp.status_code = 500
        resp.ok = False
        resp.raise_for_status.side_effect = Exception("Server error")

        with pytest.raises(Exception, match="Server error"):
            ap.fetch_accidents(session, 2024)

    def test_non_list_response_returns_empty(self, mock_session):
        session, resp = mock_session
        resp.status_code = 200
        resp.ok = True
        resp.json.return_value = {"error": "unexpected"}

        result = ap.fetch_accidents(session, 2024)
        assert result == []

    def test_network_error_propagates(self, mock_session):
        import requests as req_lib
        session, _ = mock_session
        session.get.side_effect = req_lib.exceptions.ConnectionError("unreachable")

        with pytest.raises(req_lib.exceptions.ConnectionError):
            ap.fetch_accidents(session, 2024)


# ══════════════════════════════════════════════════════════════════════════════
# 3. Message building
# ══════════════════════════════════════════════════════════════════════════════


class TestMakeMessageKey:
    def test_uses_id_field(self):
        key = ap.make_message_key({"id": 42, "other": "data"})
        assert key == b"42"

    def test_fallback_sha1_when_no_id(self):
        accident = {"severity": "Slight", "borough": "CAMDEN"}
        key = ap.make_message_key(accident)
        expected = hashlib.sha1(
            json.dumps(accident, sort_keys=True, default=str).encode()
        ).hexdigest().encode()
        assert key == expected

    def test_key_is_deterministic_no_id(self):
        accident = {"severity": "Slight"}
        assert ap.make_message_key(accident) == ap.make_message_key(accident)

    def test_different_accidents_different_keys(self):
        a1 = {"id": 1}
        a2 = {"id": 2}
        assert ap.make_message_key(a1) != ap.make_message_key(a2)


class TestEnrichAccident:
    def test_adds_metadata_fields(self):
        enriched = ap.enrich_accident({"id": 1}, "2024-06-15T09:00:00+00:00")
        assert enriched["_ingested_at"] == "2024-06-15T09:00:00+00:00"
        assert enriched["_source"] == "tfl_api_accident_stats"
        assert enriched["_topic"] == ap.KAFKA_TOPIC

    def test_original_fields_preserved(self):
        original = {"id": 1, "severity": "Serious", "borough": "HACKNEY"}
        enriched = ap.enrich_accident(original, "2024-01-01T00:00:00+00:00")
        assert enriched["id"] == 1
        assert enriched["severity"] == "Serious"
        assert enriched["borough"] == "HACKNEY"

    def test_does_not_mutate_original(self):
        original = {"id": 5}
        _ = ap.enrich_accident(original, "ts")
        assert "_ingested_at" not in original


class TestAccidentRecords:
    def test_yields_correct_number_of_tuples(self):
        records = list(ap.accident_records(SAMPLE_ACCIDENTS_LIST, "2024-06-15T00:00:00"))
        assert len(records) == len(SAMPLE_ACCIDENTS_LIST)

    def test_each_tuple_is_bytes(self):
        for key, value in ap.accident_records(SAMPLE_ACCIDENTS_LIST, "ts"):
            assert isinstance(key, bytes)
            assert isinstance(value, bytes)

    def test_value_is_valid_json(self):
        for _, value in ap.accident_records(SAMPLE_ACCIDENTS_LIST, "ts"):
            parsed = json.loads(value)
            assert "_ingested_at" in parsed
            assert "_source" in parsed

    def test_key_matches_id(self):
        first_key, _ = next(iter(ap.accident_records([SAMPLE_ACCIDENT], "ts")))
        assert first_key == str(SAMPLE_ACCIDENT["id"]).encode()


# ══════════════════════════════════════════════════════════════════════════════
# 4. Kafka topic management
# ══════════════════════════════════════════════════════════════════════════════


class TestEnsureTopic:
    @patch("ingestion.accidents_producer.AdminClient")
    @patch("ingestion.accidents_producer.NewTopic")
    def test_skips_creation_when_topic_exists(self, mock_new_topic, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value.topics = {ap.KAFKA_TOPIC: MagicMock()}

        ap.ensure_topic("localhost:9092")

        mock_admin.create_topics.assert_not_called()

    @patch("ingestion.accidents_producer.AdminClient")
    @patch("ingestion.accidents_producer.NewTopic")
    def test_creates_topic_when_missing(self, mock_new_topic, mock_admin_cls):
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value.topics = {}  # topic absent

        future = MagicMock()
        future.result.return_value = None
        mock_admin.create_topics.return_value = {ap.KAFKA_TOPIC: future}

        ap.ensure_topic("localhost:9092")

        mock_admin.create_topics.assert_called_once()
        mock_new_topic.assert_called_once_with(
            topic=ap.KAFKA_TOPIC,
            num_partitions=ap.TOPIC_PARTITIONS,
            replication_factor=ap.TOPIC_REPLICATION,
            config={"retention.ms": ap.TOPIC_RETENTION_MS},
        )

    @patch("ingestion.accidents_producer.AdminClient")
    @patch("ingestion.accidents_producer.NewTopic")
    def test_handles_already_exists_gracefully(self, mock_new_topic, mock_admin_cls):
        """A KafkaException saying 'already exists' must not propagate."""
        mock_admin = MagicMock()
        mock_admin_cls.return_value = mock_admin
        mock_admin.list_topics.return_value.topics = {}

        future = MagicMock()
        future.result.side_effect = Exception("Topic already exists")
        mock_admin.create_topics.return_value = {ap.KAFKA_TOPIC: future}

        ap.ensure_topic("localhost:9092")  # should not raise


# ══════════════════════════════════════════════════════════════════════════════
# 5. run_once — integration of fetch + produce
# ══════════════════════════════════════════════════════════════════════════════


class TestRunOnce:
    def test_produces_correct_message_count(self, mock_session, mock_producer):
        session, resp = mock_session
        resp.json.return_value = SAMPLE_ACCIDENTS_LIST

        total = ap.run_once(mock_producer, session, [2024])

        assert total == len(SAMPLE_ACCIDENTS_LIST)
        assert mock_producer.produce.call_count == len(SAMPLE_ACCIDENTS_LIST)

    def test_flushes_after_producing(self, mock_session, mock_producer):
        session, _ = mock_session
        ap.run_once(mock_producer, session, [2024])
        mock_producer.flush.assert_called_once_with(timeout=ap.FLUSH_TIMEOUT)

    def test_produces_to_correct_topic(self, mock_session, mock_producer):
        session, _ = mock_session
        ap.run_once(mock_producer, session, [2024])
        for call_args in mock_producer.produce.call_args_list:
            assert call_args.kwargs["topic"] == ap.KAFKA_TOPIC

    def test_skips_year_on_fetch_error(self, mock_session, mock_producer):
        session, _ = mock_session
        session.get.side_effect = Exception("network failure")

        total = ap.run_once(mock_producer, session, [2024])

        assert total == 0
        mock_producer.produce.assert_not_called()

    def test_multiple_years_aggregated(self, mock_session, mock_producer):
        session, resp = mock_session
        resp.json.return_value = SAMPLE_ACCIDENTS_LIST  # 2 records per year

        total = ap.run_once(mock_producer, session, [2022, 2023, 2024])

        assert total == len(SAMPLE_ACCIDENTS_LIST) * 3
        assert mock_producer.produce.call_count == len(SAMPLE_ACCIDENTS_LIST) * 3

    def test_empty_accident_list_skipped(self, mock_session, mock_producer):
        session, resp = mock_session
        resp.json.return_value = []

        total = ap.run_once(mock_producer, session, [2024])

        assert total == 0
        mock_producer.produce.assert_not_called()
        mock_producer.flush.assert_not_called()

    def test_produce_calls_have_key_and_value(self, mock_session, mock_producer):
        session, _ = mock_session
        ap.run_once(mock_producer, session, [2024])
        for produce_call in mock_producer.produce.call_args_list:
            assert "key" in produce_call.kwargs
            assert "value" in produce_call.kwargs
            assert isinstance(produce_call.kwargs["key"], bytes)
            assert isinstance(produce_call.kwargs["value"], bytes)

    def test_no_flush_when_nothing_produced(self, mock_session, mock_producer):
        session, resp = mock_session
        resp.json.return_value = []

        ap.run_once(mock_producer, session, [2024])
        mock_producer.flush.assert_not_called()

    def test_warns_on_incomplete_flush(self, mock_session, mock_producer, caplog):
        session, _ = mock_session
        mock_producer.flush.return_value = 5  # 5 messages still queued

        import logging
        with caplog.at_level(logging.WARNING):
            ap.run_once(mock_producer, session, [2024])

        assert any("still in queue" in r.message for r in caplog.records)


# ══════════════════════════════════════════════════════════════════════════════
# 6. Delivery callback
# ══════════════════════════════════════════════════════════════════════════════


class TestOnDelivery:
    def test_logs_error_on_failure(self, caplog):
        import logging
        msg = MagicMock()
        msg.topic.return_value = ap.KAFKA_TOPIC
        msg.partition.return_value = 0
        msg.offset.return_value = -1
        msg.key.return_value = b"key"

        with caplog.at_level(logging.ERROR):
            ap.on_delivery(Exception("broker unavailable"), msg)

        assert any("Delivery failed" in r.message for r in caplog.records)

    def test_no_error_does_not_log_error(self, caplog):
        import logging
        msg = MagicMock()
        msg.topic.return_value = ap.KAFKA_TOPIC
        msg.partition.return_value = 0
        msg.offset.return_value = 42
        msg.key.return_value = b"123"

        with caplog.at_level(logging.ERROR):
            ap.on_delivery(None, msg)

        assert not any(r.levelno == logging.ERROR for r in caplog.records)


# ══════════════════════════════════════════════════════════════════════════════
# 7. Constants sanity checks
# ══════════════════════════════════════════════════════════════════════════════


class TestConstants:
    def test_topic_name(self):
        assert ap.KAFKA_TOPIC == "tfl.accidents"

    def test_topic_partitions(self):
        assert ap.TOPIC_PARTITIONS == 4

    def test_retention_ms_is_90_days(self):
        ninety_days_ms = 90 * 24 * 60 * 60 * 1_000
        assert int(ap.TOPIC_RETENTION_MS) == ninety_days_ms

    def test_tfl_endpoint_contains_year_placeholder(self):
        url = ap.ACCIDENTS_ENDPOINT.format(year=2024)
        assert "2024" in url
