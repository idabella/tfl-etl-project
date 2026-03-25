"""
tests/test_producers.py
────────────────────────────────────────────────────────────────────────────
Unit tests for the remaining four TFL Kafka producers:
  • airquality_producer
  • arrivals_producer
  • bikepoint_producer
  • line_status_producer
  • stoppoint_producer

All external I/O (HTTP, Kafka) is mocked so tests run without any
real broker or TFL credentials.

Run:
    pytest tests/test_producers.py -v
    pytest tests/test_producers.py -v --tb=short
"""

from __future__ import annotations

import json
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ──────────────────────────────────────────────────────────────────────────────
# Stub out confluent_kafka BEFORE importing modules under test
# ──────────────────────────────────────────────────────────────────────────────

def _make_confluent_kafka_stub() -> None:
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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ingestion.airquality_producer as aq  # noqa: E402
import ingestion.arrivals_producer as arr  # noqa: E402
import ingestion.bikepoint_producer as bp  # noqa: E402
import ingestion.line_status_producer as ls  # noqa: E402
import ingestion.stoppoint_producer as sp  # noqa: E402


# ══════════════════════════════════════════════════════════════════════════════
# Shared fixtures
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture()
def mock_producer():
    prod = MagicMock()
    prod.flush.return_value = 0
    return prod


def _ok_session(payload):
    """Build a mock session whose GET returns a 200 with *payload*."""
    session = MagicMock()
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.json.return_value = payload
    session.get.return_value = resp
    return session, resp


# ══════════════════════════════════════════════════════════════════════════════
# ── AIR QUALITY PRODUCER ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

@pytest.fixture(autouse=True)
def clean_env_aq(monkeypatch):
    for var in ["TFL_APP_KEY", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SECURITY_PROTOCOL",
                "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "AIR_QUALITY_POLL_INTERVAL"]:
        monkeypatch.delenv(var, raising=False)


SAMPLE_AIR_QUALITY: dict = {
    "updatePeriod": "Hourly",
    "updateFrequency": "1",
    "forecastURL": "https://londonair.org.uk",
    "disclaimerText": "This forecast...",
    "currentForecast": [],
}


class TestAirQualityConstants:
    def test_topic_name(self):
        assert aq.KAFKA_TOPIC == "tfl.air_quality"

    def test_partitions(self):
        assert aq.TOPIC_PARTITIONS == 2

    def test_retention_is_7_days(self):
        assert int(aq.TOPIC_RETENTION_MS) == 7 * 24 * 60 * 60 * 1_000


class TestAirQualityRequireEnv:
    def test_returns_value(self, monkeypatch):
        monkeypatch.setenv("TFL_APP_KEY", "key123")
        assert aq._require_env("TFL_APP_KEY") == "key123"

    def test_raises_when_missing(self):
        with pytest.raises(EnvironmentError, match="TFL_APP_KEY"):
            aq._require_env("TFL_APP_KEY")


class TestAirQualityBuildKafkaConfig:
    def test_plaintext_defaults(self, monkeypatch):
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
        cfg = aq.build_kafka_config()
        assert cfg["bootstrap.servers"] == "broker:9092"
        assert cfg["security.protocol"] == "PLAINTEXT"
        assert cfg["enable.idempotence"] is True

    def test_sasl_ssl_adds_credentials(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        monkeypatch.setenv("KAFKA_SASL_USERNAME", "u")
        monkeypatch.setenv("KAFKA_SASL_PASSWORD", "p")
        cfg = aq.build_kafka_config()
        assert cfg["sasl.username"] == "u"
        assert cfg["sasl.mechanisms"] == "PLAIN"

    def test_sasl_missing_credential_raises(self, monkeypatch):
        monkeypatch.setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
        with pytest.raises(EnvironmentError):
            aq.build_kafka_config()


class TestAirQualityBuildHttpSession:
    def test_no_api_key_in_params(self):
        s = aq.build_http_session()
        assert s.params.get("app_key") is None  # type: ignore[union-attr]

    def test_accept_header(self):
        s = aq.build_http_session()
        assert s.headers.get("Accept") == "application/json"


class TestFetchAirQuality:
    def test_dict_response_wrapped_in_list(self):
        session, _ = _ok_session(SAMPLE_AIR_QUALITY)
        result = aq.fetch_air_quality(session)
        assert result == [SAMPLE_AIR_QUALITY]

    def test_list_response_returned_as_is(self):
        session, _ = _ok_session([SAMPLE_AIR_QUALITY])
        result = aq.fetch_air_quality(session)
        assert result == [SAMPLE_AIR_QUALITY]

    def test_unexpected_type_returns_empty(self):
        session, _ = _ok_session("bad_string")
        result = aq.fetch_air_quality(session)
        assert result == []

    def test_non_ok_raises(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.raise_for_status.side_effect = Exception("server error")
        session.get.return_value = resp
        with pytest.raises(Exception, match="server error"):
            aq.fetch_air_quality(session)

    def test_429_sleeps_and_retries(self):
        ok_resp = MagicMock()
        ok_resp.ok = True
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_AIR_QUALITY
        rate_resp = MagicMock()
        rate_resp.ok = False
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "1"}
        session = MagicMock()
        session.get.side_effect = [rate_resp, ok_resp]
        with patch("ingestion.airquality_producer.time.sleep") as mock_sleep:
            result = aq.fetch_air_quality(session)
        mock_sleep.assert_called_once_with(1)
        assert result == [SAMPLE_AIR_QUALITY]


class TestAirQualityMessageBuilding:
    def test_enrich_adds_metadata(self):
        enriched = aq.enrich_air_quality({"updatePeriod": "Hourly"}, "2024-01-01T00:00:00+00:00")
        assert enriched["_ingested_at"] == "2024-01-01T00:00:00+00:00"
        assert enriched["_source"] == "tfl_api_air_quality"
        assert enriched["_topic"] == aq.KAFKA_TOPIC

    def test_enrich_preserves_original_fields(self):
        enriched = aq.enrich_air_quality({"updatePeriod": "Hourly"}, "ts")
        assert enriched["updatePeriod"] == "Hourly"

    def test_enrich_does_not_mutate(self):
        original = {"k": "v"}
        aq.enrich_air_quality(original, "ts")
        assert "_ingested_at" not in original

    def test_records_yields_bytes_tuples(self):
        for key, value in aq.air_quality_records([SAMPLE_AIR_QUALITY], "ts"):
            assert isinstance(key, bytes)
            assert isinstance(value, bytes)

    def test_records_value_is_valid_json(self):
        for _, value in aq.air_quality_records([SAMPLE_AIR_QUALITY], "ts"):
            parsed = json.loads(value)
            assert "_ingested_at" in parsed
            assert "_source" in parsed


class TestAirQualityRunOnce:
    def test_produces_correct_count(self, mock_producer):
        session, _ = _ok_session(SAMPLE_AIR_QUALITY)
        total = aq.run_once(mock_producer, session)
        assert total == 1
        assert mock_producer.produce.call_count == 1

    def test_flushes_after_producing(self, mock_producer):
        session, _ = _ok_session(SAMPLE_AIR_QUALITY)
        aq.run_once(mock_producer, session)
        mock_producer.flush.assert_called_once_with(timeout=aq.FLUSH_TIMEOUT)

    def test_returns_zero_on_fetch_error(self, mock_producer):
        session = MagicMock()
        session.get.side_effect = Exception("network error")
        total = aq.run_once(mock_producer, session)
        assert total == 0

    def test_no_flush_when_nothing_produced(self, mock_producer):
        session, _ = _ok_session("unexpected")
        aq.run_once(mock_producer, session)
        mock_producer.flush.assert_not_called()

    def test_warns_on_incomplete_flush(self, mock_producer, caplog):
        import logging
        mock_producer.flush.return_value = 3
        session, _ = _ok_session(SAMPLE_AIR_QUALITY)
        with caplog.at_level(logging.WARNING):
            aq.run_once(mock_producer, session)
        assert any("still in queue" in r.message for r in caplog.records)


# ══════════════════════════════════════════════════════════════════════════════
# ── ARRIVALS PRODUCER ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_ARRIVAL: dict = {
    "vehicleId": "LTZ1234",
    "naptanId": "940GZZLUBST",
    "stationName": "Baker Street",
    "lineId": "jubilee",
    "lineName": "Jubilee",
    "platformName": "Eastbound - Platform 1",
    "direction": "inbound",
    "bearing": "",
    "destinationNaptanId": "",
    "destinationName": "Stratford",
    "timestamp": "2024-06-15T08:00:00",
    "timeToStation": 120,
    "currentLocation": "Between stations",
    "towards": "Stratford",
    "expectedArrival": "2024-06-15T08:02:00",
    "timeToLive": "2024-06-15T08:02:30",
    "modeName": "tube",
    "id": "abc123",
}

SAMPLE_ARRIVALS_LIST: list[dict] = [SAMPLE_ARRIVAL, {**SAMPLE_ARRIVAL, "vehicleId": "LTZ5678", "id": "def456"}]


class TestArrivalsConstants:
    def test_topic_name(self):
        assert arr.KAFKA_TOPIC == "tfl.arrivals"

    def test_partitions(self):
        assert arr.TOPIC_PARTITIONS == 8

    def test_endpoint_has_mode_placeholder(self):
        url = arr.ARRIVALS_ENDPOINT.format(mode="tube")
        assert "tube" in url


class TestFetchArrivals:
    def test_returns_list_on_200(self):
        session, _ = _ok_session(SAMPLE_ARRIVALS_LIST)
        result = arr.fetch_arrivals(session, "tube")
        assert result == SAMPLE_ARRIVALS_LIST

    def test_404_returns_empty(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        assert arr.fetch_arrivals(session, "unknownmode") == []

    def test_500_raises(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.raise_for_status.side_effect = Exception("server error")
        session.get.return_value = resp
        with pytest.raises(Exception):
            arr.fetch_arrivals(session, "tube")

    def test_non_list_returns_empty(self):
        session, _ = _ok_session({"unexpected": True})
        assert arr.fetch_arrivals(session, "tube") == []

    def test_429_sleeps_and_retries(self):
        ok_resp = MagicMock()
        ok_resp.ok = True
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_ARRIVALS_LIST
        rate_resp = MagicMock()
        rate_resp.ok = False
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "1"}
        session = MagicMock()
        session.get.side_effect = [rate_resp, ok_resp]
        with patch("ingestion.arrivals_producer.time.sleep"):
            result = arr.fetch_arrivals(session, "tube")
        assert result == SAMPLE_ARRIVALS_LIST


class TestArrivalsMakeKey:
    def test_uses_vehicle_id(self):
        key = arr.make_message_key({"vehicleId": "LTZ1234"})
        assert key == b"LTZ1234"

    def test_sha1_fallback_when_no_vehicle_id(self):
        record = {"naptanId": "940GZZLUBST", "lineId": "jubilee"}
        key = arr.make_message_key(record)
        assert len(key) == 40  # SHA-1 hex digest
        assert key == arr.make_message_key(record)  # deterministic

    def test_different_records_different_keys(self):
        assert arr.make_message_key({"vehicleId": "A"}) != arr.make_message_key({"vehicleId": "B"})


class TestArrivalsEnrich:
    def test_adds_metadata(self):
        enriched = arr.enrich_arrival({"vehicleId": "X"}, "ts")
        assert enriched["_source"] == "tfl_api_arrivals"
        assert enriched["_topic"] == arr.KAFKA_TOPIC
        assert enriched["vehicleId"] == "X"

    def test_does_not_mutate(self):
        original = {"vehicleId": "X"}
        arr.enrich_arrival(original, "ts")
        assert "_source" not in original


class TestArrivalsRunOnce:
    def test_produces_per_mode(self, mock_producer):
        session, _ = _ok_session(SAMPLE_ARRIVALS_LIST)
        total = arr.run_once(mock_producer, session, ["tube"])
        assert total == len(SAMPLE_ARRIVALS_LIST)

    def test_aggregates_multiple_modes(self, mock_producer):
        session, _ = _ok_session(SAMPLE_ARRIVALS_LIST)
        total = arr.run_once(mock_producer, session, ["tube", "dlr"])
        assert total == len(SAMPLE_ARRIVALS_LIST) * 2

    def test_skips_mode_on_error(self, mock_producer):
        session = MagicMock()
        session.get.side_effect = Exception("network")
        total = arr.run_once(mock_producer, session, ["tube"])
        assert total == 0

    def test_no_flush_on_empty(self, mock_producer):
        session, resp = _ok_session([])
        arr.run_once(mock_producer, session, ["tube"])
        mock_producer.flush.assert_not_called()

    def test_produces_to_correct_topic(self, mock_producer):
        session, _ = _ok_session(SAMPLE_ARRIVALS_LIST)
        arr.run_once(mock_producer, session, ["tube"])
        for call_args in mock_producer.produce.call_args_list:
            assert call_args.kwargs["topic"] == arr.KAFKA_TOPIC


# ══════════════════════════════════════════════════════════════════════════════
# ── BIKEPOINT PRODUCER ───────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_BIKEPOINT: dict = {
    "id": "BikePoints_1",
    "url": "/BikePoint/BikePoints_1",
    "commonName": "River Street, Clerkenwell",
    "distance": 0.0,
    "placeType": "BikePoint",
    "lat": 51.5292,
    "lon": -0.1098,
    "additionalProperties": [],
}

SAMPLE_BIKEPOINTS: list[dict] = [
    SAMPLE_BIKEPOINT,
    {**SAMPLE_BIKEPOINT, "id": "BikePoints_2", "commonName": "Phillimore Gardens"},
]


class TestBikePointConstants:
    def test_topic_name(self):
        assert bp.KAFKA_TOPIC == "tfl.bikepoints"

    def test_retention_is_24_hours(self):
        assert int(bp.TOPIC_RETENTION_MS) == 24 * 60 * 60 * 1_000


class TestFetchBikePoints:
    def test_returns_list_on_200(self):
        session, _ = _ok_session(SAMPLE_BIKEPOINTS)
        result = bp.fetch_bikepoints(session)
        assert result == SAMPLE_BIKEPOINTS

    def test_non_list_returns_empty(self):
        session, _ = _ok_session({"unexpected": True})
        assert bp.fetch_bikepoints(session) == []

    def test_500_raises(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.raise_for_status.side_effect = Exception("server error")
        session.get.return_value = resp
        with pytest.raises(Exception):
            bp.fetch_bikepoints(session)

    def test_429_retries(self):
        ok_resp = MagicMock()
        ok_resp.ok = True
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_BIKEPOINTS
        rate_resp = MagicMock()
        rate_resp.ok = False
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "1"}
        session = MagicMock()
        session.get.side_effect = [rate_resp, ok_resp]
        with patch("ingestion.bikepoint_producer.time.sleep"):
            result = bp.fetch_bikepoints(session)
        assert result == SAMPLE_BIKEPOINTS


class TestBikePointMakeKey:
    def test_uses_id_field(self):
        assert bp.make_message_key({"id": "BikePoints_1"}) == b"BikePoints_1"

    def test_fallback_to_station_id(self):
        assert bp.make_message_key({"stationId": "BP42"}) == b"BP42"

    def test_fallback_unknown(self):
        assert bp.make_message_key({}) == b"unknown"


class TestBikePointEnrich:
    def test_adds_metadata(self):
        enriched = bp.enrich_bikepoint({"id": "BikePoints_1"}, "ts")
        assert enriched["_source"] == "tfl_api_bikepoint"
        assert enriched["_topic"] == bp.KAFKA_TOPIC

    def test_does_not_mutate(self):
        original = {"id": "BikePoints_1"}
        bp.enrich_bikepoint(original, "ts")
        assert "_source" not in original


class TestBikePointRunOnce:
    def test_produces_correct_count(self, mock_producer):
        session, _ = _ok_session(SAMPLE_BIKEPOINTS)
        total = bp.run_once(mock_producer, session)
        assert total == len(SAMPLE_BIKEPOINTS)

    def test_flushes(self, mock_producer):
        session, _ = _ok_session(SAMPLE_BIKEPOINTS)
        bp.run_once(mock_producer, session)
        mock_producer.flush.assert_called_once_with(timeout=bp.FLUSH_TIMEOUT)

    def test_returns_zero_on_error(self, mock_producer):
        session = MagicMock()
        session.get.side_effect = Exception("network")
        assert bp.run_once(mock_producer, session) == 0

    def test_no_flush_on_empty(self, mock_producer):
        session, _ = _ok_session([])
        bp.run_once(mock_producer, session)
        mock_producer.flush.assert_not_called()

    def test_key_and_value_are_bytes(self, mock_producer):
        session, _ = _ok_session(SAMPLE_BIKEPOINTS)
        bp.run_once(mock_producer, session)
        for call_args in mock_producer.produce.call_args_list:
            assert isinstance(call_args.kwargs["key"], bytes)
            assert isinstance(call_args.kwargs["value"], bytes)


# ══════════════════════════════════════════════════════════════════════════════
# ── LINE STATUS PRODUCER ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_LINE: dict = {
    "id": "jubilee",
    "name": "Jubilee",
    "modeName": "tube",
    "lineStatuses": [{"id": 0, "statusSeverity": 10, "statusSeverityDescription": "Good Service"}],
}

SAMPLE_LINE_STATUSES: list[dict] = [
    SAMPLE_LINE,
    {**SAMPLE_LINE, "id": "central", "name": "Central"},
]


class TestLineStatusConstants:
    def test_topic_name(self):
        assert ls.KAFKA_TOPIC == "tfl.line_status"

    def test_endpoint_has_modes_placeholder(self):
        url = ls.LINE_STATUS_ENDPOINT.format(modes="tube")
        assert "tube" in url

    def test_default_modes_includes_tube(self):
        assert "tube" in ls.DEFAULT_MODES


class TestFetchLineStatus:
    def test_returns_list_on_200(self):
        session, _ = _ok_session(SAMPLE_LINE_STATUSES)
        result = ls.fetch_line_status(session, "tube")
        assert result == SAMPLE_LINE_STATUSES

    def test_404_returns_empty(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        assert ls.fetch_line_status(session, "unknownmode") == []

    def test_non_list_returns_empty(self):
        session, _ = _ok_session("invalid")
        assert ls.fetch_line_status(session, "tube") == []

    def test_500_raises(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.raise_for_status.side_effect = Exception("server error")
        session.get.return_value = resp
        with pytest.raises(Exception):
            ls.fetch_line_status(session, "tube")

    def test_429_retries(self):
        ok_resp = MagicMock()
        ok_resp.ok = True
        ok_resp.status_code = 200
        ok_resp.json.return_value = SAMPLE_LINE_STATUSES
        rate_resp = MagicMock()
        rate_resp.ok = True  # not rate limited but status_code 429
        rate_resp.status_code = 429
        rate_resp.headers = {"Retry-After": "1"}
        session = MagicMock()
        session.get.side_effect = [rate_resp, ok_resp]
        with patch("ingestion.line_status_producer.time.sleep"):
            result = ls.fetch_line_status(session, "tube")
        assert result == SAMPLE_LINE_STATUSES


class TestLineStatusMakeKey:
    def test_uses_id_field(self):
        assert ls.make_message_key({"id": "jubilee"}) == b"jubilee"

    def test_fallback_to_line_id(self):
        assert ls.make_message_key({"lineId": "dlr"}) == b"dlr"

    def test_fallback_unknown(self):
        assert ls.make_message_key({}) == b"unknown"


class TestLineStatusEnrich:
    def test_adds_metadata(self):
        enriched = ls.enrich_line_status({"id": "jubilee"}, "ts")
        assert enriched["_source"] == "tfl_api_line_status"
        assert enriched["_topic"] == ls.KAFKA_TOPIC

    def test_does_not_mutate(self):
        original = {"id": "jubilee"}
        ls.enrich_line_status(original, "ts")
        assert "_source" not in original


class TestLineStatusRunOnce:
    def test_produces_correct_count(self, mock_producer):
        session, _ = _ok_session(SAMPLE_LINE_STATUSES)
        total = ls.run_once(mock_producer, session, "tube")
        assert total == len(SAMPLE_LINE_STATUSES)

    def test_produces_to_correct_topic(self, mock_producer):
        session, _ = _ok_session(SAMPLE_LINE_STATUSES)
        ls.run_once(mock_producer, session, "tube")
        for call_args in mock_producer.produce.call_args_list:
            assert call_args.kwargs["topic"] == ls.KAFKA_TOPIC

    def test_returns_zero_on_error(self, mock_producer):
        session = MagicMock()
        session.get.side_effect = Exception("network")
        assert ls.run_once(mock_producer, session, "tube") == 0

    def test_no_flush_on_empty(self, mock_producer):
        session, _ = _ok_session([])
        ls.run_once(mock_producer, session, "tube")
        mock_producer.flush.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# ── STOPPOINT PRODUCER ───────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_STOPPOINT: dict = {
    "naptanId": "940GZZLUBST",
    "commonName": "Baker Street Underground Station",
    "lat": 51.5226,
    "lon": -0.1571,
    "stopType": "NaptanMetroStation",
    "modes": ["tube"],
    "lines": [{"id": "bakerloo"}, {"id": "jubilee"}],
}

SAMPLE_STOPPOINTS: list[dict] = [
    SAMPLE_STOPPOINT,
    {**SAMPLE_STOPPOINT, "naptanId": "940GZZLUPAC", "commonName": "Paddington Underground Station"},
]


class TestStopPointConstants:
    def test_topic_name(self):
        assert sp.KAFKA_TOPIC == "tfl.stoppoints"

    def test_endpoint_has_modes_placeholder(self):
        url = sp.STOPPOINT_ENDPOINT.format(modes="tube")
        assert "tube" in url


class TestFetchStopPoints:
    def test_handles_wrapped_dict_response(self):
        """TFL wraps stops inside {"stopPoints": [...]}."""
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        # First page has items, second page empty (stop pagination)
        resp.json.side_effect = [
            {"stopPoints": SAMPLE_STOPPOINTS, "total": 2},
            {"stopPoints": [], "total": 2},
        ]
        session.get.return_value = resp
        result = sp.fetch_stoppoints(session, "tube")
        assert len(result) == len(SAMPLE_STOPPOINTS)

    def test_handles_plain_list_response(self):
        """Some TFL endpoints omit the wrapper."""
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        # Return list with < 100 items: single page, stops pagination
        resp.json.return_value = SAMPLE_STOPPOINTS
        session.get.return_value = resp
        result = sp.fetch_stoppoints(session, "tube")
        assert result == SAMPLE_STOPPOINTS

    def test_404_returns_empty(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        assert sp.fetch_stoppoints(session, "unknownmode") == []

    def test_500_raises(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        resp.raise_for_status.side_effect = Exception("server error")
        session.get.return_value = resp
        with pytest.raises(Exception):
            sp.fetch_stoppoints(session, "tube")


class TestStopPointMakeKey:
    def test_uses_naptan_id(self):
        assert sp.make_message_key({"naptanId": "940GZZLUBST"}) == b"940GZZLUBST"

    def test_fallback_to_id(self):
        assert sp.make_message_key({"id": "some-id"}) == b"some-id"

    def test_fallback_unknown(self):
        assert sp.make_message_key({}) == b"unknown"


class TestStopPointEnrich:
    def test_adds_metadata(self):
        enriched = sp.enrich_stoppoint({"naptanId": "940GZZLUBST"}, "ts")
        assert enriched["_source"] == "tfl_api_stoppoint"
        assert enriched["_topic"] == sp.KAFKA_TOPIC

    def test_does_not_mutate(self):
        original = {"naptanId": "X"}
        sp.enrich_stoppoint(original, "ts")
        assert "_source" not in original


class TestStopPointRunOnce:
    def test_produces_correct_count(self, mock_producer):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = SAMPLE_STOPPOINTS  # < 100 → single page
        session.get.return_value = resp
        total = sp.run_once(mock_producer, session, "tube")
        assert total == len(SAMPLE_STOPPOINTS)

    def test_returns_zero_on_error(self, mock_producer):
        session = MagicMock()
        session.get.side_effect = Exception("network")
        assert sp.run_once(mock_producer, session, "tube") == 0

    def test_produces_to_correct_topic(self, mock_producer):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = SAMPLE_STOPPOINTS
        session.get.return_value = resp
        sp.run_once(mock_producer, session, "tube")
        for call_args in mock_producer.produce.call_args_list:
            assert call_args.kwargs["topic"] == sp.KAFKA_TOPIC

    def test_no_flush_on_empty(self, mock_producer):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.json.return_value = []
        session.get.return_value = resp
        sp.run_once(mock_producer, session, "tube")
        mock_producer.flush.assert_not_called()


# ══════════════════════════════════════════════════════════════════════════════
# ── Shared: ensure_topic & on_delivery (tested once for all producers) ───────
# ══════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("module,topic_attr", [
    (aq, "KAFKA_TOPIC"),
    (arr, "KAFKA_TOPIC"),
    (bp, "KAFKA_TOPIC"),
    (ls, "KAFKA_TOPIC"),
    (sp, "KAFKA_TOPIC"),
])
class TestEnsureTopicShared:
    def test_skips_creation_when_exists(self, module, topic_attr):
        # Patch the specific module's AdminClient
        admin_mock = MagicMock()
        topic_name = getattr(module, topic_attr)
        admin_mock.list_topics.return_value.topics = {topic_name: MagicMock()}
        with patch(f"{module.__name__}.AdminClient", return_value=admin_mock):
            module.ensure_topic("localhost:9092")
        admin_mock.create_topics.assert_not_called()


@pytest.mark.parametrize("module", [aq, arr, bp, ls, sp])
class TestOnDeliveryShared:
    def test_logs_error_on_failure(self, module, caplog):
        import logging
        msg = MagicMock()
        msg.topic.return_value = module.KAFKA_TOPIC
        msg.partition.return_value = 0
        msg.offset.return_value = -1
        msg.key.return_value = b"key"
        with caplog.at_level(logging.ERROR):
            module.on_delivery(Exception("broker down"), msg)
        assert any("Delivery failed" in r.message for r in caplog.records)

    def test_no_error_does_not_log_error(self, module, caplog):
        import logging
        msg = MagicMock()
        msg.topic.return_value = module.KAFKA_TOPIC
        msg.partition.return_value = 0
        msg.offset.return_value = 10
        msg.key.return_value = b"key"
        with caplog.at_level(logging.ERROR):
            module.on_delivery(None, msg)
        assert not any(r.levelno == logging.ERROR for r in caplog.records)
