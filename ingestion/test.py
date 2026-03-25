"""
TfL (Transport for London) API Test Suite
==========================================
Docs: https://api.tfl.gov.uk
Register for an API key: https://api-portal.tfl.gov.uk

Set your credentials as environment variables:
    export TFL_APP_KEY="your_app_key"

Or pass app_key directly in the config below.
"""

import os
import json
import unittest
import requests
from urllib.parse import urlencode

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BASE_URL = "https://api.tfl.gov.uk"
APP_KEY = os.getenv("TFL_APP_KEY", "")  # Set your key here or via env var

def build_url(path: str, params: dict = None) -> str:
    """Build a full TfL API URL with optional query params."""
    params = params or {}
    if APP_KEY:
        params["app_key"] = APP_KEY
    query = f"?{urlencode(params)}" if params else ""
    return f"{BASE_URL}{path}{query}"


def get(path: str, params: dict = None) -> requests.Response:
    """Make a GET request to the TfL API."""
    url = build_url(path, params)
    response = requests.get(url, timeout=10)
    return response


# ─────────────────────────────────────────────
# Helper: pretty print
# ─────────────────────────────────────────────
def pprint(data):
    print(json.dumps(data, indent=2, default=str))


# ─────────────────────────────────────────────
# Test Cases
# ─────────────────────────────────────────────
class TestTfLLineAPI(unittest.TestCase):
    """Tests for /Line endpoints."""

    def test_get_all_valid_modes(self):
        """GET /Line/Meta/Modes — list of valid transport modes."""
        r = get("/Line/Meta/Modes")
        self.assertEqual(r.status_code, 200)
        modes = r.json()
        self.assertIsInstance(modes, list)
        mode_names = [m["modeName"] for m in modes]
        print(f"\n✅ Available modes: {mode_names}")

    def test_get_tube_lines(self):
        """GET /Line/Mode/tube — all tube lines."""
        r = get("/Line/Mode/tube")
        self.assertEqual(r.status_code, 200)
        lines = r.json()
        self.assertIsInstance(lines, list)
        self.assertGreater(len(lines), 0)
        names = [l["name"] for l in lines]
        print(f"\n✅ Tube lines ({len(lines)}): {names}")

    def test_get_line_status(self):
        """GET /Line/central,jubilee/Status — status for specific lines."""
        r = get("/Line/central,jubilee/Status")
        self.assertEqual(r.status_code, 200)
        statuses = r.json()
        for line in statuses:
            for s in line.get("lineStatuses", []):
                print(f"\n✅ {line['name']}: {s['statusSeverityDescription']}")

    def test_get_line_route(self):
        """GET /Line/victoria/Route — route info for Victoria line."""
        r = get("/Line/victoria/Route")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data["id"], "victoria")
        print(f"\n✅ Victoria line route name: {data['name']}")

    def test_get_line_stoppoints(self):
        """GET /Line/northern/StopPoints — all stops on the Northern line."""
        r = get("/Line/northern/StopPoints")
        self.assertEqual(r.status_code, 200)
        stops = r.json()
        self.assertIsInstance(stops, list)
        stop_names = [s["commonName"] for s in stops[:5]]
        print(f"\n✅ Northern line first 5 stops: {stop_names}")

    def test_invalid_line_returns_404(self):
        """GET /Line/notarealline/Status — should return 404."""
        r = get("/Line/notarealline/Status")
        self.assertEqual(r.status_code, 404)
        print(f"\n✅ Invalid line correctly returned 404")


class TestTfLStopPointAPI(unittest.TestCase):
    """Tests for /StopPoint endpoints."""

    # Paddington station NaPTAN code
    PADDINGTON_ID = "940GZZLUPAC"

    def test_get_stoppoint_by_id(self):
        """GET /StopPoint/{id} — details for Paddington tube station."""
        r = get(f"/StopPoint/{self.PADDINGTON_ID}")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertIn("Paddington", data.get("commonName", ""))
        print(f"\n✅ Stop: {data['commonName']} | Zone: {data.get('zone', 'N/A')}")

    def test_search_stoppoint(self):
        """GET /StopPoint/Search/{query} — search for 'Liverpool Street'."""
        r = get("/StopPoint/Search/Liverpool Street", {"modes": "tube"})
        self.assertEqual(r.status_code, 200)
        data = r.json()
        matches = data.get("matches", [])
        self.assertGreater(len(matches), 0)
        print(f"\n✅ Search results: {[m['name'] for m in matches[:3]]}")

    def test_get_arrivals_at_stoppoint(self):
        """GET /StopPoint/{id}/Arrivals — live arrivals at Paddington."""
        r = get(f"/StopPoint/{self.PADDINGTON_ID}/Arrivals")
        self.assertEqual(r.status_code, 200)
        arrivals = r.json()
        self.assertIsInstance(arrivals, list)
        print(f"\n✅ Arrivals at Paddington: {len(arrivals)} upcoming trains")
        if arrivals:
            a = arrivals[0]
            print(f"   Next: {a.get('lineName')} → {a.get('towards')} in {a.get('timeToStation', 0)//60} min")

    def test_get_stoppoints_by_geo(self):
        """GET /StopPoint — find stops near central London by geo."""
        params = {
            "lat": 51.5074,
            "lon": -0.1278,
            "stopTypes": "NaptanMetroStation",
            "radius": 500,
        }
        r = get("/StopPoint", params)
        self.assertEqual(r.status_code, 200)
        data = r.json()
        stops = data.get("stopPoints", [])
        print(f"\n✅ Nearby tube stations: {[s['commonName'] for s in stops[:5]]}")


class TestTfLJourneyAPI(unittest.TestCase):
    """Tests for /Journey endpoints."""

    def test_journey_planner(self):
        """GET /Journey/JourneyResults — plan a trip from King's Cross to Waterloo."""
        params = {
            "from": "1000123",  # King's Cross NLC
            "to": "1000254",    # Waterloo NLC
            "mode": "tube",
        }
        r = get("/Journey/JourneyResults/1000123/to/1000254", params)
        self.assertIn(r.status_code, [200, 300])
        data = r.json()
        journeys = data.get("journeys", [])
        print(f"\n✅ Journey options found: {len(journeys)}")
        if journeys:
            j = journeys[0]
            duration = j.get("duration", "?")
            print(f"   Fastest journey: {duration} min")

    def test_journey_meta_modes(self):
        """GET /Journey/Meta/Modes — list valid journey modes."""
        r = get("/Journey/Meta/Modes")
        self.assertEqual(r.status_code, 200)
        modes = r.json()
        names = [m["modeName"] for m in modes]
        print(f"\n✅ Journey modes: {names}")


class TestTfLBikePointAPI(unittest.TestCase):
    """Tests for /BikePoint (Santander Cycles) endpoints."""

    def test_get_all_bikepoints(self):
        """GET /BikePoint — list all bike docking stations."""
        r = get("/BikePoint")
        self.assertEqual(r.status_code, 200)
        points = r.json()
        self.assertIsInstance(points, list)
        self.assertGreater(len(points), 0)
        print(f"\n✅ Total bike docking stations: {len(points)}")

    def test_search_bikepoints(self):
        """GET /BikePoint/Search — search for stations near 'Waterloo'."""
        r = get("/BikePoint/Search", {"query": "Waterloo"})
        self.assertEqual(r.status_code, 200)
        results = r.json()
        print(f"\n✅ Bike points near Waterloo: {[p['commonName'] for p in results[:3]]}")


class TestTfLAirQualityAPI(unittest.TestCase):
    """Tests for /AirQuality endpoint."""

    def test_get_air_quality(self):
        """GET /AirQuality — current and forecast air quality."""
        r = get("/AirQuality")
        self.assertEqual(r.status_code, 200)
        data = r.json()
        print(f"\n✅ Air Quality update: {data.get('updatePeriod', 'N/A')}")
        forecast = data.get("forecastSummary", "N/A")
        print(f"   Forecast: {forecast[:100]}")


# ─────────────────────────────────────────────
# Quick manual runner (outside unittest)
# ─────────────────────────────────────────────
def quick_demo():
    """Run a quick demo of key API calls without unittest."""
    print("=" * 50)
    print("TfL API Quick Demo")
    print("=" * 50)

    print("\n📍 Tube line statuses:")
    r = get("/Line/Mode/tube/Status")
    if r.ok:
        for line in r.json():
            status = line["lineStatuses"][0]["statusSeverityDescription"]
            print(f"  {line['name']:25} → {status}")

    print("\n🚲 Bike points near Oxford Circus:")
    r = get("/BikePoint/Search", {"query": "Oxford Circus"})
    if r.ok:
        for bp in r.json()[:3]:
            print(f"  {bp['commonName']}")

    print("\n🌫 Air Quality:")
    r = get("/AirQuality")
    if r.ok:
        d = r.json()
        print(f"  {d.get('forecastSummary', '')[:120]}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "demo":
        quick_demo()
    else:
        print("Running TfL API test suite...\n")
        unittest.main(verbosity=2)