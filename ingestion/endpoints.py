"""
endpoints.py
─────────────────────────────────────────────────────────────────────────────
TFL Unified API endpoint constants and URL-builder helpers.

All URLs are relative to TFL_BASE_URL. Use the build_url() helper to
construct absolute URLs with appropriate path parameters.

TFL API docs: https://api.tfl.gov.uk
"""

from __future__ import annotations

# ── Base ──────────────────────────────────────────────────────────────────────
TFL_BASE_URL: str = "https://api.tfl.gov.uk"

# ── Arrivals ─────────────────────────────────────────────────────────────────
# GET /StopPoint/{stopPointId}/Arrivals
# Returns next predicted arrivals for a stop point.
ARRIVALS_BY_STOP = "/StopPoint/{stop_id}/Arrivals"

# GET /Line/{lineId}/Arrivals/{stopPointId}
ARRIVALS_BY_LINE_AND_STOP = "/Line/{line_id}/Arrivals/{stop_id}"

# GET /Mode/{mode}/Arrivals  (mode: tube, bus, overground, dlr, elizabeth-line …)
ARRIVALS_BY_MODE = "/Mode/{mode}/Arrivals"

# ── Line Status ───────────────────────────────────────────────────────────────
# GET /Line/{lineId}/Status
LINE_STATUS_BY_ID = "/Line/{line_id}/Status"

# GET /Line/Mode/{modes}/Status  (comma-separated modes)
LINE_STATUS_BY_MODE = "/Line/Mode/{modes}/Status"

# GET /Line/{lineIds}/Status/{startDate}/to/{endDate}
LINE_STATUS_RANGE = "/Line/{line_ids}/Status/{start_date}/to/{end_date}"

# Convenience: all tube lines
TUBE_LINE_IDS = (
    "bakerloo,central,circle,district,hammersmith-city,"
    "jubilee,metropolitan,northern,piccadilly,victoria,waterloo-city"
)
ALL_MODES = "tube,overground,dlr,elizabeth-line,bus,national-rail"

# ── Bike Points (Santander Cycles) ────────────────────────────────────────────
# GET /BikePoint  → all docking stations
BIKEPOINT_ALL = "/BikePoint"

# GET /BikePoint/{id}
BIKEPOINT_BY_ID = "/BikePoint/{bikepoint_id}"

# GET /BikePoint/Search?query=…
BIKEPOINT_SEARCH = "/BikePoint/Search"

# ── Stop Points ───────────────────────────────────────────────────────────────
# GET /StopPoint/Mode/{modes}  (comma-separated)
STOPPOINT_BY_MODE = "/StopPoint/Mode/{modes}"

# GET /StopPoint/{id}
STOPPOINT_BY_ID = "/StopPoint/{stop_id}"

# GET /StopPoint/Search?query=…
STOPPOINT_SEARCH = "/StopPoint/Search"

# GET /StopPoint?stopTypes=…&radius=…&lat=…&lon=…
STOPPOINT_GEO = "/StopPoint"

# ── Accident Stats ────────────────────────────────────────────────────────────
# GET /AccidentStats/{year}
ACCIDENT_STATS = "/AccidentStats/{year}"

# ── Air Quality ───────────────────────────────────────────────────────────────
# GET /AirQuality  → current London air quality forecast
AIR_QUALITY = "/AirQuality"

# ── Journey Planner ───────────────────────────────────────────────────────────
# GET /Journey/JourneyResults/{from}/to/{to}
JOURNEY_RESULTS = "/Journey/JourneyResults/{origin}/to/{destination}"

# ── Crowd ─────────────────────────────────────────────────────────────────────
# GET /Crowding/{naptan}  → crowding data for a station
CROWDING_BY_NAPTAN = "/Crowding/{naptan}"

# ── Road ─────────────────────────────────────────────────────────────────────
# GET /Road/{ids}/Status
ROAD_STATUS = "/Road/{road_ids}/Status"

# GET /Road/{ids}/Disruption
ROAD_DISRUPTION = "/Road/{road_ids}/Disruption"


# ── URL builder ───────────────────────────────────────────────────────────────

def build_url(template: str, base: str = TFL_BASE_URL, **kwargs: str) -> str:
    """
    Return an absolute URL by combining *base* with *template*.

    Path parameters in *template* (e.g. ``{stop_id}``) are rendered from
    **kwargs.  Any remaining kwargs are ignored.

    Examples
    --------
    >>> build_url(ARRIVALS_BY_STOP, stop_id="940GZZLUBST")
    'https://api.tfl.gov.uk/StopPoint/940GZZLUBST/Arrivals'
    >>> build_url(LINE_STATUS_BY_MODE, modes="tube,dlr")
    'https://api.tfl.gov.uk/Line/Mode/tube,dlr/Status'
    """
    path = template.format(**kwargs)
    return f"{base.rstrip('/')}{path}"


def arrivals_url(stop_id: str) -> str:
    return build_url(ARRIVALS_BY_STOP, stop_id=stop_id)


def arrivals_by_mode_url(mode: str) -> str:
    return build_url(ARRIVALS_BY_MODE, mode=mode)


def line_status_url(modes: str = ALL_MODES) -> str:
    return build_url(LINE_STATUS_BY_MODE, modes=modes)


def bikepoint_url() -> str:
    return build_url(BIKEPOINT_ALL)


def stoppoint_url(modes: str = "tube,overground,dlr,elizabeth-line") -> str:
    return build_url(STOPPOINT_BY_MODE, modes=modes)


def accident_stats_url(year: int) -> str:
    return build_url(ACCIDENT_STATS, year=str(year))


def air_quality_url() -> str:
    return build_url(AIR_QUALITY)
