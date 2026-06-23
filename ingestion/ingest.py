"""Ingest NYC TLC data into the bronze zone with a normalized schema.

Two things get landed:

- **Trips** — one monthly Parquet per source month, normalized to the canonical
  schema and Hive-partitioned:
  ``bronze/yellow_trips/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet``
- **Zones** — the static taxi-zone lookup, converted CSV -> Parquet:
  ``bronze/taxi_zones/taxi_zone_lookup.parquet``

Bronze is an immutable, replayable copy of the source with a *stable* schema.
All cleaning/modeling happens later in dbt (silver -> gold).

Entry points:
- ``handler(event, context)`` — container/Lambda-style entry point.
- ``run(...)``                — plain-Python entry point (local runs / tests).
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import urllib.request
from typing import Any, Callable

import common

logger = logging.getLogger()
logger.setLevel(logging.INFO)

HTTP_TIMEOUT_S = 120

Fetcher = Callable[[str], bytes]


def _fetch(url: str) -> bytes:
    """Download a URL to bytes. Replaced with a stub in tests."""
    req = urllib.request.Request(url, headers={"User-Agent": "lakehouse-nyc-taxi/1.0"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_S) as resp:
        return resp.read()


def ingest_trips(
    month: str,
    backend: common.StorageBackend,
    fetch: Fetcher,
    now: dt.datetime,
    sample_rows: int | None = None,
) -> dict[str, Any]:
    """Download one month of trips, normalize, and land it in bronze."""
    filename = common.trip_source_filename(month)
    url = f"{common.TRIPS_BASE_URL}/{filename}"
    raw = common.read_parquet_bytes(fetch(url))
    if sample_rows is not None and raw.num_rows > sample_rows:
        # Keep local runs cheap and deterministic — take the first N rows.
        raw = raw.slice(0, sample_rows)

    table = common.normalize_table(raw, common.TRIP_COLUMNS, source_file=filename, ingested_at=now)
    key = common.trip_object_key(month)
    location = backend.put_bytes(key, common.table_to_parquet_bytes(table))
    logger.info("ingested %s rows for %s -> %s", table.num_rows, month, location)
    return {"month": month, "rows": table.num_rows, "location": location, "key": key}


def ingest_zones(
    backend: common.StorageBackend,
    fetch: Fetcher,
    now: dt.datetime,
) -> dict[str, Any]:
    """Download the zone-lookup CSV, convert to Parquet, land it in bronze."""
    raw = common.read_csv_bytes(fetch(common.ZONES_URL))
    table = common.normalize_table(
        raw, common.ZONE_COLUMNS, source_file="taxi_zone_lookup.csv", ingested_at=now
    )
    key = common.zones_object_key()
    location = backend.put_bytes(key, common.table_to_parquet_bytes(table))
    logger.info("ingested %s zones -> %s", table.num_rows, location)
    return {"rows": table.num_rows, "location": location, "key": key}


def run(
    months: list[str] | None = None,
    backend: common.StorageBackend | None = None,
    fetch: Fetcher = _fetch,
    now: dt.datetime | None = None,
    sample_rows: int | None = None,
    with_zones: bool = True,
) -> dict[str, Any]:
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    backend = backend or common.get_backend()
    cfg = common.load_config()
    months = months if months is not None else cfg["months"]
    if sample_rows is None:
        sample_rows = cfg.get("sample_rows")

    trips, failures = [], []
    for month in months:
        try:
            trips.append(ingest_trips(month, backend, fetch, now, sample_rows))
        except Exception as exc:  # noqa: BLE001 - one bad month must not kill the run
            failures.append({"month": month, "error": str(exc)})
            logger.warning("failed to ingest month %s: %s", month, exc)

    zones = ingest_zones(backend, fetch, now) if with_zones else None

    result = {
        "months_ingested": len(trips),
        "rows_ingested": sum(t["rows"] for t in trips),
        "zones_ingested": zones["rows"] if zones else 0,
        "trips": trips,
        "zones": zones,
        "failures": failures,
    }
    logger.info("ingest summary: %s", json.dumps({k: v for k, v in result.items() if k != "trips"}))
    if not trips:
        # Every month failed -> surface as an error so the orchestrator retries.
        raise RuntimeError(f"ingest produced 0 trip files: {failures}")
    return result


def handler(event, context):  # noqa: ANN001 - container/Lambda signature
    event = event or {}
    return run(months=event.get("months"))
