"""Shared configuration, storage backends, and Parquet helpers for ingestion.

The same code path runs both in AWS (S3 via boto3) and locally (a directory
tree), which is what makes ``make local-run`` and the unit tests possible
without an AWS account.

This project is *analytics-engineering first*: ingestion is deliberately thin.
Its one real job is to land the public NYC TLC data into the **bronze** zone
with a *stable, normalized schema* so the Glue catalog never flip-flops and dbt
has a dependable contract to build silver/gold on top of. Everything heavier
(cleaning, joins, aggregation, quality tests) lives in dbt.

Parquet is read/written with pyarrow directly (no pandas dependency).
"""
from __future__ import annotations

import datetime as dt
import io
import json
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #

# TLC publishes monthly Parquet trip files + a static zone-lookup CSV on a CDN.
TRIPS_BASE_URL = os.environ.get(
    "TRIPS_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
)
ZONES_URL = os.environ.get(
    "ZONES_URL", "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)
DATASET = os.environ.get("DATASET", "yellow")  # yellow | green

BRONZE_TRIPS_PREFIX = os.environ.get("BRONZE_TRIPS_PREFIX", "bronze/yellow_trips")
BRONZE_ZONES_PREFIX = os.environ.get("BRONZE_ZONES_PREFIX", "bronze/taxi_zones")


def _config_path() -> str:
    here = Path(__file__).resolve().parent
    return str(here.parent / "config" / "datasets.json")


def load_config(path: str | None = None) -> dict[str, Any]:
    path = path or os.environ.get("DATASETS_CONFIG") or _config_path()
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    if not cfg.get("months"):
        raise ValueError(f"datasets config at {path} must list at least one month")
    return cfg


# --------------------------------------------------------------------------- #
# Canonical bronze schemas                                                     #
# --------------------------------------------------------------------------- #
# TLC changes column names/types between years (e.g. ``Airport_fee`` vs
# ``airport_fee``, passenger_count as int vs double). We normalize to ONE
# canonical schema at the bronze boundary so downstream contracts never break.
#
# Each entry maps a canonical snake_case name to the list of source column names
# it may appear under, plus its target Arrow type.

TRIP_COLUMNS: list[tuple[str, list[str], str]] = [
    ("vendor_id",             ["VendorID", "vendor_id"],                       "int64"),
    ("tpep_pickup_datetime",  ["tpep_pickup_datetime", "pickup_datetime"],     "timestamp"),
    ("tpep_dropoff_datetime", ["tpep_dropoff_datetime", "dropoff_datetime"],   "timestamp"),
    ("passenger_count",       ["passenger_count"],                             "int64"),
    ("trip_distance",         ["trip_distance"],                               "double"),
    ("rate_code_id",          ["RatecodeID", "rate_code_id"],                  "int64"),
    ("store_and_fwd_flag",    ["store_and_fwd_flag"],                          "string"),
    ("pu_location_id",        ["PULocationID", "pu_location_id"],              "int64"),
    ("do_location_id",        ["DOLocationID", "do_location_id"],              "int64"),
    ("payment_type",          ["payment_type"],                                "int64"),
    ("fare_amount",           ["fare_amount"],                                 "double"),
    ("extra",                 ["extra"],                                       "double"),
    ("mta_tax",               ["mta_tax"],                                     "double"),
    ("tip_amount",            ["tip_amount"],                                  "double"),
    ("tolls_amount",          ["tolls_amount"],                               "double"),
    ("improvement_surcharge", ["improvement_surcharge"],                       "double"),
    ("total_amount",          ["total_amount"],                                "double"),
    ("congestion_surcharge",  ["congestion_surcharge"],                        "double"),
    ("airport_fee",           ["airport_fee", "Airport_fee"],                  "double"),
]

ZONE_COLUMNS: list[tuple[str, list[str], str]] = [
    ("location_id",  ["LocationID", "location_id"], "int64"),
    ("borough",      ["Borough", "borough"],        "string"),
    ("zone",         ["Zone", "zone"],              "string"),
    ("service_zone", ["service_zone"],              "string"),
]

# Ingestion metadata appended to every bronze table (lineage + idempotency).
META_COLUMNS: list[tuple[str, str]] = [
    ("source_file", "string"),
    ("ingested_at", "timestamp"),
]


# --------------------------------------------------------------------------- #
# Storage backends                                                            #
# --------------------------------------------------------------------------- #


class StorageBackend(ABC):
    """Key/value-ish object storage. Keys are bucket-relative paths."""

    @abstractmethod
    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str: ...

    @abstractmethod
    def get_bytes(self, key: str) -> bytes: ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list[str]: ...

    @abstractmethod
    def location(self, key: str) -> str:
        """Human-readable fully-qualified location (s3://... or a file path)."""


class LocalBackend(StorageBackend):
    """Stores objects as files under ``root``. Used for local runs and tests."""

    def __init__(self, root: str):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not base.exists():
            return []
        if base.is_file():
            return [prefix]
        return sorted(
            str(p.relative_to(self.root)).replace(os.sep, "/")
            for p in base.rglob("*")
            if p.is_file()
        )

    def location(self, key: str) -> str:
        return str(self._path(key))


class S3Backend(StorageBackend):
    """Stores objects in S3 via boto3. Used in AWS."""

    def __init__(self, bucket: str, client: Any | None = None):
        self.bucket = bucket
        if client is None:
            import boto3  # imported lazily so local runs don't need boto3

            client = boto3.client("s3")
        self.client = client

    def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
        return self.location(key)

    def get_bytes(self, key: str) -> bytes:
        resp = self.client.get_object(Bucket=self.bucket, Key=key)
        return resp["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        token: str | None = None
        while True:
            kwargs = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                kwargs["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kwargs)
            keys.extend(obj["Key"] for obj in resp.get("Contents", []))
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        return sorted(keys)

    def location(self, key: str) -> str:
        return f"s3://{self.bucket}/{key}"


def get_backend(client: Any | None = None) -> StorageBackend:
    """Choose a backend from the environment.

    - ``STORAGE_BACKEND=local`` (or no ``DATA_BUCKET``) -> LocalBackend at
      ``LOCAL_LAKE_DIR`` (default ``./.local_lake``).
    - otherwise -> S3Backend on ``DATA_BUCKET``.
    """
    backend = os.environ.get("STORAGE_BACKEND")
    bucket = os.environ.get("DATA_BUCKET")
    if backend == "local" or (backend is None and not bucket):
        return LocalBackend(os.environ.get("LOCAL_LAKE_DIR", "./.local_lake"))
    if not bucket:
        raise RuntimeError("DATA_BUCKET must be set when STORAGE_BACKEND is not 'local'")
    return S3Backend(bucket, client=client)


# --------------------------------------------------------------------------- #
# Partition / key helpers                                                      #
# --------------------------------------------------------------------------- #


def trip_object_key(month: str) -> str:
    """Bronze key for one month of trips, Hive-partitioned by year/month.

    ``month`` is ``YYYY-MM``. Re-ingesting a month overwrites the same object,
    so the unit of idempotency is the *source month*.
    """
    year, mon = month.split("-")
    return f"{BRONZE_TRIPS_PREFIX}/year={year}/month={mon}/{DATASET}_tripdata_{month}.parquet"


def trip_source_filename(month: str) -> str:
    return f"{DATASET}_tripdata_{month}.parquet"


def zones_object_key() -> str:
    # Small, static reference data -> single unpartitioned object.
    return f"{BRONZE_ZONES_PREFIX}/taxi_zone_lookup.parquet"


# --------------------------------------------------------------------------- #
# Parquet / schema-normalization helpers (pyarrow)                            #
# --------------------------------------------------------------------------- #


def _arrow_type(kind: str):
    import pyarrow as pa  # type: ignore

    return {
        "string": pa.string(),
        "double": pa.float64(),
        "int64": pa.int64(),
        "timestamp": pa.timestamp("us"),
    }[kind]


def target_schema(columns: list[tuple[str, list[str], str]]):
    """Build the canonical Arrow schema (data columns + ingestion metadata)."""
    import pyarrow as pa  # type: ignore

    fields = [(name, _arrow_type(kind)) for name, _aliases, kind in columns]
    fields += [(name, _arrow_type(kind)) for name, kind in META_COLUMNS]
    return pa.schema(fields)


def _resolve_source_name(table, aliases: list[str]) -> str | None:
    present = set(table.column_names)
    for alias in aliases:
        if alias in present:
            return alias
    return None


def normalize_table(raw_table, columns, source_file: str, ingested_at: dt.datetime):
    """Cast a raw TLC table onto the canonical schema.

    - Renames source columns to canonical snake_case names.
    - Casts each column to its target type (safe cast: out-of-type values become
      null rather than raising — bad *values* are dbt's job, not ingestion's).
    - Adds any canonical column missing from the source as an all-null column.
    - Appends ``source_file`` and ``ingested_at`` lineage columns.
    """
    import pyarrow as pa  # type: ignore
    import pyarrow.compute as pc  # type: ignore

    n = raw_table.num_rows
    arrays: dict[str, Any] = {}
    for name, aliases, kind in columns:
        src = _resolve_source_name(raw_table, aliases)
        target = _arrow_type(kind)
        if src is None:
            arrays[name] = pa.nulls(n, type=target)
        else:
            col = raw_table.column(src)
            arrays[name] = pc.cast(col, target, safe=False)

    arrays["source_file"] = pa.array([source_file] * n, type=pa.string())
    arrays["ingested_at"] = pa.array([ingested_at] * n, type=pa.timestamp("us"))

    schema = target_schema(columns)
    return pa.table(arrays, schema=schema)


def table_to_parquet_bytes(table) -> bytes:
    import pyarrow.parquet as pq  # type: ignore

    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue()


def read_parquet_bytes(data: bytes):
    import pyarrow.parquet as pq  # type: ignore

    return pq.read_table(io.BytesIO(data))


def read_csv_bytes(data: bytes):
    import pyarrow.csv as pacsv  # type: ignore

    return pacsv.read_csv(io.BytesIO(data))
