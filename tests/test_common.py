import datetime as dt

import pyarrow as pa

import common
from conftest import NOW, make_raw_trips_parquet


def test_trip_object_key_is_hive_partitioned():
    key = common.trip_object_key("2024-01")
    assert key == "bronze/yellow_trips/year=2024/month=01/yellow_tripdata_2024-01.parquet"


def test_zones_object_key():
    assert common.zones_object_key() == "bronze/taxi_zones/taxi_zone_lookup.parquet"


def test_target_schema_includes_metadata_columns():
    schema = common.target_schema(common.TRIP_COLUMNS)
    names = schema.names
    assert names[-2:] == ["source_file", "ingested_at"]
    assert "vendor_id" in names and "airport_fee" in names


def test_normalize_renames_casts_and_fills_missing():
    raw = common.read_parquet_bytes(make_raw_trips_parquet())
    table = common.normalize_table(
        raw, common.TRIP_COLUMNS, source_file="f.parquet", ingested_at=NOW
    )

    # Canonical schema, in canonical order.
    expected = [n for n, _a, _k in common.TRIP_COLUMNS] + ["source_file", "ingested_at"]
    assert table.column_names == expected

    # Aliased source name resolved (Airport_fee -> airport_fee).
    assert table.column("airport_fee").to_pylist() == [0.0, 1.75, 0.0]
    # Doubles cast down to int64.
    assert table.schema.field("passenger_count").type == pa.int64()
    assert table.column("passenger_count").to_pylist() == [1, 2, 1]
    # Column absent from source filled with nulls.
    assert table.column("congestion_surcharge").to_pylist() == [None, None, None]
    # Lineage stamped on every row.
    assert table.column("source_file").to_pylist() == ["f.parquet"] * 3
    assert table.column("ingested_at").to_pylist() == [NOW] * 3


def test_local_backend_roundtrip(local_backend):
    local_backend.put_bytes("a/b.txt", b"hello")
    assert local_backend.get_bytes("a/b.txt") == b"hello"
    assert local_backend.list_keys("a/") == ["a/b.txt"]
